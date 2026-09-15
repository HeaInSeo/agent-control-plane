package store_test

// Regressions for the findings of the ninth independent review, of head
// 297ef5d. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1 (HIGH): a withdrawn task could still mint a publication. Every
// other fence existed — live attempt, unreleased workspace, current epoch,
// packet authority — except the task's own scheduling state, and publication
// is the irreversible step.
func TestWithdrawnTaskCannotWriteDurableState(t *testing.T) {
	ctx := context.Background()
	commit := sha("r9-withdrawn-commit")

	// Withdrawing a task leaves its attempt live: the current-attempt pointer
	// is frozen when a task goes terminal, so nothing else notices.
	withdraw := func(t *testing.T, db *store.DB, f fixture) {
		t.Helper()
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
		}); err != nil {
			t.Fatalf("abandon task: %v", err)
		}
		if err := db.Read(ctx, func(tx *store.Tx) error {
			attempt, err := tx.WorkerAttempt(ctx, f.Attempt.AttemptID)
			if err != nil {
				return err
			}
			if attempt.Status.IsTerminal() {
				t.Fatal("test is vacuous: withdrawal already retired the attempt")
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	}

	t.Run("publication", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r9-pub", state.IntentModifying, state.LaneOperator)
		withdraw(t, db, f)

		pub := publishFor(f, commit, "refs/heads/m0/r9-pub")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, pub)
		}); !errors.Is(err, store.ErrTaskNotLive) {
			t.Fatalf("want ErrTaskNotLive, got %v", err)
		}

		if err := db.Read(ctx, func(tx *store.Tx) error {
			rows, err := tx.PublishAttemptsForAttempt(ctx, f.Attempt.AttemptID)
			if err != nil {
				return err
			}
			if len(rows) != 0 {
				t.Fatalf("a withdrawn task minted %d publications", len(rows))
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("evidence", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r9-ev", state.IntentModifying, state.LaneOperator)
		withdraw(t, db, f)

		ev := evidenceFor(f, commit, commit, nil, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); !errors.Is(err, store.ErrTaskNotLive) {
			t.Fatalf("want ErrTaskNotLive, got %v", err)
		}
	})

	t.Run("workspace", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r9-ws", state.IntentModifying, state.LaneOperator)

		// A second live attempt, so the refusal is about the task rather than
		// the attempt or a duplicate workspace.
		successor := f.Attempt
		successor.AttemptID = ids.NewAttemptID()
		successor.FenceEpoch = f.Attempt.FenceEpoch + 1
		successor.Status = state.AttemptRunning
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed); err != nil {
				return err
			}
			return tx.CreateWorkerAttempt(ctx, successor)
		}); err != nil {
			t.Fatalf("create successor: %v", err)
		}
		withdraw2 := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
		})
		if withdraw2 != nil {
			t.Fatalf("abandon: %v", withdraw2)
		}

		ws := f.Workspace
		ws.WorkspaceID = ids.NewWorkspaceID()
		ws.AttemptID = successor.AttemptID
		ws.RootPath = "/var/lib/acp/workspaces/r9-withdrawn"
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); !errors.Is(err, store.ErrTaskNotLive) {
			t.Fatalf("want ErrTaskNotLive, got %v", err)
		}
	})
}

// Finding 2: the documented publisher gate had no task-status input, so
// cancelling a task would not have stopped its push.
func TestPublishGateRefusesAWithdrawnTask(t *testing.T) {
	f := fixtureForPreconditions()

	if err := domain.CheckPublishPreconditions(f.pub, f.live); err != nil {
		t.Fatalf("a live task was rejected: %v", err)
	}
	for _, status := range []state.TaskRunStatus{state.TaskAbandoned, state.TaskCompleted} {
		t.Run(string(status), func(t *testing.T) {
			live := f.live
			live.TaskStatus = status
			if err := domain.CheckPublishPreconditions(f.pub, live); !errors.Is(err, domain.ErrPublishBindingInvalid) {
				t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
			}
		})
	}
	t.Run("unset task status fails closed", func(t *testing.T) {
		live := f.live
		live.TaskStatus = ""
		if err := domain.CheckPublishPreconditions(f.pub, live); !errors.Is(err, domain.ErrPublishBindingInvalid) {
			t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
		}
	})
}

// Finding 3: an attempt could be created already terminal — dead on arrival,
// undeletable, and having burned a fence epoch.
func TestAttemptCannotBeCreatedTerminal(t *testing.T) {
	ctx := context.Background()

	for _, status := range []state.WorkerAttemptStatus{
		state.AttemptFailed, state.AttemptAbandoned, state.AttemptEvidenceUnknown,
	} {
		t.Run(string(status), func(t *testing.T) {
			db := newDB(t)
			f := seed(t, db, "r9-attempt-terminal", state.IntentModifying, state.LaneOperator)

			successor := f.Attempt
			successor.AttemptID = ids.NewAttemptID()
			successor.FenceEpoch = f.Attempt.FenceEpoch + 1
			successor.Status = status
			if err := db.Write(ctx, func(tx *store.Tx) error {
				if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed); err != nil {
					return err
				}
				return tx.CreateWorkerAttempt(ctx, successor)
			}); !errors.Is(err, store.ErrForbiddenAttemptTransition) {
				t.Fatalf("want ErrForbiddenAttemptTransition, got %v", err)
			}

			// The fence epoch was not burned.
			if err := db.Read(ctx, func(tx *store.Tx) error {
				next, err := tx.NextFenceEpoch(ctx, f.Task.TaskID)
				if err != nil {
					return err
				}
				if next != f.Attempt.FenceEpoch+1 {
					t.Fatalf("a refused attempt burned fence epoch %d", int64(next)-1)
				}
				return nil
			}); err != nil {
				t.Fatalf("read: %v", err)
			}
		})
	}
}

// Finding 4: a task could be bound to a packet that no longer granted
// authority, and packet_id is immutable, so the task was permanently unusable.
func TestTaskRequiresAPacketThatGrantsAuthority(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r9-task-packet", state.IntentModifying, state.LaneOperator)

	stale := newPacket(ids.NewTaskID(), f.Subject.RepositorySubjectID,
		state.IntentModifying, state.LaneOperator)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.ApprovePacket(ctx, stale); err != nil {
			return err
		}
		return tx.SetPacketStatus(ctx, stale.PacketID, state.PacketStale)
	}); err != nil {
		t.Fatalf("approve and stale: %v", err)
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateTaskRun(ctx, domain.TaskRun{
			TaskID:              stale.TaskID,
			RepositorySubjectID: f.Subject.RepositorySubjectID,
			PacketID:            stale.PacketID,
			Lane:                state.LaneOperator,
			Intent:              state.IntentModifying,
			Status:              state.TaskReady,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	}); !errors.Is(err, domain.ErrPacketNoAuthority) {
		t.Fatalf("want ErrPacketNoAuthority, got %v", err)
	}
}

// Finding 5: an approval could be recorded already expired — the same
// uncorrectable dead authority as a stale status, by another route.
func TestPacketCannotBeApprovedAlreadyExpired(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r9-expired-approval", state.IntentModifying, state.LaneOperator)

	expired := newPacket(ids.NewTaskID(), f.Subject.RepositorySubjectID,
		state.IntentModifying, state.LaneOperator)
	expired.ApprovedAt = fixedNow.Add(-48 * time.Hour)
	expired.ExpiresAt = fixedNow.Add(-24 * time.Hour)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ApprovePacket(ctx, expired)
	}); !errors.Is(err, domain.ErrPacketExpired) {
		t.Fatalf("want ErrPacketExpired, got %v", err)
	}

	// A packet expiring in the future is still accepted.
	good := newPacket(ids.NewTaskID(), f.Subject.RepositorySubjectID,
		state.IntentModifying, state.LaneOperator)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ApprovePacket(ctx, good)
	}); err != nil {
		t.Fatalf("a live approval was refused: %v", err)
	}
}

// Finding 6: the migration text had a duplicated comment paragraph, and
// migration text is checksummed and forward-only — once applied it can never
// be edited.
func TestMigrationTextHasNoDuplicatedCommentLines(t *testing.T) {
	set, err := store.Migrations()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}
	for _, m := range set {
		var previous string
		for i, line := range strings.Split(m.SQL, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || !strings.HasPrefix(trimmed, "--") {
				previous = ""
				continue
			}
			if trimmed == previous {
				t.Fatalf("%04d_%s line %d repeats the previous comment line: %q",
					m.Version, m.Name, i+1, trimmed)
			}
			previous = trimmed
		}
	}
}
