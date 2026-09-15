package store_test

// Regressions for the findings of the eighth independent review, of head
// 839393f. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1: recording a publication never revalidated packet authority,
// although admission and launch/resume both do and the docs name publication
// as the third moment it must hold.
func TestPublicationRequiresLivePacketAuthority(t *testing.T) {
	ctx := context.Background()

	for _, status := range []state.PacketStatus{state.PacketStale, state.PacketSuperseded} {
		t.Run(string(status), func(t *testing.T) {
			db := newDB(t)
			f := seed(t, db, "r8-pub-authority", state.IntentModifying, state.LaneOperator)
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetPacketStatus(ctx, f.Packet.PacketID, status)
			}); err != nil {
				t.Fatalf("set packet status: %v", err)
			}

			pub := publishFor(f, sha("r8-pub-commit"), "refs/heads/m0/r8-pub")
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.RecordPublishAttempt(ctx, pub)
			}); !errors.Is(err, domain.ErrPacketNoAuthority) {
				t.Fatalf("want ErrPacketNoAuthority, got %v", err)
			}

			// No permanent PENDING row was minted.
			if err := db.Read(ctx, func(tx *store.Tx) error {
				rows, err := tx.PublishAttemptsForAttempt(ctx, f.Attempt.AttemptID)
				if err != nil {
					return err
				}
				if len(rows) != 0 {
					t.Fatalf("a %s packet minted %d publications", string(status), len(rows))
				}
				return nil
			}); err != nil {
				t.Fatalf("read: %v", err)
			}
		})
	}

	t.Run("expired packet", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r8-pub-expiry", state.IntentModifying, state.LaneOperator)

		late := db.WithClock(func() time.Time { return f.Packet.ExpiresAt.Add(time.Minute) })
		pub := publishFor(f, sha("r8-expiry-commit"), "refs/heads/m0/r8-expiry")
		if err := late.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, pub)
		}); !errors.Is(err, domain.ErrPacketExpired) {
			t.Fatalf("want ErrPacketExpired, got %v", err)
		}
	})

	// The deliberate asymmetry: completion does not require live packet
	// authority, because the effect it rests on was externally observed. If
	// the work landed and the packet went stale afterwards, the change exists
	// in the repository and refusing to complete would leave the task open
	// for ever while the effect stands.
	t.Run("completion is deliberately not gated on packet authority", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r8-complete-stale", state.IntentModifying, state.LaneOperator)

		commit := sha("r8-complete-commit")
		pub := appliedPublication(t, db, f, commit, "refs/heads/m0/r8-complete")
		ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.RecordEvidence(ctx, ev); err != nil {
				return err
			}
			// Only now does the packet go stale.
			if err := tx.SetPacketStatus(ctx, f.Packet.PacketID, state.PacketStale); err != nil {
				return err
			}
			return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
		}); err != nil {
			t.Fatalf("completion on an observed effect should not be blocked by a stale packet: %v", err)
		}
	})
}

// Finding 2: an "approved" packet could be recorded already STALE or
// SUPERSEDED — a durable approval that never granted anything, uncorrectable
// because transitions are one-way and packets are undeletable.
func TestPacketMustBeRecordedApproved(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r8-packet-status", state.IntentModifying, state.LaneOperator)

	for _, status := range []state.PacketStatus{state.PacketStale, state.PacketSuperseded} {
		t.Run(string(status), func(t *testing.T) {
			p := newPacket(ids.NewTaskID(), f.Subject.RepositorySubjectID,
				state.IntentModifying, state.LaneOperator)
			p.Status = status
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ApprovePacket(ctx, p)
			}); !errors.Is(err, domain.ErrPacketInvalid) {
				t.Fatalf("want ErrPacketInvalid, got %v", err)
			}
		})
	}

	// An unset status defaults to APPROVED.
	p := newPacket(ids.NewTaskID(), f.Subject.RepositorySubjectID, state.IntentModifying, state.LaneOperator)
	p.Status = ""
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ApprovePacket(ctx, p)
	}); err != nil {
		t.Fatalf("unset status should default to APPROVED: %v", err)
	}
	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.Packet(ctx, p.PacketID)
		if err != nil {
			return err
		}
		if stored.Status != state.PacketApproved {
			t.Fatalf("stored status is %q", string(stored.Status))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 3: CreateTaskRun rejected COMPLETED but not ABANDONED, so a task
// could be created dead on arrival and never deleted.
func TestTaskCannotBeCreatedTerminal(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r8-task-terminal", state.IntentModifying, state.LaneOperator)

	for _, status := range []state.TaskRunStatus{state.TaskAbandoned, state.TaskCompleted} {
		t.Run(string(status), func(t *testing.T) {
			taskID := ids.NewTaskID()
			packet := newPacket(taskID, f.Subject.RepositorySubjectID,
				state.IntentModifying, state.LaneOperator)
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ApprovePacket(ctx, packet)
			}); err != nil {
				t.Fatalf("approve: %v", err)
			}
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.CreateTaskRun(ctx, domain.TaskRun{
					TaskID:              taskID,
					RepositorySubjectID: f.Subject.RepositorySubjectID,
					PacketID:            packet.PacketID,
					Lane:                state.LaneOperator,
					Intent:              state.IntentModifying,
					Status:              status,
					CreatedAt:           fixedNow,
					UpdatedAt:           fixedNow,
				})
			})
			if err == nil {
				t.Fatalf("a task was created already %s", string(status))
			}
			// COMPLETED is caught earlier still, by the entity rule that
			// completion requires bound evidence. Any of the three is a
			// correct refusal.
			if !errors.Is(err, store.ErrInvalidTaskRun) &&
				!errors.Is(err, store.ErrCompletionNotDerivable) &&
				!errors.Is(err, domain.ErrInvalidEntity) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// Finding 4: recording evidence applied neither the terminal-attempt nor the
// released-workspace fence its sibling writers apply.
func TestEvidenceRequiresALiveAttemptAndWorkspace(t *testing.T) {
	ctx := context.Background()
	commit := sha("r8-evidence-commit")

	t.Run("terminal attempt", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r8-ev-dead", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed)
		}); err != nil {
			t.Fatalf("fail attempt: %v", err)
		}
		ev := evidenceFor(f, commit, commit, nil, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); !errors.Is(err, store.ErrEvidenceNotRecordable) {
			t.Fatalf("want ErrEvidenceNotRecordable, got %v", err)
		}
	})

	t.Run("released workspace", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r8-ev-released", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
		}); err != nil {
			t.Fatalf("release: %v", err)
		}
		ev := evidenceFor(f, commit, commit, nil, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); !errors.Is(err, store.ErrEvidenceNotRecordable) {
			t.Fatalf("want ErrEvidenceNotRecordable, got %v", err)
		}
	})

	t.Run("live attempt still records", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r8-ev-live", state.IntentModifying, state.LaneOperator)
		ev := evidenceFor(f, commit, commit, nil, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); err != nil {
			t.Fatalf("a live attempt was refused: %v", err)
		}
	})
}

// Finding 5: a short read left the header buffer zeroed with a nil error, so
// a valid database would be refused as "not a SQLite database".
func TestUndersizedFileIsRejectedAsNotADatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Shorter than the 16-byte header: cannot be a database, and must be
	// reported as such rather than read partially.
	for _, size := range []int{1, 8, 15} {
		path := filepath.Join(dir, "short.db")
		if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true}); !errors.Is(err, store.ErrNotSQLiteDatabase) {
			t.Fatalf("%d-byte file: want ErrNotSQLiteDatabase, got %v", size, err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}

	// A real database still opens, header read in full.
	path := filepath.Join(dir, "real.db")
	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

// Finding 6: the db_contract marker was written and never read, so it looked
// like a fail-closed guard while being inert.
func TestUnsupportedContractVersionFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "contract.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// A future build stamped a contract this one does not implement.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE control_plane_meta SET value = 'v9.9' WHERE key = 'db_contract'`)
	}); err != nil {
		t.Fatalf("stamp contract: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []store.Mode{store.ModeReadWrite, store.ModeReadOnly} {
		if _, err := store.Open(ctx, store.Config{Path: path, Mode: mode}); !errors.Is(err, store.ErrUnsupportedContract) {
			t.Fatalf("%s: want ErrUnsupportedContract, got %v", mode, err)
		}
	}
}

// Finding 7: a sub-millisecond BusyTimeout truncated to zero, turning a short
// wait into no wait at all.
func TestSubMillisecondBusyTimeoutIsRejected(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "timeout.db")

	_, err := store.Open(ctx, store.Config{
		Path:        path,
		Mode:        store.ModeReadWrite,
		AllowCreate: true,
		BusyTimeout: 500 * time.Microsecond,
	})
	if err == nil {
		t.Fatal("a BusyTimeout that rounds to 0ms was accepted")
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Fatal("a rejected configuration still created the database")
	}

	// 1ms is expressible and accepted; unset takes the default.
	for _, timeout := range []time.Duration{time.Millisecond, 0} {
		db, err := store.Open(ctx, store.Config{
			Path:        filepath.Join(t.TempDir(), "ok.db"),
			Mode:        store.ModeReadWrite,
			AllowCreate: true,
			BusyTimeout: timeout,
		})
		if err != nil {
			t.Fatalf("BusyTimeout %s rejected: %v", timeout, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Finding 8: ownership was stored unordered after activation, so a handle
// could end up believing it owned an older generation than it did.
func TestOwnershipOnlyEverRises(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	first, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "first")
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	second, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "second")
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if second.Epoch <= first.Epoch {
		t.Fatalf("epochs did not advance: %d then %d", int64(first.Epoch), int64(second.Epoch))
	}
	if db.OwnedEpoch() != second.Epoch {
		t.Fatalf("handle owns epoch %d, want %d", int64(db.OwnedEpoch()), int64(second.Epoch))
	}

	// Concurrent activations must leave the handle owning the highest.
	const activations = 6
	done := make(chan error, activations)
	for range activations {
		go func() {
			_, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "concurrent")
			done <- err
		}()
	}
	for range activations {
		if err := <-done; err != nil {
			t.Fatalf("concurrent activation: %v", err)
		}
	}

	current, err := db.CurrentEpoch(ctx)
	if err != nil {
		t.Fatalf("current epoch: %v", err)
	}
	if db.OwnedEpoch() != current {
		t.Fatalf("handle owns epoch %d while current is %d",
			int64(db.OwnedEpoch()), int64(current))
	}

	// And the handle still works, rather than being wedged as stale.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.AppendEvent(ctx, eventFor(current, "post.activation"))
		return err
	}); err != nil {
		t.Fatalf("the handle was wedged after concurrent activation: %v", err)
	}
}
