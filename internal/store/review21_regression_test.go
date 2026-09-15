package store_test

// Regressions for the findings of the twenty-first independent review, of
// head 8d77b30.

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

// Finding 1: created_at was bounded against the clock and updated_at was not,
// so monotonicNow clamped every later write up to a future value and the row
// kept claiming it changed in the future — uncorrectably.
func TestUpdatedAtIsBoundedAgainstTheClock(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r21-updated-at", state.IntentModifying, state.LaneOperator)

	future := fixedNow.Add(10 * time.Hour)

	t.Run("task run", func(t *testing.T) {
		taskID := ids.NewTaskID()
		packet := newPacket(taskID, f.Subject.RepositorySubjectID,
			state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ApprovePacket(ctx, packet)
		}); err != nil {
			t.Fatalf("approve: %v", err)
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateTaskRun(ctx, domain.TaskRun{
				TaskID:              taskID,
				RepositorySubjectID: f.Subject.RepositorySubjectID,
				PacketID:            packet.PacketID,
				Lane:                state.LaneOperator,
				Intent:              state.IntentModifying,
				Status:              state.TaskReady,
				CreatedAt:           fixedNow,
				UpdatedAt:           future,
			})
		}); !errors.Is(err, store.ErrFutureTimestamp) {
			t.Fatalf("want ErrFutureTimestamp, got %v", err)
		}
	})

	t.Run("worker attempt", func(t *testing.T) {
		successor := f.Attempt
		successor.AttemptID = ids.NewAttemptID()
		successor.FenceEpoch = f.Attempt.FenceEpoch + 1
		successor.Status = state.AttemptClaimed
		successor.CreatedAt = fixedNow
		successor.UpdatedAt = future
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed); err != nil {
				return err
			}
			return tx.CreateWorkerAttempt(ctx, successor)
		}); !errors.Is(err, store.ErrFutureTimestamp) {
			t.Fatalf("want ErrFutureTimestamp, got %v", err)
		}
	})
}

// Finding 2: the completion fences lived only in Go, so a raw UPDATE could
// mark a task COMPLETED on a fenced-out attempt's evidence, or with no
// publication at all — the I1 invariant, bypassed and permanent.
func TestCompletionIsDerivableAtSchemaLevel(t *testing.T) {
	ctx := context.Background()

	t.Run("evidence from a fenced-out attempt", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r21-fenced", state.IntentModifying, state.LaneOperator)

		commit := sha("r21-fenced-commit")
		pub := appliedPublication(t, db, f, commit, "refs/heads/m0/r21-fenced")
		ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); err != nil {
			t.Fatalf("record evidence: %v", err)
		}

		// Hand over to a successor, so the evidence's attempt is no longer
		// the task's current one.
		successor := f.Attempt
		successor.AttemptID = ids.NewAttemptID()
		successor.FenceEpoch = f.Attempt.FenceEpoch + 1
		successor.Status = state.AttemptRunning
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptAbandoned); err != nil {
				return err
			}
			if err := tx.CreateWorkerAttempt(ctx, successor); err != nil {
				return err
			}
			return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, successor.AttemptID)
		}); err != nil {
			t.Fatalf("hand over: %v", err)
		}

		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`UPDATE task_run SET status = 'COMPLETED', completed_evidence_id = ?
				   WHERE task_id = ?`, string(ev.EvidenceID), string(f.Task.TaskID))
		})
		if err == nil {
			t.Fatal("a raw UPDATE completed the task on a fenced-out attempt's evidence")
		}
		if !strings.Contains(err.Error(), "derivable") {
			t.Fatalf("expected the derivability trigger to fire, got: %v", err)
		}

		if err := db.Read(ctx, func(tx *store.Tx) error {
			run, err := tx.TaskRun(ctx, f.Task.TaskID)
			if err != nil {
				return err
			}
			if run.Status == state.TaskCompleted {
				t.Fatal("the task is COMPLETED")
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("modifying task with no publication", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r21-nopub", state.IntentModifying, state.LaneOperator)

		commit := sha("r21-nopub-commit")
		ev := evidenceFor(f, commit, commit, nil, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); err != nil {
			t.Fatalf("record evidence: %v", err)
		}

		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`UPDATE task_run SET status = 'COMPLETED', completed_evidence_id = ?
				   WHERE task_id = ?`, string(ev.EvidenceID), string(f.Task.TaskID))
		}); err == nil {
			t.Fatal("a modifying task was completed with no publication")
		}
	})

	t.Run("observed sha differing from published", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r21-advanced", state.IntentModifying, state.LaneOperator)

		published := sha("r21-advanced-ours")
		human := sha("r21-advanced-theirs")
		pub := appliedPublication(t, db, f, published, "refs/heads/m0/r21-advanced")
		ev := evidenceFor(f, published, human, &pub.PublishAttemptID, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); err != nil {
			t.Fatalf("record evidence: %v", err)
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`UPDATE task_run SET status = 'COMPLETED', completed_evidence_id = ?
				   WHERE task_id = ?`, string(ev.EvidenceID), string(f.Task.TaskID))
		}); err == nil {
			t.Fatal("an advanced ref completed the task")
		}
	})

	// The derivable case still completes through the normal path.
	t.Run("a derivable completion still works", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r21-ok", state.IntentReadOnly, state.LaneReview)
		ev := reviewEvidenceFor(f, "r21-ok-artifact")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.RecordEvidence(ctx, ev); err != nil {
				return err
			}
			return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
		}); err != nil {
			t.Fatalf("a derivable completion was refused: %v", err)
		}
	})
}

// Finding 3: json_valid accepts any JSON value, and one non-object made an
// entire epoch's history permanently undecodable.
func TestEventFieldsMustBeAJSONObject(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r21-fields", state.IntentReadOnly, state.LaneReview)

	for name, blob := range map[string]string{
		"array":  `[1,2,3]`,
		"number": `42`,
		"string": `"text"`,
		"null":   `null`,
	} {
		t.Run(name, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx,
					`INSERT INTO event (scheduler_epoch, seq, event_id, occurred_at, event_type,
					                    subject_kind, subject_id, task_id, attempt_id, fields)
					 VALUES (?, 9000, ?, '2026-09-12T09:00:00.000000000Z', 'r21.bad',
					         'SCHEDULER', 'probe', NULL, NULL, ?)`,
					int64(f.Epoch), string(ids.NewEventID()), blob)
			})
			if err == nil {
				t.Fatalf("event.fields accepted a JSON %s", name)
			}
		})
	}

	// History still reads, which is the property the CHECK protects.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		_, err := tx.EventsInEpoch(ctx, f.Epoch)
		return err
	}); err != nil {
		t.Fatalf("history became undecodable: %v", err)
	}

	// The packet scope columns are arrays for the same reason.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO execution_packet (packet_id, task_id, lane, intent, repository_subject_id,
			                               source_revision, source_digest, packet_digest,
			                               approved_at, expires_at, allowed_scope, forbidden_scope,
			                               stop_conditions, acceptance_contract, status)
			 VALUES (?, ?, 'review', 'READ_ONLY', ?, 'rev', ?, ?,
			         '2026-09-12T09:00:00.000000000Z', '2026-09-13T09:00:00.000000000Z',
			         '["edit"]', '{}', '[]', 'contract', 'APPROVED')`,
			string(ids.NewPacketID()), string(ids.NewTaskID()),
			string(f.Subject.RepositorySubjectID),
			string(digestOf("s")), string(digestOf("p")))
	}); err == nil {
		t.Fatal("forbidden_scope accepted a JSON object")
	}
}

// Finding 4: a lease already expired against real time was accepted, because
// the guard compared it only to created_at, which may itself be in the past.
func TestLeaseCannotBeAlreadyExpired(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r21-lease", state.IntentModifying, state.LaneOperator)

	pastCreated := fixedNow.Add(-2 * time.Hour)
	expiredLease := fixedNow.Add(-time.Hour)

	successor := f.Attempt
	successor.AttemptID = ids.NewAttemptID()
	successor.FenceEpoch = f.Attempt.FenceEpoch + 1
	successor.Status = state.AttemptClaimed
	successor.CreatedAt = pastCreated
	successor.UpdatedAt = pastCreated
	successor.LeaseExpiresAt = &expiredLease

	// Free the modifying slot in its own committed transaction, so the
	// refused insert below cannot roll back its own setup.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed)
	}); err != nil {
		t.Fatalf("retire: %v", err)
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, successor)
	}); !errors.Is(err, domain.ErrInvalidEntity) {
		t.Fatalf("want ErrInvalidEntity for an already-expired lease, got %v", err)
	}

	// A lease in the future is accepted, which is the normal case.
	future := fixedNow.Add(time.Hour)
	successor.LeaseExpiresAt = &future
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, successor)
	}); err != nil {
		t.Fatalf("a future lease was refused: %v", err)
	}
}

// Finding 5: WorkspaceReleased was the one precondition field whose zero
// value was permissive, in a gate documented as failing closed on every
// branch.
func TestPublishGateFailsClosedOnAnUnsetWorkspaceField(t *testing.T) {
	f := fixtureForPreconditions()

	if err := domain.CheckPublishPreconditions(f.pub, f.live); err != nil {
		t.Fatalf("a coherent publication was rejected: %v", err)
	}

	// The zero value now means "not known to be live", so an incrementally
	// built struct is refused rather than passed.
	unset := f.live
	unset.WorkspaceLive = false
	if err := domain.CheckPublishPreconditions(f.pub, unset); !errors.Is(err, domain.ErrPublishBindingInvalid) {
		t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
	}

	// A struct with only the identity fields filled in fails closed too.
	var bare domain.PublishPreconditions
	if err := domain.CheckPublishPreconditions(f.pub, bare); err == nil {
		t.Fatal("an entirely unset precondition struct was accepted")
	}
}

// Finding 6: the integrity check reported success when the pragma returned no
// rows, in the one check deciding whether a corrupt database is opened.
func TestIntegrityCheckRequiresExactlyOneOkRow(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	// Both checks pass on a sound database, seeing exactly one "ok".
	if err := db.QuickIntegrityCheck(ctx); err != nil {
		t.Fatalf("quick check: %v", err)
	}
	if err := db.IntegrityCheck(ctx); err != nil {
		t.Fatalf("full check: %v", err)
	}

	// A pragma that yields no rows is now a failure rather than a pass.
	if err := db.RunIntegrityCheckForTest(ctx, "PRAGMA application_id"); err == nil {
		t.Fatal("a pragma returning no ok row was reported as success")
	}
}
