package store_test

// Regressions for the findings of the fourth independent review, of head
// 4e30d8d. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1: Open never read the migration ledger, so a database written by a
// newer build opened fine as long as the caller never migrated.
func TestOpenRejectsNewerThanSupportedSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "future.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// A build we do not know about migrated this database further.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO schema_migration (version, name, checksum, applied_at)
			 VALUES (99, 'from_the_future', 'deadbeef', '2027-01-01T00:00:00.000000000Z')`)
	}); err != nil {
		t.Fatalf("seed future migration: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Every open path must refuse it, including the ones that never migrate.
	for _, mode := range []store.Mode{store.ModeReadWrite, store.ModeReadOnly} {
		t.Run(mode.String(), func(t *testing.T) {
			if _, err := store.Open(ctx, store.Config{Path: path, Mode: mode}); !errors.Is(err, store.ErrSchemaVersionUnsupported) {
				t.Fatalf("want ErrSchemaVersionUnsupported, got %v", err)
			}
		})
	}
}

// Finding 2: the epoch fence was one-way. A handle from a retired generation
// could still mutate the current generation's live state.
func TestRetiredHandleCannotMutateCurrentState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "handover.db")

	// Handle A owns epoch 1 and seeds a task.
	handleA, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer handleA.Close()
	if err := handleA.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	clockedA := handleA.WithClock(func() time.Time { return fixedNow })
	f := seed(t, clockedA, "r4-handover", state.IntentModifying, state.LaneOperator)

	// Handle B takes over as epoch 2 and admits its own attempt.
	handleB, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer handleB.Close()
	clockedB := handleB.WithClock(func() time.Time { return fixedNow })
	epochB, err := clockedB.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "successor")
	if err != nil {
		t.Fatalf("activate B: %v", err)
	}

	successor := f.Attempt
	successor.AttemptID = ids.NewAttemptID()
	successor.FenceEpoch = f.Attempt.FenceEpoch + 1
	successor.SchedulerEpoch = epochB.Epoch
	successor.Status = state.AttemptRunning
	if err := clockedB.Write(ctx, func(tx *store.Tx) error {
		if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptAbandoned); err != nil {
			return err
		}
		if err := tx.CreateWorkerAttempt(ctx, successor); err != nil {
			return err
		}
		return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, successor.AttemptID)
	}); err != nil {
		t.Fatalf("B takes over: %v", err)
	}

	// Handle A is retired. It must not be able to interfere with B's live
	// attempt, workspace or packet, even though those rows are current.
	interference := map[string]func(tx *store.Tx) error{
		"abandon the live attempt": func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, successor.AttemptID, state.AttemptAbandoned)
		},
		"release the workspace": func(tx *store.Tx) error {
			return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
		},
		"mark the packet stale": func(tx *store.Tx) error {
			return tx.SetPacketStatus(ctx, f.Packet.PacketID, state.PacketStale)
		},
		"block the task": func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskBlockedDesign)
		},
		"move the current attempt": func(tx *store.Tx) error {
			return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, successor.AttemptID)
		},
		"append to history": func(tx *store.Tx) error {
			_, err := tx.AppendEvent(ctx, eventFor(epochB.Epoch, "interference"))
			return err
		},
	}
	for name, mutate := range interference {
		t.Run(name, func(t *testing.T) {
			if err := clockedA.Write(ctx, mutate); !errors.Is(err, store.ErrStaleEpoch) {
				t.Fatalf("want ErrStaleEpoch, got %v", err)
			}
		})
	}

	// B, which owns the current generation, still works.
	if err := clockedB.Write(ctx, func(tx *store.Tx) error {
		return tx.SetWorkerAttemptStatus(ctx, successor.AttemptID, state.AttemptVerifying)
	}); err != nil {
		t.Fatalf("the owning handle must still be able to act: %v", err)
	}
}

// A handle that never acquired ownership may not mutate execution state at all.
func TestHandleWithoutOwnershipCannotMutate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "unowned.db")

	owner, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer owner.Close()
	if err := owner.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	clocked := owner.WithClock(func() time.Time { return fixedNow })
	f := seed(t, clocked, "r4-unowned", state.IntentModifying, state.LaneOperator)

	bystander, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err != nil {
		t.Fatalf("open bystander: %v", err)
	}
	defer bystander.Close()
	if bystander.OwnedEpoch() != 0 {
		t.Fatalf("a handle that never activated reports ownership of epoch %d", int64(bystander.OwnedEpoch()))
	}

	if err := bystander.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
	}); !errors.Is(err, store.ErrNoSchedulerOwnership) {
		t.Fatalf("want ErrNoSchedulerOwnership, got %v", err)
	}

	// Reading is unaffected.
	if err := bystander.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status != state.TaskReady {
			t.Fatalf("task status changed to %q", string(run.Status))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 3: an attempt could be admitted under a packet that no longer
// granted execution authority.
func TestAttemptRequiresAPacketThatGrantsAuthority(t *testing.T) {
	ctx := context.Background()

	for _, status := range []state.PacketStatus{state.PacketStale, state.PacketSuperseded} {
		t.Run(string(status), func(t *testing.T) {
			db := newDB(t)
			f := seed(t, db, "r4-packet-authority", state.IntentModifying, state.LaneOperator)

			if err := db.Write(ctx, func(tx *store.Tx) error {
				if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed); err != nil {
					return err
				}
				return tx.SetPacketStatus(ctx, f.Packet.PacketID, status)
			}); err != nil {
				t.Fatalf("setup: %v", err)
			}

			successor := f.Attempt
			successor.AttemptID = ids.NewAttemptID()
			successor.FenceEpoch = f.Attempt.FenceEpoch + 1
			successor.Status = state.AttemptClaimed

			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.CreateWorkerAttempt(ctx, successor)
			}); !errors.Is(err, domain.ErrPacketNoAuthority) {
				t.Fatalf("want ErrPacketNoAuthority, got %v", err)
			}

			// And at schema level.
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx,
					`INSERT INTO worker_attempt (attempt_id, task_id, packet_id, repository_subject_id,
					                             lane, intent, scheduler_epoch, fence_epoch, status,
					                             created_at, updated_at)
					 VALUES (?, ?, ?, ?, 'operator', 'MODIFYING', ?, ?, 'CLAIMED',
					         '2026-09-12T09:00:00.000000000Z', '2026-09-12T09:00:00.000000000Z')`,
					string(successor.AttemptID), string(f.Task.TaskID), string(f.Packet.PacketID),
					string(f.Subject.RepositorySubjectID), int64(f.Epoch), int64(successor.FenceEpoch))
			})
			if err == nil {
				t.Fatalf("an attempt was admitted under a %s packet", string(status))
			}
			if !strings.Contains(err.Error(), "packet that grants execution authority") {
				t.Fatalf("expected the packet-authority trigger to fire, got: %v", err)
			}
		})
	}

	t.Run("expired packet", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r4-packet-expiry", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed)
		}); err != nil {
			t.Fatalf("setup: %v", err)
		}

		// Move the clock past the packet's expiry.
		late := db.WithClock(func() time.Time { return f.Packet.ExpiresAt.Add(time.Minute) })
		if _, err := late.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "late generation"); err != nil {
			t.Fatalf("activate: %v", err)
		}
		successor := f.Attempt
		successor.AttemptID = ids.NewAttemptID()
		successor.FenceEpoch = f.Attempt.FenceEpoch + 1
		successor.Status = state.AttemptClaimed
		successor.SchedulerEpoch = late.OwnedEpoch()

		if err := late.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkerAttempt(ctx, successor)
		}); !errors.Is(err, domain.ErrPacketExpired) {
			t.Fatalf("want ErrPacketExpired, got %v", err)
		}
	})
}

// Finding 5: completing a task whose scheduling state cannot reach COMPLETED
// surfaced a raw driver constraint error instead of a typed refusal.
func TestCompletionRefusalIsTyped(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r4-typed-refusal", state.IntentReadOnly, state.LaneReview)

	ev := reviewEvidenceFor(f, "r4-typed-artifact")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordEvidence(ctx, ev); err != nil {
			return err
		}
		return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskBlockedDesign)
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	})
	if !errors.Is(err, store.ErrForbiddenTaskTransition) {
		t.Fatalf("want ErrForbiddenTaskTransition, got %v", err)
	}
	if strings.Contains(err.Error(), "constraint failed") {
		t.Fatalf("a legitimate refusal surfaced as a driver error: %v", err)
	}
}

// Finding 8: a MODIFYING attempt could own a READ_ONLY_CHECKOUT workspace and
// publish from it.
func TestWorkspaceIsolationMustMatchIntent(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r4-isolation", state.IntentModifying, state.LaneOperator)

	// A successor modifying attempt with a read-only checkout.
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

	ws := f.Workspace
	ws.WorkspaceID = ids.NewWorkspaceID()
	ws.AttemptID = successor.AttemptID
	ws.RootPath = "/var/lib/acp/workspaces/r4-isolation-mismatch"
	ws.IsolationKind = domain.IsolationReadOnlyCheckout

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkspace(ctx, ws)
	})
	// Refused as a typed conflict before it reaches the trigger.
	if !errors.Is(err, store.ErrWorkspaceConflict) {
		t.Fatalf("want ErrWorkspaceConflict, got %v", err)
	}

	// The trigger remains the backstop, proven by bypassing the Go gate.
	err = db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO workspace (workspace_id, attempt_id, task_id, repository_subject_id,
			                        base_sha, isolation_kind, root_path, created_at, released_at)
			 VALUES (?, ?, ?, ?, ?, 'READ_ONLY_CHECKOUT', ?,
			         '2026-09-12T09:00:00.000000000Z', NULL)`,
			string(ids.NewWorkspaceID()), string(successor.AttemptID), string(ws.TaskID),
			string(ws.RepositorySubjectID), string(ws.BaseSHA),
			"/var/lib/acp/workspaces/r4-isolation-raw")
	})
	if err == nil {
		t.Fatal("a modifying attempt was given a read-only checkout")
	}
	if !strings.Contains(err.Error(), "isolation_kind must match the attempt intent") {
		t.Fatalf("expected the isolation trigger to fire, got: %v", err)
	}

	// And the converse: a read-only attempt cannot hold an isolated clone.
	review := seed(t, db, "r4-isolation-review", state.IntentReadOnly, state.LaneReview)
	clone := review.Workspace
	clone.WorkspaceID = ids.NewWorkspaceID()
	clone.RootPath = "/var/lib/acp/workspaces/r4-isolation-clone"
	clone.IsolationKind = domain.IsolationIsolatedClone
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkspace(ctx, clone)
	}); err == nil {
		t.Fatal("a read-only attempt was given an isolated clone")
	}
}

// Finding 11: the idempotency key was unique but unqueryable, so a publisher
// restarting after a crash could not resolve the existing intent.
func TestPublicationIsResolvableByIdentity(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r4-resolve", state.IntentModifying, state.LaneOperator)

	commit := sha("r4-resolve-commit")
	pub := publishFor(f, commit, "refs/heads/m0/r4-resolve")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	// A restarting publisher re-derives the key from the intent it was about
	// to publish, is told it is a duplicate, and can then resolve it.
	retry := publishFor(f, commit, "refs/heads/m0/r4-resolve")
	retry.PublishAttemptID = ids.NewPublishAttemptID()
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, retry)
	}); !errors.Is(err, store.ErrDuplicatePublishIdentity) {
		t.Fatalf("want ErrDuplicatePublishIdentity, got %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		existing, err := tx.PublishAttemptByIdempotencyKey(ctx, domain.DeriveIdempotencyKey(retry))
		if err != nil {
			return err
		}
		if existing.PublishAttemptID != pub.PublishAttemptID {
			t.Fatalf("resolved %s, want %s", string(existing.PublishAttemptID), string(pub.PublishAttemptID))
		}
		if existing.Status != state.PublishApplied {
			t.Fatalf("resolved status is %q, want APPLIED", string(existing.Status))
		}

		forAttempt, err := tx.PublishAttemptsForAttempt(ctx, f.Attempt.AttemptID)
		if err != nil {
			return err
		}
		if len(forAttempt) != 1 || forAttempt[0].PublishAttemptID != pub.PublishAttemptID {
			t.Fatalf("per-attempt lookup returned %d rows: %+v", len(forAttempt), forAttempt)
		}

		if _, err := tx.PublishAttemptByIdempotencyKey(ctx, "no-such-key"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 4 is NOT fixed here: it is an architecture contradiction escalated
// for central adjudication, and this test pins the constrained behaviour so
// the gap is visible in the suite rather than latent.
//
// Completion requires a non-terminal attempt, and WorkerAttemptStatus has no
// success-flavoured terminal state — every terminal value (FAILED, ABANDONED,
// EVIDENCE_UNKNOWN) describes a failure. So a successfully completed task
// necessarily leaves a live attempt holding the repository's modifying slot,
// and the only way to free it is to record a successful attempt as a failed
// one, which would falsify the record.
//
// Resolving it needs either a new terminal WorkerAttemptStatus, or redefining
// the CC9 slot to exclude attempts of terminal tasks. Both change a
// packet-specified contract, so neither is taken unilaterally.
func TestCompletedTaskStillHoldsModifyingSlot_KnownEscalation(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	first := seed(t, db, "r4-slot-a", state.IntentModifying, state.LaneOperator)

	commit := sha("r4-slot-commit")
	pub := appliedPublication(t, db, first, commit, "refs/heads/m0/r4-slot")
	ev := evidenceFor(first, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordEvidence(ctx, ev); err != nil {
			return err
		}
		return tx.CompleteTaskRunFromEvidence(ctx, first.Task.TaskID, ev.EvidenceID)
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// The completing attempt is still live, and still holds the slot.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		attempt, err := tx.WorkerAttempt(ctx, first.Attempt.AttemptID)
		if err != nil {
			return err
		}
		if !attempt.Status.HoldsModifyingSlot() {
			t.Fatalf("attempt status is %q: the escalation may have been resolved, "+
				"so revisit this test and the report", string(attempt.Status))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// A second task on the same repository subject therefore cannot be
	// admitted, even though the first task is finished.
	secondTask := ids.NewTaskID()
	secondPacket := newPacket(secondTask, first.Subject.RepositorySubjectID, state.IntentModifying, state.LaneGuardrail)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.ApprovePacket(ctx, secondPacket); err != nil {
			return err
		}
		return tx.CreateTaskRun(ctx, domain.TaskRun{
			TaskID:              secondTask,
			RepositorySubjectID: first.Subject.RepositorySubjectID,
			PacketID:            secondPacket.PacketID,
			Lane:                state.LaneGuardrail,
			Intent:              state.IntentModifying,
			Status:              state.TaskReady,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	}); err != nil {
		t.Fatalf("seed second task: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, domain.WorkerAttempt{
			AttemptID:           ids.NewAttemptID(),
			TaskID:              secondTask,
			PacketID:            secondPacket.PacketID,
			RepositorySubjectID: first.Subject.RepositorySubjectID,
			Lane:                state.LaneGuardrail,
			Intent:              state.IntentModifying,
			SchedulerEpoch:      first.Epoch,
			FenceEpoch:          1,
			Status:              state.AttemptClaimed,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	})
	if err == nil {
		t.Fatal("the modifying slot was free: the escalation may have been resolved, " +
			"so revisit this test and the report")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Fatalf("expected the modifying-slot index to reject it, got: %v", err)
	}

	// The precise shape of the escalation, pinned so the issue's framing
	// cannot drift: the slot IS recoverable — retiring the completing attempt
	// frees it — but only by recording a successful attempt as failed,
	// abandoned or evidence-unknown. So the cost is a false durable record
	// rather than a permanently wedged repository.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetWorkerAttemptStatus(ctx, first.Attempt.AttemptID, state.AttemptAbandoned)
	}); err != nil {
		t.Fatalf("retiring the completing attempt should be possible: %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, domain.WorkerAttempt{
			AttemptID:           ids.NewAttemptID(),
			TaskID:              secondTask,
			PacketID:            secondPacket.PacketID,
			RepositorySubjectID: first.Subject.RepositorySubjectID,
			Lane:                state.LaneGuardrail,
			Intent:              state.IntentModifying,
			SchedulerEpoch:      first.Epoch,
			FenceEpoch:          1,
			Status:              state.AttemptClaimed,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	}); err != nil {
		t.Fatalf("the slot should be free once the completing attempt is retired: %v", err)
	}

	// And the falsification is now durable: the attempt that succeeded is on
	// record as abandoned.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		attempt, err := tx.WorkerAttempt(ctx, first.Attempt.AttemptID)
		if err != nil {
			return err
		}
		if attempt.Status != state.AttemptAbandoned {
			t.Fatalf("attempt status is %q", string(attempt.Status))
		}
		run, err := tx.TaskRun(ctx, first.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status != state.TaskCompleted {
			t.Fatalf("task status is %q, want COMPLETED", string(run.Status))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}
