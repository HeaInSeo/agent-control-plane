package store_test

// Regressions for the findings of the second independent review, of head
// 64801ad. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// appliedPublication records a publication for f and drives it to APPLIED.
func appliedPublication(t *testing.T, db *store.DB, f fixture, commit domain.CommitSHA, ref string) domain.PublishAttempt {
	t.Helper()
	ctx := context.Background()
	pub := publishFor(f, commit, ref)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied)
	}); err != nil {
		t.Fatalf("record publication: %v", err)
	}
	return pub
}

// Finding 1 (HIGH): a superseded scheduler generation drove a task to
// COMPLETED, because completion had no current-epoch check while admission
// and publication both did.
func TestSupersededEpochCannotCompleteTask(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r2-epoch", state.IntentModifying, state.LaneOperator)

	commit := sha("r2-epoch-commit")
	pub := appliedPublication(t, db, f, commit, "refs/heads/m0/r2-epoch")
	ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordEvidence(ctx, ev)
	}); err != nil {
		t.Fatalf("record evidence: %v", err)
	}

	// A successor scheduler takes ownership.
	if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "successor"); err != nil {
		t.Fatalf("activate successor: %v", err)
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	}); !errors.Is(err, store.ErrStaleEpoch) {
		t.Fatalf("want ErrStaleEpoch, got %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status == state.TaskCompleted {
			t.Fatal("a retired scheduler generation completed the task")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 2: evidence could be appended under a retired generation, which is
// the input finding 1 consumed.
func TestSupersededEpochCannotRecordEvidence(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r2-evidence-epoch", state.IntentModifying, state.LaneOperator)

	commit := sha("r2-evidence-epoch-commit")
	pub := appliedPublication(t, db, f, commit, "refs/heads/m0/r2-ev-epoch")

	if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "successor"); err != nil {
		t.Fatalf("activate successor: %v", err)
	}

	ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordEvidence(ctx, ev)
	}); !errors.Is(err, store.ErrStaleEpoch) {
		t.Fatalf("want ErrStaleEpoch, got %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		if _, err := tx.Evidence(ctx, ev.EvidenceID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("stale-epoch evidence was persisted: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 3: the task's current-attempt pointer could be moved backwards onto
// a fenced-out attempt, undoing the guard completion depends on.
func TestCurrentAttemptPointerOnlyMovesForward(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r2-pointer", state.IntentModifying, state.LaneOperator)

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

	// Backwards, onto the fenced-out attempt.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, f.Attempt.AttemptID)
	}); !errors.Is(err, store.ErrInvalidTaskRun) {
		t.Fatalf("want ErrInvalidTaskRun, got %v", err)
	}

	// And at schema level.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE task_run SET current_attempt_id = ? WHERE task_id = ?`,
			string(f.Attempt.AttemptID), string(f.Task.TaskID))
	})
	if err == nil {
		t.Fatal("the current-attempt pointer moved backwards")
	}
	if !strings.Contains(err.Error(), "must move forward to a live attempt") {
		t.Fatalf("expected the forward-only trigger to fire, got: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.CurrentAttemptID == nil || *run.CurrentAttemptID != successor.AttemptID {
			t.Fatalf("current attempt is %v, want the successor", run.CurrentAttemptID)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 4: a terminal attempt could be revived, making IsTerminal a
// non-durable property.
func TestWorkerAttemptStatusTransitions(t *testing.T) {
	ctx := context.Background()

	terminal := []state.WorkerAttemptStatus{
		state.AttemptFailed, state.AttemptAbandoned, state.AttemptEvidenceUnknown,
	}
	live := []state.WorkerAttemptStatus{
		state.AttemptClaimed, state.AttemptStarting, state.AttemptRunning, state.AttemptVerifying,
	}

	for _, from := range terminal {
		for _, to := range append(append([]state.WorkerAttemptStatus{}, live...), terminal...) {
			if from == to {
				continue
			}
			t.Run("forbidden "+string(from)+"->"+string(to), func(t *testing.T) {
				db := newDB(t)
				f := seed(t, db, "r2-attempt-status", state.IntentModifying, state.LaneOperator)
				if err := db.Write(ctx, func(tx *store.Tx) error {
					return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, from)
				}); err != nil {
					t.Fatalf("reaching %s: %v", string(from), err)
				}
				if err := db.Write(ctx, func(tx *store.Tx) error {
					return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, to)
				}); !errors.Is(err, store.ErrForbiddenAttemptTransition) {
					t.Fatalf("want ErrForbiddenAttemptTransition, got %v", err)
				}
			})
		}
	}

	// Live states must not move backwards either: a retry is a new attempt.
	t.Run("forbidden RUNNING->CLAIMED", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r2-backwards", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptClaimed)
		}); !errors.Is(err, store.ErrForbiddenAttemptTransition) {
			t.Fatalf("want ErrForbiddenAttemptTransition, got %v", err)
		}
	})

	// Forward progress and live -> terminal are permitted.
	t.Run("permitted forward progress", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r2-forward", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptVerifying); err != nil {
				return err
			}
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed)
		}); err != nil {
			t.Fatalf("permitted transitions refused: %v", err)
		}
	})

	// A revived attempt must not be able to complete a task.
	t.Run("revived attempt cannot complete", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r2-revive", state.IntentModifying, state.LaneOperator)
		commit := sha("r2-revive-commit")
		pub := appliedPublication(t, db, f, commit, "refs/heads/m0/r2-revive")
		ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.RecordEvidence(ctx, ev); err != nil {
				return err
			}
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptEvidenceUnknown)
		}); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptRunning)
		}); err == nil {
			t.Fatal("a terminal attempt was revived")
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
		}); !errors.Is(err, store.ErrCompletionNotDerivable) {
			t.Fatalf("want ErrCompletionNotDerivable, got %v", err)
		}
	})
}

// Finding 5: a completed task could be silently re-completed against
// different evidence, losing which observation established completion.
func TestCompletedTaskCannotBeRebound(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r2-recomplete", state.IntentReadOnly, state.LaneReview)

	first := reviewEvidenceFor(f, "artifact-one")
	second := reviewEvidenceFor(f, "artifact-two")

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordEvidence(ctx, first); err != nil {
			return err
		}
		if err := tx.RecordEvidence(ctx, second); err != nil {
			return err
		}
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, first.EvidenceID)
	}); err != nil {
		t.Fatalf("first completion: %v", err)
	}

	// Re-completing with the same evidence is idempotent.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, first.EvidenceID)
	}); err != nil {
		t.Fatalf("idempotent re-completion should succeed: %v", err)
	}

	// Re-completing with other evidence is refused.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, second.EvidenceID)
	}); !errors.Is(err, store.ErrCompletionNotDerivable) {
		t.Fatalf("want ErrCompletionNotDerivable, got %v", err)
	}

	// And the schema refuses a raw rebind.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE task_run SET completed_evidence_id = ? WHERE task_id = ?`,
			string(second.EvidenceID), string(f.Task.TaskID))
	})
	if err == nil {
		t.Fatal("completion evidence was rebound")
	}
	if !strings.Contains(err.Error(), "final once bound") {
		t.Fatalf("expected the finality trigger to fire, got: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.CompletedEvidenceID == nil || *run.CompletedEvidenceID != first.EvidenceID {
			t.Fatalf("completion is bound to %v, want the first observation", run.CompletedEvidenceID)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 6 and 11: durable execution records are undeletable, and the
// immutability triggers cover every column that is not meant to change.
func TestDurableRecordsCannotBeDeletedOrRelabelled(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r2-durable", state.IntentModifying, state.LaneOperator)

	commit := sha("r2-durable-commit")
	pub := appliedPublication(t, db, f, commit, "refs/heads/m0/r2-durable")
	ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordEvidence(ctx, ev)
	}); err != nil {
		t.Fatalf("record evidence: %v", err)
	}

	deletes := map[string]string{
		"evidence_observation": `DELETE FROM evidence_observation WHERE evidence_id = '` + string(ev.EvidenceID) + `'`,
		"publish_attempt":      `DELETE FROM publish_attempt WHERE publish_attempt_id = '` + string(pub.PublishAttemptID) + `'`,
		"workspace":            `DELETE FROM workspace WHERE workspace_id = '` + string(f.Workspace.WorkspaceID) + `'`,
		"worker_attempt":       `DELETE FROM worker_attempt WHERE attempt_id = '` + string(f.Attempt.AttemptID) + `'`,
		"execution_packet":     `DELETE FROM execution_packet WHERE packet_id = '` + string(f.Packet.PacketID) + `'`,
		"event":                `DELETE FROM event WHERE scheduler_epoch = ` + "1",
	}
	for table, stmt := range deletes {
		t.Run("delete "+table, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, stmt)
			})
			if err == nil {
				t.Fatalf("a row was deleted from %s", table)
			}
			if !strings.Contains(err.Error(), "cannot be deleted") &&
				!strings.Contains(err.Error(), "append-only") {
				t.Fatalf("expected a no-delete trigger to fire, got: %v", err)
			}
		})
	}

	relabels := map[string]string{
		"workspace isolation_kind": `UPDATE workspace SET isolation_kind = 'READ_ONLY_CHECKOUT' WHERE workspace_id = '` + string(f.Workspace.WorkspaceID) + `'`,
		"workspace created_at":     `UPDATE workspace SET created_at = '2020-01-01T00:00:00.000000000Z' WHERE workspace_id = '` + string(f.Workspace.WorkspaceID) + `'`,
		"attempt lane":             `UPDATE worker_attempt SET lane = 'guardrail' WHERE attempt_id = '` + string(f.Attempt.AttemptID) + `'`,
		"attempt created_at":       `UPDATE worker_attempt SET created_at = '2020-01-01T00:00:00.000000000Z' WHERE attempt_id = '` + string(f.Attempt.AttemptID) + `'`,
	}
	for what, stmt := range relabels {
		t.Run("relabel "+what, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, stmt)
			})
			if err == nil {
				t.Fatalf("%s was changed after the fact", what)
			}
			if !strings.Contains(err.Error(), "immutable") {
				t.Fatalf("expected an immutability trigger to fire, got: %v", err)
			}
		})
	}

	// released_at must still be settable: it is the one mutable field.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
	}); err != nil {
		t.Fatalf("releasing a workspace should remain possible: %v", err)
	}
}

// Finding 7: a relative Config.Path produced an opaque driver error.
func TestRelativeDatabasePathWorks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	for _, rel := range []string{"cp.db", "sub/cp.db", "./nested/deeper/cp.db"} {
		t.Run(rel, func(t *testing.T) {
			db, err := store.Open(ctx, store.Config{
				Path: rel, Mode: store.ModeReadWrite, AllowCreate: true,
			})
			if err != nil {
				t.Fatalf("relative path %q: %v", rel, err)
			}
			defer db.Close()
			if err := db.MigrateEmbedded(ctx); err != nil {
				t.Fatalf("migrate: %v", err)
			}
			if !filepath.IsAbs(db.Path()) {
				t.Fatalf("handle reports a relative path: %q", db.Path())
			}
			if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "relative path"); err != nil {
				t.Fatalf("activate: %v", err)
			}
		})
	}
}

// Finding 8: a write inside db.Read committed on a read-write handle.
func TestReadTransactionRefusesWrites(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r2-readonly-tx", state.IntentModifying, state.LaneOperator)

	writes := map[string]func(tx *store.Tx) error{
		"SetTaskRunStatus": func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskBlockedDesign)
		},
		"SetWorkerAttemptStatus": func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptVerifying)
		},
		"SetPacketStatus": func(tx *store.Tx) error {
			return tx.SetPacketStatus(ctx, f.Packet.PacketID, state.PacketStale)
		},
		"ReleaseWorkspace": func(tx *store.Tx) error {
			return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
		},
		"AppendEvent": func(tx *store.Tx) error {
			_, err := tx.AppendEvent(ctx, eventFor(f.Epoch, "should.not.persist"))
			return err
		},
		"ObserveRepositorySubject": func(tx *store.Tx) error {
			renamed := f.Subject
			renamed.CurrentFullName = "HeaInSeo/should-not-persist"
			renamed.ObservedAt = fixedNow.Add(time.Hour)
			_, err := tx.ObserveRepositorySubject(ctx, renamed)
			return err
		},
		"raw exec": func(tx *store.Tx) error {
			return tx.ExecForTest(ctx, `UPDATE task_run SET status = 'ABANDONED' WHERE task_id = ?`,
				string(f.Task.TaskID))
		},
	}

	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			if err := db.Read(ctx, write); !errors.Is(err, store.ErrReadOnly) {
				t.Fatalf("want ErrReadOnly, got %v", err)
			}
		})
	}

	// Nothing leaked through, and the handle still works for real writes.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status != state.TaskReady {
			t.Fatalf("a write inside Read persisted: task status is %q", string(run.Status))
		}
		subject, err := tx.RepositorySubject(ctx, f.Subject.RepositorySubjectID)
		if err != nil {
			return err
		}
		if subject.CurrentFullName != f.Subject.CurrentFullName {
			t.Fatalf("a write inside Read persisted: alias is %q", subject.CurrentFullName)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskBlockedDesign)
	}); err != nil {
		t.Fatalf("the handle must still accept writes: %v", err)
	}
}

// Finding 9: confirming an unchanged alias left observed_at behind, so a
// later stale observation could still overwrite the alias.
func TestUnchangedObservationRefreshesObservedAt(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r2-observed-at", state.IntentReadOnly, state.LaneReview)

	t1 := f.Subject.ObservedAt
	t2 := t1.Add(time.Hour)
	t3 := t1.Add(2 * time.Hour)

	// Confirm the same alias at t3, with the clock reading t3: a
	// future-dated observation is refused.
	confirm := f.Subject
	confirm.ObservedAt = t3
	atT3 := db.WithClock(func() time.Time { return t3 })
	if err := atT3.Write(ctx, func(tx *store.Tx) error {
		got, err := tx.ObserveRepositorySubject(ctx, confirm)
		if err != nil {
			return err
		}
		if !got.ObservedAt.Equal(t3) {
			t.Fatalf("confirmation did not advance observed_at: %v", got.ObservedAt)
		}
		return nil
	}); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	// A queued observation from t2 carrying a different name must now lose.
	stale := f.Subject
	stale.CurrentFullName = "HeaInSeo/stale-name"
	stale.ObservedAt = t2
	if err := atT3.Write(ctx, func(tx *store.Tx) error {
		got, err := tx.ObserveRepositorySubject(ctx, stale)
		if err != nil {
			return err
		}
		if got.CurrentFullName != f.Subject.CurrentFullName {
			t.Fatalf("a stale observation overwrote the alias with %q", got.CurrentFullName)
		}
		return nil
	}); err != nil {
		t.Fatalf("stale observation: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		current, err := tx.RepositorySubjectByNodeID(ctx, f.Subject.GitHubNodeID)
		if err != nil {
			return err
		}
		if current.CurrentFullName != f.Subject.CurrentFullName {
			t.Fatalf("stored alias is %q", current.CurrentFullName)
		}
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.EventType == "repository_subject.renamed" {
				t.Fatal("a stale observation wrote a rename into append-only history")
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 10: CreateTaskRun silently discarded CurrentAttemptID.
func TestCreateTaskRunRejectsACurrentAttempt(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r2-createtask", state.IntentModifying, state.LaneOperator)

	taskID := ids.NewTaskID()
	packet := newPacket(taskID, f.Subject.RepositorySubjectID, state.IntentModifying, state.LaneOperator)
	run := domain.TaskRun{
		TaskID:              taskID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		PacketID:            packet.PacketID,
		Lane:                state.LaneOperator,
		Intent:              state.IntentModifying,
		Status:              state.TaskReady,
		CurrentAttemptID:    &f.Attempt.AttemptID,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.ApprovePacket(ctx, packet); err != nil {
			return err
		}
		return tx.CreateTaskRun(ctx, run)
	}); !errors.Is(err, store.ErrInvalidTaskRun) {
		t.Fatalf("want ErrInvalidTaskRun, got %v", err)
	}
}
