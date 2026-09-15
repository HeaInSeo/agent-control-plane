package store_test

// Regressions for the findings of the seventh independent review, of head
// e86e40b. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1 (HIGH): task_run was the only durable entity without an identity
// trigger, so the packet a task was approved against could be swapped after
// the fact — re-authorising it under a wider scope.
func TestTaskRunIdentityIsImmutable(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r7-identity", state.IntentModifying, state.LaneOperator)
	other := seed(t, db, "r7-identity-other", state.IntentReadOnly, state.LaneReview)

	// A second packet for this task, with a much wider scope.
	wider := newPacket(f.Task.TaskID, f.Subject.RepositorySubjectID, state.IntentModifying, state.LaneOperator)
	wider.AllowedScope = []string{"edit:ANYTHING"}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ApprovePacket(ctx, wider)
	}); err != nil {
		t.Fatalf("approve wider packet: %v", err)
	}

	mutations := map[string]string{
		"packet_id": `UPDATE task_run SET packet_id = '` + string(wider.PacketID) + `' WHERE task_id = ?`,
		"intent":    `UPDATE task_run SET intent = 'READ_ONLY' WHERE task_id = ?`,
		"lane":      `UPDATE task_run SET lane = 'guardrail' WHERE task_id = ?`,
		"repository_subject_id": `UPDATE task_run SET repository_subject_id = '` +
			string(other.Subject.RepositorySubjectID) + `' WHERE task_id = ?`,
		"task_id":    `UPDATE task_run SET task_id = '` + string(ids.NewTaskID()) + `' WHERE task_id = ?`,
		"created_at": `UPDATE task_run SET created_at = '2020-01-01T00:00:00.000000000Z' WHERE task_id = ?`,
	}
	for column, stmt := range mutations {
		t.Run(column, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, stmt, string(f.Task.TaskID))
			})
			if err == nil {
				t.Fatalf("task_run accepted a change to %s", column)
			}
			// Renaming task_id trips the current-attempt ownership trigger
			// first, which is created earlier; either rejection is correct,
			// and the row is verified unchanged below.
			if !strings.Contains(err.Error(), "immutable") &&
				!strings.Contains(err.Error(), "must belong to this task") {
				t.Fatalf("expected an identity trigger to fire, got: %v", err)
			}
		})
	}

	// The mutable columns still move.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskBlockedDesign)
	}); err != nil {
		t.Fatalf("status must remain mutable: %v", err)
	}

	// And the task is still bound to the packet it was approved against.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.PacketID != f.Packet.PacketID {
			t.Fatalf("task packet drifted to %s", string(run.PacketID))
		}
		if run.Intent != state.IntentModifying {
			t.Fatalf("task intent drifted to %q", string(run.Intent))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 2: a terminal task could be resurrected by DELETE + INSERT, and the
// docs claimed the no-delete rule covered every durable record.
func TestDurableRecordsAreUndeletable(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r7-undeletable", state.IntentModifying, state.LaneOperator)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
	}); err != nil {
		t.Fatalf("abandon: %v", err)
	}

	deletes := map[string]string{
		"task_run": `DELETE FROM task_run WHERE task_id = '` + string(f.Task.TaskID) + `'`,
		"repository_subject": `DELETE FROM repository_subject WHERE repository_subject_id = '` +
			string(f.Subject.RepositorySubjectID) + `'`,
		"scheduler_epoch":     `DELETE FROM scheduler_epoch WHERE epoch = 1`,
		"scheduler_ownership": `DELETE FROM scheduler_ownership WHERE id = 1`,
	}
	for table, stmt := range deletes {
		t.Run(table, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, stmt)
			})
			if err == nil {
				t.Fatalf("a row was deleted from %s", table)
			}
			if !strings.Contains(err.Error(), "cannot be deleted") {
				t.Fatalf("expected a no-delete trigger to fire, got: %v", err)
			}
		})
	}

	// The withdrawn task stays withdrawn.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status != state.TaskAbandoned {
			t.Fatalf("task status is %q, want ABANDONED", string(run.Status))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 3: SetTaskCurrentAttempt had no epoch fence, so a task could be
// pointed at an attempt from a retired generation and wedged.
func TestCurrentAttemptMustBelongToTheCurrentGeneration(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r7-wedge", state.IntentModifying, state.LaneOperator)

	// A successor generation takes over.
	if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "successor"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, f.Attempt.AttemptID)
	}); !errors.Is(err, store.ErrStaleEpoch) {
		t.Fatalf("want ErrStaleEpoch, got %v", err)
	}
}

// Finding 5: CreateWorkspace accepted a terminal attempt and skipped the
// epoch fence, permanently burning a unique root_path on a workspace that
// could never publish or complete anything.
func TestWorkspaceRequiresALiveCurrentAttempt(t *testing.T) {
	ctx := context.Background()

	t.Run("terminal attempt", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r7-dead-ws", state.IntentModifying, state.LaneOperator)

		successor := f.Attempt
		successor.AttemptID = ids.NewAttemptID()
		successor.FenceEpoch = f.Attempt.FenceEpoch + 1
		successor.Status = state.AttemptRunning
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed); err != nil {
				return err
			}
			if err := tx.CreateWorkerAttempt(ctx, successor); err != nil {
				return err
			}
			return tx.SetWorkerAttemptStatus(ctx, successor.AttemptID, state.AttemptFailed)
		}); err != nil {
			t.Fatalf("setup: %v", err)
		}

		ws := f.Workspace
		ws.WorkspaceID = ids.NewWorkspaceID()
		ws.AttemptID = successor.AttemptID
		ws.RootPath = "/var/lib/acp/workspaces/r7-dead"
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); !errors.Is(err, store.ErrWorkspaceConflict) {
			t.Fatalf("want ErrWorkspaceConflict, got %v", err)
		}

		// The directory was not burned.
		if err := db.Read(ctx, func(tx *store.Tx) error {
			n, err := tx.QueryIntForTest(ctx,
				`SELECT count(*) FROM workspace WHERE root_path = ?`, ws.RootPath)
			if err != nil {
				return err
			}
			if n != 0 {
				t.Fatalf("a terminal attempt burned %d root_path rows", n)
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("stale epoch", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r7-stale-ws", state.IntentModifying, state.LaneOperator)

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
			t.Fatalf("setup: %v", err)
		}
		if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "successor gen"); err != nil {
			t.Fatalf("activate: %v", err)
		}

		ws := f.Workspace
		ws.WorkspaceID = ids.NewWorkspaceID()
		ws.AttemptID = successor.AttemptID
		ws.RootPath = "/var/lib/acp/workspaces/r7-stale"
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); !errors.Is(err, store.ErrStaleEpoch) {
			t.Fatalf("want ErrStaleEpoch, got %v", err)
		}
	})
}

// Finding 6: the nesting guard covered Write and Read, but the handle-level
// query helpers went straight to the pool and hung on the same connection.
func TestHandleQueriesInsideATransactionFailFast(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	seed(t, db, "r7-nesting", state.IntentReadOnly, state.LaneReview)

	calls := map[string]func() error{
		"SchemaVersion":           func() error { _, err := db.SchemaVersion(ctx); return err },
		"VerifySchema":            func() error { return db.VerifySchema(ctx) },
		"AppliedMigrations":       func() error { _, err := db.AppliedMigrations(ctx); return err },
		"IntegrityCheck":          func() error { return db.IntegrityCheck(ctx) },
		"VerifyConnectionPragmas": func() error { return db.VerifyConnectionPragmas(ctx) },
		"MigrateEmbedded":         func() error { return db.MigrateEmbedded(ctx) },
		"CurrentEpoch":            func() error { _, err := db.CurrentEpoch(ctx); return err },
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				done <- db.Write(ctx, func(*store.Tx) error { return call() })
			}()
			select {
			case err := <-done:
				if !errors.Is(err, store.ErrNestedTransaction) {
					t.Fatalf("want ErrNestedTransaction, got %v", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("%s deadlocked inside a transaction", name)
			}
		})
	}

	// Outside a transaction they all work.
	for name, call := range calls {
		if err := call(); err != nil {
			t.Fatalf("%s outside a transaction: %v", name, err)
		}
	}
}
