package store_test

// Regressions for the findings of the fifteenth independent review, of head
// 2827ba9. Every finding was the same shape: a fix applied to one sibling and
// not the rest, so each test here sweeps all of them.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Findings 1 and 3: only publish_attempt got the monotonic clamp and the
// ordering CHECK, so a backward clock step stored rows claiming they changed
// before they existed — uncorrectably, since these tables reject DELETE.
func TestTimestampWritesAreMonotonicAcrossEveryWriter(t *testing.T) {
	ctx := context.Background()

	// The clock steps back two hours between creation and mutation.
	steppedBack := func(db *store.DB) *store.DB {
		return db.WithClock(func() time.Time { return fixedNow.Add(-2 * time.Hour) })
	}

	t.Run("task status", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r15-task", state.IntentModifying, state.LaneOperator)
		if err := steppedBack(db).Write(ctx, func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskBlockedDesign)
		}); err != nil {
			t.Fatalf("set status: %v", err)
		}
		assertTaskTimestamps(t, db, f.Task.TaskID)
	})

	t.Run("task current attempt", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r15-pointer", state.IntentModifying, state.LaneOperator)
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
		if err := steppedBack(db).Write(ctx, func(tx *store.Tx) error {
			return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, successor.AttemptID)
		}); err != nil {
			t.Fatalf("set current attempt: %v", err)
		}
		assertTaskTimestamps(t, db, f.Task.TaskID)
	})

	t.Run("completion", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r15-complete", state.IntentReadOnly, state.LaneReview)
		ev := reviewEvidenceFor(f, "r15-artifact")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); err != nil {
			t.Fatalf("record evidence: %v", err)
		}
		if err := steppedBack(db).Write(ctx, func(tx *store.Tx) error {
			return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
		}); err != nil {
			t.Fatalf("complete: %v", err)
		}
		assertTaskTimestamps(t, db, f.Task.TaskID)
	})

	t.Run("attempt status", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r15-attempt", state.IntentModifying, state.LaneOperator)
		if err := steppedBack(db).Write(ctx, func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptVerifying)
		}); err != nil {
			t.Fatalf("set status: %v", err)
		}
		if err := db.Read(ctx, func(tx *store.Tx) error {
			attempt, err := tx.WorkerAttempt(ctx, f.Attempt.AttemptID)
			if err != nil {
				return err
			}
			if attempt.UpdatedAt.Before(attempt.CreatedAt) {
				t.Fatalf("updated_at %v precedes created_at %v", attempt.UpdatedAt, attempt.CreatedAt)
			}
			if err := attempt.Validate(); err != nil {
				t.Fatalf("a stored attempt fails its own validator: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("workspace release", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r15-release", state.IntentModifying, state.LaneOperator)
		if err := steppedBack(db).Write(ctx, func(tx *store.Tx) error {
			return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
		}); err != nil {
			t.Fatalf("release: %v", err)
		}
		if err := db.Read(ctx, func(tx *store.Tx) error {
			ws, err := tx.Workspace(ctx, f.Workspace.WorkspaceID)
			if err != nil {
				return err
			}
			if ws.ReleasedAt == nil {
				t.Fatal("release was not recorded")
			}
			if ws.ReleasedAt.Before(ws.CreatedAt) {
				t.Fatalf("released_at %v precedes created_at %v", ws.ReleasedAt, ws.CreatedAt)
			}
			if err := ws.Validate(); err != nil {
				t.Fatalf("a stored workspace fails its own validator: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("publication status", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r15-publish", state.IntentModifying, state.LaneOperator)
		pub := publishFor(f, sha("r15-publish-commit"), "refs/heads/m0/r15")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, pub)
		}); err != nil {
			t.Fatalf("record: %v", err)
		}
		if err := steppedBack(db).Write(ctx, func(tx *store.Tx) error {
			return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied)
		}); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if err := db.Read(ctx, func(tx *store.Tx) error {
			stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
			if err != nil {
				return err
			}
			if stored.UpdatedAt.Before(stored.CreatedAt) {
				t.Fatalf("updated_at %v precedes created_at %v", stored.UpdatedAt, stored.CreatedAt)
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})
}

func assertTaskTimestamps(t *testing.T, db *store.DB, id ids.TaskID) {
	t.Helper()
	ctx := context.Background()
	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, id)
		if err != nil {
			return err
		}
		if run.UpdatedAt.Before(run.CreatedAt) {
			t.Fatalf("updated_at %v precedes created_at %v", run.UpdatedAt, run.CreatedAt)
		}
		if err := run.Validate(); err != nil {
			t.Fatalf("a stored task run fails its own validator: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// The schema must refuse the same thing, so no code path can store an
// out-of-order timestamp.
func TestSchemaRefusesOutOfOrderTimestamps(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r15-schema-order", state.IntentModifying, state.LaneOperator)

	writes := map[string]string{
		"task_run": `UPDATE task_run SET updated_at = '2020-01-01T00:00:00.000000000Z' WHERE task_id = '` +
			string(f.Task.TaskID) + `'`,
		"worker_attempt": `UPDATE worker_attempt SET updated_at = '2020-01-01T00:00:00.000000000Z' WHERE attempt_id = '` +
			string(f.Attempt.AttemptID) + `'`,
		"workspace": `UPDATE workspace SET released_at = '2020-01-01T00:00:00.000000000Z' WHERE workspace_id = '` +
			string(f.Workspace.WorkspaceID) + `'`,
	}
	for table, stmt := range writes {
		t.Run(table, func(t *testing.T) {
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, stmt)
			}); err == nil {
				t.Fatalf("%s accepted a timestamp before created_at", table)
			}
		})
	}
}

// Finding 2: SetTaskCurrentAttempt was the remaining setter without the
// same-value no-op guard, and unlike execution_packet its table has a real
// updated_at column.
func TestReassertingCurrentAttemptIsANoOp(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r15-reassert", state.IntentModifying, state.LaneOperator)

	var before time.Time
	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		before = run.UpdatedAt
		if run.CurrentAttemptID == nil || *run.CurrentAttemptID != f.Attempt.AttemptID {
			t.Fatalf("fixture does not point at its attempt: %v", run.CurrentAttemptID)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	advanced := db.WithClock(func() time.Time { return fixedNow.Add(3 * time.Hour) })
	for range 3 {
		if err := advanced.Write(ctx, func(tx *store.Tx) error {
			return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, f.Attempt.AttemptID)
		}); err != nil {
			t.Fatalf("re-assert: %v", err)
		}
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if !run.UpdatedAt.Equal(before) {
			t.Fatalf("updated_at moved to %v on a no-op; want %v", run.UpdatedAt, before)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 4: an unrecognised source status must fail closed in every
// transition table, not only two of the three.
func TestUnknownSourceStatusFailsClosedEverywhere(t *testing.T) {
	if state.WorkerAttemptStatus("BOGUS").CanTransitionTo(state.AttemptRunning) {
		t.Fatal("an unknown WorkerAttemptStatus permitted a transition")
	}
	if state.WorkerAttemptStatus("").CanTransitionTo(state.AttemptClaimed) {
		t.Fatal("an empty WorkerAttemptStatus permitted a transition")
	}
	if state.WorkerAttemptStatus("BOGUS").CanTransitionTo(state.AttemptFailed) {
		t.Fatal("an unknown WorkerAttemptStatus permitted a terminal transition")
	}
	if state.TaskRunStatus("BOGUS").CanTransitionTo(state.TaskRunning) {
		t.Fatal("an unknown TaskRunStatus permitted a transition")
	}
	if state.PublishStatus("BOGUS").CanTransitionTo(state.PublishApplied) {
		t.Fatal("an unknown PublishStatus permitted a transition")
	}
	if state.PacketStatus("BOGUS").CanTransitionTo(state.PacketStale) {
		t.Fatal("an unknown PacketStatus permitted a transition")
	}

	// Recognised transitions still work.
	if !state.AttemptClaimed.CanTransitionTo(state.AttemptRunning) {
		t.Fatal("CLAIMED -> RUNNING was refused")
	}
	if !state.AttemptRunning.CanTransitionTo(state.AttemptFailed) {
		t.Fatal("RUNNING -> FAILED was refused")
	}
	if state.AttemptFailed.CanTransitionTo(state.AttemptRunning) {
		t.Fatal("a terminal attempt was revived")
	}
	var zero state.WorkerAttemptStatus
	if !zero.CanTransitionTo(zero) {
		t.Fatal("an identity transition was refused")
	}
	if !errors.Is(state.WorkerAttemptStatus("BOGUS").Validate(), state.ErrInvalidState) {
		t.Fatal("an unknown status validated")
	}
}
