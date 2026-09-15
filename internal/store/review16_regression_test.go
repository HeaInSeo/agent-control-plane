package store_test

// Regressions for the findings of the sixteenth independent review, of head
// d021a17. Each test reproduces the reported defect and pins the fix.

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

// Finding 1: timestamps were defaulted after Validate, so the ordering check
// never saw them and the refusal arrived as a raw driver constraint error.
func TestTimestampDefaultingHappensBeforeValidation(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r16-defaulting", state.IntentModifying, state.LaneOperator)

	// A caller supplies created_at and leaves updated_at unset. The store's
	// clock being behind it is now refused outright (round 19, finding 3):
	// a future-dated creation time is uncorrectable, because monotonicNow
	// clamps every later update up to it. So the defaulting order is
	// exercised with a clock at or ahead of the supplied time, and the
	// behind-clock case asserts the rejection.
	behind := db.WithClock(func() time.Time { return fixedNow.Add(-2 * time.Hour) })
	ahead := db.WithClock(func() time.Time { return fixedNow.Add(2 * time.Hour) })

	t.Run("task run", func(t *testing.T) {
		taskID := ids.NewTaskID()
		packet := newPacket(taskID, f.Subject.RepositorySubjectID,
			state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ApprovePacket(ctx, packet)
		}); err != nil {
			t.Fatalf("approve: %v", err)
		}
		run := domain.TaskRun{
			TaskID:              taskID,
			RepositorySubjectID: f.Subject.RepositorySubjectID,
			PacketID:            packet.PacketID,
			Lane:                state.LaneOperator,
			Intent:              state.IntentModifying,
			Status:              state.TaskReady,
			CreatedAt:           fixedNow,
		}
		// Behind the supplied created_at: refused, not silently reconciled.
		if err := behind.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateTaskRun(ctx, run)
		}); !errors.Is(err, store.ErrFutureTimestamp) {
			t.Fatalf("want ErrFutureTimestamp, got %v", err)
		}
		// At or ahead of it: the store fills updated_at coherently.
		if err := ahead.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateTaskRun(ctx, run)
		}); err != nil {
			t.Fatalf("the store should fill updated_at coherently: %v", err)
		}
		if err := db.Read(ctx, func(tx *store.Tx) error {
			run, err := tx.TaskRun(ctx, taskID)
			if err != nil {
				return err
			}
			if run.UpdatedAt.Before(run.CreatedAt) {
				t.Fatalf("updated_at %v precedes created_at %v", run.UpdatedAt, run.CreatedAt)
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("worker attempt", func(t *testing.T) {
		successor := f.Attempt
		successor.AttemptID = ids.NewAttemptID()
		successor.FenceEpoch = f.Attempt.FenceEpoch + 1
		successor.Status = state.AttemptClaimed
		successor.CreatedAt = fixedNow
		successor.UpdatedAt = time.Time{}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed)
		}); err != nil {
			t.Fatalf("retire: %v", err)
		}
		if err := behind.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkerAttempt(ctx, successor)
		}); !errors.Is(err, store.ErrFutureTimestamp) {
			t.Fatalf("want ErrFutureTimestamp, got %v", err)
		}
		if err := ahead.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkerAttempt(ctx, successor)
		}); err != nil {
			t.Fatalf("the store should fill updated_at coherently: %v", err)
		}
		if err := db.Read(ctx, func(tx *store.Tx) error {
			attempt, err := tx.WorkerAttempt(ctx, successor.AttemptID)
			if err != nil {
				return err
			}
			if attempt.UpdatedAt.Before(attempt.CreatedAt) {
				t.Fatalf("updated_at %v precedes created_at %v", attempt.UpdatedAt, attempt.CreatedAt)
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	// An explicitly out-of-order pair is a typed refusal, not a driver error.
	t.Run("explicit out-of-order pair", func(t *testing.T) {
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
				Status:              state.TaskReady,
				CreatedAt:           fixedNow,
				UpdatedAt:           fixedNow.Add(-time.Hour),
			})
		})
		if !errors.Is(err, domain.ErrInvalidEntity) {
			t.Fatalf("want ErrInvalidEntity, got %v", err)
		}
		if strings.Contains(err.Error(), "constraint failed") {
			t.Fatalf("an ordering refusal surfaced as a driver error: %v", err)
		}
	})
}

// Finding 2: the fence-reuse branch could never match, because the monotonic
// trigger aborts before the uniqueness constraint is evaluated.
func TestFenceEpochReuseIsTyped(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r16-fence", state.IntentModifying, state.LaneOperator)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed)
	}); err != nil {
		t.Fatalf("retire: %v", err)
	}

	replay := f.Attempt
	replay.AttemptID = ids.NewAttemptID()
	replay.FenceEpoch = f.Attempt.FenceEpoch
	replay.Status = state.AttemptClaimed
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, replay)
	})
	if !errors.Is(err, store.ErrInvalidTaskRun) {
		t.Fatalf("want ErrInvalidTaskRun for a reused fencing token, got %v", err)
	}
}

// Finding 3: scheduler_epoch.released_at was the one optional timestamp with
// none of the guards its siblings carry, in a table that rejects DELETE.
func TestSchedulerEpochReleaseIsGuarded(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r16-epoch-release", state.IntentReadOnly, state.LaneReview)

	for name, stmt := range map[string]string{
		"zero released_at":          `UPDATE scheduler_epoch SET released_at = '0001-01-01T00:00:00.000000000Z' WHERE epoch = 1`,
		"release before activation": `UPDATE scheduler_epoch SET released_at = '2020-01-01T00:00:00.000000000Z' WHERE epoch = 1`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, stmt)
			}); err == nil {
				t.Fatalf("scheduler_epoch accepted %s", name)
			}
		})
	}

	// A coherent release is accepted, and the entity validates both ways.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE scheduler_epoch SET released_at = '2026-09-13T09:00:00.000000000Z' WHERE epoch = 1`)
	}); err != nil {
		t.Fatalf("a coherent release was refused: %v", err)
	}
	if err := db.Read(ctx, func(tx *store.Tx) error {
		record, err := tx.SchedulerEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		if record.ReleasedAt == nil {
			t.Fatal("release was not read back")
		}
		if err := record.Validate(); err != nil {
			t.Fatalf("a stored epoch fails its own validator: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	zero := time.Time{}
	bad := domain.SchedulerEpoch{
		Epoch:            1,
		OwnerID:          ids.NewSchedulerOwnerID(),
		ActivatedAt:      fixedNow,
		ActivationReason: "test",
		ReleasedAt:       &zero,
	}
	if err := bad.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
		t.Fatalf("want ErrInvalidEntity for a zero released_at, got %v", err)
	}
	early := fixedNow.Add(-time.Hour)
	bad.ReleasedAt = &early
	if err := bad.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
		t.Fatalf("want ErrInvalidEntity for a release before activation, got %v", err)
	}
}

// Finding 4: stop conditions were not checked for emptiness, on a packet that
// can never be corrected or replaced.
func TestPacketStopConditionsAreChecked(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r16-stop", state.IntentModifying, state.LaneOperator)

	p := newPacket(ids.NewTaskID(), f.Subject.RepositorySubjectID,
		state.IntentModifying, state.LaneOperator)
	p.StopConditions = []string{"architecture contradiction", ""}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ApprovePacket(ctx, p)
	}); !errors.Is(err, domain.ErrPacketInvalid) {
		t.Fatalf("want ErrPacketInvalid, got %v", err)
	}

	// No stop conditions at all remains valid; an empty one does not.
	p.StopConditions = nil
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ApprovePacket(ctx, p)
	}); err != nil {
		t.Fatalf("a packet with no stop conditions was refused: %v", err)
	}
}

// Finding 5: the forward-only rule on the current-attempt pointer was
// bypassable by a NULL round-trip.
func TestCurrentAttemptPointerCannotBeCleared(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r16-clear", state.IntentModifying, state.LaneOperator)

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE task_run SET current_attempt_id = NULL WHERE task_id = ?`,
			string(f.Task.TaskID))
	})
	if err == nil {
		t.Fatal("the current-attempt pointer was cleared")
	}
	if !strings.Contains(err.Error(), "cannot be cleared once set") {
		t.Fatalf("expected the no-clear trigger to fire, got: %v", err)
	}

	// So the laundering route is closed: a fenced-out attempt cannot become
	// the current attempt again by way of NULL.
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
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.ExecForTest(ctx,
			`UPDATE task_run SET current_attempt_id = NULL WHERE task_id = ?`,
			string(f.Task.TaskID)); err != nil {
			return err
		}
		return tx.ExecForTest(ctx,
			`UPDATE task_run SET current_attempt_id = ? WHERE task_id = ?`,
			string(f.Attempt.AttemptID), string(f.Task.TaskID))
	}); err == nil {
		t.Fatal("a NULL round-trip laundered the pointer backwards")
	}
}

// Finding 6: ErrDuplicateEventIdentity was production surface that production
// code never returned.
func TestDuplicateEventIdentityIsTyped(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r16-event-id", state.IntentReadOnly, state.LaneReview)

	e := eventFor(f.Epoch, "r16.duplicate")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.AppendEvent(ctx, e)
		return err
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.AppendEvent(ctx, e)
		return err
	}); !errors.Is(err, store.ErrDuplicateEventIdentity) {
		t.Fatalf("want ErrDuplicateEventIdentity, got %v", err)
	}
}

// Finding 7: an out-of-range Mode produced a writable handle with WAL
// silently off, voiding the durability story.
func TestOutOfRangeModeIsRejected(t *testing.T) {
	ctx := context.Background()

	for _, mode := range []store.Mode{store.Mode(2), store.Mode(-1), store.Mode(99)} {
		path := filepath.Join(t.TempDir(), "mode.db")
		if _, err := store.Open(ctx, store.Config{
			Path: path, Mode: mode, AllowCreate: true,
		}); err == nil {
			t.Fatalf("mode %s was accepted", mode)
		}
		if err := mode.Validate(); err == nil {
			t.Fatalf("mode %s validated", mode)
		}
	}

	// The two defined modes still work, and a read-write handle really is in
	// WAL — the guarantee the unvalidated mode silently voided.
	path := filepath.Join(t.TempDir(), "wal.db")
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
	if mode := journalModeOf(t, path); mode != "wal" {
		t.Fatalf("journal mode is %q, want wal", mode)
	}
	ro, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadOnly})
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}
}

// Noted by the review as correct but untested: a read-only handle really can
// read, despite the DSN carrying _txlock=immediate alongside mode=ro. The
// driver issues a plain BEGIN for a read-only transaction, so the two do not
// conflict — but that is a driver detail worth pinning rather than assuming.
func TestReadOnlyHandleCanRead(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "readonly.db")

	writer, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := writer.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	clocked := writer.WithClock(func() time.Time { return fixedNow })
	f := seed(t, clocked, "r16-readonly", state.IntentReadOnly, state.LaneReview)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadOnly})
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}
	defer ro.Close()

	if err := ro.VerifySchema(ctx); err != nil {
		t.Fatalf("verify schema: %v", err)
	}
	if err := ro.QuickIntegrityCheck(ctx); err != nil {
		t.Fatalf("quick check: %v", err)
	}
	if err := ro.IntegrityCheck(ctx); err != nil {
		t.Fatalf("full check: %v", err)
	}
	epoch, err := ro.CurrentEpoch(ctx)
	if err != nil {
		t.Fatalf("current epoch: %v", err)
	}
	if epoch != f.Epoch {
		t.Fatalf("read epoch %d, want %d", int64(epoch), int64(f.Epoch))
	}

	if err := ro.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.PacketID != f.Packet.PacketID {
			t.Fatalf("task packet is %s", string(run.PacketID))
		}
		if _, err := tx.WorkerAttempt(ctx, f.Attempt.AttemptID); err != nil {
			return err
		}
		if _, err := tx.WorkspaceForAttempt(ctx, f.Attempt.AttemptID); err != nil {
			return err
		}
		if _, err := tx.Packet(ctx, f.Packet.PacketID); err != nil {
			return err
		}
		if _, err := tx.RepositorySubjectByNodeID(ctx, f.Subject.GitHubNodeID); err != nil {
			return err
		}
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			t.Fatal("read-only handle saw no history")
		}
		return nil
	}); err != nil {
		t.Fatalf("read-only read: %v", err)
	}

	// And it still refuses to write, or to advance the epoch.
	if err := ro.Write(ctx, func(*store.Tx) error { return nil }); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("read-only Write: want ErrReadOnly, got %v", err)
	}
	if _, err := ro.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "nope"); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("read-only ActivateScheduler: want ErrReadOnly, got %v", err)
	}
	if got := ro.OwnedEpoch(); got != 0 {
		t.Fatalf("a read-only handle claims ownership of epoch %d", int64(got))
	}
}
