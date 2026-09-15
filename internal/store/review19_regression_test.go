package store_test

// Regressions for the findings of the nineteenth independent review, of head
// c2503ef.

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1: CreateWorkspace was the only execution-setup writer without a
// packet-authority guard, so a workspace could be minted under a packet that
// had gone stale — permanently burning a unique directory for work that must
// stop.
func TestWorkspaceRequiresLivePacketAuthority(t *testing.T) {
	ctx := context.Background()

	for _, status := range []state.PacketStatus{state.PacketStale, state.PacketSuperseded} {
		t.Run(string(status), func(t *testing.T) {
			db := newDB(t)
			spare := seedAttemptWithoutWorkspace(t, db, "r19-ws-authority")
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetPacketStatus(ctx, spare.Packet.PacketID, status)
			}); err != nil {
				t.Fatalf("set packet status: %v", err)
			}

			ws := domain.Workspace{
				WorkspaceID:         ids.NewWorkspaceID(),
				AttemptID:           spare.Attempt.AttemptID,
				TaskID:              spare.Task.TaskID,
				RepositorySubjectID: spare.Subject.RepositorySubjectID,
				BaseSHA:             sha("r19-ws-base"),
				IsolationKind:       domain.IsolationReadOnlyCheckout,
				RootPath:            "/var/lib/acp/workspaces/r19-" + string(status),
				CreatedAt:           fixedNow,
			}
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.CreateWorkspace(ctx, ws)
			}); !errors.Is(err, domain.ErrPacketNoAuthority) {
				t.Fatalf("want ErrPacketNoAuthority, got %v", err)
			}

			// The directory was not burned.
			if err := db.Read(ctx, func(tx *store.Tx) error {
				n, err := tx.QueryIntForTest(ctx,
					`SELECT count(*) FROM workspace WHERE root_path = ?`, ws.RootPath)
				if err != nil {
					return err
				}
				if n != 0 {
					t.Fatalf("a %s packet burned %d directories", string(status), n)
				}
				return nil
			}); err != nil {
				t.Fatalf("read: %v", err)
			}
		})
	}

	t.Run("expired packet", func(t *testing.T) {
		db := newDB(t)
		spare := seedAttemptWithoutWorkspace(t, db, "r19-ws-expiry")
		// Only the clock moves: activating a new generation would make the
		// attempt's epoch stale, and that check fires first.
		late := db.WithClock(func() time.Time { return spare.Packet.ExpiresAt.Add(time.Minute) })
		ws := domain.Workspace{
			WorkspaceID:         ids.NewWorkspaceID(),
			AttemptID:           spare.Attempt.AttemptID,
			TaskID:              spare.Task.TaskID,
			RepositorySubjectID: spare.Subject.RepositorySubjectID,
			BaseSHA:             sha("r19-ws-expiry-base"),
			IsolationKind:       domain.IsolationReadOnlyCheckout,
			RootPath:            "/var/lib/acp/workspaces/r19-expired",
			CreatedAt:           spare.Packet.ExpiresAt.Add(time.Minute),
		}
		if err := late.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); !errors.Is(err, domain.ErrPacketExpired) {
			t.Fatalf("want ErrPacketExpired, got %v", err)
		}
	})
}

// Finding 3: a future-dated CreatedAt permanently froze updated_at, the one
// field a resuming publisher relies on — monotonicNow clamps every later
// update up to it, and these rows are immutable or undeletable.
func TestCreationTimeCannotBeInTheFuture(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r19-future", state.IntentModifying, state.LaneOperator)
	spare := seedAttemptWithoutWorkspace(t, db, "r19-future-spare")

	future := fixedNow.Add(time.Hour)

	t.Run("publication", func(t *testing.T) {
		pub := publishFor(f, sha("r19-future-commit"), "refs/heads/m0/r19-future")
		pub.CreatedAt = future
		pub.UpdatedAt = future
		pub.IdempotencyKey = ""
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, pub)
		}); !errors.Is(err, store.ErrFutureTimestamp) {
			t.Fatalf("want ErrFutureTimestamp, got %v", err)
		}
	})

	t.Run("workspace", func(t *testing.T) {
		ws := domain.Workspace{
			WorkspaceID:         ids.NewWorkspaceID(),
			AttemptID:           spare.Attempt.AttemptID,
			TaskID:              spare.Task.TaskID,
			RepositorySubjectID: spare.Subject.RepositorySubjectID,
			BaseSHA:             sha("r19-future-base"),
			IsolationKind:       domain.IsolationReadOnlyCheckout,
			RootPath:            "/var/lib/acp/workspaces/r19-future",
			CreatedAt:           future,
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); !errors.Is(err, store.ErrFutureTimestamp) {
			t.Fatalf("want ErrFutureTimestamp, got %v", err)
		}
	})

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
				CreatedAt:           future,
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
		successor.CreatedAt = future
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

	// The current instant is accepted: the bound is "not after now", not
	// "strictly before".
	t.Run("now is accepted", func(t *testing.T) {
		pub := publishFor(f, sha("r19-now-commit"), "refs/heads/m0/r19-now")
		pub.CreatedAt = fixedNow
		pub.UpdatedAt = fixedNow
		pub.IdempotencyKey = ""
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, pub)
		}); err != nil {
			t.Fatalf("the current instant was refused: %v", err)
		}
	})
}

// Finding 4: the sentinel's documentation named a collision the code never
// mapped to it, so a caller following the comment would never match.
func TestBothEventIdentityCollisionsAreTyped(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r19-event-identity", state.IntentReadOnly, state.LaneReview)

	t.Run("duplicate event_id", func(t *testing.T) {
		e := eventFor(f.Epoch, "r19.dup-id")
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
	})

	t.Run("duplicate (epoch, seq)", func(t *testing.T) {
		var seq int64
		if err := db.Write(ctx, func(tx *store.Tx) error {
			var err error
			seq, err = tx.AppendEvent(ctx, eventFor(f.Epoch, "r19.dup-seq"))
			return err
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.AppendEventAt(ctx, eventFor(f.Epoch, "r19.collider"), seq)
		}); !errors.Is(err, store.ErrDuplicateEventIdentity) {
			t.Fatalf("want ErrDuplicateEventIdentity, got %v", err)
		}
	})
}

// Finding 5: withDefaults ran before validate, so a negative BusyTimeout was
// silently replaced by the default instead of rejected — the asymmetric case
// being that a sub-millisecond value was rejected while a negative one was not.
func TestNegativeBusyTimeoutIsRejected(t *testing.T) {
	ctx := context.Background()

	for _, timeout := range []time.Duration{-time.Second, -time.Nanosecond, -time.Hour} {
		path := filepath.Join(t.TempDir(), "timeout.db")
		if _, err := store.Open(ctx, store.Config{
			Path:        path,
			Mode:        store.ModeReadWrite,
			AllowCreate: true,
			BusyTimeout: timeout,
		}); err == nil {
			t.Fatalf("BusyTimeout %s was accepted", timeout)
		}
	}

	// Unset still means "use the default", and a real value still works.
	for _, timeout := range []time.Duration{0, time.Millisecond, 5 * time.Second} {
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
