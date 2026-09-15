package store_test

// Regressions for the findings of the fourteenth independent review, of head
// 55b8079. Each test reproduces the reported defect and pins the fix.

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

// Finding 1: a clock step back between recording an intent and moving its
// status wrote a durable row that the domain validator then refuses — so the
// crash-recovery path reported a binding failure for a sound publication.
func TestPublishUpdatedAtNeverPrecedesCreatedAt(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r14-clock", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("r14-clock-commit"), "refs/heads/m0/r14-clock")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	// The clock steps back an hour, as an NTP correction can.
	stepped := db.WithClock(func() time.Time { return fixedNow.Add(-time.Hour) })
	if err := stepped.Write(ctx, func(tx *store.Tx) error {
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied)
	}); err != nil {
		t.Fatalf("apply under a stepped-back clock: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if stored.UpdatedAt.Before(stored.CreatedAt) {
			t.Fatalf("updated_at %v precedes created_at %v", stored.UpdatedAt, stored.CreatedAt)
		}
		// The row must still load and validate, which is what the recovery
		// path depends on.
		if err := stored.Validate(); err != nil {
			t.Fatalf("a stored publication fails its own validator: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// The schema refuses it too, so no code path can poison the row.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE publish_attempt SET updated_at = '2020-01-01T00:00:00.000000000Z'
			   WHERE publish_attempt_id = ?`, string(pub.PublishAttemptID))
	}); err == nil {
		t.Fatal("updated_at was written behind created_at")
	}
}

// Finding 2: CreateWorkspace silently dropped a supplied ReleasedAt, so the
// caller believed it had stored a released workspace while every downstream
// guard saw a live one.
func TestWorkspaceCannotBeCreatedReleased(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	spare := seedAttemptWithoutWorkspace(t, db, "r14-released")

	released := fixedNow.Add(time.Minute)
	ws := domain.Workspace{
		WorkspaceID:         ids.NewWorkspaceID(),
		AttemptID:           spare.Attempt.AttemptID,
		TaskID:              spare.Task.TaskID,
		RepositorySubjectID: spare.Subject.RepositorySubjectID,
		BaseSHA:             sha("r14-released-base"),
		IsolationKind:       domain.IsolationReadOnlyCheckout,
		RootPath:            "/var/lib/acp/workspaces/r14-released",
		CreatedAt:           fixedNow,
		ReleasedAt:          &released,
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkspace(ctx, ws)
	}); !errors.Is(err, store.ErrWorkspaceConflict) {
		t.Fatalf("want ErrWorkspaceConflict, got %v", err)
	}

	// A present-but-zero release time is refused by the validator, as it is
	// for the attempt's optional timestamps.
	zero := time.Time{}
	ws.ReleasedAt = &zero
	if err := ws.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
		t.Fatalf("want ErrInvalidEntity for a zero released_at, got %v", err)
	}
	// So is a release before creation.
	early := fixedNow.Add(-time.Hour)
	ws.ReleasedAt = &early
	if err := ws.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
		t.Fatalf("want ErrInvalidEntity for released_at before created_at, got %v", err)
	}

	// Absent is the only accepted form at creation.
	ws.ReleasedAt = nil
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkspace(ctx, ws)
	}); err != nil {
		t.Fatalf("a live workspace was refused: %v", err)
	}
}

// Finding 3: a mis-wired workspace surfaced as a raw driver error, the shape
// round 10 declared unacceptable and fixed for publications only.
func TestWorkspaceCoherenceRefusalsAreTyped(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	spare := seedAttemptWithoutWorkspace(t, db, "r14-coherence")
	other := seedAttemptWithoutWorkspace(t, db, "r14-coherence-other")

	base := domain.Workspace{
		WorkspaceID:         ids.NewWorkspaceID(),
		AttemptID:           spare.Attempt.AttemptID,
		TaskID:              spare.Task.TaskID,
		RepositorySubjectID: spare.Subject.RepositorySubjectID,
		BaseSHA:             sha("r14-coherence-base"),
		IsolationKind:       domain.IsolationReadOnlyCheckout,
		RootPath:            "/var/lib/acp/workspaces/r14-coherence",
		CreatedAt:           fixedNow,
	}

	for name, mutate := range map[string]func(*domain.Workspace){
		"foreign task": func(w *domain.Workspace) {
			w.TaskID = other.Task.TaskID
		},
		"foreign repository subject": func(w *domain.Workspace) {
			w.RepositorySubjectID = other.Subject.RepositorySubjectID
		},
		"isolation that contradicts the intent": func(w *domain.Workspace) {
			w.IsolationKind = domain.IsolationIsolatedClone
		},
	} {
		t.Run(name, func(t *testing.T) {
			ws := base
			ws.WorkspaceID = ids.NewWorkspaceID()
			ws.RootPath = base.RootPath + "-" + strings.ReplaceAll(name, " ", "-")
			mutate(&ws)
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.CreateWorkspace(ctx, ws)
			})
			if !errors.Is(err, store.ErrWorkspaceConflict) {
				t.Fatalf("want ErrWorkspaceConflict, got %v", err)
			}
			if strings.Contains(err.Error(), "constraint failed") {
				t.Fatalf("a coherence refusal surfaced as a driver error: %v", err)
			}
		})
	}
}

// Finding 4: SetPacketStatus was the last setter without the same-status
// no-op guard.
func TestReassertingPacketStatusIsANoOp(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r14-packet-noop", state.IntentModifying, state.LaneOperator)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetPacketStatus(ctx, f.Packet.PacketID, state.PacketSuperseded)
	}); err != nil {
		t.Fatalf("supersede: %v", err)
	}
	for range 3 {
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetPacketStatus(ctx, f.Packet.PacketID, state.PacketSuperseded)
		}); err != nil {
			t.Fatalf("re-assert: %v", err)
		}
	}
	if err := db.Read(ctx, func(tx *store.Tx) error {
		p, err := tx.Packet(ctx, f.Packet.PacketID)
		if err != nil {
			return err
		}
		if p.Status != state.PacketSuperseded {
			t.Fatalf("status drifted to %q", string(p.Status))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 5: Open ran the full integrity check by default, whose cost grows
// with a history that can never be pruned.
func TestOpenUsesTheQuickIntegrityCheck(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "integrity.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Both checks remain available and pass on a sound database.
	if err := db.QuickIntegrityCheck(ctx); err != nil {
		t.Fatalf("quick check: %v", err)
	}
	if err := db.IntegrityCheck(ctx); err != nil {
		t.Fatalf("full check: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Opting into the full check on open still works.
	full, err := store.Open(ctx, store.Config{
		Path: path, Mode: store.ModeReadWrite, FullIntegrityCheckOnOpen: true,
	})
	if err != nil {
		t.Fatalf("open with the full check: %v", err)
	}
	if err := full.Close(); err != nil {
		t.Fatal(err)
	}

	// And a corrupt database is still refused by the default path.
	corruptPath := filepath.Join(t.TempDir(), "corrupt.db")
	seedDB, err := store.Open(ctx, store.Config{Path: corruptPath, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := seedDB.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := seedDB.Close(); err != nil {
		t.Fatal(err)
	}
	corruptInterior(t, corruptPath)
	if _, err := store.Open(ctx, store.Config{Path: corruptPath, Mode: store.ModeReadWrite}); err == nil {
		t.Fatal("a corrupt database opened under the quick check")
	}
}

// Finding 6: EventsInEpoch read a whole generation's history into memory with
// no limit or cursor, on a table that can never be pruned.
func TestHistoryReadsArePaginated(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r14-paging", state.IntentReadOnly, state.LaneReview)

	const appended = 25
	if err := db.Write(ctx, func(tx *store.Tx) error {
		for range appended {
			if _, err := tx.AppendEvent(ctx, eventFor(f.Epoch, "r14.page")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		// Paging on (epoch, seq) walks the whole epoch exactly once.
		var (
			seen  []int64
			after int64
		)
		for {
			page, err := tx.EventsInEpochPage(ctx, f.Epoch, after, 7)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			for _, e := range page {
				seen = append(seen, e.Seq)
			}
			after = page[len(page)-1].Seq
		}

		whole, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		if len(seen) != len(whole) {
			t.Fatalf("paging saw %d events, whole-epoch read saw %d", len(seen), len(whole))
		}
		for i, seq := range seen {
			if seq != whole[i].Seq {
				t.Fatalf("page order diverges at %d: %d vs %d", i, seq, whole[i].Seq)
			}
			if i > 0 && seq <= seen[i-1] {
				t.Fatalf("paging returned a non-increasing sequence at %d", i)
			}
		}

		if _, err := tx.EventsInEpochPage(ctx, f.Epoch, 0, 0); err == nil {
			t.Fatal("a non-positive limit was accepted")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}
