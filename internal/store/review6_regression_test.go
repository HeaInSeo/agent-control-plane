package store_test

// Regressions for the findings of the sixth independent review, of head
// 3a8cef5. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 2: the nested-transaction guard was per-handle, so a second
// goroutine doing independent work was rejected as if it were nesting.
//
// This is a regression on the previous round's own fix: the guard has to tell
// concurrency and nesting apart, since one should wait and the other must not.
func TestConcurrentTransactionsSerialiseInsteadOfFailing(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r6-concurrent", state.IntentReadOnly, state.LaneReview)

	const writers = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := db.Write(ctx, func(tx *store.Tx) error {
				_, err := tx.AppendEvent(ctx, eventFor(f.Epoch, "concurrent.write"))
				return err
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
		}(i)
	}

	// Readers must not be rejected either.
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := db.Read(ctx, func(tx *store.Tx) error {
				_, err := tx.TaskRun(ctx, f.Task.TaskID)
				return err
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent transactions deadlocked")
	}

	for _, err := range errs {
		if errors.Is(err, store.ErrNestedTransaction) {
			t.Fatalf("a concurrent transaction was rejected as nested: %v", err)
		}
		t.Fatalf("concurrent transaction failed: %v", err)
	}

	// Every write landed, with no gap in the sequence.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		appended := 0
		for i, e := range events {
			if e.Seq != int64(i+1) {
				t.Fatalf("history has a hole at index %d: seq %d", i, e.Seq)
			}
			if e.EventType == "concurrent.write" {
				appended++
			}
		}
		if appended != writers {
			t.Fatalf("%d of %d concurrent writes landed", appended, writers)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// True nesting must still fail fast rather than deadlock.
	nested := make(chan error, 1)
	go func() {
		nested <- db.Write(ctx, func(*store.Tx) error {
			return db.Write(ctx, func(*store.Tx) error { return nil })
		})
	}()
	select {
	case err := <-nested:
		if !errors.Is(err, store.ErrNestedTransaction) {
			t.Fatalf("want ErrNestedTransaction, got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a nested transaction deadlocked instead of failing fast")
	}
}

// Finding 3: RecordPublishAttempt skipped the liveness fence its siblings
// apply, so a fenced-out attempt could mint an undeletable pending intent.
func TestPublicationRequiresALiveAttemptAndWorkspace(t *testing.T) {
	ctx := context.Background()

	t.Run("terminal attempt", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r6-dead-attempt", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptAbandoned)
		}); err != nil {
			t.Fatalf("abandon: %v", err)
		}
		pub := publishFor(f, sha("r6-dead-commit"), "refs/heads/m0/r6-dead")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, pub)
		}); !errors.Is(err, domain.ErrPublishBindingInvalid) {
			t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
		}

		// Nothing was recorded, so the pending queue stays clean.
		if err := db.Read(ctx, func(tx *store.Tx) error {
			rows, err := tx.PublishAttemptsForAttempt(ctx, f.Attempt.AttemptID)
			if err != nil {
				return err
			}
			if len(rows) != 0 {
				t.Fatalf("a terminal attempt recorded %d publications", len(rows))
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("released workspace", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r6-released-ws", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
		}); err != nil {
			t.Fatalf("release: %v", err)
		}
		pub := publishFor(f, sha("r6-released-commit"), "refs/heads/m0/r6-released")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, pub)
		}); !errors.Is(err, domain.ErrPublishBindingInvalid) {
			t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
		}
	})

	t.Run("live attempt still publishes", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r6-live", state.IntentModifying, state.LaneOperator)
		pub := publishFor(f, sha("r6-live-commit"), "refs/heads/m0/r6-live")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, pub)
		}); err != nil {
			t.Fatalf("a live attempt was refused: %v", err)
		}
	})
}

// Finding 4 (round 6), revisited in round 7: the canonical-path check is
// lexical, and says so.
//
// Resolving symlinks here was tried and reverted. A pure validator that does
// filesystem I/O answered differently for the same path depending on whether
// the directory existed yet — accepting a workspace recorded before creation
// and rejecting the identical one recorded after, which is backwards for an
// allocator that materialises the clone first — and it put a stat of a
// possibly-hung mount inside the store's write transaction. This test pins
// the guarantee that is actually made, and marks the part that is not.
func TestWorkspacePathGuaranteeIsLexical(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r6-path", state.IntentReadOnly, state.LaneReview)

	// Lexical aliases are rejected outright.
	for _, alias := range []string{
		"/var/lib/acp/ws/.", "/var/lib/acp/ws/", "/var/lib/acp/./ws",
		"/var/lib/acp/x/../ws", "relative/ws", "",
	} {
		ws := f.Workspace
		ws.WorkspaceID = ids.NewWorkspaceID()
		ws.RootPath = alias
		if err := ws.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
			t.Fatalf("alias %q accepted: %v", alias, err)
		}
	}

	// Validation is pure: the same path validates identically whether or not
	// the directory exists, and whether or not a parent is a symlink.
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	underLink := filepath.Join(link, "ws-1")
	ws := f.Workspace
	ws.WorkspaceID = ids.NewWorkspaceID()
	ws.RootPath = underLink
	if err := ws.Validate(); err != nil {
		t.Fatalf("a canonical path under a symlinked parent was refused before creation: %v", err)
	}
	if err := os.Mkdir(underLink, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ws.Validate(); err != nil {
		t.Fatalf("the same path was refused after creation: %v", err)
	}

	// The documented gap: two canonical strings can still name one physical
	// tree, and the store does not detect it. The workspace allocator that
	// creates the directory has to. See docs/threat-boundary.md.
	viaLink := filepath.Join(link, "shared")
	viaReal := filepath.Join(real, "shared")
	if viaLink == viaReal {
		t.Fatal("test is vacuous: the two spellings are identical")
	}
	for _, path := range []string{viaLink, viaReal} {
		probe := f.Workspace
		probe.WorkspaceID = ids.NewWorkspaceID()
		probe.RootPath = path
		if err := probe.Validate(); err != nil {
			t.Fatalf("expected the lexical check to accept %q (the symlink gap is documented, "+
				"not enforced here): %v", path, err)
		}
	}

	// What the store does enforce: one attempt per stored path.
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
	owned := f.Workspace
	owned.WorkspaceID = ids.NewWorkspaceID()
	owned.AttemptID = successor.AttemptID
	owned.RootPath = viaReal
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkspace(ctx, owned)
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		clash := owned
		clash.WorkspaceID = ids.NewWorkspaceID()
		return tx.CreateWorkspace(ctx, clash)
	}); !errors.Is(err, store.ErrWorkspaceConflict) {
		t.Fatalf("want ErrWorkspaceConflict for a duplicate path, got %v", err)
	}
}
