package store_test

// Regressions for the findings of the sixth independent review, of head
// 3a8cef5. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

// Finding 4: a canonical path is not a unique directory when symlinks are in
// play, so two attempts could share one physical tree.
func TestWorkspacePathResolvesSymlinks(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r6-symlink", state.IntentReadOnly, state.LaneReview)

	dir := t.TempDir()
	real := filepath.Join(dir, "ws-real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "ws-link")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	// A successor attempt to own the workspace under test.
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
	ws.RootPath = link

	// The symlinked spelling is canonical as a string but names another
	// directory, so it must be refused with the resolved path named.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkspace(ctx, ws)
	})
	if !errors.Is(err, domain.ErrInvalidEntity) {
		t.Fatalf("want ErrInvalidEntity for a symlinked root_path, got %v", err)
	}
	if !strings.Contains(err.Error(), real) {
		t.Fatalf("the error should name the resolved path %q: %v", real, err)
	}

	// The resolved spelling is accepted.
	ws.RootPath = real
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkspace(ctx, ws)
	}); err != nil {
		t.Fatalf("the resolved path was refused: %v", err)
	}

	// A path that does not exist yet is still accepted: recording a workspace
	// before it is materialised is the normal case.
	pending := f.Workspace
	pending.WorkspaceID = ids.NewWorkspaceID()
	pending.AttemptID = successor.AttemptID
	pending.RootPath = filepath.Join(dir, "not-created-yet")
	if err := pending.Validate(); err != nil {
		t.Fatalf("an unmaterialised path was refused: %v", err)
	}
}
