package store_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

func TestOpenMissingDatabaseFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.db")
	_, err := store.Open(context.Background(), store.Config{Path: path, Mode: store.ModeReadWrite})
	if !errors.Is(err, store.ErrDatabaseNotFound) {
		t.Fatalf("want ErrDatabaseNotFound, got %v", err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatal("a refused open must not create the database file")
	}
}

func TestOpenReadOnlyCannotCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.db")
	_, err := store.Open(context.Background(), store.Config{
		Path: path, Mode: store.ModeReadOnly, AllowCreate: true,
	})
	if !errors.Is(err, store.ErrDatabaseNotFound) {
		t.Fatalf("want ErrDatabaseNotFound for read-only create, got %v", err)
	}
}

func TestOpenNonSQLiteFileFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(path, []byte("this is not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := store.Open(context.Background(), store.Config{
		Path: path, Mode: store.ModeReadWrite, AllowCreate: true,
	})
	if !errors.Is(err, store.ErrNotSQLiteDatabase) {
		t.Fatalf("want ErrNotSQLiteDatabase, got %v", err)
	}
	// The file must survive untouched: no silent replacement.
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "this is not a database" {
		t.Fatalf("refused open modified the file: %q", string(body))
	}
}

func TestOpenForeignSQLiteDatabaseFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "someone-elses.db")

	// Build a real SQLite database that is not a control plane database.
	other, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := other.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx, `CREATE TABLE unrelated_app (id INTEGER PRIMARY KEY)`)
	}); err != nil {
		t.Fatalf("seed foreign database: %v", err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if !errors.Is(err, store.ErrForeignDatabase) {
		t.Fatalf("want ErrForeignDatabase, got %v", err)
	}
}

func TestOpenCorruptDatabaseFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "corrupt.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "seed"); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the interior of the file, keeping the SQLite header intact so
	// the failure has to be caught by the integrity check rather than by the
	// header sniff.
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	garbage := make([]byte, info.Size()/2)
	for i := range garbage {
		garbage[i] = 0xBA
	}
	if _, err := f.WriteAt(garbage, 4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err == nil {
		t.Fatal("opening a corrupt database must fail closed, not succeed")
	}
	t.Logf("corrupt open rejected with: %v", err)
}

func TestOpenDoesNotAdvanceSchedulerEpoch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control-plane.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	activated, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "initial activation")
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if activated.Epoch != 1 {
		t.Fatalf("first activation should be epoch 1, got %d", int64(activated.Epoch))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// CC4: opening, integrity checking, schema verification and read-only
	// inspection must all leave the epoch alone. Only an activation bumps it.
	for i := range 3 {
		reopened, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
		if err != nil {
			t.Fatalf("reopen %d: %v", i, err)
		}
		if err := reopened.IntegrityCheck(ctx); err != nil {
			t.Fatalf("integrity check %d: %v", i, err)
		}
		if err := reopened.VerifySchema(ctx); err != nil {
			t.Fatalf("verify schema %d: %v", i, err)
		}
		if err := reopened.MigrateEmbedded(ctx); err != nil {
			t.Fatalf("replay migrate %d: %v", i, err)
		}
		epoch, err := reopened.CurrentEpoch(ctx)
		if err != nil {
			t.Fatalf("current epoch %d: %v", i, err)
		}
		if epoch != 1 {
			t.Fatalf("epoch advanced to %d by opening the database alone", int64(epoch))
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}

		ro, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadOnly})
		if err != nil {
			t.Fatalf("read-only open %d: %v", i, err)
		}
		if err := ro.IntegrityCheck(ctx); err != nil {
			t.Fatalf("read-only integrity check %d: %v", i, err)
		}
		epoch, err = ro.CurrentEpoch(ctx)
		if err != nil {
			t.Fatalf("read-only current epoch %d: %v", i, err)
		}
		if epoch != 1 {
			t.Fatalf("epoch advanced to %d by a read-only inspection", int64(epoch))
		}
		if err := ro.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadOnlyHandleRefusesWrites(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control-plane.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "seed"); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadOnly})
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}
	defer ro.Close()

	if err := ro.Write(ctx, func(*store.Tx) error { return nil }); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("read-only Write: want ErrReadOnly, got %v", err)
	}
	if _, err := ro.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "hostile takeover"); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("read-only ActivateScheduler: want ErrReadOnly, got %v", err)
	}
	if err := ro.MigrateEmbedded(ctx); !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("read-only Migrate: want ErrReadOnly, got %v", err)
	}
}
