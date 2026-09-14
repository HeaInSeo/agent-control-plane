package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

func TestFreshMigration(t *testing.T) {
	ctx := context.Background()
	db := openFresh(t)

	version, err := db.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("schema version: %v", err)
	}
	if version != 0 {
		t.Fatalf("fresh database should be at version 0, got %d", version)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("fresh migration: %v", err)
	}

	set, err := store.Migrations()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}
	version, err = db.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("schema version: %v", err)
	}
	if want := set[len(set)-1].Version; version != want {
		t.Fatalf("schema version %d, want %d", version, want)
	}
	if err := db.VerifySchema(ctx); err != nil {
		t.Fatalf("verify schema: %v", err)
	}
}

func TestMigrationReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	before, err := db.AppliedMigrations(ctx)
	if err != nil {
		t.Fatalf("applied: %v", err)
	}
	for range 3 {
		if err := db.MigrateEmbedded(ctx); err != nil {
			t.Fatalf("replay: %v", err)
		}
	}
	after, err := db.AppliedMigrations(ctx)
	if err != nil {
		t.Fatalf("applied: %v", err)
	}
	if len(before) != len(after) {
		t.Fatalf("replay changed history length: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("replay rewrote migration %d: %+v -> %+v", i, before[i], after[i])
		}
	}
}

func TestUnsupportedSchemaVersionFailsClosed(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	// Simulate a database migrated by a newer build of the control plane.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO schema_migration (version, name, checksum, applied_at)
			 VALUES (9999, 'from_the_future', 'deadbeef', '2027-01-01T00:00:00.000000000Z')`)
	}); err != nil {
		t.Fatalf("seed future migration: %v", err)
	}

	err := db.MigrateEmbedded(ctx)
	if !errors.Is(err, store.ErrSchemaVersionUnsupported) {
		t.Fatalf("want ErrSchemaVersionUnsupported, got %v", err)
	}
	if err := db.VerifySchema(ctx); !errors.Is(err, store.ErrSchemaVersionUnsupported) {
		t.Fatalf("VerifySchema: want ErrSchemaVersionUnsupported, got %v", err)
	}
}

func TestMigrationChecksumMismatchFailsClosed(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE schema_migration SET checksum = 'tampered' WHERE version = 1`)
	}); err != nil {
		t.Fatalf("tamper checksum: %v", err)
	}

	if err := db.MigrateEmbedded(ctx); !errors.Is(err, store.ErrMigrationChecksumMismatch) {
		t.Fatalf("want ErrMigrationChecksumMismatch, got %v", err)
	}
}

func TestUnknownAppliedMigrationFailsClosed(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	// A recorded name that this build does not know about means the applied
	// schema and this source tree have diverged.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE schema_migration SET name = 'not_our_migration' WHERE version = 1`)
	}); err != nil {
		t.Fatalf("tamper name: %v", err)
	}

	if err := db.MigrateEmbedded(ctx); !errors.Is(err, store.ErrMigrationHistoryDivergent) {
		t.Fatalf("want ErrMigrationHistoryDivergent, got %v", err)
	}
}

// TestFutureAdditiveMigrationOnPopulatedDatabase proves an M0 database that
// already holds identity rows can take a later additive migration without
// those rows being rewritten or re-keyed.
//
// The extra migration is supplied by the test rather than shipped in the
// embedded set: M0 must not ship a speculative future schema step.
func TestFutureAdditiveMigrationOnPopulatedDatabase(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	f := seed(t, db, "populated", state.IntentModifying, state.LaneOperator)
	pub := publishFor(f, sha("commit-1"), "refs/heads/m0/work")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record publish: %v", err)
	}

	set, err := store.Migrations()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}
	future := append(set, store.Migration{
		Version: len(set) + 1,
		Name:    "additive_probe",
		// Representative of the deferred entities: a new table plus a nullable
		// column on an existing one. Neither touches identity rows.
		SQL: `
CREATE TABLE usage_observation (
    usage_observation_id TEXT PRIMARY KEY,
    task_id              TEXT NOT NULL REFERENCES task_run (task_id),
    observed_at          TEXT NOT NULL
) WITHOUT ROWID;
ALTER TABLE task_run ADD COLUMN projection_synced_at TEXT;`,
	})

	if err := db.MigrateForTest(ctx, future); err != nil {
		t.Fatalf("additive migration on populated database: %v", err)
	}

	// Every identity relation must survive unchanged.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.PacketID != f.Packet.PacketID || run.RepositorySubjectID != f.Subject.RepositorySubjectID {
			t.Fatalf("task identity changed: %+v", run)
		}
		attempt, err := tx.WorkerAttempt(ctx, f.Attempt.AttemptID)
		if err != nil {
			return err
		}
		if attempt.FenceEpoch != f.Attempt.FenceEpoch || attempt.SchedulerEpoch != f.Attempt.SchedulerEpoch {
			t.Fatalf("attempt fencing changed: %+v", attempt)
		}
		stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if stored.SourceCommitSHA != pub.SourceCommitSHA || stored.IdempotencyKey != pub.IdempotencyKey {
			t.Fatalf("publish identity changed: %+v", stored)
		}
		ws, err := tx.WorkspaceForAttempt(ctx, f.Attempt.AttemptID)
		if err != nil {
			return err
		}
		if ws.WorkspaceID != f.Workspace.WorkspaceID {
			t.Fatalf("workspace ownership changed: %+v", ws)
		}
		return nil
	}); err != nil {
		t.Fatalf("post-migration read: %v", err)
	}
}

func TestEmbeddedMigrationSetIsWellFormed(t *testing.T) {
	set, err := store.Migrations()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}
	if len(set) == 0 {
		t.Fatal("embedded migration set is empty")
	}
	for i, m := range set {
		if m.Version != i+1 {
			t.Fatalf("migration %d has version %d; versions must be contiguous from 1", i, m.Version)
		}
		if m.SQL == "" {
			t.Fatalf("migration %04d_%s is empty", m.Version, m.Name)
		}
		if m.Checksum() == "" {
			t.Fatalf("migration %04d_%s has no checksum", m.Version, m.Name)
		}
	}
}
