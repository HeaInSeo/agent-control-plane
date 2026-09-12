package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Migration errors. All of them are terminal: the correct response to an
// unexpected schema state is to stop, not to repair or re-create.
var (
	// ErrSchemaVersionUnsupported means the database was migrated by a newer
	// build than this one. Running against it could corrupt invariants this
	// build does not know about.
	ErrSchemaVersionUnsupported = errors.New("database schema version is newer than this build supports")
	// ErrMigrationChecksumMismatch means an already-applied migration's text
	// changed. The applied schema and the source of truth have diverged.
	ErrMigrationChecksumMismatch = errors.New("applied migration checksum mismatch")
	// ErrMigrationHistoryDivergent means the applied versions are not a prefix
	// of the known migrations, e.g. a version was applied that this build has
	// never heard of, or an earlier version is missing.
	ErrMigrationHistoryDivergent = errors.New("applied migration history is divergent")
	// ErrMigrationsMalformed means the embedded migration set is unusable.
	ErrMigrationsMalformed = errors.New("migration set malformed")
)

// Migration is one versioned, forward-only schema step.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// Checksum is the SHA-256 of the migration text.
func (m Migration) Checksum() string {
	sum := sha256.Sum256([]byte(m.SQL))
	return hex.EncodeToString(sum[:])
}

// AppliedMigration is a recorded migration step.
type AppliedMigration struct {
	Version   int
	Name      string
	Checksum  string
	AppliedAt time.Time
}

// Migrations returns the embedded migration set, ordered by version.
func Migrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("%w: read embedded migrations: %w", ErrMigrationsMalformed, err)
	}

	var out []Migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		version, name, err := parseMigrationName(e.Name())
		if err != nil {
			return nil, err
		}
		body, err := fs.ReadFile(migrationFS, path.Join("migrations", e.Name()))
		if err != nil {
			return nil, fmt.Errorf("%w: read %s: %w", ErrMigrationsMalformed, e.Name(), err)
		}
		out = append(out, Migration{Version: version, Name: name, SQL: string(body)})
	}
	slices.SortFunc(out, func(a, b Migration) int { return a.Version - b.Version })

	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("%w: migration versions must be contiguous from 1, found %d at position %d",
				ErrMigrationsMalformed, m.Version, i+1)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: no migrations found", ErrMigrationsMalformed)
	}
	return out, nil
}

// parseMigrationName splits "0001_init.sql" into version 1 and name "init".
func parseMigrationName(filename string) (int, string, error) {
	base := strings.TrimSuffix(filename, ".sql")
	prefix, name, ok := strings.Cut(base, "_")
	if !ok || name == "" {
		return 0, "", fmt.Errorf("%w: %q must be NNNN_name.sql", ErrMigrationsMalformed, filename)
	}
	version, err := strconv.Atoi(prefix)
	if err != nil || version < 1 {
		return 0, "", fmt.Errorf("%w: %q has a non-numeric version prefix", ErrMigrationsMalformed, filename)
	}
	return version, name, nil
}

// Migrate brings the database up to the given migration set.
//
// It is idempotent: running it against an already-current database applies
// nothing and instead re-verifies the recorded history, so a replay is also a
// consistency check. Anything unexpected stops the process rather than
// attempting a repair.
func (db *DB) Migrate(ctx context.Context, set []Migration) error {
	if db.mode == ModeReadOnly {
		return fmt.Errorf("%w: cannot migrate", ErrReadOnly)
	}
	if len(set) == 0 {
		return fmt.Errorf("%w: empty migration set", ErrMigrationsMalformed)
	}

	if err := db.ensureMigrationTable(ctx); err != nil {
		return err
	}
	applied, err := db.AppliedMigrations(ctx)
	if err != nil {
		return err
	}
	if err := verifyHistory(applied, set); err != nil {
		return err
	}

	for _, m := range set[len(applied):] {
		if err := db.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", m.Version, m.Name, err)
		}
	}
	return nil
}

// MigrateEmbedded brings the database up to the embedded migration set.
func (db *DB) MigrateEmbedded(ctx context.Context) error {
	set, err := Migrations()
	if err != nil {
		return err
	}
	return db.Migrate(ctx, set)
}

func (db *DB) ensureMigrationTable(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS schema_migration (
    version    INTEGER PRIMARY KEY CHECK (version >= 1),
    name       TEXT NOT NULL,
    checksum   TEXT NOT NULL,
    applied_at TEXT NOT NULL
) WITHOUT ROWID;`
	if _, err := db.sql.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("store: create schema_migration: %w", err)
	}
	return nil
}

// AppliedMigrations returns the recorded migration history, ordered by version.
func (db *DB) AppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	if err := db.ensureMigrationTableIfWritable(ctx); err != nil {
		return nil, err
	}
	rows, err := db.sql.QueryContext(ctx,
		`SELECT version, name, checksum, applied_at FROM schema_migration ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("store: read schema_migration: %w", err)
	}
	defer rows.Close()

	var out []AppliedMigration
	for rows.Next() {
		var (
			m  AppliedMigration
			at string
		)
		if err := rows.Scan(&m.Version, &m.Name, &m.Checksum, &at); err != nil {
			return nil, fmt.Errorf("store: scan schema_migration: %w", err)
		}
		ts, err := parseTime(at)
		if err != nil {
			return nil, fmt.Errorf("store: schema_migration.applied_at: %w", err)
		}
		m.AppliedAt = ts
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate schema_migration: %w", err)
	}
	return out, nil
}

// ensureMigrationTableIfWritable lets a read-only handle inspect the history of
// a migrated database without trying to create anything.
func (db *DB) ensureMigrationTableIfWritable(ctx context.Context) error {
	if db.mode == ModeReadOnly {
		return nil
	}
	return db.ensureMigrationTable(ctx)
}

// SchemaVersion reports the highest applied migration version, or 0 for a
// database that has never been migrated.
func (db *DB) SchemaVersion(ctx context.Context) (int, error) {
	applied, err := db.AppliedMigrations(ctx)
	if err != nil {
		return 0, err
	}
	if len(applied) == 0 {
		return 0, nil
	}
	return applied[len(applied)-1].Version, nil
}

// VerifySchema checks that the database's applied history matches the embedded
// migration set exactly, without writing anything. This is the check a
// read-only inspector or an integrity checker uses: it never migrates and
// never advances the scheduler epoch.
func (db *DB) VerifySchema(ctx context.Context) error {
	set, err := Migrations()
	if err != nil {
		return err
	}
	applied, err := db.AppliedMigrations(ctx)
	if err != nil {
		return err
	}
	if err := verifyHistory(applied, set); err != nil {
		return err
	}
	if len(applied) != len(set) {
		return fmt.Errorf("%w: applied %d of %d known migrations",
			ErrMigrationHistoryDivergent, len(applied), len(set))
	}
	return nil
}

// verifyHistory checks that applied is an exact prefix of set.
//
// A database migrated further than this build knows about is the dangerous
// case: it fails closed with ErrSchemaVersionUnsupported rather than being
// treated as "close enough".
func verifyHistory(applied []AppliedMigration, set []Migration) error {
	if len(applied) > len(set) {
		return fmt.Errorf("%w: database at version %d, this build knows up to %d",
			ErrSchemaVersionUnsupported, applied[len(applied)-1].Version, set[len(set)-1].Version)
	}
	for i, a := range applied {
		want := set[i]
		if a.Version != want.Version {
			return fmt.Errorf("%w: position %d has version %d, expected %d",
				ErrMigrationHistoryDivergent, i, a.Version, want.Version)
		}
		if a.Name != want.Name {
			return fmt.Errorf("%w: version %d is recorded as %q, expected %q",
				ErrMigrationHistoryDivergent, a.Version, a.Name, want.Name)
		}
		if a.Checksum != want.Checksum() {
			return fmt.Errorf("%w: version %d recorded %s, embedded %s",
				ErrMigrationChecksumMismatch, a.Version, a.Checksum, want.Checksum())
		}
	}
	return nil
}

// applyMigration runs one migration and records it in the same transaction, so
// a failure can never leave a half-applied step recorded as complete.
func (db *DB) applyMigration(ctx context.Context, m Migration) error {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migration (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`,
		m.Version, m.Name, m.Checksum(), formatTime(time.Now().UTC()),
	); err != nil {
		return fmt.Errorf("record: %w", err)
	}
	return tx.Commit()
}

// timeLayout is the stored timestamp format: RFC 3339 with nanoseconds in UTC,
// which sorts lexicographically in the same order as chronologically.
const timeLayout = "2006-01-02T15:04:05.000000000Z"

func formatTime(t time.Time) string { return t.UTC().Format(timeLayout) }

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(timeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse timestamp %q: %w", s, err)
	}
	return t, nil
}

func formatTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return formatTime(*t)
}

func parseTimePtr(s sql.NullString) (*time.Time, error) {
	if !s.Valid || s.String == "" {
		return nil, nil
	}
	t, err := parseTime(s.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
