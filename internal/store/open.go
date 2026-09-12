// Package store is the durable backend of the control plane.
//
// The first backend is SQLite in WAL mode under a single-active-scheduler
// model. Every open path fails closed: a missing, foreign, corrupt or
// newer-than-supported database is an error, never a reason to start over with
// a fresh one. Silently replacing a damaged scheduler database would destroy
// exactly the execution history the control plane exists to preserve.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver; no cgo, no system libsqlite
)

// Open errors. Each one is terminal for the caller.
var (
	// ErrDatabaseNotFound means the file does not exist and creation was not
	// explicitly requested.
	ErrDatabaseNotFound = errors.New("control plane database not found")
	// ErrNotSQLiteDatabase means the file exists but is not a SQLite database.
	ErrNotSQLiteDatabase = errors.New("file is not a sqlite database")
	// ErrForeignDatabase means the file is a SQLite database that does not
	// carry this control plane's identity marker.
	ErrForeignDatabase = errors.New("database is not an agent-control-plane database")
	// ErrIntegrityCheckFailed means SQLite reported corruption.
	ErrIntegrityCheckFailed = errors.New("database integrity check failed")
	// ErrReadOnly means a write operation was attempted on a read-only handle.
	ErrReadOnly = errors.New("database handle is read-only")
)

// sqliteMagic is the 16-byte header every SQLite database file starts with.
const sqliteMagic = "SQLite format 3\x00"

// Mode is the access mode of a database handle.
type Mode int

const (
	// ModeReadWrite is the scheduler's handle. It may migrate and write, and
	// it is the only mode that can activate a scheduler epoch.
	ModeReadWrite Mode = iota
	// ModeReadOnly is for inspectors, integrity checkers and backup tools.
	// A read-only handle cannot migrate and cannot advance the epoch (CC4).
	ModeReadOnly
)

// String implements fmt.Stringer.
func (m Mode) String() string {
	switch m {
	case ModeReadWrite:
		return "read-write"
	case ModeReadOnly:
		return "read-only"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// Config configures a database handle.
type Config struct {
	// Path is the database file path. A relative path is resolved against the
	// working directory at open time. In-memory databases are not supported:
	// the control plane's whole purpose is durability.
	Path string
	// Mode selects read-write or read-only access.
	Mode Mode
	// AllowCreate permits creating a new database when Path does not exist.
	// It is off by default so that a mistyped path cannot silently produce an
	// empty control plane.
	AllowCreate bool
	// BusyTimeout bounds how long a statement waits for a lock.
	BusyTimeout time.Duration
	// SkipIntegrityCheck disables the open-time integrity check. It exists for
	// tooling that has already verified the file; leave it false.
	SkipIntegrityCheck bool
}

func (c Config) withDefaults() Config {
	if c.BusyTimeout <= 0 {
		c.BusyTimeout = 10 * time.Second
	}
	return c
}

// DB is a handle on the control plane database.
type DB struct {
	sql   *sql.DB
	mode  Mode
	path  string
	clock func() time.Time

	// ownerEpoch is the scheduler generation this handle activated, or 0 if it
	// never did. Mutating execution state requires owning the current
	// generation, so a handle from a retired generation is fenced out in both
	// directions: it cannot act on old rows, and it cannot act on new ones.
	ownerEpoch atomic.Int64
	// assumeOwnership is set only on the internal handle the activation
	// transaction runs under, since that is what establishes ownership.
	assumeOwnership bool
}

// Open validates and opens the control plane database.
//
// Open never advances the scheduler epoch. Epoch advancement happens only in
// ActivateScheduler, which requires a read-write handle and a successful
// activation transaction (CC4).
func Open(ctx context.Context, cfg Config) (*DB, error) {
	cfg = cfg.withDefaults()
	if cfg.Path == "" {
		return nil, errors.New("store: Config.Path is required")
	}
	// The DSN is a file: URI, and a relative path in one is read as a URI
	// authority rather than a path — SQLite would reject "file://sub/cp.db"
	// with an opaque "invalid uri authority" error after stat and MkdirAll
	// had already run against the relative path. Resolving up front keeps the
	// path one thing everywhere: the stat, the directory creation, the DSN
	// and the path reported in errors.
	absPath, err := filepath.Abs(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("store: resolve database path %q: %w", cfg.Path, err)
	}
	cfg.Path = absPath

	state, err := inspectFile(cfg.Path)
	if err != nil {
		return nil, err
	}

	// A zero-length file holds no database. It is what SQLite itself leaves
	// behind before its first write, and it is also what a stray `touch` or an
	// abandoned path leaves behind — so for the purpose of the create gates it
	// counts as absent. Treating it as existing would let a mistyped path
	// silently yield a fresh, empty control plane, which is the exact case
	// AllowCreate exists to prevent.
	if !state.holdsDatabase() {
		if cfg.Mode == ModeReadOnly {
			return nil, fmt.Errorf("%w: %s (read-only open cannot create)", ErrDatabaseNotFound, cfg.Path)
		}
		if !cfg.AllowCreate {
			return nil, fmt.Errorf("%w: %s (set AllowCreate to bootstrap a new database)",
				ErrDatabaseNotFound, cfg.Path)
		}
		if dir := filepath.Dir(cfg.Path); dir != "" {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return nil, fmt.Errorf("store: create database directory: %w", err)
			}
		}
	}

	sqlDB, err := sql.Open("sqlite", dsn(cfg))
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite: %w", err)
	}
	// A single connection matches the single-active-scheduler model and keeps
	// write serialisation in one place rather than relying on lock retries.
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	db := &DB{sql: sqlDB, mode: cfg.Mode, path: cfg.Path}

	if err := db.configure(ctx, cfg); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	if state.holdsDatabase() {
		if err := db.verifyOwnMarker(ctx); err != nil {
			_ = sqlDB.Close()
			return nil, err
		}
		// Fail closed on a database written by a newer build. Migrate and
		// VerifySchema already refuse one, but neither is on the Open path: a
		// read-only inspector, a backup job or an integrity checker would
		// otherwise read — and a read-write handle would write — a database
		// carrying invariants this build does not know about.
		if err := db.rejectNewerSchema(ctx); err != nil {
			_ = sqlDB.Close()
			return nil, err
		}
	}
	return db, nil
}

// rejectNewerSchema refuses a database migrated beyond what this build knows.
//
// Only the "newer than supported" case is checked here. Full history
// verification — contiguity, names, checksums — belongs to Migrate and
// VerifySchema, which callers that intend to migrate or audit will run; this
// is the check every caller needs, whatever it came to do.
func (db *DB) rejectNewerSchema(ctx context.Context) error {
	applied, err := db.AppliedMigrations(ctx)
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return nil
	}
	known, err := Migrations()
	if err != nil {
		return err
	}
	highestApplied := applied[len(applied)-1].Version
	highestKnown := known[len(known)-1].Version
	if highestApplied > highestKnown {
		return fmt.Errorf("%w: %s is at version %d, this build knows up to %d",
			ErrSchemaVersionUnsupported, db.path, highestApplied, highestKnown)
	}
	return nil
}

// dsn builds the driver connection string.
//
// Every per-connection pragma goes in the DSN, not into a one-off Exec on the
// pool. foreign_keys, synchronous and query_only are connection state, and
// database/sql may discard and re-establish the pooled connection at any time
// (a driver.ErrBadConn, a failed session reset). A replacement connection gets
// only what the DSN carries, so a pragma applied once to the pool can silently
// disappear — and with it the enforcement of every identity relation in the
// schema, all of which are foreign keys.
//
// The driver applies each `_pragma` query parameter on every connection it
// opens, so the query string is built by hand: url.Values.Encode would
// collapse the repeated `_pragma` key.
func dsn(cfg Config) string {
	pragmas := []string{
		fmt.Sprintf("busy_timeout(%d)", cfg.BusyTimeout.Milliseconds()),
		// Load-bearing: the schema expresses identity relations as foreign
		// keys, and SQLite leaves enforcement off per connection by default.
		"foreign_keys(1)",
	}

	params := []string{
		// _txlock=immediate takes the write lock at BEGIN, so a read-then-write
		// transaction cannot fail late with a lock upgrade error.
		"_txlock=immediate",
	}
	if cfg.Mode == ModeReadOnly {
		params = append(params, "mode=ro")
		pragmas = append(pragmas, "query_only(1)")
	} else {
		pragmas = append(pragmas, "synchronous(FULL)")
	}
	for _, pragma := range pragmas {
		params = append(params, "_pragma="+url.QueryEscape(pragma))
	}

	// journal_mode is a property of the database file rather than of the
	// connection, so it is set once in configure() instead of on every
	// connection, and a replaced connection cannot lose it.
	return (&url.URL{Scheme: "file", Path: cfg.Path}).String() + "?" + strings.Join(params, "&")
}

// configure sets the database-level journal mode, verifies that the
// connection pragmas the DSN asked for actually took effect, and checks the
// file is usable.
func (db *DB) configure(ctx context.Context, cfg Config) error {
	if db.mode == ModeReadWrite {
		// journal_mode is stored in the database file, so it is set once here
		// rather than per connection.
		var journal string
		if err := db.sql.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journal); err != nil {
			return fmt.Errorf("store: enable WAL: %w", err)
		}
		if journal != "wal" {
			return fmt.Errorf("store: journal_mode is %q, want wal", journal)
		}
	}
	if err := db.VerifyConnectionPragmas(ctx); err != nil {
		return err
	}
	if !cfg.SkipIntegrityCheck {
		if err := db.IntegrityCheck(ctx); err != nil {
			return err
		}
	}
	return nil
}

// VerifyConnectionPragmas checks that the per-connection settings the DSN
// requested are in force on the connection actually serving queries.
//
// This is worth asserting rather than assuming: a silently missing
// foreign_keys pragma would not fail anything loudly, it would just stop
// enforcing every identity relation in the schema.
func (db *DB) VerifyConnectionPragmas(ctx context.Context) error {
	var fk int
	if err := db.sql.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil {
		return fmt.Errorf("store: read foreign_keys pragma: %w", err)
	}
	if fk != 1 {
		return errors.New("store: foreign key enforcement is not enabled on this connection")
	}

	if db.mode == ModeReadOnly {
		var queryOnly int
		if err := db.sql.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly); err != nil {
			return fmt.Errorf("store: read query_only pragma: %w", err)
		}
		if queryOnly != 1 {
			return errors.New("store: query_only is not enabled on this read-only connection")
		}
		return nil
	}

	var synchronous int
	if err := db.sql.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("store: read synchronous pragma: %w", err)
	}
	// 2 == FULL.
	if synchronous != 2 {
		return fmt.Errorf("store: synchronous is %d, want 2 (FULL)", synchronous)
	}
	return nil
}

// IntegrityCheck runs SQLite's integrity check and fails closed on any result
// other than a single "ok".
func (db *DB) IntegrityCheck(ctx context.Context) error {
	rows, err := db.sql.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("%w: %w", ErrIntegrityCheckFailed, err)
	}
	defer rows.Close()

	var problems []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return fmt.Errorf("%w: %w", ErrIntegrityCheckFailed, err)
		}
		if line != "ok" {
			problems = append(problems, line)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrIntegrityCheckFailed, err)
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s: %v", ErrIntegrityCheckFailed, db.path, problems)
	}
	return nil
}

// verifyOwnMarker refuses a pre-existing database that is not ours.
//
// A database with no tables at all is accepted: that is a file this process is
// about to bootstrap. A database with tables but without our marker is
// refused, so the control plane cannot adopt or damage an unrelated SQLite
// file.
//
// The marker is written in the bootstrap transaction, together with the
// migration ledger and before any migration runs, so a database of ours
// always carries it from the first commit onwards. A half-bootstrapped
// database is therefore recognised as ours and can be retried; there is no
// intermediate state with tables but no marker to make an allowance for.
func (db *DB) verifyOwnMarker(ctx context.Context) error {
	var tables int
	if err := db.sql.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
	).Scan(&tables); err != nil {
		return fmt.Errorf("store: read schema catalogue: %w", err)
	}
	if tables == 0 {
		return nil
	}

	var kind string
	err := db.sql.QueryRowContext(ctx,
		`SELECT value FROM control_plane_meta WHERE key = 'db_kind'`).Scan(&kind)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: %s has %d tables but no db_kind marker", ErrForeignDatabase, db.path, tables)
	case err != nil:
		return fmt.Errorf("%w: %s: %w", ErrForeignDatabase, db.path, err)
	case kind != dbKind:
		return fmt.Errorf("%w: %s declares db_kind %q", ErrForeignDatabase, db.path, kind)
	}
	return nil
}

// fileState describes what is at the database path before opening it.
type fileState int

const (
	// fileAbsent means nothing is at the path.
	fileAbsent fileState = iota
	// fileEmpty means a zero-length file is at the path. It holds no
	// database, so it is not a database to be opened.
	fileEmpty
	// fileDatabase means a file with a valid SQLite header is at the path.
	fileDatabase
)

// holdsDatabase reports whether the path actually holds a database.
func (s fileState) holdsDatabase() bool { return s == fileDatabase }

// inspectFile reports what is at the database path, rejecting anything that
// exists and is not a SQLite database.
func inspectFile(path string) (fileState, error) {
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fileAbsent, nil
	case err != nil:
		return fileAbsent, fmt.Errorf("store: stat database: %w", err)
	case info.IsDir():
		return fileAbsent, fmt.Errorf("%w: %s is a directory", ErrNotSQLiteDatabase, path)
	case info.Size() == 0:
		return fileEmpty, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return fileAbsent, fmt.Errorf("store: open database for header check: %w", err)
	}
	defer f.Close()

	header := make([]byte, len(sqliteMagic))
	if _, err := f.Read(header); err != nil {
		return fileAbsent, fmt.Errorf("%w: %s: %w", ErrNotSQLiteDatabase, path, err)
	}
	if string(header) != sqliteMagic {
		return fileAbsent, fmt.Errorf("%w: %s has an unexpected file header", ErrNotSQLiteDatabase, path)
	}
	return fileDatabase, nil
}

// shallowCopy returns a handle sharing the same connection and ownership.
//
// atomic.Int64 must not be copied by value, so the ownership generation is
// carried across explicitly. The copy shares the underlying *sql.DB, which is
// the point: it is the same database handle with one field changed.
func (db *DB) shallowCopy() *DB {
	clone := &DB{
		sql:             db.sql,
		mode:            db.mode,
		path:            db.path,
		clock:           db.clock,
		assumeOwnership: db.assumeOwnership,
	}
	clone.ownerEpoch.Store(db.ownerEpoch.Load())
	return clone
}

// Mode reports the handle's access mode.
func (db *DB) Mode() Mode { return db.mode }

// Path reports the database path.
func (db *DB) Path() string { return db.path }

// Close releases the handle.
func (db *DB) Close() error { return db.sql.Close() }
