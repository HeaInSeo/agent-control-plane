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
	// Path is the database file path. In-memory databases are not supported:
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

	existed, err := inspectFile(cfg.Path)
	if err != nil {
		return nil, err
	}
	if !existed {
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
	if existed {
		if err := db.verifyOwnMarker(ctx); err != nil {
			_ = sqlDB.Close()
			return nil, err
		}
	}
	return db, nil
}

// dsn builds the driver connection string.
func dsn(cfg Config) string {
	q := url.Values{}
	// _txlock=immediate takes the write lock at BEGIN, so a read-then-write
	// transaction cannot fail late with a lock upgrade error.
	q.Set("_txlock", "immediate")
	q.Set("_pragma", fmt.Sprintf("busy_timeout(%d)", cfg.BusyTimeout.Milliseconds()))
	if cfg.Mode == ModeReadOnly {
		q.Set("mode", "ro")
	}
	// url.Values.Encode sorts keys and would collapse repeated _pragma values,
	// so pragmas that must all be present are applied in configure() instead.
	return "file:" + cfg.Path + "?" + q.Encode()
}

// configure applies connection pragmas and verifies the file is usable.
func (db *DB) configure(ctx context.Context, cfg Config) error {
	// foreign_keys is per-connection and off by default in SQLite. Every
	// identity relation in this schema is a foreign key, so this is
	// load-bearing rather than hygiene.
	if _, err := db.sql.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("store: enable foreign keys: %w", err)
	}
	var fk int
	if err := db.sql.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil {
		return fmt.Errorf("store: read foreign_keys pragma: %w", err)
	}
	if fk != 1 {
		return errors.New("store: foreign key enforcement could not be enabled")
	}

	if db.mode == ModeReadWrite {
		var journal string
		if err := db.sql.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journal); err != nil {
			return fmt.Errorf("store: enable WAL: %w", err)
		}
		if journal != "wal" {
			return fmt.Errorf("store: journal_mode is %q, want wal", journal)
		}
		if _, err := db.sql.ExecContext(ctx, "PRAGMA synchronous = FULL"); err != nil {
			return fmt.Errorf("store: set synchronous: %w", err)
		}
	} else {
		if _, err := db.sql.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
			return fmt.Errorf("store: set query_only: %w", err)
		}
	}

	if !cfg.SkipIntegrityCheck {
		if err := db.IntegrityCheck(ctx); err != nil {
			return err
		}
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

// inspectFile reports whether the database file already exists, rejecting
// anything that exists but cannot be a SQLite database.
func inspectFile(path string) (bool, error) {
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("store: stat database: %w", err)
	case info.IsDir():
		return false, fmt.Errorf("%w: %s is a directory", ErrNotSQLiteDatabase, path)
	case info.Size() == 0:
		// A zero-length file is what SQLite itself produces before the first
		// write, so it is a legitimate pre-creation state.
		return true, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("store: open database for header check: %w", err)
	}
	defer f.Close()

	header := make([]byte, len(sqliteMagic))
	if _, err := f.Read(header); err != nil {
		return false, fmt.Errorf("%w: %s: %w", ErrNotSQLiteDatabase, path, err)
	}
	if string(header) != sqliteMagic {
		return false, fmt.Errorf("%w: %s has an unexpected file header", ErrNotSQLiteDatabase, path)
	}
	return true, nil
}

// Mode reports the handle's access mode.
func (db *DB) Mode() Mode { return db.mode }

// Path reports the database path.
func (db *DB) Path() string { return db.path }

// Close releases the handle.
func (db *DB) Close() error { return db.sql.Close() }
