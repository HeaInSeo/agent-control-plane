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
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	// ErrUnsupportedContract means the database declares a durable-layout
	// contract this build does not implement.
	ErrUnsupportedContract = errors.New("database declares an unsupported contract version")
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

// Validate reports whether the mode is one of the defined access modes.
//
// An out-of-range value used to fail open in the worst direction: dsn took
// the read-write branch (no mode=ro, no query_only) and the write guards
// passed, while enableWAL returned early because the mode was not exactly
// ModeReadWrite — so the handle wrote durable state in rollback-journal mode,
// silently voiding the WAL and synchronous=FULL durability story.
func (m Mode) Validate() error {
	switch m {
	case ModeReadWrite, ModeReadOnly:
		return nil
	default:
		return fmt.Errorf("store: %s is not a defined access mode", m)
	}
}

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
	// FullIntegrityCheckOnOpen runs the complete integrity check rather than
	// the quick one at open time.
	//
	// Open uses PRAGMA quick_check by default. The full check cross-checks
	// every index against its table, so its cost grows with the database —
	// and event history is append-only with no prune path, so on a
	// long-serving scheduler every restart would pay a full-file scan before
	// the epoch could even be activated. The quick check still detects a
	// malformed page, which is the failure open-time verification exists to
	// catch; the full check belongs on a schedule or in an operator tool,
	// where IntegrityCheck remains available.
	FullIntegrityCheckOnOpen bool
}

func (c Config) withDefaults() Config {
	if c.BusyTimeout <= 0 {
		c.BusyTimeout = 10 * time.Second
	}
	return c
}

// validate rejects a configuration whose values cannot be expressed
// faithfully.
//
// It runs on the caller's configuration, before defaults are applied.
// Defaulting first would hide exactly the values worth rejecting: a negative
// BusyTimeout — which a caller computing time.Until(deadline) against an
// already-passed deadline produces — looks identical to "unset" once
// withDefaults has replaced it, so the caller would silently get a ten-second
// lock wait instead of the error this function promises.
func (c Config) validate() error {
	if c.Path == "" {
		return errors.New("store: Config.Path is required")
	}
	if err := c.Mode.Validate(); err != nil {
		return err
	}
	if c.BusyTimeout < 0 {
		return fmt.Errorf("store: BusyTimeout %s is negative; leave it unset for the default",
			c.BusyTimeout)
	}
	// SQLite's busy_timeout is in milliseconds, so a sub-millisecond value
	// truncates to zero — which means "do not wait at all", the opposite of
	// what a caller asking for a short wait intended.
	if c.BusyTimeout > 0 && c.BusyTimeout.Milliseconds() == 0 {
		return fmt.Errorf("store: BusyTimeout %s rounds to 0ms; use at least 1ms or leave it unset",
			c.BusyTimeout)
	}
	return nil
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
	//
	// It is a pointer because derived handles (WithClock, the activation
	// handle) share one underlying *sql.DB and therefore one ownership.
	ownerEpoch *atomic.Int64
	// txMu serialises transactions. The store holds a single connection to
	// match the single-active-scheduler model, so concurrent callers have to
	// take turns; holding a real mutex across the transaction makes them
	// queue instead of contending for a connection nobody will release.
	txMu *sync.Mutex
	// txOwner is the goroutine currently inside a transaction, or 0. It is
	// what lets a nested call be told apart from a concurrent one: waiting
	// would deadlock only for the goroutine that already holds the lock.
	txOwner *atomic.Uint64
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
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
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

	db := newHandle(sqlDB, cfg)

	// Everything that inspects the file runs before anything that writes to
	// it. Order matters here: PRAGMA journal_mode = WAL rewrites the database
	// header and leaves -wal/-shm sidecars, so setting it before the identity
	// check would convert an unrelated service's database on the way to
	// refusing it — damaging exactly the file the check exists to protect.
	if err := db.verifyConnection(ctx, cfg); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	if state.holdsDatabase() {
		if err := db.verifyOwnMarker(ctx); err != nil {
			_ = sqlDB.Close()
			return nil, err
		}
		if err := db.verifyAppliedSchema(ctx); err != nil {
			_ = sqlDB.Close()
			return nil, err
		}
	}
	if err := db.enableWAL(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return db, nil
}

// newHandle builds a handle with its shared per-database state.
func newHandle(sqlDB *sql.DB, cfg Config) *DB {
	return &DB{
		sql:        sqlDB,
		mode:       cfg.Mode,
		path:       cfg.Path,
		ownerEpoch: new(atomic.Int64),
		txMu:       new(sync.Mutex),
		txOwner:    new(atomic.Uint64),
	}
}

// verifyAppliedSchema refuses a database whose recorded migration history
// does not match this build's.
//
// This is the whole check, not only the newer-than-supported case: a tampered
// checksum or a migration name this build has never heard of means the
// applied schema and this source tree have diverged, and reading or writing
// such a database is exactly as unsafe. Migrate and VerifySchema run the same
// verification, but neither is on the Open path, so a read-only inspector or
// a backup job would otherwise proceed happily.
func (db *DB) verifyAppliedSchema(ctx context.Context) error {
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
	// A partially migrated database is legitimate: it is one this process may
	// be about to migrate forward. verifyHistory allows a proper prefix and
	// rejects divergence.
	return verifyHistory(applied, known)
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

// verifyConnection runs the read-only half of open-time validation: the
// connection pragmas the DSN asked for, and the integrity check. Nothing here
// writes to the database.
func (db *DB) verifyConnection(ctx context.Context, cfg Config) error {
	if err := db.VerifyConnectionPragmas(ctx); err != nil {
		return err
	}
	if !cfg.SkipIntegrityCheck {
		check := db.QuickIntegrityCheck
		if cfg.FullIntegrityCheckOnOpen {
			check = db.IntegrityCheck
		}
		if err := check(ctx); err != nil {
			return err
		}
	}
	return nil
}

// enableWAL sets the journal mode, which is a property of the database file
// rather than of the connection, so it is set once rather than per connection.
//
// This is the first write Open performs, and it only happens after the file
// has been established as ours.
func (db *DB) enableWAL(ctx context.Context) error {
	if db.mode != ModeReadWrite {
		return nil
	}
	var journal string
	if err := db.sql.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journal); err != nil {
		return fmt.Errorf("store: enable WAL: %w", err)
	}
	if journal != "wal" {
		return fmt.Errorf("store: journal_mode is %q, want wal", journal)
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
	if err := db.requireNoOpenTransaction(); err != nil {
		return err
	}
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

// QuickIntegrityCheck runs SQLite's quick integrity check.
//
// It verifies page structure without the index-versus-table cross-checks, so
// its cost does not grow with history the way the full check does. This is
// what Open uses; IntegrityCheck is the thorough one.
func (db *DB) QuickIntegrityCheck(ctx context.Context) error {
	return db.runIntegrityCheck(ctx, "PRAGMA quick_check")
}

// IntegrityCheck runs SQLite's full integrity check and fails closed on any
// result other than a single "ok".
func (db *DB) IntegrityCheck(ctx context.Context) error {
	return db.runIntegrityCheck(ctx, "PRAGMA integrity_check")
}

func (db *DB) runIntegrityCheck(ctx context.Context, pragma string) error {
	if err := db.requireNoOpenTransaction(); err != nil {
		return err
	}
	rows, err := db.sql.QueryContext(ctx, pragma)
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

	// The contract marker is checked, not merely written. Left unread it
	// would look like a fail-closed guard while being inert: a future build
	// that bumps dbContract would open an older database without noticing.
	var contract string
	err = db.sql.QueryRowContext(ctx,
		`SELECT value FROM control_plane_meta WHERE key = 'db_contract'`).Scan(&contract)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: %s carries no db_contract marker", ErrUnsupportedContract, db.path)
	case err != nil:
		return fmt.Errorf("%w: %s: %w", ErrUnsupportedContract, db.path, err)
	case !contractSupported(contract):
		return fmt.Errorf("%w: %s declares db_contract %q, this build supports %v",
			ErrUnsupportedContract, db.path, contract, supportedContracts)
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

	// A file too small to hold the header cannot be a database.
	if info.Size() < int64(len(sqliteMagic)) {
		return fileAbsent, fmt.Errorf("%w: %s is %d bytes, too small for a database header",
			ErrNotSQLiteDatabase, path, info.Size())
	}
	header := make([]byte, len(sqliteMagic))
	// ReadFull rather than Read: a short read returns n < 16 with a nil
	// error, leaving the tail of the buffer zeroed, so the magic comparison
	// would fail and a perfectly good scheduler database would be refused as
	// "not a SQLite database" — a refusal no retry fixes. Short reads are
	// rare on local regular files but reachable on network or FUSE-backed
	// storage and on an interrupted read.
	if _, err := io.ReadFull(f, header); err != nil {
		return fileAbsent, fmt.Errorf("%w: %s: %w", ErrNotSQLiteDatabase, path, err)
	}
	if string(header) != sqliteMagic {
		return fileAbsent, fmt.Errorf("%w: %s has an unexpected file header", ErrNotSQLiteDatabase, path)
	}
	return fileDatabase, nil
}

// shallowCopy returns a handle sharing the same connection, ownership and
// transaction state.
//
// Sharing rather than copying is the point: it is the same database handle
// with one field changed, so ownership acquired through one is visible through
// the other, and a transaction open on one is seen by the other.
func (db *DB) shallowCopy() *DB {
	return &DB{
		sql:             db.sql,
		mode:            db.mode,
		path:            db.path,
		clock:           db.clock,
		ownerEpoch:      db.ownerEpoch,
		txMu:            db.txMu,
		txOwner:         db.txOwner,
		assumeOwnership: db.assumeOwnership,
	}
}

// Mode reports the handle's access mode.
func (db *DB) Mode() Mode { return db.mode }

// Path reports the database path.
func (db *DB) Path() string { return db.path }

// Close releases the handle.
func (db *DB) Close() error { return db.sql.Close() }
