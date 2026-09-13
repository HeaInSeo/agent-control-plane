package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
)

// Scheduler ownership errors.
var (
	// ErrNoSchedulerEpoch means no scheduler generation has been activated, so
	// nothing may claim, execute or publish yet.
	ErrNoSchedulerEpoch = errors.New("no scheduler epoch has been activated")
	// ErrStaleEpoch means the caller acted under a scheduler generation that is
	// no longer current. Old-epoch rows stay readable; they never regain
	// ownership semantics.
	ErrStaleEpoch = errors.New("scheduler epoch is stale")
	// ErrNotFound means the requested row does not exist.
	ErrNotFound = errors.New("not found")
	// ErrAmbiguous means a lookup matched more than one row and cannot be
	// resolved safely.
	ErrAmbiguous = errors.New("lookup is ambiguous")
	// ErrNoSchedulerOwnership means this handle never acquired scheduler
	// ownership, so it may not mutate execution state.
	ErrNoSchedulerOwnership = errors.New("handle has not acquired scheduler ownership")
	// ErrNestedTransaction means a transaction was started while another was
	// already open on the same handle.
	ErrNestedTransaction = errors.New("transaction is already open on this handle")
)

// Tx is a transactional view of the store.
//
// Every multi-row identity relation is written through one Tx, so a partially
// persisted relation — an attempt without its workspace, a publication without
// its attempt — is not a reachable state.
type Tx struct {
	tx       *sql.Tx
	now      func() time.Time
	readOnly bool
	// ownerEpoch is the generation this handle acquired ownership as, or 0 if
	// it never activated a scheduler.
	ownerEpoch domain.Epoch
	// ownershipAssumed lets the activation transaction itself write, since it
	// is the operation that establishes ownership in the first place.
	ownershipAssumed bool
}

// RequireOwnership rejects a mutation by a handle that does not currently own
// the scheduler.
//
// RequireCurrentEpoch asks whether a *row* belongs to the current generation.
// This asks whether the *caller* does, which is the other half of the fence:
// without it a process still holding a read-write handle from a retired
// generation could drive the current generation's live attempt to ABANDONED,
// release its workspace, or mark its packet STALE — all writes that target
// current-epoch rows and so pass every row-level check.
func (t *Tx) RequireOwnership(ctx context.Context) error {
	if t.ownershipAssumed {
		return nil
	}
	current, err := t.CurrentEpoch(ctx)
	if err != nil {
		return err
	}
	if t.ownerEpoch == 0 {
		return fmt.Errorf("%w: current epoch is %d", ErrNoSchedulerOwnership, int64(current))
	}
	if t.ownerEpoch != current {
		return fmt.Errorf("%w: this handle owns epoch %d, current epoch is %d",
			ErrStaleEpoch, int64(t.ownerEpoch), int64(current))
	}
	return nil
}

// exec runs a statement that changes data.
//
// Every write in this package goes through here, so a read-only transaction
// cannot mutate anything regardless of which method the caller reached for.
func (t *Tx) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if t.readOnly {
		return nil, fmt.Errorf("%w: this transaction is read-only", ErrReadOnly)
	}
	return t.tx.ExecContext(ctx, query, args...)
}

// Now returns the transaction's clock. Tests can pin it via WithClock.
func (t *Tx) Now() time.Time { return t.now().UTC() }

// monotonicNow returns the later of the transaction clock and floor.
//
// Every timestamp that records "when this row last changed" goes through
// here. A backward clock step — an NTP correction, a VM resume, a manual set
// — would otherwise store a row claiming it changed before it was created,
// and since these rows are immutable or undeletable that claim could never be
// corrected. Clamping loses a little precision on a clock glitch and keeps
// the ordering invariant, which is the better trade.
func (t *Tx) monotonicNow(floor time.Time) time.Time {
	now := t.Now()
	if now.Before(floor) {
		return floor
	}
	return now
}

// WithClock returns a handle that uses the given clock. It is intended for
// tests that need deterministic timestamps.
func (db *DB) WithClock(now func() time.Time) *DB {
	clone := db.shallowCopy()
	clone.clock = now
	return clone
}

func (db *DB) nowFunc() func() time.Time {
	if db.clock != nil {
		return db.clock
	}
	return func() time.Time { return time.Now().UTC() }
}

// requireNoOpenTransaction rejects a handle-level query issued from inside a
// transaction on the same goroutine.
//
// The nesting guard in runTx covers Write and Read, but the *DB query helpers
// go straight to the pool, and with a single connection they would wait for
// the connection the caller's own transaction is holding — the same silent
// hang, reached by a different door.
func (db *DB) requireNoOpenTransaction() error {
	self := goroutineID()
	if self != 0 && db.txOwner.Load() == self {
		return fmt.Errorf("%w: use the transaction already in hand", ErrNestedTransaction)
	}
	return nil
}

// goroutineID returns the current goroutine's id, or 0 if it cannot be read.
//
// Go does not expose this, and needing it is a smell — but the alternative is
// worse. The store has a single connection, so a nested transaction can only
// ever deadlock, while a concurrent one merely has to wait. Telling the two
// apart requires knowing whether the caller is the goroutine already holding
// the lock, and there is no way to ask that without the id: a plain flag
// rejects legitimate concurrency, and a plain mutex hangs on nesting.
//
// If the id cannot be parsed the function returns 0, and nesting detection is
// then unavailable for that call: runTx skips the check and takes the lock, so
// a nested call on that goroutine would block on a mutex only it can release.
// That is the deadlock this mechanism exists to prevent, so the fallback is
// not "safe" — it is merely unreachable. runtime.Stack's first line is
// "goroutine <id> [<state>]:", which fits the 64-byte buffer for any id Go
// can produce. The honest statement is that there is no graceful degradation
// here, only an unreachable branch.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// The first line is "goroutine <id> [<state>]:".
	fields := bytes.Fields(buf[:n])
	if len(fields) < 2 {
		return 0
	}
	id, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// Write runs fn inside a single write transaction.
//
// The transaction is rolled back if fn returns an error or panics, and the
// error from fn is returned unchanged so callers can match on domain errors.
//
// Concurrent callers are safe: the store holds a single connection, matching
// the single-active-scheduler model, so transactions serialise and a second
// goroutine waits its turn.
//
// Transactions do not nest, though. An inner transaction on the same
// goroutine would wait for the connection the outer one holds and never get
// it, so Write and Read called from inside either return
// ErrNestedTransaction rather than deadlocking; use the *Tx already in hand.
func (db *DB) Write(ctx context.Context, fn func(*Tx) error) error {
	if db.mode == ModeReadOnly {
		return fmt.Errorf("%w: cannot start a write transaction", ErrReadOnly)
	}
	return db.runTx(ctx, false, fn, nil)
}

// Read runs fn inside a read-only transaction.
//
// Like Write, it does not nest: see Write for why.
//
// Read-only is enforced, not merely requested. The driver treats
// sql.TxOptions.ReadOnly as a hint about which BEGIN to issue and applies no
// enforcement of its own, so a write inside a Read on a read-write handle
// would otherwise commit. Every write in this package therefore goes through
// Tx.exec, which refuses to run in a read-only transaction.
//
// The gate is in Go rather than a query_only pragma because the pragma is
// connection state: on a single-connection store, resetting it has to happen
// while the transaction still holds that connection, and getting the ordering
// wrong deadlocks or — worse — leaves the handle read-only for good.
func (db *DB) Read(ctx context.Context, fn func(*Tx) error) error {
	return db.runTx(ctx, true, fn, nil)
}

// runTx runs fn in a transaction. afterCommit, when non-nil, runs after a
// successful commit and before the transaction lock is released, so a caller
// that has to publish state derived from the commit can make it visible
// before the next transaction is allowed to observe it.
func (db *DB) runTx(ctx context.Context, readOnly bool, fn func(*Tx) error, afterCommit func()) (err error) {
	// Concurrency and nesting need opposite answers, so they are told apart
	// before taking the lock. A second goroutine should wait its turn — the
	// store has one connection, so transactions serialise. The goroutine that
	// already holds the lock must not wait, because nothing will release it.
	self := goroutineID()
	if self != 0 && db.txOwner.Load() == self {
		return fmt.Errorf("%w: use the transaction already in hand", ErrNestedTransaction)
	}
	db.txMu.Lock()
	db.txOwner.Store(self)
	defer func() {
		db.txOwner.Store(0)
		db.txMu.Unlock()
	}()

	sqlTx, err := db.sql.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	if err != nil {
		return fmt.Errorf("store: begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// Rollback errors are not reported over the original failure: the
			// caller needs to see why the work failed, not how cleanup went.
			_ = sqlTx.Rollback()
		}
	}()

	if err := fn(&Tx{
		tx:               sqlTx,
		now:              db.nowFunc(),
		readOnly:         readOnly,
		ownerEpoch:       domain.Epoch(db.ownerEpoch.Load()),
		ownershipAssumed: db.assumeOwnership,
	}); err != nil {
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	committed = true
	if afterCommit != nil {
		afterCommit()
	}
	return nil
}

// ActivateScheduler acquires scheduler ownership and advances the epoch (CC4).
//
// This is the only code path in the package that inserts a scheduler epoch.
// Opening the database, checking integrity, verifying the schema, reading
// history or taking a backup all leave the epoch untouched — an epoch bump
// means "a scheduler activated", never "somebody opened the file".
func (db *DB) ActivateScheduler(ctx context.Context, owner ids.SchedulerOwnerID, reason string) (domain.SchedulerEpoch, error) {
	if db.mode == ModeReadOnly {
		return domain.SchedulerEpoch{}, fmt.Errorf("%w: cannot activate a scheduler", ErrReadOnly)
	}
	if err := owner.Validate(); err != nil {
		return domain.SchedulerEpoch{}, fmt.Errorf("activate scheduler: owner_id: %w", err)
	}
	if reason == "" {
		return domain.SchedulerEpoch{}, errors.New("activate scheduler: reason is required")
	}

	// The activation transaction is the operation that establishes ownership,
	// so it is the one write that cannot be gated on already having it.
	activating := db.shallowCopy()
	activating.assumeOwnership = true

	var activated domain.SchedulerEpoch
	// Ownership is published after the commit but before the transaction lock
	// is released. Publishing it after the lock would leave a window in which
	// a goroutine waiting on that lock starts its transaction, snapshots an
	// ownership that activation has already superseded, and is refused as
	// unowned or stale — an error indistinguishable from a real fencing
	// refusal, so the caller could not safely retry.
	publishOwnership := func() {
		for {
			current := db.ownerEpoch.Load()
			if current >= int64(activated.Epoch) {
				return
			}
			if db.ownerEpoch.CompareAndSwap(current, int64(activated.Epoch)) {
				return
			}
		}
	}
	err := activating.runTx(ctx, false, func(tx *Tx) error {
		var current int64
		err := tx.tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(epoch), 0) FROM scheduler_epoch`).Scan(&current)
		if err != nil {
			return fmt.Errorf("activate scheduler: read current epoch: %w", err)
		}

		next := domain.SchedulerEpoch{
			Epoch:            domain.Epoch(current + 1),
			OwnerID:          owner,
			ActivatedAt:      tx.Now(),
			ActivationReason: reason,
		}
		if err := next.Validate(); err != nil {
			return err
		}

		if _, err := tx.exec(ctx,
			`INSERT INTO scheduler_epoch (epoch, owner_id, activated_at, activation_reason) VALUES (?, ?, ?, ?)`,
			int64(next.Epoch), string(next.OwnerID), formatTime(next.ActivatedAt), next.ActivationReason,
		); err != nil {
			return fmt.Errorf("activate scheduler: insert epoch: %w", err)
		}
		if _, err := tx.exec(ctx,
			`INSERT INTO scheduler_ownership (id, current_epoch, owner_id, acquired_at) VALUES (1, ?, ?, ?)
			 ON CONFLICT (id) DO UPDATE SET current_epoch = excluded.current_epoch,
			                                owner_id      = excluded.owner_id,
			                                acquired_at   = excluded.acquired_at`,
			int64(next.Epoch), string(next.OwnerID), formatTime(next.ActivatedAt),
		); err != nil {
			return fmt.Errorf("activate scheduler: take ownership: %w", err)
		}

		if _, err := tx.AppendEvent(ctx, domain.Event{
			SchedulerEpoch: next.Epoch,
			EventID:        ids.NewEventID(),
			OccurredAt:     next.ActivatedAt,
			EventType:      "scheduler.activated",
			SubjectKind:    domain.SubjectScheduler,
			SubjectID:      string(next.OwnerID),
			Fields:         map[string]any{"reason": reason, "epoch": int64(next.Epoch)},
		}); err != nil {
			return err
		}

		activated = next
		return nil
	}, publishOwnership)
	if err != nil {
		return domain.SchedulerEpoch{}, err
	}
	return activated, nil
}

// OwnedEpoch reports the generation this handle acquired ownership as, or 0.
func (db *DB) OwnedEpoch() domain.Epoch { return domain.Epoch(db.ownerEpoch.Load()) }

// CurrentEpoch returns the current scheduler ownership generation.
func (db *DB) CurrentEpoch(ctx context.Context) (domain.Epoch, error) {
	var epoch domain.Epoch
	err := db.Read(ctx, func(tx *Tx) error {
		var err error
		epoch, err = tx.CurrentEpoch(ctx)
		return err
	})
	return epoch, err
}

// CurrentEpoch returns the current scheduler ownership generation.
func (t *Tx) CurrentEpoch(ctx context.Context) (domain.Epoch, error) {
	var epoch int64
	err := t.tx.QueryRowContext(ctx, `SELECT current_epoch FROM scheduler_ownership WHERE id = 1`).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNoSchedulerEpoch
	}
	if err != nil {
		return 0, fmt.Errorf("read current epoch: %w", err)
	}
	return domain.Epoch(epoch), nil
}

// RequireCurrentEpoch rejects an action taken under a stale generation.
func (t *Tx) RequireCurrentEpoch(ctx context.Context, epoch domain.Epoch) error {
	current, err := t.CurrentEpoch(ctx)
	if err != nil {
		return err
	}
	if epoch != current {
		return fmt.Errorf("%w: acting under epoch %d, current epoch is %d",
			ErrStaleEpoch, int64(epoch), int64(current))
	}
	return nil
}

// SchedulerEpoch loads one epoch record. Old epochs remain readable.
func (t *Tx) SchedulerEpoch(ctx context.Context, epoch domain.Epoch) (domain.SchedulerEpoch, error) {
	var (
		out        domain.SchedulerEpoch
		owner      string
		activated  string
		reason     string
		releasedAt sql.NullString
		raw        int64
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT epoch, owner_id, activated_at, activation_reason, released_at
		   FROM scheduler_epoch WHERE epoch = ?`, int64(epoch),
	).Scan(&raw, &owner, &activated, &reason, &releasedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("%w: scheduler epoch %d", ErrNotFound, int64(epoch))
	}
	if err != nil {
		return out, fmt.Errorf("read scheduler epoch: %w", err)
	}

	at, err := parseTime(activated)
	if err != nil {
		return out, err
	}
	released, err := parseTimePtr(releasedAt)
	if err != nil {
		return out, err
	}
	return domain.SchedulerEpoch{
		Epoch:            domain.Epoch(raw),
		OwnerID:          ids.SchedulerOwnerID(owner),
		ActivatedAt:      at,
		ActivationReason: reason,
		ReleasedAt:       released,
	}, nil
}
