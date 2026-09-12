package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

// WithClock returns a handle that uses the given clock. It is intended for
// tests that need deterministic timestamps.
func (db *DB) WithClock(now func() time.Time) *DB {
	clone := *db
	clone.clock = now
	return &clone
}

func (db *DB) nowFunc() func() time.Time {
	if db.clock != nil {
		return db.clock
	}
	return func() time.Time { return time.Now().UTC() }
}

// Write runs fn inside a single write transaction.
//
// The transaction is rolled back if fn returns an error or panics, and the
// error from fn is returned unchanged so callers can match on domain errors.
func (db *DB) Write(ctx context.Context, fn func(*Tx) error) error {
	if db.mode == ModeReadOnly {
		return fmt.Errorf("%w: cannot start a write transaction", ErrReadOnly)
	}
	return db.runTx(ctx, false, fn)
}

// Read runs fn inside a read-only transaction.
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
	return db.runTx(ctx, true, fn)
}

func (db *DB) runTx(ctx context.Context, readOnly bool, fn func(*Tx) error) (err error) {
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

	if err := fn(&Tx{tx: sqlTx, now: db.nowFunc(), readOnly: readOnly}); err != nil {
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	committed = true
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

	var activated domain.SchedulerEpoch
	err := db.Write(ctx, func(tx *Tx) error {
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
	})
	if err != nil {
		return domain.SchedulerEpoch{}, err
	}
	return activated, nil
}

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
