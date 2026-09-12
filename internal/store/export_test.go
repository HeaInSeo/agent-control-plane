package store

import (
	"context"
	"fmt"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
)

// BootstrapForTest runs only the bootstrap step — the database identity
// marker and the migration ledger — without applying any migration.
//
// It exists so tests can stop at the exact crash point between bootstrap and
// the first migration and prove that the next open recovers. Production code
// reaches bootstrap only through Migrate.
func (db *DB) BootstrapForTest(ctx context.Context) error {
	return db.bootstrap(ctx)
}

// ExecForTest runs a raw statement inside the transaction.
//
// It exists only for tests, and only in the test build: adversarial tests need
// to bypass the Go-level validation in order to prove that the schema's own
// constraints and triggers are load-bearing rather than decorative. No
// production code path can reach it.
func (t *Tx) ExecForTest(ctx context.Context, query string, args ...any) error {
	_, err := t.exec(ctx, query, args...)
	return err
}

// QueryIntForTest runs a scalar query inside the transaction.
func (t *Tx) QueryIntForTest(ctx context.Context, query string, args ...any) (int64, error) {
	var out int64
	err := t.tx.QueryRowContext(ctx, query, args...).Scan(&out)
	return out, err
}

// RecycleConnectionsForTest forces the pool to discard its pooled connection,
// so a test can check that a replacement connection still carries the
// per-connection pragmas the DSN asked for.
func (db *DB) RecycleConnectionsForTest() {
	db.sql.SetMaxIdleConns(0)
	db.sql.SetMaxIdleConns(1)
}

// QueryStringForTest runs a scalar text query inside the transaction.
func (t *Tx) QueryStringForTest(ctx context.Context, query string, args ...any) (string, error) {
	var out string
	err := t.tx.QueryRowContext(ctx, query, args...).Scan(&out)
	return out, err
}

// AppendEventAt inserts an event at an explicit sequence number.
//
// It exists only for tests, and only in the test build: it lets a test prove
// the (epoch, seq) uniqueness constraint is real. It is deliberately not
// production surface — a caller that could choose its own seq could punch a
// permanent hole in an append-only replay stream, or pick MaxInt64 and wedge
// the epoch, since the next allocation would overflow and fail the
// seq >= 1 check. Normal callers use AppendEvent and let the store allocate.
func (t *Tx) AppendEventAt(ctx context.Context, e domain.Event, seq int64) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	if err := t.RequireCurrentEpoch(ctx, e.SchedulerEpoch); err != nil {
		return err
	}
	fields, err := domain.EncodeFields(e.Fields)
	if err != nil {
		return err
	}
	if _, err := t.exec(ctx,
		`INSERT INTO event (scheduler_epoch, seq, event_id, occurred_at, event_type,
		                    subject_kind, subject_id, task_id, attempt_id, fields)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		int64(e.SchedulerEpoch), seq, string(e.EventID), formatTime(e.OccurredAt),
		e.EventType, string(e.SubjectKind), e.SubjectID,
		taskIDArg(e.TaskID), attemptIDArg(e.AttemptID), fields,
	); err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: event (epoch %d, seq %d) already exists: %w",
				ErrDuplicateEventIdentity, int64(e.SchedulerEpoch), seq, err)
		}
		return fmt.Errorf("append event at seq %d: %w", seq, err)
	}
	return nil
}
