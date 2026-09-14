package store

import (
	"context"
	"strings"

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
		return mapEventInsertError(err, e, seq)
	}
	return nil
}

// QueryPlanForTest returns SQLite's plan for a query as a single string.
//
// Used to assert that an invariant check is index-backed rather than a table
// scan — a property that matters for a table which only ever grows.
func (t *Tx) QueryPlanForTest(ctx context.Context, query string, args ...any) (string, error) {
	rows, err := t.tx.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var details []string
	for rows.Next() {
		var (
			id, parent, notUsed int64
			detail              string
		)
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			return "", err
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(details, " | "), nil
}

// MigrateForTest applies an arbitrary migration set.
//
// Test-only: a set other than the embedded one leaves a ledger Open will
// reject for ever, with no in-tree way back, so production code has only
// MigrateEmbedded.
func (db *DB) MigrateForTest(ctx context.Context, set []Migration) error {
	return db.migrate(ctx, set)
}
