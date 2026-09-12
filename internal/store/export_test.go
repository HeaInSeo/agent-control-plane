package store

import "context"

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
