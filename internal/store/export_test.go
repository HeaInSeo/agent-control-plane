package store

import "context"

// ExecForTest runs a raw statement inside the transaction.
//
// It exists only for tests, and only in the test build: adversarial tests need
// to bypass the Go-level validation in order to prove that the schema's own
// constraints and triggers are load-bearing rather than decorative. No
// production code path can reach it.
func (t *Tx) ExecForTest(ctx context.Context, query string, args ...any) error {
	_, err := t.tx.ExecContext(ctx, query, args...)
	return err
}

// QueryIntForTest runs a scalar query inside the transaction.
func (t *Tx) QueryIntForTest(ctx context.Context, query string, args ...any) (int64, error) {
	var out int64
	err := t.tx.QueryRowContext(ctx, query, args...).Scan(&out)
	return out, err
}
