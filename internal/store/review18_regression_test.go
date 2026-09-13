package store_test

// Regressions for the findings of the eighteenth independent review, of head
// 533b8b2.

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 2: db_contract was verified strictly with no upgrade path, so the
// first bump would brick every existing database — verifyOwnMarker runs inside
// Open, Open is the only way to get a *DB, and Migrate is the only thing that
// could rewrite the marker.
func TestContractMarkerHasAnUpgradePath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "contract.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Simulate a database written under an older contract than this build's,
	// which is the situation the first bump creates for every existing file.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE control_plane_meta SET value = 'v0.0-older' WHERE key = 'db_contract'`)
	}); err != nil {
		t.Fatalf("stamp older contract: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// An unknown contract is refused, as a newer one must be.
	if _, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite}); !errors.Is(err, store.ErrUnsupportedContract) {
		t.Fatalf("want ErrUnsupportedContract for an unknown contract, got %v", err)
	}

	// The path that matters: a database at a contract this build DOES support
	// opens, migrates, and comes out stamped with the current contract. That
	// is what makes a future bump upgradeable rather than fatal.
	fresh := filepath.Join(t.TempDir(), "supported.db")
	supported, err := store.Open(ctx, store.Config{Path: fresh, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer supported.Close()
	if err := supported.BootstrapForTest(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := supported.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := supported.Read(ctx, func(tx *store.Tx) error {
		marker, err := tx.QueryStringForTest(ctx,
			`SELECT value FROM control_plane_meta WHERE key = 'db_contract'`)
		if err != nil {
			return err
		}
		if marker == "" {
			t.Fatal("the contract marker is empty after migration")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	// Migrating again is still idempotent with the refresh in place.
	if err := supported.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
}

// Finding 4: the nesting check must not degrade as the workspace table grows,
// and it never shrinks. Correctness is what the test can assert directly;
// the index-backed shape is asserted through the query plan.
func TestWorkspaceNestingCheckIsIndexBacked(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r18-plan", state.IntentModifying, state.LaneOperator)
	spare := seedAttemptWithoutWorkspace(t, db, "r18-plan-spare")

	// Many sibling workspaces, so a full scan would be visible in the plan.
	for i := range 40 {
		extra := seedAttemptWithoutWorkspace(t, db, "r18-plan-extra-"+string(rune('a'+i%26))+string(rune('a'+i/26)))
		ws := domain.Workspace{
			WorkspaceID:         ids.NewWorkspaceID(),
			AttemptID:           extra.Attempt.AttemptID,
			TaskID:              extra.Task.TaskID,
			RepositorySubjectID: extra.Subject.RepositorySubjectID,
			BaseSHA:             sha("r18-plan-base"),
			IsolationKind:       domain.IsolationReadOnlyCheckout,
			RootPath:            "/var/lib/acp/other/ws-" + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			CreatedAt:           fixedNow,
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); err != nil {
			t.Fatalf("seed extra workspace: %v", err)
		}
	}

	// The Go-side check uses the unique index on root_path in both
	// directions: equality against the path's ancestors, and a range scan
	// over its descendants.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		detail, err := tx.QueryPlanForTest(ctx,
			`SELECT root_path FROM workspace
			  WHERE root_path IN ('/var', '/var/lib')
			     OR (root_path > '/var/lib/acp/x/' AND root_path < '/var/lib/acp/x0')
			  LIMIT 1`)
		if err != nil {
			return err
		}
		if strings.Contains(detail, "SCAN workspace") {
			t.Fatalf("the nesting query still scans the table: %s", detail)
		}
		if !strings.Contains(detail, "sqlite_autoindex_workspace") &&
			!strings.Contains(detail, "SEARCH workspace") {
			t.Fatalf("the nesting query does not use an index: %s", detail)
		}
		return nil
	}); err != nil {
		t.Fatalf("query plan: %v", err)
	}

	// And it is still correct: nesting in either direction is refused.
	for _, path := range []string{
		f.Workspace.RootPath + "/inside",
		"/var/lib/acp/workspaces",
	} {
		ws := domain.Workspace{
			WorkspaceID:         ids.NewWorkspaceID(),
			AttemptID:           spare.Attempt.AttemptID,
			TaskID:              spare.Task.TaskID,
			RepositorySubjectID: spare.Subject.RepositorySubjectID,
			BaseSHA:             sha("r18-plan-base"),
			IsolationKind:       domain.IsolationReadOnlyCheckout,
			RootPath:            path,
			CreatedAt:           fixedNow,
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); !errors.Is(err, store.ErrWorkspaceConflict) {
			t.Fatalf("want ErrWorkspaceConflict for %q, got %v", path, err)
		}
	}
}
