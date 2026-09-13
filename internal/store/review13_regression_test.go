package store_test

// Regressions for the findings of the thirteenth independent review, of head
// 611883f. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1: two processes starting together both saw an unmigrated database
// and the loser's DDL failed with an opaque "table already exists" — nothing
// fences them, since the epoch fence only begins at ActivateScheduler.
func TestConcurrentMigrationIsIdempotent(t *testing.T) {
	ctx := context.Background()

	for round := range 12 {
		path := filepath.Join(t.TempDir(), "concurrent.db")

		const starters = 4
		handles := make([]*store.DB, starters)
		for i := range handles {
			db, err := store.Open(ctx, store.Config{
				Path: path, Mode: store.ModeReadWrite, AllowCreate: true,
			})
			if err != nil {
				t.Fatalf("round %d open %d: %v", round, i, err)
			}
			handles[i] = db
		}

		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			errs []error
		)
		for _, db := range handles {
			wg.Add(1)
			go func(db *store.DB) {
				defer wg.Done()
				if err := db.MigrateEmbedded(ctx); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(db)
		}
		wg.Wait()

		for _, err := range errs {
			t.Fatalf("round %d: concurrent migration failed: %v", round, err)
		}
		if err := handles[0].VerifySchema(ctx); err != nil {
			t.Fatalf("round %d: verify schema: %v", round, err)
		}
		applied, err := handles[0].AppliedMigrations(ctx)
		if err != nil {
			t.Fatalf("round %d: applied: %v", round, err)
		}
		seen := map[int]bool{}
		for _, m := range applied {
			if seen[m.Version] {
				t.Fatalf("round %d: migration %d recorded twice", round, m.Version)
			}
			seen[m.Version] = true
		}
		for _, db := range handles {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// A divergent history must still fail closed, even reached through the
// in-transaction ledger re-read that makes concurrent migration idempotent.
func TestConcurrentMigrationStillFailsClosedOnDivergence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "diverged.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.BootstrapForTest(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	// Another build recorded version 1 under a different checksum, without
	// this build's tables.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO schema_migration (version, name, checksum, applied_at)
			 VALUES (1, 'init', 'deadbeef', '2026-01-01T00:00:00.000000000Z')`)
	}); err != nil {
		t.Fatalf("seed divergence: %v", err)
	}

	if err := db.MigrateEmbedded(ctx); !errors.Is(err, store.ErrMigrationChecksumMismatch) {
		t.Fatalf("want ErrMigrationChecksumMismatch, got %v", err)
	}
}

// Findings 2 and 3: re-asserting a status rewrote updated_at, and a terminal
// task could be re-written — the guard the twelfth round added for
// publications was not given to its two siblings.
func TestReassertingStatusIsANoOpForTasksAndAttempts(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r13-noop", state.IntentModifying, state.LaneOperator)

	later := fixedNow.Add(time.Hour)
	advanced := db.WithClock(func() time.Time { return later })

	t.Run("attempt", func(t *testing.T) {
		for range 3 {
			if err := advanced.Write(ctx, func(tx *store.Tx) error {
				return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, f.Attempt.Status)
			}); err != nil {
				t.Fatalf("re-assert: %v", err)
			}
		}
		if err := db.Read(ctx, func(tx *store.Tx) error {
			attempt, err := tx.WorkerAttempt(ctx, f.Attempt.AttemptID)
			if err != nil {
				return err
			}
			if !attempt.UpdatedAt.Equal(fixedNow) {
				t.Fatalf("updated_at moved to %v on a no-op", attempt.UpdatedAt)
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("task", func(t *testing.T) {
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
		}); err != nil {
			t.Fatalf("abandon: %v", err)
		}
		var abandonedAt time.Time
		if err := db.Read(ctx, func(tx *store.Tx) error {
			run, err := tx.TaskRun(ctx, f.Task.TaskID)
			if err != nil {
				return err
			}
			abandonedAt = run.UpdatedAt
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}

		// A withdrawn task must not be made to look freshly touched.
		for range 3 {
			if err := advanced.Write(ctx, func(tx *store.Tx) error {
				return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
			}); err != nil {
				t.Fatalf("re-assert: %v", err)
			}
		}
		if err := db.Read(ctx, func(tx *store.Tx) error {
			run, err := tx.TaskRun(ctx, f.Task.TaskID)
			if err != nil {
				return err
			}
			if !run.UpdatedAt.Equal(abandonedAt) {
				t.Fatalf("a finished task was touched: updated_at %v, want %v",
					run.UpdatedAt, abandonedAt)
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})
}

// Finding 5: the terminal-attempt fence was the last Go-only guard in the
// evidence, publication and workspace writers.
func TestTerminalAttemptFenceIsEnforcedBySchema(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r13-dead-attempt", state.IntentModifying, state.LaneOperator)
	spare := seedAttemptWithoutWorkspace(t, db, "r13-spare")

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed); err != nil {
			return err
		}
		return tx.SetWorkerAttemptStatus(ctx, spare.Attempt.AttemptID, state.AttemptFailed)
	}); err != nil {
		t.Fatalf("retire attempts: %v", err)
	}

	commit := string(sha("r13-dead-commit"))
	raws := map[string]func(tx *store.Tx) error{
		"publish_attempt": func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`INSERT INTO publish_attempt (publish_attempt_id, task_id, attempt_id,
				                              scheduler_epoch, fence_epoch, workspace_id,
				                              repository_subject_id, base_sha, source_commit_sha,
				                              target_ref, idempotency_key, status,
				                              created_at, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'refs/heads/m0/r13', 'r13-key', 'PENDING',
				         '2026-09-12T09:00:00.000000000Z', '2026-09-12T09:00:00.000000000Z')`,
				string(ids.NewPublishAttemptID()), string(f.Task.TaskID),
				string(f.Attempt.AttemptID), int64(f.Attempt.SchedulerEpoch),
				int64(f.Attempt.FenceEpoch), string(f.Workspace.WorkspaceID),
				string(f.Subject.RepositorySubjectID), string(f.Workspace.BaseSHA), commit)
		},
		"evidence_observation": func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`INSERT INTO evidence_observation (evidence_id, task_id, attempt_id, scheduler_epoch,
				                                   fence_epoch, repository_subject_id, workspace_id,
				                                   publish_attempt_id, published_sha, observed_sha,
				                                   reviewed_sha, artifact_digest,
				                                   observed_at, evidence_kind)
				 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL, NULL,
				         '2026-09-12T09:00:00.000000000Z', 'BRANCH_HEAD')`,
				string(ids.NewEvidenceID()), string(f.Task.TaskID), string(f.Attempt.AttemptID),
				int64(f.Attempt.SchedulerEpoch), int64(f.Attempt.FenceEpoch),
				string(f.Subject.RepositorySubjectID), string(f.Workspace.WorkspaceID),
				commit, commit)
		},
		"workspace": func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`INSERT INTO workspace (workspace_id, attempt_id, task_id, repository_subject_id,
				                        base_sha, isolation_kind, root_path, created_at, released_at)
				 VALUES (?, ?, ?, ?, ?, 'READ_ONLY_CHECKOUT', '/var/lib/acp/workspaces/r13-dead',
				         '2026-09-12T09:00:00.000000000Z', NULL)`,
				string(ids.NewWorkspaceID()), string(spare.Attempt.AttemptID),
				string(spare.Task.TaskID), string(spare.Subject.RepositorySubjectID),
				string(f.Workspace.BaseSHA))
		},
	}

	for table, insert := range raws {
		t.Run(table, func(t *testing.T) {
			err := db.Write(ctx, insert)
			if err == nil {
				t.Fatalf("a terminal attempt wrote a %s row", table)
			}
			if !strings.Contains(err.Error(), "terminal attempt cannot") {
				t.Fatalf("expected the terminal-attempt trigger, got: %v", err)
			}
		})
	}
}

// Finding 6: TaskRun.Validate checked the current-attempt identifier's shape
// but not the completion evidence identifier's.
func TestTaskRunValidatesBothOptionalIdentifiers(t *testing.T) {
	garbageEvidence := ids.EvidenceID("not-an-identifier")
	garbageAttempt := ids.AttemptID("also-not-one")

	base := domain.TaskRun{
		TaskID:              ids.NewTaskID(),
		RepositorySubjectID: ids.NewRepositorySubjectID(),
		PacketID:            ids.NewPacketID(),
		Lane:                state.LaneOperator,
		Intent:              state.IntentModifying,
		Status:              state.TaskReady,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("a valid task run was rejected: %v", err)
	}

	withAttempt := base
	withAttempt.CurrentAttemptID = &garbageAttempt
	if err := withAttempt.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
		t.Fatalf("malformed current_attempt_id accepted: %v", err)
	}

	withEvidence := base
	withEvidence.Status = state.TaskCompleted
	withEvidence.CompletedEvidenceID = &garbageEvidence
	if err := withEvidence.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
		t.Fatalf("malformed completed_evidence_id accepted: %v", err)
	}
}
