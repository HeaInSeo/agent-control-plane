package store_test

// Regressions for the findings of the tenth independent review, of head
// 8b16fcf. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1: AttemptStatus was the one precondition field that did not fail
// closed on its zero value, because "".IsTerminal() is false.
func TestPublishGateValidatesAttemptStatus(t *testing.T) {
	f := fixtureForPreconditions()

	if err := domain.CheckPublishPreconditions(f.pub, f.live); err != nil {
		t.Fatalf("a coherent publication was rejected: %v", err)
	}
	for _, status := range []state.WorkerAttemptStatus{"", "BOGUS", "completed"} {
		t.Run("status "+string(status), func(t *testing.T) {
			live := f.live
			live.AttemptStatus = status
			if err := domain.CheckPublishPreconditions(f.pub, live); !errors.Is(err, domain.ErrPublishBindingInvalid) {
				t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
			}
		})
	}
}

// Finding 2: the task-liveness fence was enforced only in Go. The schema is
// meant to hold invariants a future code path could forget.
func TestTerminalTaskFenceIsEnforcedBySchema(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r10-schema-fence", state.IntentModifying, state.LaneOperator)

	// A second live attempt, so the workspace insert below is refused for the
	// task's sake rather than the attempt's.
	successor := f.Attempt
	successor.AttemptID = ids.NewAttemptID()
	successor.FenceEpoch = f.Attempt.FenceEpoch + 1
	successor.Status = state.AttemptRunning
	if err := db.Write(ctx, func(tx *store.Tx) error {
		// Free the repository's modifying slot before admitting the successor.
		if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed); err != nil {
			return err
		}
		return tx.CreateWorkerAttempt(ctx, successor)
	}); err != nil {
		t.Fatalf("create successor: %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
	}); err != nil {
		t.Fatalf("abandon: %v", err)
	}

	commit := string(sha("r10-schema-commit"))
	raws := map[string]func(tx *store.Tx) error{
		"publish_attempt": func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`INSERT INTO publish_attempt (publish_attempt_id, task_id, attempt_id,
				                              scheduler_epoch, fence_epoch, workspace_id,
				                              repository_subject_id, base_sha, source_commit_sha,
				                              target_ref, idempotency_key, status, created_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'refs/heads/m0/r10', 'r10-key', 'PENDING',
				         '2026-09-12T09:00:00.000000000Z')`,
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
				 VALUES (?, ?, ?, ?, ?, 'ISOLATED_CLONE', '/var/lib/acp/workspaces/r10-raw',
				         '2026-09-12T09:00:00.000000000Z', NULL)`,
				string(ids.NewWorkspaceID()), string(successor.AttemptID), string(f.Task.TaskID),
				string(f.Subject.RepositorySubjectID), string(f.Workspace.BaseSHA))
		},
	}

	for table, insert := range raws {
		t.Run(table, func(t *testing.T) {
			err := db.Write(ctx, insert)
			if err == nil {
				t.Fatalf("a terminal task wrote a %s row", table)
			}
			if !strings.Contains(err.Error(), "terminal task cannot") {
				t.Fatalf("expected a terminal-task trigger to fire, got: %v", err)
			}
		})
	}
}

// Finding 4: a mis-wired publication surfaced as a raw driver constraint
// error, which a caller cannot tell apart from a corrupt database.
func TestPublicationBindingRefusalsAreTyped(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	owner := seed(t, db, "r10-owner", state.IntentModifying, state.LaneOperator)
	review := seed(t, db, "r10-reviewer", state.IntentReadOnly, state.LaneReview)
	other := seed(t, db, "r10-other", state.IntentModifying, state.LaneGuardrail)

	commit := sha("r10-typed-commit")

	t.Run("foreign workspace", func(t *testing.T) {
		p := publishFor(other, commit, "refs/heads/m0/r10-foreign")
		p.WorkspaceID = owner.Workspace.WorkspaceID
		p.IdempotencyKey = ""
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, p)
		}); !errors.Is(err, domain.ErrPublishBindingInvalid) {
			t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
		}
	})

	t.Run("read-only attempt", func(t *testing.T) {
		p := publishFor(review, commit, "refs/heads/m0/r10-readonly")
		p.IdempotencyKey = ""
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, p)
		})
		if !errors.Is(err, domain.ErrPublishBindingInvalid) {
			t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
		}
		if strings.Contains(err.Error(), "constraint failed") {
			t.Fatalf("a binding refusal surfaced as a driver error: %v", err)
		}
	})
}

// Finding 5: ownership was published after the transaction lock was released,
// so a goroutine waiting on that lock could snapshot a superseded ownership
// and be refused as unowned.
func TestActivationPublishesOwnershipBeforeReleasingTheLock(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	// A waiter blocked on the transaction lock for the whole activation.
	const waiters = 8
	start := make(chan struct{})
	results := make(chan error, waiters)
	for range waiters {
		go func() {
			<-start
			results <- db.Write(ctx, func(tx *store.Tx) error {
				epoch, err := tx.CurrentEpoch(ctx)
				if err != nil {
					return err
				}
				_, err = tx.AppendEvent(ctx, eventFor(epoch, "post.activation.race"))
				return err
			})
		}()
	}

	activated := make(chan error, 1)
	go func() {
		close(start)
		_, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "racing activation")
		activated <- err
	}()

	if err := <-activated; err != nil {
		t.Fatalf("activate: %v", err)
	}
	for range waiters {
		select {
		case err := <-results:
			if errors.Is(err, store.ErrNoSchedulerOwnership) {
				t.Fatalf("a waiter was refused as unowned although activation had committed: %v", err)
			}
			// A waiter that ran entirely before activation legitimately sees
			// no epoch at all; anything else must have succeeded.
			if err != nil && !errors.Is(err, store.ErrNoSchedulerEpoch) {
				t.Fatalf("waiter failed: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("a waiter never completed")
		}
	}

	current, err := db.CurrentEpoch(ctx)
	if err != nil {
		t.Fatalf("current epoch: %v", err)
	}
	if db.OwnedEpoch() != current {
		t.Fatalf("handle owns %d while current is %d", int64(db.OwnedEpoch()), int64(current))
	}
}
