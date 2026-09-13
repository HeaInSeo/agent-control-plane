package store_test

// Regressions for the findings of the twelfth independent review, of head
// 47acc19, plus the two Go-only invariants and the tokenizer gap it noted
// without filing.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// countEvents returns how many events of a type exist in an epoch.
func countEvents(t *testing.T, db *store.DB, epoch domain.Epoch, eventType string) int {
	t.Helper()
	ctx := context.Background()
	n := 0
	if err := db.Read(ctx, func(tx *store.Tx) error {
		events, err := tx.EventsInEpoch(ctx, epoch)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.EventType == eventType {
				n++
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	return n
}

// Finding 1: re-asserting the current status rewrote updated_at and appended
// a phantom transition, on the recovery path updated_at was added to serve.
func TestReassertingPublishStatusIsANoOp(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r12-noop", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("r12-noop-commit"), "refs/heads/m0/r12-noop")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	applied := fixedNow.Add(time.Hour)
	at := db.WithClock(func() time.Time { return applied })
	if err := at.Write(ctx, func(tx *store.Tx) error {
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied)
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := countEvents(t, db, f.Epoch, "publish_attempt.status_changed"); got != 1 {
		t.Fatalf("%d transition events after one move, want 1", got)
	}

	// Re-asserting, as the crash-recovery flow and a periodic reconciler both
	// do, must change nothing.
	later := applied.Add(3 * time.Hour)
	again := db.WithClock(func() time.Time { return later })
	for range 3 {
		if err := again.Write(ctx, func(tx *store.Tx) error {
			return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied)
		}); err != nil {
			t.Fatalf("re-assert: %v", err)
		}
	}

	if got := countEvents(t, db, f.Epoch, "publish_attempt.status_changed"); got != 1 {
		t.Fatalf("%d transition events after re-asserting, want 1", got)
	}
	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if !stored.UpdatedAt.Equal(applied) {
			t.Fatalf("updated_at moved to %v on a no-op; want %v", stored.UpdatedAt, applied)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// A terminal status can be re-asserted too, without becoming a move.
	if err := again.Write(ctx, func(tx *store.Tx) error {
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishObserved); err != nil {
			return err
		}
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishObserved)
	}); err != nil {
		t.Fatalf("observe then re-assert: %v", err)
	}
	if got := countEvents(t, db, f.Epoch, "publish_attempt.status_changed"); got != 2 {
		t.Fatalf("%d transition events after one further move, want 2", got)
	}
}

// Finding 2: a PENDING intent has by definition never moved, so its
// updated_at must equal its created_at whatever the caller supplied.
func TestPendingPublicationHasNotMoved(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r12-pending", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("r12-pending-commit"), "refs/heads/m0/r12-pending")
	pub.UpdatedAt = pub.CreatedAt.Add(2 * time.Hour)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if !stored.UpdatedAt.Equal(stored.CreatedAt) {
			t.Fatalf("a never-moved intent reports updated_at %v against created_at %v",
				stored.UpdatedAt, stored.CreatedAt)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 3: event.task_id and event.attempt_id carried no foreign key, so a
// wrong pointer wrote a permanently misattributed row into append-only
// history.
func TestEventAttributionIsReferentiallyChecked(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r12-event-fk", state.IntentReadOnly, state.LaneReview)

	ghostTask := ids.NewTaskID()
	ghostAttempt := ids.NewAttemptID()

	for name, mutate := range map[string]func(*domain.Event){
		"task that does not exist":    func(e *domain.Event) { e.TaskID = &ghostTask },
		"attempt that does not exist": func(e *domain.Event) { e.AttemptID = &ghostAttempt },
	} {
		t.Run(name, func(t *testing.T) {
			e := eventFor(f.Epoch, "r12.misattributed")
			mutate(&e)
			err := db.Write(ctx, func(tx *store.Tx) error {
				_, err := tx.AppendEvent(ctx, e)
				return err
			})
			if err == nil {
				t.Fatal("a misattributed event was appended")
			}
			if !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
				t.Fatalf("expected a foreign key failure, got: %v", err)
			}
		})
	}

	// Real identifiers, and no identifiers at all, both remain valid.
	e := eventFor(f.Epoch, "r12.attributed")
	e.TaskID = &f.Task.TaskID
	e.AttemptID = &f.Attempt.AttemptID
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if _, err := tx.AppendEvent(ctx, e); err != nil {
			return err
		}
		_, err := tx.AppendEvent(ctx, eventFor(f.Epoch, "r12.unattributed"))
		return err
	}); err != nil {
		t.Fatalf("valid attribution was refused: %v", err)
	}
}

// Noted but not filed by the review: invariants enforced only in Go, which
// earlier rounds treated as findings. These now hold in the schema too.
func TestGoOnlyInvariantsAreAlsoEnforcedBySchema(t *testing.T) {
	ctx := context.Background()

	t.Run("released workspace cannot publish or record evidence", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r12-released", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
		}); err != nil {
			t.Fatalf("release: %v", err)
		}

		commit := string(sha("r12-released-commit"))
		raws := map[string]func(tx *store.Tx) error{
			"publish_attempt": func(tx *store.Tx) error {
				return tx.ExecForTest(ctx,
					`INSERT INTO publish_attempt (publish_attempt_id, task_id, attempt_id,
					                              scheduler_epoch, fence_epoch, workspace_id,
					                              repository_subject_id, base_sha, source_commit_sha,
					                              target_ref, idempotency_key, status,
					                              created_at, updated_at)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'refs/heads/m0/r12', 'r12-key', 'PENDING',
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
		}
		for table, insert := range raws {
			err := db.Write(ctx, insert)
			if err == nil {
				t.Fatalf("a released workspace wrote a %s row", table)
			}
			if !strings.Contains(err.Error(), "released workspace cannot") {
				t.Fatalf("%s: expected the released-workspace trigger, got: %v", table, err)
			}
		}
	})

	t.Run("current attempt must bind the current epoch", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r12-epoch-ptr", state.IntentModifying, state.LaneOperator)
		if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "successor"); err != nil {
			t.Fatalf("activate: %v", err)
		}
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`UPDATE task_run SET current_attempt_id = NULL WHERE task_id = ?`,
				string(f.Task.TaskID))
		})
		if err != nil {
			t.Fatalf("clear pointer: %v", err)
		}
		err = db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`UPDATE task_run SET current_attempt_id = ? WHERE task_id = ?`,
				string(f.Attempt.AttemptID), string(f.Task.TaskID))
		})
		if err == nil {
			t.Fatal("a stale-epoch attempt became the task's current attempt")
		}
		if !strings.Contains(err.Error(), "current scheduler epoch") {
			t.Fatalf("expected the epoch trigger to fire, got: %v", err)
		}
	})
}
