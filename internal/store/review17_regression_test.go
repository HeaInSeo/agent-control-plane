package store_test

// Regressions for the findings of the seventeenth independent review, of head
// e29e945. Each test reproduces the reported defect and pins the fix.

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

// Finding 1: UNIQUE(root_path) rules out aliases of one string, not
// containment, so a second attempt's workspace could live inside another's
// isolated clone — visible to its git status, publishable by it, and
// destroyed together with it.
func TestWorkspacesCannotNest(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r17-nesting", state.IntentModifying, state.LaneOperator)
	spare := seedAttemptWithoutWorkspace(t, db, "r17-nesting-spare")

	for name, path := range map[string]string{
		"inside the existing workspace":        f.Workspace.RootPath + "/review",
		"deeper inside the existing workspace": f.Workspace.RootPath + "/a/b/c",
		"containing the existing workspace":    "/var/lib/acp/workspaces",
	} {
		t.Run(name, func(t *testing.T) {
			ws := domain.Workspace{
				WorkspaceID:         ids.NewWorkspaceID(),
				AttemptID:           spare.Attempt.AttemptID,
				TaskID:              spare.Task.TaskID,
				RepositorySubjectID: spare.Subject.RepositorySubjectID,
				BaseSHA:             sha("r17-nesting-base"),
				IsolationKind:       domain.IsolationReadOnlyCheckout,
				RootPath:            path,
				CreatedAt:           fixedNow,
			}
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.CreateWorkspace(ctx, ws)
			}); !errors.Is(err, store.ErrWorkspaceConflict) {
				t.Fatalf("want ErrWorkspaceConflict for %q, got %v", path, err)
			}
		})
	}

	// A sibling path that merely shares a prefix string is fine — the check
	// is on path components, not on raw string prefixes.
	sibling := domain.Workspace{
		WorkspaceID:         ids.NewWorkspaceID(),
		AttemptID:           spare.Attempt.AttemptID,
		TaskID:              spare.Task.TaskID,
		RepositorySubjectID: spare.Subject.RepositorySubjectID,
		BaseSHA:             sha("r17-nesting-base"),
		IsolationKind:       domain.IsolationReadOnlyCheckout,
		RootPath:            f.Workspace.RootPath + "-sibling",
		CreatedAt:           fixedNow,
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkspace(ctx, sibling)
	}); err != nil {
		t.Fatalf("a sibling path sharing a string prefix was refused: %v", err)
	}

	// And the schema refuses nesting too.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO workspace (workspace_id, attempt_id, task_id, repository_subject_id,
			                        base_sha, isolation_kind, root_path, created_at, released_at)
			 VALUES (?, ?, ?, ?, ?, 'READ_ONLY_CHECKOUT', ?,
			         '2026-09-12T09:00:00.000000000Z', NULL)`,
			string(ids.NewWorkspaceID()), string(spare.Attempt.AttemptID),
			string(spare.Task.TaskID), string(spare.Subject.RepositorySubjectID),
			string(sha("r17-raw-base")), f.Workspace.RootPath+"/raw")
	})
	if err == nil {
		t.Fatal("the schema accepted a nested workspace")
	}
	if !strings.Contains(err.Error(), "cannot nest inside another workspace") {
		t.Fatalf("expected the nesting trigger to fire, got: %v", err)
	}
}

// Finding 2: read-only completion never bound reviewed_sha to the workspace
// the review actually ran in, so it was only compared with other
// caller-supplied values — the self-certification CC7 forbids.
func TestReviewEvidenceBindsTheWorkspaceCommit(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r17-review-binding", state.IntentReadOnly, state.LaneReview)

	// A commit unrelated to the workspace checkout.
	unrelated := sha("r17-unrelated")
	if unrelated == f.Workspace.BaseSHA {
		t.Fatal("test is vacuous: the commits coincide")
	}
	ev := domain.EvidenceObservation{
		EvidenceID:          ids.NewEvidenceID(),
		TaskID:              f.Task.TaskID,
		AttemptID:           f.Attempt.AttemptID,
		SchedulerEpoch:      f.Attempt.SchedulerEpoch,
		FenceEpoch:          f.Attempt.FenceEpoch,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		WorkspaceID:         f.Workspace.WorkspaceID,
		PublishedSHA:        unrelated,
		ObservedSHA:         unrelated,
		ReviewedSHA:         unrelated,
		ArtifactDigest:      digestOf("r17-artifact"),
		ObservedAt:          fixedNow,
		EvidenceKind:        domain.EvidenceReadOnlyReview,
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordEvidence(ctx, ev)
	}); !errors.Is(err, domain.ErrEvidenceMisattributed) {
		t.Fatalf("want ErrEvidenceMisattributed, got %v", err)
	}

	// The workspace's own commit is accepted and completes the task.
	bound := reviewEvidenceFor(f, "r17-bound-artifact")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordEvidence(ctx, bound); err != nil {
			return err
		}
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, bound.EvidenceID)
	}); err != nil {
		t.Fatalf("workspace-bound review evidence was refused: %v", err)
	}
}

// Finding 3: the approval window had no lower bound, so a future-dated packet
// granted live authority before its own stated start.
func TestPacketCannotBeApprovedInTheFuture(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r17-future", state.IntentModifying, state.LaneOperator)

	future := newPacket(ids.NewTaskID(), f.Subject.RepositorySubjectID,
		state.IntentModifying, state.LaneOperator)
	future.ApprovedAt = fixedNow.Add(30 * 24 * time.Hour)
	future.ExpiresAt = future.ApprovedAt.Add(24 * time.Hour)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ApprovePacket(ctx, future)
	}); !errors.Is(err, domain.ErrPacketInvalid) {
		t.Fatalf("want ErrPacketInvalid, got %v", err)
	}

	// Authorize refuses it too, so a packet stored by another route cannot
	// grant authority before its start.
	binding := domain.SourceBinding{Revision: future.SourceRevision, Digest: future.SourceDigest}
	if err := future.Authorize(fixedNow, binding); !errors.Is(err, domain.ErrPacketInvalid) {
		t.Fatalf("Authorize: want ErrPacketInvalid, got %v", err)
	}
	// And accepts it once the window has opened.
	if err := future.Authorize(future.ApprovedAt, binding); err != nil {
		t.Fatalf("Authorize at the approval instant: %v", err)
	}
}

// Finding 4: repository_subject was the one entity whose own primary key was
// mutable, which is the identity fork CC5 exists to prevent.
func TestRepositorySubjectIdentifierIsImmutable(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	// A subject nothing references yet: referenced rows are only incidentally
	// protected by their foreign keys.
	subject := newSubject("r17-unreferenced")
	if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "r17 identity"); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.ObserveRepositorySubject(ctx, subject)
		return err
	}); err != nil {
		t.Fatalf("observe: %v", err)
	}

	for name, stmt := range map[string]string{
		"repository_subject_id": `UPDATE repository_subject SET repository_subject_id = '` +
			string(ids.NewRepositorySubjectID()) + `' WHERE github_node_id = ?`,
		"github_node_id": `UPDATE repository_subject SET github_node_id = 'R_node_other' WHERE github_node_id = ?`,
	} {
		t.Run(name, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, stmt, subject.GitHubNodeID)
			})
			if err == nil {
				t.Fatalf("repository_subject accepted a change to %s", name)
			}
			if !strings.Contains(err.Error(), "identity is immutable") {
				t.Fatalf("expected the immutability trigger to fire, got: %v", err)
			}
		})
	}

	// The alias and its observation time remain mutable, which is the point
	// of separating them from identity.
	renamed := subject
	renamed.CurrentFullName = "HeaInSeo/r17-renamed"
	renamed.ObservedAt = fixedNow.Add(time.Hour)
	later := db.WithClock(func() time.Time { return renamed.ObservedAt })
	if err := later.Write(ctx, func(tx *store.Tx) error {
		got, err := tx.ObserveRepositorySubject(ctx, renamed)
		if err != nil {
			return err
		}
		if got.RepositorySubjectID != subject.RepositorySubjectID {
			t.Fatalf("rename changed the identifier to %s", string(got.RepositorySubjectID))
		}
		return nil
	}); err != nil {
		t.Fatalf("rename: %v", err)
	}
}
