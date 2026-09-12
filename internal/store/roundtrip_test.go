package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

func TestEntityRoundtrip(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "roundtrip", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("round-commit"), "refs/heads/m0/roundtrip")
	ev := evidenceFor(f, pub.SourceCommitSHA, pub.SourceCommitSHA, &pub.PublishAttemptID, domain.EvidenceBranchHead)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied); err != nil {
			return err
		}
		return tx.RecordEvidence(ctx, ev)
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		subject, err := tx.RepositorySubject(ctx, f.Subject.RepositorySubjectID)
		if err != nil {
			return err
		}
		if subject.GitHubNodeID != f.Subject.GitHubNodeID || subject.CurrentFullName != f.Subject.CurrentFullName {
			t.Fatalf("repository subject roundtrip: got %+v want %+v", subject, f.Subject)
		}

		packet, err := tx.Packet(ctx, f.Packet.PacketID)
		if err != nil {
			return err
		}
		if packet.SourceRevision != f.Packet.SourceRevision ||
			packet.SourceDigest != f.Packet.SourceDigest ||
			packet.PacketDigest != f.Packet.PacketDigest ||
			packet.AcceptanceContract != f.Packet.AcceptanceContract ||
			packet.Status != f.Packet.Status ||
			!packet.ApprovedAt.Equal(f.Packet.ApprovedAt) ||
			!packet.ExpiresAt.Equal(f.Packet.ExpiresAt) {
			t.Fatalf("packet roundtrip: got %+v want %+v", packet, f.Packet)
		}
		if len(packet.AllowedScope) != len(f.Packet.AllowedScope) {
			t.Fatalf("allowed scope roundtrip: got %v want %v", packet.AllowedScope, f.Packet.AllowedScope)
		}
		for i := range packet.AllowedScope {
			if packet.AllowedScope[i] != f.Packet.AllowedScope[i] {
				t.Fatalf("allowed scope roundtrip: got %v want %v", packet.AllowedScope, f.Packet.AllowedScope)
			}
		}

		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.CurrentAttemptID == nil || *run.CurrentAttemptID != f.Attempt.AttemptID {
			t.Fatalf("task current attempt roundtrip: %+v", run)
		}
		if err := run.Validate(); err != nil {
			t.Fatalf("stored task run fails validation: %v", err)
		}

		attempt, err := tx.WorkerAttempt(ctx, f.Attempt.AttemptID)
		if err != nil {
			return err
		}
		if attempt.SchedulerEpoch != f.Attempt.SchedulerEpoch || attempt.FenceEpoch != f.Attempt.FenceEpoch {
			t.Fatalf("attempt roundtrip: got %+v want %+v", attempt, f.Attempt)
		}

		ws, err := tx.Workspace(ctx, f.Workspace.WorkspaceID)
		if err != nil {
			return err
		}
		if ws.AttemptID != f.Attempt.AttemptID || ws.BaseSHA != f.Workspace.BaseSHA ||
			ws.IsolationKind != domain.IsolationIsolatedClone || ws.RootPath != f.Workspace.RootPath {
			t.Fatalf("workspace roundtrip: got %+v want %+v", ws, f.Workspace)
		}

		stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if stored.SourceCommitSHA != pub.SourceCommitSHA || stored.BaseSHA != pub.BaseSHA ||
			stored.TargetRef != pub.TargetRef || stored.IdempotencyKey != pub.IdempotencyKey ||
			stored.Status != state.PublishApplied {
			t.Fatalf("publish roundtrip: got %+v want %+v", stored, pub)
		}

		observed, err := tx.Evidence(ctx, ev.EvidenceID)
		if err != nil {
			return err
		}
		if observed.PublishedSHA != ev.PublishedSHA || observed.ObservedSHA != ev.ObservedSHA ||
			observed.EvidenceKind != ev.EvidenceKind ||
			observed.PublishAttemptID == nil || *observed.PublishAttemptID != pub.PublishAttemptID {
			t.Fatalf("evidence roundtrip: got %+v want %+v", observed, ev)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestTransactionRollbackLeavesNoPartialIdentity(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rollback", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("rollback-commit"), "refs/heads/m0/rollback")
	boom := errors.New("deliberate failure after partial writes")

	// Write several identity relations, then fail. None may survive.
	err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ctx, eventFor(f.Epoch, "publish.recorded")); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want the caller's error back unchanged, got %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		if _, err := tx.PublishAttempt(ctx, pub.PublishAttemptID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("rolled-back publish attempt is still present: %v", err)
		}
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.EventType == "publish.recorded" {
				t.Fatal("rolled-back event survived in append-only history")
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// The same identity must still be usable afterwards: a rolled-back
	// attempt must not burn its idempotency key.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("re-recording after rollback: %v", err)
	}
}

func TestEventSequenceIsMonotonicWithinEpoch(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "events", state.IntentReadOnly, state.LaneReview)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		for i := range 5 {
			seq, err := tx.AppendEvent(ctx, eventFor(f.Epoch, "probe"))
			if err != nil {
				return err
			}
			// Epoch 1 already carries the scheduler.activated event.
			if want := int64(i + 2); seq != want {
				t.Fatalf("append %d got seq %d, want %d", i, seq, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		if len(events) != 6 {
			t.Fatalf("want 6 events, got %d", len(events))
		}
		for i, e := range events {
			if e.Seq != int64(i+1) {
				t.Fatalf("event %d has seq %d", i, e.Seq)
			}
			if e.SchedulerEpoch != f.Epoch {
				t.Fatalf("event %d bound to epoch %d", i, int64(e.SchedulerEpoch))
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestCompletionIsDerivedFromEvidence(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "completion", state.IntentModifying, state.LaneOperator)

	commit := sha("completion-commit")
	pub := publishFor(f, commit, "refs/heads/m0/completion")
	ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishObserved); err != nil {
			return err
		}
		if err := tx.RecordEvidence(ctx, ev); err != nil {
			return err
		}
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status != state.TaskCompleted {
			t.Fatalf("task status is %q, want COMPLETED", string(run.Status))
		}
		if run.CompletedEvidenceID == nil || *run.CompletedEvidenceID != ev.EvidenceID {
			t.Fatalf("completed task is not bound to its evidence: %+v", run)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestRepositorySubjectRenameKeepsOneIdentity(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "renamed", state.IntentModifying, state.LaneOperator)

	renamed := f.Subject
	renamed.CurrentFullName = "HeaInSeo/renamed-elsewhere"
	renamed.RepositorySubjectID = ids.NewRepositorySubjectID() // a caller minting a new id
	renamed.ObservedAt = fixedNow.Add(time.Hour)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		got, err := tx.ObserveRepositorySubject(ctx, renamed)
		if err != nil {
			return err
		}
		// The pre-existing identity wins: a rename is an alias change.
		if got.RepositorySubjectID != f.Subject.RepositorySubjectID {
			t.Fatalf("rename produced identity %s, want the existing %s",
				string(got.RepositorySubjectID), string(f.Subject.RepositorySubjectID))
		}
		if got.CurrentFullName != renamed.CurrentFullName {
			t.Fatalf("rename did not update the alias: %+v", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("observe rename: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		count, err := tx.QueryIntForTest(ctx,
			`SELECT count(*) FROM repository_subject WHERE github_node_id = ?`, f.Subject.GitHubNodeID)
		if err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("rename created %d repository subjects for one node id", count)
		}
		// The old alias must no longer resolve.
		if _, err := tx.RepositorySubjectByFullName(ctx, f.Subject.CurrentFullName); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("stale alias still resolves: %v", err)
		}
		byNode, err := tx.RepositorySubjectByNodeID(ctx, f.Subject.GitHubNodeID)
		if err != nil {
			return err
		}
		if byNode.CurrentFullName != renamed.CurrentFullName {
			t.Fatalf("node id lookup returned stale alias: %+v", byNode)
		}
		// The rename is recorded in history.
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		var sawRename bool
		for _, e := range events {
			if e.EventType == "repository_subject.renamed" {
				sawRename = true
			}
		}
		if !sawRename {
			t.Fatal("rename was not recorded as an event")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}
