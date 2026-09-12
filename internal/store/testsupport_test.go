package store_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// fixedNow keeps timestamps deterministic across tests.
var fixedNow = time.Date(2026, 9, 12, 9, 0, 0, 0, time.UTC)

// newDB opens a fresh migrated database in a temporary directory.
func newDB(t *testing.T) *store.DB {
	t.Helper()
	db := openFresh(t)
	if err := db.MigrateEmbedded(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// openFresh opens an unmigrated database.
func openFresh(t *testing.T) *store.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control-plane.db")
	db, err := store.Open(context.Background(), store.Config{
		Path:        path,
		Mode:        store.ModeReadWrite,
		AllowCreate: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db.WithClock(func() time.Time { return fixedNow })
}

func digestOf(s string) domain.Digest {
	sum := sha256.Sum256([]byte(s))
	return domain.Digest(hex.EncodeToString(sum[:]))
}

// sha builds a distinct, valid 40-hex commit name from a seed.
func sha(seed string) domain.CommitSHA {
	sum := sha256.Sum256([]byte(seed))
	return domain.CommitSHA(hex.EncodeToString(sum[:])[:40])
}

// fixture is a fully wired task: repository subject, approved packet, task
// run, attempt and workspace, all identity-coherent.
type fixture struct {
	Epoch     domain.Epoch
	Subject   domain.RepositorySubject
	Packet    domain.ExecutionPacket
	Task      domain.TaskRun
	Attempt   domain.WorkerAttempt
	Workspace domain.Workspace
}

func newSubject(name string) domain.RepositorySubject {
	return domain.RepositorySubject{
		RepositorySubjectID: ids.NewRepositorySubjectID(),
		GitHubNodeID:        "R_node_" + name,
		CurrentFullName:     "HeaInSeo/" + name,
		ObservedAt:          fixedNow,
	}
}

func newPacket(taskID ids.TaskID, subject ids.RepositorySubjectID, intent state.Intent, lane state.Lane) domain.ExecutionPacket {
	return domain.ExecutionPacket{
		PacketID:            ids.NewPacketID(),
		TaskID:              taskID,
		Lane:                lane,
		Intent:              intent,
		RepositorySubjectID: subject,
		SourceRevision:      "notion:rev-1",
		SourceDigest:        digestOf("source-1"),
		PacketDigest:        digestOf("packet-1"),
		ApprovedAt:          fixedNow,
		ExpiresAt:           fixedNow.Add(24 * time.Hour),
		AllowedScope:        []string{"edit:internal/store", "run:go test ./..."},
		ForbiddenScope:      []string{"edit:.github/workflows"},
		StopConditions:      []string{"architecture contradiction"},
		AcceptanceContract:  "go test ./... passes and the diff stays inside allowed scope",
		Status:              state.PacketApproved,
	}
}

// seed builds a complete, coherent task fixture, activating a scheduler epoch
// first if none is active.
func seed(t *testing.T, db *store.DB, name string, intent state.Intent, lane state.Lane) fixture {
	t.Helper()
	ctx := context.Background()

	epoch, err := db.CurrentEpoch(ctx)
	if err != nil {
		activated, aerr := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "test activation")
		if aerr != nil {
			t.Fatalf("activate scheduler: %v", aerr)
		}
		epoch = activated.Epoch
	}

	f := fixture{Epoch: epoch, Subject: newSubject(name)}
	taskID := ids.NewTaskID()
	f.Packet = newPacket(taskID, f.Subject.RepositorySubjectID, intent, lane)
	f.Task = domain.TaskRun{
		TaskID:              taskID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		PacketID:            f.Packet.PacketID,
		Lane:                lane,
		Intent:              intent,
		Status:              state.TaskReady,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
	}
	f.Attempt = domain.WorkerAttempt{
		AttemptID:           ids.NewAttemptID(),
		TaskID:              taskID,
		PacketID:            f.Packet.PacketID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		Lane:                lane,
		Intent:              intent,
		SchedulerEpoch:      epoch,
		FenceEpoch:          1,
		Status:              state.AttemptRunning,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
	}
	isolation := domain.IsolationIsolatedClone
	if intent == state.IntentReadOnly {
		isolation = domain.IsolationReadOnlyCheckout
	}
	f.Workspace = domain.Workspace{
		WorkspaceID:         ids.NewWorkspaceID(),
		AttemptID:           f.Attempt.AttemptID,
		TaskID:              taskID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		BaseSHA:             sha("base-" + name),
		IsolationKind:       isolation,
		RootPath:            "/var/lib/acp/workspaces/" + name + "-" + string(f.Attempt.AttemptID),
		CreatedAt:           fixedNow,
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if _, err := tx.ObserveRepositorySubject(ctx, f.Subject); err != nil {
			return err
		}
		if err := tx.ApprovePacket(ctx, f.Packet); err != nil {
			return err
		}
		if err := tx.CreateTaskRun(ctx, f.Task); err != nil {
			return err
		}
		if err := tx.CreateWorkerAttempt(ctx, f.Attempt); err != nil {
			return err
		}
		if err := tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, f.Attempt.AttemptID); err != nil {
			return err
		}
		return tx.CreateWorkspace(ctx, f.Workspace)
	}); err != nil {
		t.Fatalf("seed %s: %v", name, err)
	}
	return f
}

// publishFor builds a coherent publication intent for a fixture.
func publishFor(f fixture, source domain.CommitSHA, ref string) domain.PublishAttempt {
	p := domain.PublishAttempt{
		PublishAttemptID:    ids.NewPublishAttemptID(),
		TaskID:              f.Task.TaskID,
		AttemptID:           f.Attempt.AttemptID,
		SchedulerEpoch:      f.Attempt.SchedulerEpoch,
		FenceEpoch:          f.Attempt.FenceEpoch,
		WorkspaceID:         f.Workspace.WorkspaceID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		BaseSHA:             f.Workspace.BaseSHA,
		SourceCommitSHA:     source,
		TargetRef:           ref,
		Status:              state.PublishPending,
		CreatedAt:           fixedNow,
	}
	p.IdempotencyKey = domain.DeriveIdempotencyKey(p)
	return p
}

// evidenceFor builds a coherent observation for a fixture.
func evidenceFor(f fixture, published, observed domain.CommitSHA, pub *ids.PublishAttemptID, kind domain.EvidenceKind) domain.EvidenceObservation {
	return domain.EvidenceObservation{
		EvidenceID:          ids.NewEvidenceID(),
		TaskID:              f.Task.TaskID,
		AttemptID:           f.Attempt.AttemptID,
		SchedulerEpoch:      f.Attempt.SchedulerEpoch,
		FenceEpoch:          f.Attempt.FenceEpoch,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		WorkspaceID:         f.Workspace.WorkspaceID,
		PublishAttemptID:    pub,
		PublishedSHA:        published,
		ObservedSHA:         observed,
		ObservedAt:          fixedNow,
		EvidenceKind:        kind,
	}
}

// reviewEvidenceFor builds valid READ_ONLY_REVIEW evidence for a fixture:
// bound to the fixed reviewed commit, carrying an artifact digest, and
// referencing no publication (CC7).
func reviewEvidenceFor(f fixture, reviewed domain.CommitSHA, artifact string) domain.EvidenceObservation {
	return domain.EvidenceObservation{
		EvidenceID:          ids.NewEvidenceID(),
		TaskID:              f.Task.TaskID,
		AttemptID:           f.Attempt.AttemptID,
		SchedulerEpoch:      f.Attempt.SchedulerEpoch,
		FenceEpoch:          f.Attempt.FenceEpoch,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		WorkspaceID:         f.Workspace.WorkspaceID,
		PublishedSHA:        reviewed,
		ObservedSHA:         reviewed,
		ReviewedSHA:         reviewed,
		ArtifactDigest:      digestOf(artifact),
		ObservedAt:          fixedNow,
		EvidenceKind:        domain.EvidenceReadOnlyReview,
	}
}

func eventFor(epoch domain.Epoch, typ string) domain.Event {
	return domain.Event{
		SchedulerEpoch: epoch,
		EventID:        ids.NewEventID(),
		OccurredAt:     fixedNow,
		EventType:      typ,
		SubjectKind:    domain.SubjectScheduler,
		SubjectID:      "test",
		Fields:         map[string]any{"note": fmt.Sprintf("%s in epoch %d", typ, int64(epoch))},
	}
}

// preconditionFixture is a coherent publication plus the live state it would
// be validated against.
type preconditionFixture struct {
	pub  domain.PublishAttempt
	live domain.PublishPreconditions
}

// fixtureForPreconditions builds a coherent publication and matching live
// state without needing a database.
func fixtureForPreconditions() preconditionFixture {
	taskID := ids.NewTaskID()
	attemptID := ids.NewAttemptID()
	workspaceID := ids.NewWorkspaceID()
	subjectID := ids.NewRepositorySubjectID()

	pub := domain.PublishAttempt{
		PublishAttemptID:    ids.NewPublishAttemptID(),
		TaskID:              taskID,
		AttemptID:           attemptID,
		SchedulerEpoch:      1,
		FenceEpoch:          1,
		WorkspaceID:         workspaceID,
		RepositorySubjectID: subjectID,
		BaseSHA:             sha("precondition-base"),
		SourceCommitSHA:     sha("precondition-source"),
		TargetRef:           "refs/heads/m0/preconditions",
		Status:              state.PublishPending,
		CreatedAt:           fixedNow,
	}
	pub.IdempotencyKey = domain.DeriveIdempotencyKey(pub)

	return preconditionFixture{
		pub: pub,
		live: domain.PublishPreconditions{
			CurrentSchedulerEpoch: 1,
			TaskCurrentAttemptID:  attemptID,
			AttemptStatus:         state.AttemptVerifying,
			AttemptFenceEpoch:     1,
			WorkspaceAttemptID:    attemptID,
			WorkspaceReleased:     false,
			RepositorySubjectID:   subjectID,
			CommitInWorkspace:     true,
			PacketStatus:          state.PacketApproved,
			PacketExpiresAt:       fixedNow.Add(24 * time.Hour),
			Now:                   fixedNow,
		},
	}
}
