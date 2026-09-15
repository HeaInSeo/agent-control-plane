package store_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
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
		UpdatedAt:           fixedNow,
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

// reviewEvidenceFor builds valid READ_ONLY_REVIEW evidence for a fixture.
//
// The reviewed commit is the fixture workspace's base_sha, not a free
// parameter: review evidence names the commit the review actually ran
// against, so it has to be the one the workspace was checked out at (CC7).
func reviewEvidenceFor(f fixture, artifact string) domain.EvidenceObservation {
	reviewed := f.Workspace.BaseSHA
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
		UpdatedAt:           fixedNow,
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
			WorkspaceLive:         true,
			RepositorySubjectID:   subjectID,
			CommitInWorkspace:     true,
			TaskStatus:            state.TaskRunning,
			PacketStatus:          state.PacketApproved,
			PacketExpiresAt:       fixedNow.Add(24 * time.Hour),
			Now:                   fixedNow,
		},
	}
}

// seedAttemptWithoutWorkspace builds a read-only task on its own repository
// subject with a live attempt that owns no workspace yet.
//
// Useful where a test needs to exercise workspace creation without tripping
// the per-attempt workspace uniqueness or the per-repository modifying slot.
func seedAttemptWithoutWorkspace(t *testing.T, db *store.DB, name string) fixture {
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
	f.Packet = newPacket(taskID, f.Subject.RepositorySubjectID, state.IntentReadOnly, state.LaneReview)
	f.Task = domain.TaskRun{
		TaskID:              taskID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		PacketID:            f.Packet.PacketID,
		Lane:                state.LaneReview,
		Intent:              state.IntentReadOnly,
		Status:              state.TaskReady,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
	}
	f.Attempt = domain.WorkerAttempt{
		AttemptID:           ids.NewAttemptID(),
		TaskID:              taskID,
		PacketID:            f.Packet.PacketID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		Lane:                state.LaneReview,
		Intent:              state.IntentReadOnly,
		SchedulerEpoch:      epoch,
		FenceEpoch:          1,
		Status:              state.AttemptRunning,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
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
		return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, f.Attempt.AttemptID)
	}); err != nil {
		t.Fatalf("seed %s: %v", name, err)
	}
	return f
}

// corruptInterior overwrites the middle of a database file while leaving the
// SQLite header intact, so the damage has to be caught by an integrity check
// rather than by the header sniff.
func corruptInterior(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	garbage := make([]byte, info.Size()/2)
	for i := range garbage {
		garbage[i] = 0xBA
	}
	if _, err := f.WriteAt(garbage, 4096); err != nil {
		t.Fatal(err)
	}
}

// seedAttemptUnpointed builds a read-only task on its own repository subject
// with a live attempt, leaving the task's current-attempt pointer unset.
//
// Useful where a test needs to exercise the first write of that pointer:
// clearing it once set is forbidden by the schema, so a test cannot simply
// reset a pointed task.
func seedAttemptUnpointed(t *testing.T, db *store.DB, name string) fixture {
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
	f.Packet = newPacket(taskID, f.Subject.RepositorySubjectID, state.IntentReadOnly, state.LaneReview)
	f.Task = domain.TaskRun{
		TaskID:              taskID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		PacketID:            f.Packet.PacketID,
		Lane:                state.LaneReview,
		Intent:              state.IntentReadOnly,
		Status:              state.TaskReady,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
	}
	f.Attempt = domain.WorkerAttempt{
		AttemptID:           ids.NewAttemptID(),
		TaskID:              taskID,
		PacketID:            f.Packet.PacketID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		Lane:                state.LaneReview,
		Intent:              state.IntentReadOnly,
		SchedulerEpoch:      epoch,
		FenceEpoch:          1,
		Status:              state.AttemptRunning,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
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
		return tx.CreateWorkerAttempt(ctx, f.Attempt)
	}); err != nil {
		t.Fatalf("seed %s: %v", name, err)
	}
	return f
}
