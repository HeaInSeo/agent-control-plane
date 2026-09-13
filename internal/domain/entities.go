package domain

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

// ErrInvalidEntity is returned when a durable entity fails validation.
var ErrInvalidEntity = errors.New("invalid entity")

// Epoch is a scheduler ownership generation (CC4).
//
// An Epoch advances only on successful scheduler singleton ownership
// acquisition plus a successful activation transaction. Opening the database —
// for an integrity check, a backup, or read-only inspection — must never
// advance it.
type Epoch int64

// Validate reports whether the epoch is a usable generation number.
func (e Epoch) Validate() error {
	if e < 1 {
		return fmt.Errorf("%w: epoch %d must be >= 1", ErrInvalidEntity, int64(e))
	}
	return nil
}

// FenceEpoch is a per-task attempt fencing token. It is monotonic within a
// TaskID and is independent of the scheduler epoch: a scheduler restart does
// not renumber a task's attempts, and a task's new attempt does not imply a
// new scheduler generation.
type FenceEpoch int64

// Validate reports whether the fence epoch is usable.
func (f FenceEpoch) Validate() error {
	if f < 1 {
		return fmt.Errorf("%w: fence_epoch %d must be >= 1", ErrInvalidEntity, int64(f))
	}
	return nil
}

// SchedulerEpoch is the durable record of one scheduler ownership generation.
type SchedulerEpoch struct {
	Epoch            Epoch
	OwnerID          ids.SchedulerOwnerID
	ActivatedAt      time.Time
	ActivationReason string
	ReleasedAt       *time.Time
}

// Validate checks the scheduler epoch record.
func (s SchedulerEpoch) Validate() error {
	if err := s.Epoch.Validate(); err != nil {
		return err
	}
	if err := s.OwnerID.Validate(); err != nil {
		return fmt.Errorf("%w: owner_id: %w", ErrInvalidEntity, err)
	}
	if s.ActivatedAt.IsZero() {
		return fmt.Errorf("%w: activated_at is zero", ErrInvalidEntity)
	}
	if s.ActivationReason == "" {
		return fmt.Errorf("%w: activation_reason is empty", ErrInvalidEntity)
	}
	return nil
}

// RepositorySubject is the control plane's stable identity for a repository
// (CC5).
//
// GitHubNodeID is the identity. CurrentFullName is a mutable human-readable
// alias: a rename changes the alias of the same subject, it does not create a
// second subject. Lookups by alias are therefore advisory and must fail closed
// when ambiguous.
type RepositorySubject struct {
	RepositorySubjectID ids.RepositorySubjectID
	GitHubNodeID        string
	CurrentFullName     string
	ObservedAt          time.Time
}

// Validate checks the repository subject record.
func (r RepositorySubject) Validate() error {
	if err := r.RepositorySubjectID.Validate(); err != nil {
		return fmt.Errorf("%w: repository_subject_id: %w", ErrInvalidEntity, err)
	}
	if r.GitHubNodeID == "" {
		return fmt.Errorf("%w: github_node_id is empty; owner/name is not an identity", ErrInvalidEntity)
	}
	if r.CurrentFullName == "" {
		return fmt.Errorf("%w: current_full_name is empty", ErrInvalidEntity)
	}
	if r.ObservedAt.IsZero() {
		return fmt.Errorf("%w: observed_at is zero", ErrInvalidEntity)
	}
	return nil
}

// TaskRun is the durable scheduling subject. Its stable identity is TaskID.
//
// A TaskRun is not an execution: executions are WorkerAttempts. A TaskRun
// reaches COMPLETED only through evidence-derived completion (I1).
type TaskRun struct {
	TaskID              ids.TaskID
	RepositorySubjectID ids.RepositorySubjectID
	PacketID            ids.PacketID
	Lane                state.Lane
	Intent              state.Intent
	Status              state.TaskRunStatus
	CurrentAttemptID    *ids.AttemptID
	CompletedEvidenceID *ids.EvidenceID
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Validate checks the task run record.
func (t TaskRun) Validate() error {
	if err := t.TaskID.Validate(); err != nil {
		return fmt.Errorf("%w: task_id: %w", ErrInvalidEntity, err)
	}
	if err := t.RepositorySubjectID.Validate(); err != nil {
		return fmt.Errorf("%w: repository_subject_id: %w", ErrInvalidEntity, err)
	}
	if err := t.PacketID.Validate(); err != nil {
		return fmt.Errorf("%w: packet_id: %w", ErrInvalidEntity, err)
	}
	if err := t.Lane.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEntity, err)
	}
	if err := t.Intent.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEntity, err)
	}
	if err := t.Status.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEntity, err)
	}
	if t.CurrentAttemptID != nil {
		if err := t.CurrentAttemptID.Validate(); err != nil {
			return fmt.Errorf("%w: current_attempt_id: %w", ErrInvalidEntity, err)
		}
	}
	if t.Status == state.TaskCompleted && t.CompletedEvidenceID == nil {
		return fmt.Errorf("%w: COMPLETED requires completed_evidence_id (worker exit is not completion)", ErrInvalidEntity)
	}
	if t.Status != state.TaskCompleted && t.CompletedEvidenceID != nil {
		return fmt.Errorf("%w: completed_evidence_id set on non-COMPLETED task", ErrInvalidEntity)
	}
	return nil
}

// WorkerAttempt is one bounded execution of a TaskRun under a specific
// scheduler generation and fencing token.
type WorkerAttempt struct {
	AttemptID           ids.AttemptID
	TaskID              ids.TaskID
	PacketID            ids.PacketID
	RepositorySubjectID ids.RepositorySubjectID
	Lane                state.Lane
	Intent              state.Intent
	SchedulerEpoch      Epoch
	FenceEpoch          FenceEpoch
	Status              state.WorkerAttemptStatus
	LeaseExpiresAt      *time.Time
	LastCheckpointAt    *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Validate checks the worker attempt record.
func (a WorkerAttempt) Validate() error {
	if err := a.AttemptID.Validate(); err != nil {
		return fmt.Errorf("%w: attempt_id: %w", ErrInvalidEntity, err)
	}
	if err := a.TaskID.Validate(); err != nil {
		return fmt.Errorf("%w: task_id: %w", ErrInvalidEntity, err)
	}
	if err := a.PacketID.Validate(); err != nil {
		return fmt.Errorf("%w: packet_id: %w", ErrInvalidEntity, err)
	}
	if err := a.RepositorySubjectID.Validate(); err != nil {
		return fmt.Errorf("%w: repository_subject_id: %w", ErrInvalidEntity, err)
	}
	if err := a.Lane.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEntity, err)
	}
	if err := a.Intent.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEntity, err)
	}
	if err := a.SchedulerEpoch.Validate(); err != nil {
		return err
	}
	if err := a.FenceEpoch.Validate(); err != nil {
		return err
	}
	if err := a.Status.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEntity, err)
	}
	return nil
}

// IsolationKind describes how a workspace is isolated.
//
// v0.1 prefers an independent clone per modifying attempt. A shared Git
// worktree is not adopted as the isolation primitive (CC3), so it is not part
// of this bounded set.
type IsolationKind string

// Bounded IsolationKind set.
const (
	// IsolationIsolatedClone is an independent clone with its own .git,
	// config and refs, used by exactly one attempt and never reused.
	IsolationIsolatedClone IsolationKind = "ISOLATED_CLONE"
	// IsolationReadOnlyCheckout is a read-only checkout for review work.
	IsolationReadOnlyCheckout IsolationKind = "READ_ONLY_CHECKOUT"
)

// Validate reports whether the isolation kind is in its bounded set.
func (k IsolationKind) Validate() error {
	switch k {
	case IsolationIsolatedClone, IsolationReadOnlyCheckout:
		return nil
	default:
		return fmt.Errorf("%w: %q is not an IsolationKind", ErrInvalidEntity, string(k))
	}
}

// validateWorkspacePath requires an absolute, already-canonical path.
//
// The schema's UNIQUE(root_path) is the filesystem half of the isolation
// invariant, but uniqueness of a string is weaker than uniqueness of a
// directory, and it is worth being exact about how much weaker.
//
// Lexical aliases are ruled out completely: "/a/ws", "/a/ws/", "/a/./ws",
// "/a/b/../ws" and a relative "ws" are five strings naming at most one
// directory, and requiring an absolute Clean-stable form rejects four of them.
//
// Symlinks are not. If /srv/ws-a links to /srv/ws-b then /srv/ws-a/t1 and
// /srv/ws-b/t1 are both canonical strings naming one physical tree, and the
// constraint would not notice. Resolving them here was tried and reverted:
// this is a pure validator, and giving it filesystem I/O made it answer
// differently for the same path depending on whether the directory existed
// yet — accepting a workspace recorded before creation and rejecting the
// identical one recorded after, which is backwards for an allocator that
// materialises the clone first. It also put a stat of a possibly-hung mount
// inside the store's write transaction, behind the single connection.
//
// So the guarantee here is lexical, deliberately, and resolving symlinks
// belongs to the workspace allocator that actually creates the directory and
// can check the real filesystem once. See docs/threat-boundary.md.
func validateWorkspacePath(path string) error {
	if path == "" {
		return fmt.Errorf("%w: root_path is empty", ErrInvalidEntity)
	}
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%w: root_path %q must be absolute", ErrInvalidEntity, path)
	}
	if cleaned := filepath.Clean(path); cleaned != path {
		return fmt.Errorf("%w: root_path %q is not canonical (want %q)", ErrInvalidEntity, path, cleaned)
	}
	// "/" and "/etc" are absolute and canonical, and neither is a workspace.
	// The allocator materialises and later releases these trees, and
	// UNIQUE(root_path) burns whatever is recorded for good, so a
	// mis-computed root — an empty join producing "/", or a config default —
	// must not be storable. This is a blast-radius guard, not a security
	// boundary: it bounds the damage of a mistake, it does not stop a
	// determined caller from naming a bad directory two levels down.
	if depth := len(strings.Split(strings.Trim(path, "/"), "/")); depth < 2 {
		return fmt.Errorf("%w: root_path %q is too shallow to be a workspace", ErrInvalidEntity, path)
	}
	return nil
}

// Workspace is the filesystem identity owned by exactly one WorkerAttempt.
//
// The store enforces one workspace per attempt and one attempt per root path,
// so a workspace can never be shared between attempts and a stale workspace
// can never be re-bound to a newer attempt.
type Workspace struct {
	WorkspaceID         ids.WorkspaceID
	AttemptID           ids.AttemptID
	TaskID              ids.TaskID
	RepositorySubjectID ids.RepositorySubjectID
	BaseSHA             CommitSHA
	IsolationKind       IsolationKind
	RootPath            string
	CreatedAt           time.Time
	ReleasedAt          *time.Time
}

// Validate checks the workspace record.
func (w Workspace) Validate() error {
	if err := w.WorkspaceID.Validate(); err != nil {
		return fmt.Errorf("%w: workspace_id: %w", ErrInvalidEntity, err)
	}
	if err := w.AttemptID.Validate(); err != nil {
		return fmt.Errorf("%w: attempt_id: %w", ErrInvalidEntity, err)
	}
	if err := w.TaskID.Validate(); err != nil {
		return fmt.Errorf("%w: task_id: %w", ErrInvalidEntity, err)
	}
	if err := w.RepositorySubjectID.Validate(); err != nil {
		return fmt.Errorf("%w: repository_subject_id: %w", ErrInvalidEntity, err)
	}
	if err := w.BaseSHA.Validate(); err != nil {
		return fmt.Errorf("%w: base_sha: %w", ErrInvalidEntity, err)
	}
	if err := w.IsolationKind.Validate(); err != nil {
		return err
	}
	if err := validateWorkspacePath(w.RootPath); err != nil {
		return err
	}
	if w.CreatedAt.IsZero() {
		return fmt.Errorf("%w: created_at is zero", ErrInvalidEntity)
	}
	return nil
}
