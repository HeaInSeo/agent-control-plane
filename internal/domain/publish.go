package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

// Publication errors.
var (
	// ErrPublishBindingInvalid means the publication intent is not bound to an
	// exact immutable commit and exact attempt identity (CC1).
	ErrPublishBindingInvalid = errors.New("publish binding invalid")
	// ErrPublishTargetInvalid means the target ref is unusable or unsafe.
	ErrPublishTargetInvalid = errors.New("publish target invalid")
)

// PublishAttempt is a scheduler-owned intent to mutate a remote repository.
//
// M0 records the identity only: no push, no GitHub mutation, no credential
// handling exists in this milestone. The identity has to exist now so that the
// future publisher cannot be built without it.
//
// The rule this identity exists to enforce:
//
//	publish branch HEAD      = forbidden
//	publish exact commit SHA = required
type PublishAttempt struct {
	PublishAttemptID    ids.PublishAttemptID
	TaskID              ids.TaskID
	AttemptID           ids.AttemptID
	SchedulerEpoch      Epoch
	FenceEpoch          FenceEpoch
	WorkspaceID         ids.WorkspaceID
	RepositorySubjectID ids.RepositorySubjectID

	// BaseSHA is the immutable commit the work started from.
	BaseSHA CommitSHA
	// SourceCommitSHA is the exact immutable commit to publish. It is never a
	// branch name, a symbolic ref, or "the current workspace result".
	SourceCommitSHA CommitSHA
	// TargetRef is the remote ref to update, as a full ref name.
	TargetRef string

	// IdempotencyKey makes a publication intent unique. The store holds a
	// uniqueness constraint on it so a retry cannot become a second publication.
	IdempotencyKey string

	Status    state.PublishStatus
	CreatedAt time.Time
}

// Validate checks publication identity and binding.
func (p PublishAttempt) Validate() error {
	if err := p.PublishAttemptID.Validate(); err != nil {
		return fmt.Errorf("%w: publish_attempt_id: %w", ErrPublishBindingInvalid, err)
	}
	if err := p.TaskID.Validate(); err != nil {
		return fmt.Errorf("%w: task_id: %w", ErrPublishBindingInvalid, err)
	}
	if err := p.AttemptID.Validate(); err != nil {
		return fmt.Errorf("%w: attempt_id: %w", ErrPublishBindingInvalid, err)
	}
	if err := p.WorkspaceID.Validate(); err != nil {
		return fmt.Errorf("%w: workspace_id: %w", ErrPublishBindingInvalid, err)
	}
	if err := p.RepositorySubjectID.Validate(); err != nil {
		return fmt.Errorf("%w: repository_subject_id: %w", ErrPublishBindingInvalid, err)
	}
	if err := p.SchedulerEpoch.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrPublishBindingInvalid, err)
	}
	if err := p.FenceEpoch.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrPublishBindingInvalid, err)
	}
	if err := p.BaseSHA.Validate(); err != nil {
		return fmt.Errorf("%w: base_sha: %w", ErrPublishBindingInvalid, err)
	}
	if err := p.SourceCommitSHA.Validate(); err != nil {
		return fmt.Errorf("%w: source_commit_sha must be an exact immutable commit: %w",
			ErrPublishBindingInvalid, err)
	}
	if err := ValidateTargetRef(p.TargetRef); err != nil {
		return err
	}
	if p.IdempotencyKey == "" {
		return fmt.Errorf("%w: idempotency_key is empty", ErrPublishBindingInvalid)
	}
	if err := p.Status.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrPublishBindingInvalid, err)
	}
	if p.CreatedAt.IsZero() {
		return fmt.Errorf("%w: created_at is zero", ErrPublishBindingInvalid)
	}
	return nil
}

// ValidateTargetRef requires a fully qualified ref name and rejects symbolic
// or wildcard targets. "HEAD" as a target hides which ref actually moves.
func ValidateTargetRef(ref string) error {
	if ref == "" {
		return fmt.Errorf("%w: target_ref is empty", ErrPublishTargetInvalid)
	}
	if !strings.HasPrefix(ref, "refs/") {
		return fmt.Errorf("%w: target_ref %q must be a fully qualified ref (refs/...)",
			ErrPublishTargetInvalid, ref)
	}
	if strings.Contains(ref, "HEAD") {
		return fmt.Errorf("%w: target_ref %q must not be symbolic", ErrPublishTargetInvalid, ref)
	}
	for _, bad := range []string{"*", "..", " ", "~", "^", ":", "?", "[", "\\"} {
		if strings.Contains(ref, bad) {
			return fmt.Errorf("%w: target_ref %q contains %q", ErrPublishTargetInvalid, ref, bad)
		}
	}
	if strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".lock") {
		return fmt.Errorf("%w: target_ref %q has an invalid suffix", ErrPublishTargetInvalid, ref)
	}
	return nil
}

// DeriveIdempotencyKey builds the canonical idempotency key for a publication
// intent.
//
// The key covers the full identity of the intent, so two different attempts,
// two different scheduler generations, or two different source commits can
// never collide onto one publication identity — while an honest retry of the
// very same intent reproduces the same key and is rejected as a duplicate by
// the store's uniqueness constraint.
func DeriveIdempotencyKey(p PublishAttempt) string {
	h := sha256.New()
	for _, part := range []string{
		string(p.TaskID),
		string(p.AttemptID),
		fmt.Sprintf("%d", int64(p.SchedulerEpoch)),
		fmt.Sprintf("%d", int64(p.FenceEpoch)),
		string(p.WorkspaceID),
		string(p.RepositorySubjectID),
		string(p.BaseSHA),
		string(p.SourceCommitSHA),
		p.TargetRef,
	} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// PublishPreconditions is the live state a publisher must revalidate against
// immediately before any remote mutation.
//
// M0 implements the check, not the mutation. Wiring an actual publisher to it
// is M3 work.
type PublishPreconditions struct {
	CurrentSchedulerEpoch Epoch
	TaskCurrentAttemptID  ids.AttemptID
	AttemptStatus         state.WorkerAttemptStatus
	AttemptFenceEpoch     FenceEpoch
	WorkspaceAttemptID    ids.AttemptID
	WorkspaceReleased     bool
	RepositorySubjectID   ids.RepositorySubjectID
	CommitInWorkspace     bool

	// PacketStatus and PacketExpiresAt are the authority the publication is
	// made under. Packet.Authorize covers launch and resume; publication is
	// the third moment where authority has to still hold, because a packet
	// can be marked STALE, be superseded, or expire while an attempt is
	// mid-run. Publishing is the irreversible step, so it revalidates too.
	PacketStatus    state.PacketStatus
	PacketExpiresAt time.Time
	// Now is the instant the preconditions are evaluated at.
	Now time.Time
}

// CheckPublishPreconditions rejects any publication whose binding no longer
// holds. Every branch fails closed.
func CheckPublishPreconditions(p PublishAttempt, live PublishPreconditions) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if live.CurrentSchedulerEpoch != p.SchedulerEpoch {
		return fmt.Errorf("%w: publish bound to scheduler epoch %d, current epoch is %d",
			ErrPublishBindingInvalid, int64(p.SchedulerEpoch), int64(live.CurrentSchedulerEpoch))
	}
	if live.TaskCurrentAttemptID != p.AttemptID {
		return fmt.Errorf("%w: task %s current attempt is %s, publish is bound to %s",
			ErrPublishBindingInvalid, string(p.TaskID),
			string(live.TaskCurrentAttemptID), string(p.AttemptID))
	}
	if live.AttemptFenceEpoch != p.FenceEpoch {
		return fmt.Errorf("%w: attempt fence epoch is %d, publish is bound to %d",
			ErrPublishBindingInvalid, int64(live.AttemptFenceEpoch), int64(p.FenceEpoch))
	}
	if live.AttemptStatus.IsTerminal() {
		return fmt.Errorf("%w: attempt %s is terminal (%s)",
			ErrPublishBindingInvalid, string(p.AttemptID), string(live.AttemptStatus))
	}
	if live.WorkspaceAttemptID != p.AttemptID {
		return fmt.Errorf("%w: workspace %s belongs to attempt %s, not %s",
			ErrPublishBindingInvalid, string(p.WorkspaceID),
			string(live.WorkspaceAttemptID), string(p.AttemptID))
	}
	if live.WorkspaceReleased {
		return fmt.Errorf("%w: workspace %s is released; a stale workspace cannot be published",
			ErrPublishBindingInvalid, string(p.WorkspaceID))
	}
	if live.RepositorySubjectID != p.RepositorySubjectID {
		return fmt.Errorf("%w: repository subject mismatch: live %s, publish %s",
			ErrPublishBindingInvalid, string(live.RepositorySubjectID), string(p.RepositorySubjectID))
	}
	if !live.CommitInWorkspace {
		return fmt.Errorf("%w: source_commit_sha %s is not reachable in workspace %s",
			ErrPublishBindingInvalid, string(p.SourceCommitSHA), string(p.WorkspaceID))
	}
	if !live.PacketStatus.GrantsExecutionAuthority() {
		return fmt.Errorf("%w: packet status is %q",
			ErrPacketNoAuthority, string(live.PacketStatus))
	}
	if live.Now.IsZero() {
		return fmt.Errorf("%w: precondition evaluation time is unset", ErrPublishBindingInvalid)
	}
	if live.PacketExpiresAt.IsZero() {
		return fmt.Errorf("%w: packet expiry is unset", ErrPublishBindingInvalid)
	}
	if !live.Now.Before(live.PacketExpiresAt) {
		return fmt.Errorf("%w: packet expired at %s, now %s",
			ErrPacketExpired, live.PacketExpiresAt.UTC(), live.Now.UTC())
	}
	return nil
}
