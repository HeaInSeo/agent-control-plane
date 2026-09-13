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
	// UpdatedAt is when the status last moved. A publisher resuming after a
	// crash resolves an intent by its idempotency key, and needs to know not
	// only what state it is in but when it got there.
	UpdatedAt time.Time
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
	if p.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: updated_at is zero", ErrPublishBindingInvalid)
	}
	if p.UpdatedAt.Before(p.CreatedAt) {
		return fmt.Errorf("%w: updated_at %s precedes created_at %s",
			ErrPublishBindingInvalid, p.UpdatedAt.UTC(), p.CreatedAt.UTC())
	}
	return nil
}

// ValidateTargetRef requires a fully qualified, non-symbolic ref name that
// Git itself will accept.
//
// The rules follow git check-ref-format, applied per path component. Getting
// this wrong in either direction costs something real: too loose and the
// future publisher builds a push against a name Git refuses; too strict and a
// legitimate branch becomes permanently unpublishable, because the schema
// mirrors these rules and such a publication could never even be recorded.
func ValidateTargetRef(ref string) error {
	if ref == "" {
		return fmt.Errorf("%w: target_ref is empty", ErrPublishTargetInvalid)
	}
	if !strings.HasPrefix(ref, "refs/") {
		return fmt.Errorf("%w: target_ref %q must be a fully qualified ref (refs/...)",
			ErrPublishTargetInvalid, ref)
	}
	if strings.HasSuffix(ref, "/") {
		return fmt.Errorf("%w: target_ref %q ends with a separator", ErrPublishTargetInvalid, ref)
	}
	if strings.Contains(ref, "..") {
		return fmt.Errorf("%w: target_ref %q contains %q", ErrPublishTargetInvalid, ref, "..")
	}
	if strings.Contains(ref, "@{") {
		return fmt.Errorf("%w: target_ref %q contains %q", ErrPublishTargetInvalid, ref, "@{")
	}
	// Whole-refname rules, not per-component ones. git check-ref-format
	// accepts refs/heads/v2./fix and refs/heads/@ — only the refname as a
	// whole may not end in "." or be the single character "@". Applying
	// these per component rejected legitimate branches, and since the schema
	// mirrors this validator such a branch would be permanently
	// unpublishable.
	if strings.HasSuffix(ref, ".") {
		return fmt.Errorf("%w: target_ref %q ends with %q", ErrPublishTargetInvalid, ref, ".")
	}
	if ref == "@" {
		return fmt.Errorf("%w: target_ref cannot be %q", ErrPublishTargetInvalid, "@")
	}

	components := strings.Split(ref, "/")
	for _, component := range components {
		if err := validateRefComponent(ref, component); err != nil {
			return err
		}
	}
	// Only a whole component named HEAD is symbolic. A branch such as
	// refs/heads/fix-HEADER-parsing is an ordinary ref, and rejecting it on a
	// substring match would make it unpublishable for ever.
	if components[len(components)-1] == "HEAD" {
		return fmt.Errorf("%w: target_ref %q must not be symbolic", ErrPublishTargetInvalid, ref)
	}
	return nil
}

// refBadChars are the characters git check-ref-format rejects outright.
const refBadChars = " ~^:?*[\\"

// validateRefComponent applies the per-component rules of git check-ref-format.
func validateRefComponent(ref, component string) error {
	if component == "" {
		return fmt.Errorf("%w: target_ref %q has an empty path component", ErrPublishTargetInvalid, ref)
	}
	if strings.HasPrefix(component, ".") {
		return fmt.Errorf("%w: target_ref %q has a component starting with %q",
			ErrPublishTargetInvalid, ref, ".")
	}
	if strings.HasSuffix(component, ".lock") {
		return fmt.Errorf("%w: target_ref %q has a component ending with %q",
			ErrPublishTargetInvalid, ref, ".lock")
	}
	if strings.ContainsAny(component, refBadChars) {
		return fmt.Errorf("%w: target_ref %q contains a character Git rejects",
			ErrPublishTargetInvalid, ref)
	}
	for _, r := range component {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: target_ref %q contains a control character",
				ErrPublishTargetInvalid, ref)
		}
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

	// TaskStatus is the scheduling state of the task being published for. A
	// withdrawn task must not have its push go out: nothing else in the gate
	// notices, because the current-attempt pointer is frozen when a task goes
	// terminal and withdrawal does not mark the packet stale.
	TaskStatus state.TaskRunStatus

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
	// A publication whose lifecycle has already been decided must not be
	// performed again. This matters most on the recovery path: a publisher
	// restarting after a crash resolves the existing intent by its
	// idempotency key and re-runs this gate, and an APPLIED or OBSERVED
	// intent means the remote mutation already happened. Resolving an UNKNOWN
	// outcome is reconciliation, not re-publication.
	if p.Status != state.PublishPending {
		return fmt.Errorf("%w: publication status is %q, so it is not pending",
			ErrPublishBindingInvalid, string(p.Status))
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
	// Validated, not just inspected: "".IsTerminal() is false, so an unset or
	// unrecognised status would sail through the one check meant to catch a
	// dead attempt. Every other field in this struct already fails closed on
	// its zero value, and TaskStatus was added with exactly this guard.
	if err := live.AttemptStatus.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrPublishBindingInvalid, err)
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
	if err := live.TaskStatus.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrPublishBindingInvalid, err)
	}
	if live.TaskStatus.IsTerminal() {
		return fmt.Errorf("%w: task %s is %s",
			ErrPublishBindingInvalid, string(p.TaskID), string(live.TaskStatus))
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
