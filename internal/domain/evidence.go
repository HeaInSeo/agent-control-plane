package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

// Evidence errors.
var (
	// ErrEvidenceInvalid means the observation is not well formed.
	ErrEvidenceInvalid = errors.New("evidence observation invalid")
	// ErrEvidenceMisattributed means the observation's identity fields do not
	// match the attempt it claims to be evidence for (CC7).
	ErrEvidenceMisattributed = errors.New("evidence misattributed")
	// ErrEvidenceInsufficient means the observation does not establish the
	// claimed effect. Completion is withheld rather than assumed.
	ErrEvidenceInsufficient = errors.New("evidence insufficient for completion")
)

// EvidenceKind names the kind of externally observed effect.
type EvidenceKind string

// Bounded EvidenceKind set.
const (
	// EvidenceBranchHead is an observation of a remote branch head.
	EvidenceBranchHead EvidenceKind = "BRANCH_HEAD"
	// EvidencePullRequestHead is an observation of a pull request head.
	EvidencePullRequestHead EvidenceKind = "PULL_REQUEST_HEAD"
	// EvidenceReadOnlyReview is an observation of a review artifact produced
	// against a fixed reviewed commit.
	EvidenceReadOnlyReview EvidenceKind = "READ_ONLY_REVIEW"
)

// Validate reports whether the evidence kind is in its bounded set.
func (k EvidenceKind) Validate() error {
	switch k {
	case EvidenceBranchHead, EvidencePullRequestHead, EvidenceReadOnlyReview:
		return nil
	default:
		return fmt.Errorf("%w: %q is not an EvidenceKind", ErrEvidenceInvalid, string(k))
	}
}

// EvidenceObservation is an externally observed repository effect, bound to the
// exact attempt, scheduler generation, repository subject and workspace that
// claim it (CC7).
type EvidenceObservation struct {
	EvidenceID          ids.EvidenceID
	TaskID              ids.TaskID
	AttemptID           ids.AttemptID
	SchedulerEpoch      Epoch
	FenceEpoch          FenceEpoch
	RepositorySubjectID ids.RepositorySubjectID
	WorkspaceID         ids.WorkspaceID
	PublishAttemptID    *ids.PublishAttemptID

	// PublishedSHA is the exact commit the control plane published.
	PublishedSHA CommitSHA
	// ObservedSHA is the exact commit observed at the target.
	ObservedSHA CommitSHA

	// ReviewedSHA and ArtifactDigest are the read-only review binding (CC7):
	// the fixed commit the review was performed against, and the digest of
	// the immutable review artifact it produced. Both are required for
	// READ_ONLY_REVIEW evidence and must be empty for repository-effect
	// evidence.
	ReviewedSHA    CommitSHA
	ArtifactDigest Digest

	ObservedAt   time.Time
	EvidenceKind EvidenceKind
}

// IsReadOnlyReview reports whether this observation is review evidence rather
// than repository-effect evidence.
func (e EvidenceObservation) IsReadOnlyReview() bool {
	return e.EvidenceKind == EvidenceReadOnlyReview
}

// Validate checks the observation's own shape.
func (e EvidenceObservation) Validate() error {
	if err := e.EvidenceID.Validate(); err != nil {
		return fmt.Errorf("%w: evidence_id: %w", ErrEvidenceInvalid, err)
	}
	if err := e.TaskID.Validate(); err != nil {
		return fmt.Errorf("%w: task_id: %w", ErrEvidenceInvalid, err)
	}
	if err := e.AttemptID.Validate(); err != nil {
		return fmt.Errorf("%w: attempt_id: %w", ErrEvidenceInvalid, err)
	}
	if err := e.RepositorySubjectID.Validate(); err != nil {
		return fmt.Errorf("%w: repository_subject_id: %w", ErrEvidenceInvalid, err)
	}
	if err := e.WorkspaceID.Validate(); err != nil {
		return fmt.Errorf("%w: workspace_id: %w", ErrEvidenceInvalid, err)
	}
	if err := e.SchedulerEpoch.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrEvidenceInvalid, err)
	}
	if err := e.FenceEpoch.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrEvidenceInvalid, err)
	}
	if err := e.PublishedSHA.Validate(); err != nil {
		return fmt.Errorf("%w: published_sha: %w", ErrEvidenceInvalid, err)
	}
	if err := e.ObservedSHA.Validate(); err != nil {
		return fmt.Errorf("%w: observed_sha: %w", ErrEvidenceInvalid, err)
	}
	if e.ObservedAt.IsZero() {
		return fmt.Errorf("%w: observed_at is zero", ErrEvidenceInvalid)
	}
	if err := e.EvidenceKind.Validate(); err != nil {
		return err
	}
	if e.PublishAttemptID != nil {
		if err := e.PublishAttemptID.Validate(); err != nil {
			return fmt.Errorf("%w: publish_attempt_id: %w", ErrEvidenceInvalid, err)
		}
	}
	return e.validateReviewBinding()
}

// validateReviewBinding enforces the CC7 read-only review contract.
//
// Review evidence must name the exact commit it reviewed and the digest of
// the artifact it produced, and it must not carry a publication: a review
// changes nothing, so it has nothing to publish. Repository-effect evidence
// must not carry a review binding, so the two kinds cannot blur together.
func (e EvidenceObservation) validateReviewBinding() error {
	if !e.IsReadOnlyReview() {
		if e.ReviewedSHA != "" {
			return fmt.Errorf("%w: %s evidence must not carry reviewed_sha",
				ErrEvidenceInvalid, string(e.EvidenceKind))
		}
		if e.ArtifactDigest != "" {
			return fmt.Errorf("%w: %s evidence must not carry artifact_digest",
				ErrEvidenceInvalid, string(e.EvidenceKind))
		}
		return nil
	}

	if err := e.ReviewedSHA.Validate(); err != nil {
		return fmt.Errorf("%w: READ_ONLY_REVIEW requires a fixed reviewed_sha: %w",
			ErrEvidenceInvalid, err)
	}
	if err := e.ArtifactDigest.Validate(); err != nil {
		return fmt.Errorf("%w: READ_ONLY_REVIEW requires an artifact_digest: %w",
			ErrEvidenceInvalid, err)
	}
	if e.ObservedSHA != e.ReviewedSHA {
		return fmt.Errorf("%w: observed_sha %s does not match reviewed_sha %s",
			ErrEvidenceInvalid, string(e.ObservedSHA), string(e.ReviewedSHA))
	}
	if e.PublishedSHA != e.ReviewedSHA {
		return fmt.Errorf("%w: published_sha %s does not match reviewed_sha %s",
			ErrEvidenceInvalid, string(e.PublishedSHA), string(e.ReviewedSHA))
	}
	if e.PublishAttemptID != nil {
		return fmt.Errorf("%w: READ_ONLY_REVIEW evidence must not reference a publication",
			ErrEvidenceInvalid)
	}
	return nil
}

// AttemptIdentity is the identity of the attempt an observation is checked
// against.
type AttemptIdentity struct {
	AttemptID           ids.AttemptID
	TaskID              ids.TaskID
	SchedulerEpoch      Epoch
	FenceEpoch          FenceEpoch
	RepositorySubjectID ids.RepositorySubjectID
	WorkspaceID         ids.WorkspaceID
}

// CheckAttribution rejects an observation whose identity fields do not all
// match the attempt it claims. A single mismatched field is enough to reject:
// evidence that is only mostly about the right attempt is not evidence.
func (e EvidenceObservation) CheckAttribution(want AttemptIdentity) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if e.AttemptID != want.AttemptID {
		return fmt.Errorf("%w: attempt_id %s, want %s",
			ErrEvidenceMisattributed, string(e.AttemptID), string(want.AttemptID))
	}
	if e.TaskID != want.TaskID {
		return fmt.Errorf("%w: task_id %s, want %s",
			ErrEvidenceMisattributed, string(e.TaskID), string(want.TaskID))
	}
	if e.SchedulerEpoch != want.SchedulerEpoch {
		return fmt.Errorf("%w: scheduler_epoch %d, want %d",
			ErrEvidenceMisattributed, int64(e.SchedulerEpoch), int64(want.SchedulerEpoch))
	}
	if e.FenceEpoch != want.FenceEpoch {
		return fmt.Errorf("%w: fence_epoch %d, want %d",
			ErrEvidenceMisattributed, int64(e.FenceEpoch), int64(want.FenceEpoch))
	}
	if e.RepositorySubjectID != want.RepositorySubjectID {
		return fmt.Errorf("%w: repository_subject_id %s, want %s",
			ErrEvidenceMisattributed, string(e.RepositorySubjectID), string(want.RepositorySubjectID))
	}
	if e.WorkspaceID != want.WorkspaceID {
		return fmt.Errorf("%w: workspace_id %s, want %s",
			ErrEvidenceMisattributed, string(e.WorkspaceID), string(want.WorkspaceID))
	}
	return nil
}

// SatisfiesExactEffect applies the default repository-effect rule:
//
//	observed PR/branch head == exact published SHA
//
// Generic ancestor-or-equal is deliberately not the default. An unrelated
// human commit can advance a ref, and "my commit is an ancestor of whatever
// is there now" does not establish that the approved effect is the current
// state of the ref.
func (e EvidenceObservation) SatisfiesExactEffect() error {
	if err := e.Validate(); err != nil {
		return err
	}
	if e.ObservedSHA != e.PublishedSHA {
		return fmt.Errorf("%w: observed_sha %s != published_sha %s",
			ErrEvidenceInsufficient, string(e.ObservedSHA), string(e.PublishedSHA))
	}
	return nil
}

// CompletionInput is everything required to derive task completion.
type CompletionInput struct {
	Attempt  AttemptIdentity
	Evidence EvidenceObservation
	// PublishStatus is the lifecycle state of the publication the evidence
	// refers to. Only an applied-or-observed publication can support
	// completion of a modifying task.
	PublishStatus state.PublishStatus
	// Intent decides which evidence contract applies.
	Intent state.Intent
}

// DeriveTaskCompletion is the only way to obtain state.TaskCompleted.
//
// There is deliberately no function that maps a worker process exit code, or a
// worker self-report, onto a TaskRunStatus. Completion is derived here from an
// externally observed effect bound to the correct identity, or it is not
// derived at all (I1).
func DeriveTaskCompletion(in CompletionInput) (state.TaskRunStatus, error) {
	if err := in.Intent.Validate(); err != nil {
		return "", err
	}
	if err := in.Evidence.CheckAttribution(in.Attempt); err != nil {
		return "", err
	}
	if err := in.Evidence.SatisfiesExactEffect(); err != nil {
		return "", err
	}
	switch in.Intent {
	case state.IntentModifying:
		if in.Evidence.PublishAttemptID == nil {
			return "", fmt.Errorf("%w: modifying completion requires a publish attempt binding",
				ErrEvidenceInsufficient)
		}
		switch in.PublishStatus {
		case state.PublishApplied, state.PublishObserved:
		default:
			return "", fmt.Errorf("%w: publish status is %q",
				ErrEvidenceInsufficient, string(in.PublishStatus))
		}
		if in.Evidence.IsReadOnlyReview() {
			return "", fmt.Errorf("%w: read-only review evidence cannot complete a modifying task",
				ErrEvidenceInsufficient)
		}

	case state.IntentReadOnly:
		// Generic repository-effect evidence must not complete a read-only
		// task. A BRANCH_HEAD observation says only that some ref holds some
		// commit; for review work, where nothing is published, the two SHAs
		// being equal is self-selected and establishes nothing. Completion
		// requires review evidence bound to the fixed reviewed commit and to
		// the digest of the immutable artifact the review produced (CC7).
		if !in.Evidence.IsReadOnlyReview() {
			return "", fmt.Errorf(
				"%w: read-only completion requires READ_ONLY_REVIEW evidence, got %s",
				ErrEvidenceInsufficient, string(in.Evidence.EvidenceKind))
		}
		// The binding itself is checked by Validate, which CheckAttribution
		// has already run; restating the requirement here keeps the reason
		// for refusal legible at the completion boundary.
		if in.Evidence.ReviewedSHA == "" || in.Evidence.ArtifactDigest == "" {
			return "", fmt.Errorf("%w: read-only completion requires reviewed_sha and artifact_digest",
				ErrEvidenceInsufficient)
		}

	default:
		// Fail closed on an intent this function has no evidence contract
		// for. Intent.Validate above keeps the switch total today, so this is
		// unreachable — but this is the only function that can produce
		// TaskCompleted, and a third intent added to the bounded set would
		// otherwise skip the switch body entirely and complete the task with
		// no contract applied at all.
		return "", fmt.Errorf("%w: no completion contract for intent %q",
			ErrEvidenceInsufficient, string(in.Intent))
	}
	return state.TaskCompleted, nil
}
