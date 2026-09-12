package domain_test

import (
	"errors"
	"testing"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

func attemptIdentity() domain.AttemptIdentity {
	return domain.AttemptIdentity{
		AttemptID:           ids.NewAttemptID(),
		TaskID:              ids.NewTaskID(),
		SchedulerEpoch:      3,
		FenceEpoch:          2,
		RepositorySubjectID: ids.NewRepositorySubjectID(),
		WorkspaceID:         ids.NewWorkspaceID(),
	}
}

func evidenceMatching(id domain.AttemptIdentity, published, observed domain.CommitSHA) domain.EvidenceObservation {
	pub := ids.NewPublishAttemptID()
	return domain.EvidenceObservation{
		EvidenceID:          ids.NewEvidenceID(),
		TaskID:              id.TaskID,
		AttemptID:           id.AttemptID,
		SchedulerEpoch:      id.SchedulerEpoch,
		FenceEpoch:          id.FenceEpoch,
		RepositorySubjectID: id.RepositorySubjectID,
		WorkspaceID:         id.WorkspaceID,
		PublishAttemptID:    &pub,
		PublishedSHA:        published,
		ObservedSHA:         observed,
		ObservedAt:          now,
		EvidenceKind:        domain.EvidenceBranchHead,
	}
}

const (
	ourCommit   = domain.CommitSHA("829777a860de1f3c4a2a3f6d19d7919ca52a7c89")
	humanCommit = domain.CommitSHA("f00dcafe60de1f3c4a2a3f6d19d7919ca52a7c89")
)

func TestEvidenceAttributionRequiresEveryIdentityField(t *testing.T) {
	id := attemptIdentity()
	if err := evidenceMatching(id, ourCommit, ourCommit).CheckAttribution(id); err != nil {
		t.Fatalf("matching evidence rejected: %v", err)
	}

	mutations := map[string]func(*domain.EvidenceObservation){
		"attempt":    func(e *domain.EvidenceObservation) { e.AttemptID = ids.NewAttemptID() },
		"task":       func(e *domain.EvidenceObservation) { e.TaskID = ids.NewTaskID() },
		"epoch":      func(e *domain.EvidenceObservation) { e.SchedulerEpoch = 99 },
		"fence":      func(e *domain.EvidenceObservation) { e.FenceEpoch = 99 },
		"repository": func(e *domain.EvidenceObservation) { e.RepositorySubjectID = ids.NewRepositorySubjectID() },
		"workspace":  func(e *domain.EvidenceObservation) { e.WorkspaceID = ids.NewWorkspaceID() },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			e := evidenceMatching(id, ourCommit, ourCommit)
			mutate(&e)
			if err := e.CheckAttribution(id); !errors.Is(err, domain.ErrEvidenceMisattributed) {
				t.Fatalf("want ErrEvidenceMisattributed, got %v", err)
			}
		})
	}
}

// The default completion rule is exact equality, not ancestor-or-equal.
func TestExactPublishedSHAIsTheDefaultEvidenceRule(t *testing.T) {
	id := attemptIdentity()

	exact := evidenceMatching(id, ourCommit, ourCommit)
	if err := exact.SatisfiesExactEffect(); err != nil {
		t.Fatalf("exact match rejected: %v", err)
	}

	// An unrelated human commit advanced the ref. Our commit may well be an
	// ancestor of it, and that is still not evidence of the approved effect.
	advanced := evidenceMatching(id, ourCommit, humanCommit)
	if err := advanced.SatisfiesExactEffect(); !errors.Is(err, domain.ErrEvidenceInsufficient) {
		t.Fatalf("want ErrEvidenceInsufficient for an advanced ref, got %v", err)
	}

	status, err := domain.DeriveTaskCompletion(domain.CompletionInput{
		Attempt:       id,
		Evidence:      advanced,
		PublishStatus: state.PublishApplied,
		Intent:        state.IntentModifying,
	})
	if err == nil {
		t.Fatalf("an advanced ref derived status %q", string(status))
	}
}

func TestCompletionDerivation(t *testing.T) {
	id := attemptIdentity()
	good := domain.CompletionInput{
		Attempt:       id,
		Evidence:      evidenceMatching(id, ourCommit, ourCommit),
		PublishStatus: state.PublishObserved,
		Intent:        state.IntentModifying,
	}
	status, err := domain.DeriveTaskCompletion(good)
	if err != nil {
		t.Fatalf("valid completion refused: %v", err)
	}
	if status != state.TaskCompleted {
		t.Fatalf("derived status %q, want COMPLETED", string(status))
	}

	t.Run("modifying work needs a publication binding", func(t *testing.T) {
		in := good
		in.Evidence.PublishAttemptID = nil
		if _, err := domain.DeriveTaskCompletion(in); !errors.Is(err, domain.ErrEvidenceInsufficient) {
			t.Fatalf("want ErrEvidenceInsufficient, got %v", err)
		}
	})

	for _, status := range []state.PublishStatus{
		state.PublishPending, state.PublishRejected, state.PublishUnknown,
	} {
		t.Run("publication is "+string(status), func(t *testing.T) {
			in := good
			in.PublishStatus = status
			if _, err := domain.DeriveTaskCompletion(in); !errors.Is(err, domain.ErrEvidenceInsufficient) {
				t.Fatalf("want ErrEvidenceInsufficient, got %v", err)
			}
		})
	}

	t.Run("review evidence cannot complete modifying work", func(t *testing.T) {
		in := good
		in.Evidence.EvidenceKind = domain.EvidenceReadOnlyReview
		if _, err := domain.DeriveTaskCompletion(in); !errors.Is(err, domain.ErrEvidenceInsufficient) {
			t.Fatalf("want ErrEvidenceInsufficient, got %v", err)
		}
	})

	t.Run("read-only work completes on a fixed reviewed commit", func(t *testing.T) {
		in := good
		in.Intent = state.IntentReadOnly
		in.Evidence.EvidenceKind = domain.EvidenceReadOnlyReview
		in.Evidence.PublishAttemptID = nil
		in.PublishStatus = state.PublishUnknown
		derived, err := domain.DeriveTaskCompletion(in)
		if err != nil {
			t.Fatalf("read-only completion refused: %v", err)
		}
		if derived != state.TaskCompleted {
			t.Fatalf("derived %q", string(derived))
		}
	})

	t.Run("misattributed evidence completes nothing", func(t *testing.T) {
		in := good
		in.Evidence.AttemptID = ids.NewAttemptID()
		if _, err := domain.DeriveTaskCompletion(in); !errors.Is(err, domain.ErrEvidenceMisattributed) {
			t.Fatalf("want ErrEvidenceMisattributed, got %v", err)
		}
	})
}

// There must be no path from a worker's exit code to a task status. This test
// documents the absence: completion is derived from evidence only.
func TestTaskCompletedIsOnlyReachableThroughEvidence(t *testing.T) {
	id := attemptIdentity()

	// A worker that "succeeded" but produced no observable effect.
	noEffect := domain.CompletionInput{
		Attempt:       id,
		Evidence:      evidenceMatching(id, ourCommit, humanCommit),
		PublishStatus: state.PublishApplied,
		Intent:        state.IntentModifying,
	}
	if _, err := domain.DeriveTaskCompletion(noEffect); err == nil {
		t.Fatal("completion was derived without a matching observed effect")
	}

	// And an attempt status can never be COMPLETED at all.
	if err := state.WorkerAttemptStatus("COMPLETED").Validate(); !errors.Is(err, state.ErrInvalidState) {
		t.Fatal("COMPLETED is a valid WorkerAttemptStatus; it must not be")
	}
}
