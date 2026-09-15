package state_test

import (
	"errors"
	"testing"

	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

// Each domain accepts only its own values. Cross-domain assignment is already
// a compile error because the types are distinct; this checks that a value
// smuggled in as a string is rejected at validation too.
func TestStateDomainsRejectForeignValues(t *testing.T) {
	foreign := []string{
		"APPROVED", "STALE", "SUPERSEDED",
		"READY", "RUNNING", "BLOCKED_DESIGN", "COMPLETED", "ABANDONED",
		"CLAIMED", "STARTING", "VERIFYING", "FAILED", "EVIDENCE_UNKNOWN",
		"PENDING", "APPLIED", "OBSERVED", "REJECTED", "UNKNOWN",
		"PACKET_STALE", "", "idle",
	}

	own := map[string]map[string]bool{
		"PacketStatus":        {"APPROVED": true, "STALE": true, "SUPERSEDED": true},
		"TaskRunStatus":       {"READY": true, "RUNNING": true, "BLOCKED_DESIGN": true, "COMPLETED": true, "ABANDONED": true},
		"WorkerAttemptStatus": {"CLAIMED": true, "STARTING": true, "RUNNING": true, "VERIFYING": true, "FAILED": true, "ABANDONED": true, "EVIDENCE_UNKNOWN": true},
		"PublishStatus":       {"PENDING": true, "APPLIED": true, "OBSERVED": true, "REJECTED": true, "UNKNOWN": true},
	}

	validators := map[string]func(string) error{
		"PacketStatus":        func(s string) error { return state.PacketStatus(s).Validate() },
		"TaskRunStatus":       func(s string) error { return state.TaskRunStatus(s).Validate() },
		"WorkerAttemptStatus": func(s string) error { return state.WorkerAttemptStatus(s).Validate() },
		"PublishStatus":       func(s string) error { return state.PublishStatus(s).Validate() },
	}

	for domain, validate := range validators {
		for _, value := range foreign {
			err := validate(value)
			if own[domain][value] {
				if err != nil {
					t.Fatalf("%s rejected its own value %q: %v", domain, value, err)
				}
				continue
			}
			if !errors.Is(err, state.ErrInvalidState) {
				t.Fatalf("%s accepted foreign value %q", domain, value)
			}
		}
	}
}

// PACKET_STALE belongs to PacketStatus and COMPLETED to TaskRunStatus. Neither
// may be a worker attempt state.
func TestWorkerAttemptStatusExcludesOtherDomains(t *testing.T) {
	for _, value := range []string{"PACKET_STALE", "STALE", "SUPERSEDED", "COMPLETED", "READY", "APPLIED", "OBSERVED"} {
		if err := state.WorkerAttemptStatus(value).Validate(); !errors.Is(err, state.ErrInvalidState) {
			t.Fatalf("%q is a valid WorkerAttemptStatus; it must not be", value)
		}
	}
}

func TestModifyingSlotIsHeldOnlyByLiveAttempts(t *testing.T) {
	live := []state.WorkerAttemptStatus{
		state.AttemptClaimed, state.AttemptStarting, state.AttemptRunning, state.AttemptVerifying,
	}
	terminal := []state.WorkerAttemptStatus{
		state.AttemptFailed, state.AttemptAbandoned, state.AttemptEvidenceUnknown,
	}
	for _, s := range live {
		if !s.HoldsModifyingSlot() {
			t.Fatalf("%q should hold the modifying slot", string(s))
		}
		if s.IsTerminal() {
			t.Fatalf("%q should not be terminal", string(s))
		}
	}
	for _, s := range terminal {
		if s.HoldsModifyingSlot() {
			t.Fatalf("%q should release the modifying slot", string(s))
		}
		if !s.IsTerminal() {
			t.Fatalf("%q should be terminal", string(s))
		}
	}
}

func TestOnlyApprovedPacketsGrantAuthority(t *testing.T) {
	if !state.PacketApproved.GrantsExecutionAuthority() {
		t.Fatal("an approved packet must grant authority")
	}
	for _, s := range []state.PacketStatus{state.PacketStale, state.PacketSuperseded, state.PacketStatus("")} {
		if s.GrantsExecutionAuthority() {
			t.Fatalf("%q must not grant execution authority", string(s))
		}
	}
}

func TestLaneAndIntentAreBounded(t *testing.T) {
	for _, l := range []state.Lane{state.LaneOperator, state.LaneGuardrail, state.LaneReview} {
		if err := l.Validate(); err != nil {
			t.Fatalf("lane %q rejected: %v", string(l), err)
		}
	}
	for _, l := range []string{"", "Operator", "ops", "MODIFYING"} {
		if err := state.Lane(l).Validate(); !errors.Is(err, state.ErrInvalidState) {
			t.Fatalf("lane %q accepted", l)
		}
	}
	for _, i := range []state.Intent{state.IntentModifying, state.IntentReadOnly} {
		if err := i.Validate(); err != nil {
			t.Fatalf("intent %q rejected: %v", string(i), err)
		}
	}
	for _, i := range []string{"", "modifying", "operator", "READONLY"} {
		if err := state.Intent(i).Validate(); !errors.Is(err, state.ErrInvalidState) {
			t.Fatalf("intent %q accepted", i)
		}
	}
}
