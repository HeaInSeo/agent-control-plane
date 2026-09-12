// Package state defines the separated state domains of the control plane (CC6).
//
// There is deliberately no single "status" enum. Packet staleness, task
// scheduling state, worker attempt execution state, and publication lifecycle
// are four different concerns with four different owners. Each is a distinct
// Go type, so a value from one domain cannot be assigned into a field of
// another, and each validates against only its own bounded set.
package state

import (
	"errors"
	"fmt"
)

// ErrInvalidState is returned when a value is outside a domain's bounded set.
var ErrInvalidState = errors.New("invalid state value")

// PacketStatus is the lifecycle of an ExecutionPacket's execution authority.
type PacketStatus string

// Bounded PacketStatus set.
const (
	// PacketApproved means the packet is a valid closed-world execution authority.
	PacketApproved PacketStatus = "APPROVED"
	// PacketStale means source binding or expiry no longer holds; execution must stop.
	PacketStale PacketStatus = "STALE"
	// PacketSuperseded means a newer approved packet replaced this one.
	PacketSuperseded PacketStatus = "SUPERSEDED"
)

// TaskRunStatus is the scheduling state of a TaskRun.
type TaskRunStatus string

// Bounded TaskRunStatus set.
const (
	// TaskReady means the task is eligible for admission.
	TaskReady TaskRunStatus = "READY"
	// TaskRunning means an attempt currently owns the task.
	TaskRunning TaskRunStatus = "RUNNING"
	// TaskBlockedDesign means the task cannot proceed without design-plane action.
	TaskBlockedDesign TaskRunStatus = "BLOCKED_DESIGN"
	// TaskCompleted is derived only from valid evidence (I1). It is never
	// written from a worker process exit or a worker self-report.
	TaskCompleted TaskRunStatus = "COMPLETED"
	// TaskAbandoned means the task was withdrawn without completion evidence.
	TaskAbandoned TaskRunStatus = "ABANDONED"
)

// WorkerAttemptStatus is the execution state of a single WorkerAttempt.
//
// Note that PACKET_STALE is absent by design: packet staleness belongs to
// PacketStatus, and COMPLETED is absent because completion is a property of a
// TaskRun derived from evidence, not of a worker process.
type WorkerAttemptStatus string

// Bounded WorkerAttemptStatus set.
const (
	// AttemptClaimed means the attempt holds the task's modifying slot.
	AttemptClaimed WorkerAttemptStatus = "CLAIMED"
	// AttemptStarting means workspace and worker setup is in progress.
	AttemptStarting WorkerAttemptStatus = "STARTING"
	// AttemptRunning means the worker is executing inside its workspace.
	AttemptRunning WorkerAttemptStatus = "RUNNING"
	// AttemptVerifying means the worker stopped and evidence is being gathered.
	AttemptVerifying WorkerAttemptStatus = "VERIFYING"
	// AttemptFailed means the attempt ended without a usable result.
	AttemptFailed WorkerAttemptStatus = "FAILED"
	// AttemptAbandoned means the attempt was fenced out or released.
	AttemptAbandoned WorkerAttemptStatus = "ABANDONED"
	// AttemptEvidenceUnknown means the attempt ended and evidence could not be
	// resolved. This is an explicit unknown, never coerced into success.
	AttemptEvidenceUnknown WorkerAttemptStatus = "EVIDENCE_UNKNOWN"
)

// PublishStatus is the lifecycle of a scheduler-owned publication (CC1).
type PublishStatus string

// Bounded PublishStatus set.
const (
	// PublishPending means the publication intent is recorded but not applied.
	PublishPending PublishStatus = "PENDING"
	// PublishApplied means the remote mutation was reported as applied.
	PublishApplied PublishStatus = "APPLIED"
	// PublishObserved means the published effect was independently observed.
	PublishObserved PublishStatus = "OBSERVED"
	// PublishRejected means preconditions failed and nothing was published.
	PublishRejected PublishStatus = "REJECTED"
	// PublishUnknown means the outcome could not be determined. Fails closed.
	PublishUnknown PublishStatus = "UNKNOWN"
)

// Intent separates repository-modifying work from read-only work. Modifying
// admission is lane-agnostic (CC9), so the exclusion key is intent plus
// RepositorySubject, never intent plus lane.
type Intent string

// Bounded Intent set.
const (
	// IntentModifying means the attempt may change repository content.
	IntentModifying Intent = "MODIFYING"
	// IntentReadOnly means the attempt only reads and reports.
	IntentReadOnly Intent = "READ_ONLY"
)

// Lane is an organisational label for work. It carries no admission authority.
type Lane string

// Bounded Lane set.
const (
	// LaneOperator is the operator work lane.
	LaneOperator Lane = "operator"
	// LaneGuardrail is the guardrail work lane.
	LaneGuardrail Lane = "guardrail"
	// LaneReview is the read-only review lane.
	LaneReview Lane = "review"
)

var (
	packetStatuses = map[PacketStatus]struct{}{
		PacketApproved: {}, PacketStale: {}, PacketSuperseded: {},
	}
	taskRunStatuses = map[TaskRunStatus]struct{}{
		TaskReady: {}, TaskRunning: {}, TaskBlockedDesign: {},
		TaskCompleted: {}, TaskAbandoned: {},
	}
	attemptStatuses = map[WorkerAttemptStatus]struct{}{
		AttemptClaimed: {}, AttemptStarting: {}, AttemptRunning: {},
		AttemptVerifying: {}, AttemptFailed: {}, AttemptAbandoned: {},
		AttemptEvidenceUnknown: {},
	}
	publishStatuses = map[PublishStatus]struct{}{
		PublishPending: {}, PublishApplied: {}, PublishObserved: {},
		PublishRejected: {}, PublishUnknown: {},
	}
	intents = map[Intent]struct{}{IntentModifying: {}, IntentReadOnly: {}}
	lanes   = map[Lane]struct{}{LaneOperator: {}, LaneGuardrail: {}, LaneReview: {}}
)

// Validate reports whether the PacketStatus is in its bounded set.
func (s PacketStatus) Validate() error {
	if _, ok := packetStatuses[s]; !ok {
		return fmt.Errorf("%w: %q is not a PacketStatus", ErrInvalidState, string(s))
	}
	return nil
}

// Validate reports whether the TaskRunStatus is in its bounded set.
func (s TaskRunStatus) Validate() error {
	if _, ok := taskRunStatuses[s]; !ok {
		return fmt.Errorf("%w: %q is not a TaskRunStatus", ErrInvalidState, string(s))
	}
	return nil
}

// Validate reports whether the WorkerAttemptStatus is in its bounded set.
func (s WorkerAttemptStatus) Validate() error {
	if _, ok := attemptStatuses[s]; !ok {
		return fmt.Errorf("%w: %q is not a WorkerAttemptStatus", ErrInvalidState, string(s))
	}
	return nil
}

// Validate reports whether the PublishStatus is in its bounded set.
func (s PublishStatus) Validate() error {
	if _, ok := publishStatuses[s]; !ok {
		return fmt.Errorf("%w: %q is not a PublishStatus", ErrInvalidState, string(s))
	}
	return nil
}

// Validate reports whether the Intent is in its bounded set.
func (i Intent) Validate() error {
	if _, ok := intents[i]; !ok {
		return fmt.Errorf("%w: %q is not an Intent", ErrInvalidState, string(i))
	}
	return nil
}

// Validate reports whether the Lane is in its bounded set.
func (l Lane) Validate() error {
	if _, ok := lanes[l]; !ok {
		return fmt.Errorf("%w: %q is not a Lane", ErrInvalidState, string(l))
	}
	return nil
}

// HoldsModifyingSlot reports whether an attempt in this status occupies the
// per-RepositorySubject modifying slot (CC9). Terminal statuses release it.
func (s WorkerAttemptStatus) HoldsModifyingSlot() bool {
	switch s {
	case AttemptClaimed, AttemptStarting, AttemptRunning, AttemptVerifying:
		return true
	default:
		return false
	}
}

// IsTerminal reports whether the attempt has stopped for good.
func (s WorkerAttemptStatus) IsTerminal() bool {
	switch s {
	case AttemptFailed, AttemptAbandoned, AttemptEvidenceUnknown:
		return true
	default:
		return false
	}
}

// GrantsExecutionAuthority reports whether a packet in this status may still
// authorise execution. Anything other than APPROVED fails closed.
func (s PacketStatus) GrantsExecutionAuthority() bool { return s == PacketApproved }
