// Package ids defines the stable identifiers of the control plane (CC10).
//
// Every identifier kind is a distinct Go type. This is deliberate: the control
// plane must never confuse a TaskRun with a WorkerAttempt, nor an attempt's
// workspace with another attempt's workspace. Making the identifier types
// distinct moves that class of mistake from runtime to compile time.
package ids

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ErrMalformedID is returned when a string cannot be a valid identifier.
var ErrMalformedID = errors.New("malformed identifier")

// Prefixes identify the kind of subject an identifier refers to. They are part
// of the stored value so that a misrouted identifier is detectable in data, not
// only in types.
const (
	prefixPacket     = "pkt"
	prefixTask       = "task"
	prefixAttempt    = "att"
	prefixWorkspace  = "ws"
	prefixRepoSubj   = "rsub"
	prefixPublish    = "pub"
	prefixEvidence   = "evd"
	prefixEvent      = "evt"
	prefixSchedOwner = "sched"
)

// randomSuffix returns 128 bits of hex-encoded randomness.
func randomSuffix() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is not recoverable for an identity generator.
		// Failing loudly is correct: a degraded identifier would silently
		// weaken every uniqueness invariant built on top of it.
		panic(fmt.Sprintf("ids: crypto/rand unavailable: %v", err))
	}
	return hex.EncodeToString(b[:])
}

func mint(prefix string) string { return prefix + "_" + randomSuffix() }

// validate checks that s is a well-formed identifier carrying the given prefix.
func validate(prefix, s string) error {
	rest, ok := strings.CutPrefix(s, prefix+"_")
	if !ok {
		return fmt.Errorf("%w: %q does not carry prefix %q", ErrMalformedID, s, prefix)
	}
	if len(rest) != 32 {
		return fmt.Errorf("%w: %q has a %d-character suffix, want 32", ErrMalformedID, s, len(rest))
	}
	if _, err := hex.DecodeString(rest); err != nil {
		return fmt.Errorf("%w: %q suffix is not hex", ErrMalformedID, s)
	}
	return nil
}

// PacketID identifies an ExecutionPacket.
type PacketID string

// TaskID identifies a TaskRun, the durable scheduling subject.
type TaskID string

// AttemptID identifies a WorkerAttempt, one bounded execution of a TaskRun.
type AttemptID string

// WorkspaceID identifies an isolated workspace owned by exactly one attempt.
type WorkspaceID string

// RepositorySubjectID identifies a RepositorySubject (CC5).
type RepositorySubjectID string

// PublishAttemptID identifies a PublishAttempt (CC1).
type PublishAttemptID string

// EvidenceID identifies an EvidenceObservation (CC7).
type EvidenceID string

// EventID identifies an appended Event.
type EventID string

// SchedulerOwnerID identifies a scheduler process generation candidate (CC4).
type SchedulerOwnerID string

// NewPacketID mints a fresh PacketID.
func NewPacketID() PacketID { return PacketID(mint(prefixPacket)) }

// NewTaskID mints a fresh TaskID.
func NewTaskID() TaskID { return TaskID(mint(prefixTask)) }

// NewAttemptID mints a fresh AttemptID.
func NewAttemptID() AttemptID { return AttemptID(mint(prefixAttempt)) }

// NewWorkspaceID mints a fresh WorkspaceID.
func NewWorkspaceID() WorkspaceID { return WorkspaceID(mint(prefixWorkspace)) }

// NewRepositorySubjectID mints a fresh RepositorySubjectID.
func NewRepositorySubjectID() RepositorySubjectID {
	return RepositorySubjectID(mint(prefixRepoSubj))
}

// NewPublishAttemptID mints a fresh PublishAttemptID.
func NewPublishAttemptID() PublishAttemptID { return PublishAttemptID(mint(prefixPublish)) }

// NewEvidenceID mints a fresh EvidenceID.
func NewEvidenceID() EvidenceID { return EvidenceID(mint(prefixEvidence)) }

// NewEventID mints a fresh EventID.
func NewEventID() EventID { return EventID(mint(prefixEvent)) }

// NewSchedulerOwnerID mints a fresh SchedulerOwnerID.
func NewSchedulerOwnerID() SchedulerOwnerID { return SchedulerOwnerID(mint(prefixSchedOwner)) }

// Validate reports whether the PacketID is well formed.
func (id PacketID) Validate() error { return validate(prefixPacket, string(id)) }

// Validate reports whether the TaskID is well formed.
func (id TaskID) Validate() error { return validate(prefixTask, string(id)) }

// Validate reports whether the AttemptID is well formed.
func (id AttemptID) Validate() error { return validate(prefixAttempt, string(id)) }

// Validate reports whether the WorkspaceID is well formed.
func (id WorkspaceID) Validate() error { return validate(prefixWorkspace, string(id)) }

// Validate reports whether the RepositorySubjectID is well formed.
func (id RepositorySubjectID) Validate() error { return validate(prefixRepoSubj, string(id)) }

// Validate reports whether the PublishAttemptID is well formed.
func (id PublishAttemptID) Validate() error { return validate(prefixPublish, string(id)) }

// Validate reports whether the EvidenceID is well formed.
func (id EvidenceID) Validate() error { return validate(prefixEvidence, string(id)) }

// Validate reports whether the EventID is well formed.
func (id EventID) Validate() error { return validate(prefixEvent, string(id)) }

// Validate reports whether the SchedulerOwnerID is well formed.
func (id SchedulerOwnerID) Validate() error { return validate(prefixSchedOwner, string(id)) }
