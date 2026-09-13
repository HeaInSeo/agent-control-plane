package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

// Packet authority errors. All of them fail closed: the caller must treat any
// of these as "no execution authority", never as a recoverable warning.
var (
	// ErrPacketInvalid means the packet is not a usable execution authority.
	ErrPacketInvalid = errors.New("execution packet invalid")
	// ErrPacketStale means the packet's source binding no longer resolves to
	// the approved canonical design source revision/digest (CC8).
	ErrPacketStale = errors.New("execution packet stale")
	// ErrPacketExpired means the approval window has closed.
	ErrPacketExpired = errors.New("execution packet expired")
	// ErrPacketNoAuthority means the packet status does not grant execution.
	ErrPacketNoAuthority = errors.New("execution packet grants no authority")
)

// ExecutionPacket v0.1 is the closed-world bounded execution authority for one
// approved execution (CC8).
//
// After approval the worker executes against this packet alone. It does not
// re-read live canonical prose and silently reinterpret scope. An action that
// is not explicitly allowed is denied, and an omission from ForbiddenScope is
// never a permission.
type ExecutionPacket struct {
	PacketID ids.PacketID

	// SourceRevision and SourceDigest bind the packet to the exact canonical
	// design source it was approved against. The scheduler revalidates both
	// before launch and before resume.
	SourceRevision string
	SourceDigest   Digest

	// PacketDigest is the digest of the approved packet body itself.
	PacketDigest Digest

	ApprovedAt time.Time
	ExpiresAt  time.Time

	TaskID              ids.TaskID
	Lane                state.Lane
	Intent              state.Intent
	RepositorySubjectID ids.RepositorySubjectID

	// AllowedScope enumerates the permitted actions. An empty AllowedScope is
	// invalid rather than permissive.
	AllowedScope []string
	// ForbiddenScope is defence in depth on top of the closed world.
	ForbiddenScope []string

	AcceptanceContract string
	StopConditions     []string

	Status state.PacketStatus
}

// Validate checks packet identity, binding and scope coherence.
func (p ExecutionPacket) Validate() error {
	if err := p.PacketID.Validate(); err != nil {
		return fmt.Errorf("%w: packet_id: %w", ErrPacketInvalid, err)
	}
	if err := p.TaskID.Validate(); err != nil {
		return fmt.Errorf("%w: task_id: %w", ErrPacketInvalid, err)
	}
	if err := p.RepositorySubjectID.Validate(); err != nil {
		return fmt.Errorf("%w: repository_subject_id: %w", ErrPacketInvalid, err)
	}
	if err := p.Lane.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrPacketInvalid, err)
	}
	if err := p.Intent.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrPacketInvalid, err)
	}
	if err := p.Status.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrPacketInvalid, err)
	}
	if p.SourceRevision == "" {
		return fmt.Errorf("%w: source_revision is empty", ErrPacketInvalid)
	}
	if err := p.SourceDigest.Validate(); err != nil {
		return fmt.Errorf("%w: source_digest: %w", ErrPacketInvalid, err)
	}
	if err := p.PacketDigest.Validate(); err != nil {
		return fmt.Errorf("%w: packet_digest: %w", ErrPacketInvalid, err)
	}
	if p.ApprovedAt.IsZero() {
		return fmt.Errorf("%w: approved_at is zero", ErrPacketInvalid)
	}
	if p.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expires_at is zero", ErrPacketInvalid)
	}
	if !p.ExpiresAt.After(p.ApprovedAt) {
		return fmt.Errorf("%w: expires_at %s is not after approved_at %s",
			ErrPacketInvalid, p.ExpiresAt.UTC(), p.ApprovedAt.UTC())
	}
	// A packet with no allowed scope authorises nothing. Rejecting it here
	// stops an empty scope from being mistaken for an unbounded one.
	if len(p.AllowedScope) == 0 {
		return fmt.Errorf("%w: allowed_scope is empty; an empty scope is not permissive", ErrPacketInvalid)
	}
	for i, a := range p.AllowedScope {
		if a == "" {
			return fmt.Errorf("%w: allowed_scope[%d] is empty", ErrPacketInvalid, i)
		}
	}
	for i, f := range p.ForbiddenScope {
		if f == "" {
			return fmt.Errorf("%w: forbidden_scope[%d] is empty", ErrPacketInvalid, i)
		}
		// A value in both lists is a contradiction, and the safe reading of a
		// contradiction is not "allow".
		if slices.Contains(p.AllowedScope, f) {
			return fmt.Errorf("%w: %q is both allowed and forbidden", ErrPacketInvalid, f)
		}
	}
	if p.AcceptanceContract == "" {
		return fmt.Errorf("%w: acceptance_contract is empty", ErrPacketInvalid)
	}
	return nil
}

// ScopeDecision is the outcome of a closed-world scope check.
type ScopeDecision struct {
	Allowed bool
	Reason  string
}

// DecideScope resolves one action against the packet's closed world.
//
//	explicitly allowed  = allowed
//	explicitly forbidden = denied
//	unspecified         = denied
func (p ExecutionPacket) DecideScope(action string) ScopeDecision {
	if action == "" {
		return ScopeDecision{false, "empty action is not an explicitly allowed action"}
	}
	if slices.Contains(p.ForbiddenScope, action) {
		return ScopeDecision{false, fmt.Sprintf("action %q is in forbidden_scope", action)}
	}
	if slices.Contains(p.AllowedScope, action) {
		return ScopeDecision{true, fmt.Sprintf("action %q is in allowed_scope", action)}
	}
	return ScopeDecision{false, fmt.Sprintf("action %q is unspecified; unspecified is denied", action)}
}

// SourceBinding is an observation of the canonical design source made at
// launch or resume time.
type SourceBinding struct {
	Revision string
	Digest   Digest
}

// Authorize is the single gate a scheduler uses before launching or resuming
// work against this packet. Every failure mode is a stop, not a downgrade.
func (p ExecutionPacket) Authorize(now time.Time, observed SourceBinding) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if !p.Status.GrantsExecutionAuthority() {
		return fmt.Errorf("%w: status is %q", ErrPacketNoAuthority, string(p.Status))
	}
	// A zero clock is before every real expiry, so without this an unset time
	// would silently skip the expiry check in the primary launch and resume
	// gate. CheckPublishPreconditions guards the identical hazard.
	if now.IsZero() {
		return fmt.Errorf("%w: authorization time is unset", ErrPacketInvalid)
	}
	if !now.Before(p.ExpiresAt) {
		return fmt.Errorf("%w: expired at %s, now %s", ErrPacketExpired, p.ExpiresAt.UTC(), now.UTC())
	}
	if observed.Revision != p.SourceRevision {
		return fmt.Errorf("%w: source_revision approved %q, observed %q",
			ErrPacketStale, p.SourceRevision, observed.Revision)
	}
	if observed.Digest != p.SourceDigest {
		return fmt.Errorf("%w: source_digest approved %q, observed %q",
			ErrPacketStale, string(p.SourceDigest), string(observed.Digest))
	}
	return nil
}

// EncodeStringList serialises a scope or condition list for storage.
func EncodeStringList(v []string) (string, error) {
	if v == nil {
		v = []string{}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DecodeStringList deserialises a stored scope or condition list.
func DecodeStringList(s string) ([]string, error) {
	var v []string
	if s == "" {
		return nil, errors.New("scope column is empty; expected a JSON array")
	}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, fmt.Errorf("decode scope column: %w", err)
	}
	return v, nil
}
