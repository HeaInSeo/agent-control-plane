package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
)

// ErrEventInvalid is returned when an event is not well formed.
var ErrEventInvalid = errors.New("event invalid")

// Redacted is the placeholder substituted for sensitive field values.
//
// Ported from infra-lab PR #39's structured-event redaction, widened beyond
// token/secret/password because the control plane will handle Git credential
// helpers, SSH material and authorization headers.
const Redacted = "[REDACTED]"

// SubjectKind names the entity an event is about.
type SubjectKind string

// Bounded SubjectKind set.
const (
	// SubjectScheduler is an event about scheduler ownership.
	SubjectScheduler SubjectKind = "SCHEDULER"
	// SubjectRepositorySubject is an event about a repository subject,
	// including alias/rename observations.
	SubjectRepositorySubject SubjectKind = "REPOSITORY_SUBJECT"
	// SubjectPacket is an event about an execution packet.
	SubjectPacket SubjectKind = "PACKET"
	// SubjectTaskRun is an event about a task run.
	SubjectTaskRun SubjectKind = "TASK_RUN"
	// SubjectWorkerAttempt is an event about a worker attempt.
	SubjectWorkerAttempt SubjectKind = "WORKER_ATTEMPT"
	// SubjectWorkspace is an event about a workspace.
	SubjectWorkspace SubjectKind = "WORKSPACE"
	// SubjectPublishAttempt is an event about a publication intent.
	SubjectPublishAttempt SubjectKind = "PUBLISH_ATTEMPT"
	// SubjectEvidence is an event about an evidence observation.
	SubjectEvidence SubjectKind = "EVIDENCE"
)

// Validate reports whether the subject kind is in its bounded set.
func (k SubjectKind) Validate() error {
	switch k {
	case SubjectScheduler, SubjectRepositorySubject, SubjectPacket, SubjectTaskRun,
		SubjectWorkerAttempt, SubjectWorkspace, SubjectPublishAttempt, SubjectEvidence:
		return nil
	default:
		return fmt.Errorf("%w: %q is not a SubjectKind", ErrEventInvalid, string(k))
	}
}

// Event is an append-only history record.
//
// Replay identity and order are (SchedulerEpoch, Seq), with Seq monotonic
// within an epoch (CC10). Seq is assigned by the store inside the appending
// transaction; callers do not choose it.
type Event struct {
	SchedulerEpoch Epoch
	Seq            int64
	EventID        ids.EventID
	OccurredAt     time.Time
	EventType      string
	SubjectKind    SubjectKind
	SubjectID      string
	TaskID         *ids.TaskID
	AttemptID      *ids.AttemptID
	Fields         map[string]any
}

// Validate checks the event's shape. Seq is not validated here because the
// store assigns it.
func (e Event) Validate() error {
	if err := e.SchedulerEpoch.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrEventInvalid, err)
	}
	if err := e.EventID.Validate(); err != nil {
		return fmt.Errorf("%w: event_id: %w", ErrEventInvalid, err)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is zero", ErrEventInvalid)
	}
	if e.EventType == "" {
		return fmt.Errorf("%w: event_type is empty", ErrEventInvalid)
	}
	if err := e.SubjectKind.Validate(); err != nil {
		return err
	}
	if e.SubjectID == "" {
		return fmt.Errorf("%w: subject_id is empty", ErrEventInvalid)
	}
	if e.TaskID != nil {
		if err := e.TaskID.Validate(); err != nil {
			return fmt.Errorf("%w: task_id: %w", ErrEventInvalid, err)
		}
	}
	if e.AttemptID != nil {
		if err := e.AttemptID.Validate(); err != nil {
			return fmt.Errorf("%w: attempt_id: %w", ErrEventInvalid, err)
		}
	}
	return nil
}

// sensitiveFragments are substrings that mark a field name as sensitive.
var sensitiveFragments = []string{
	"token", "secret", "password", "passwd", "credential", "authorization",
	"bearer", "cookie", "private_key", "privatekey", "api_key", "apikey",
	"access_key", "session_key", "ssh_key", "signature", "pat",
}

// RedactFields returns a copy of in with sensitive values replaced.
//
// Redaction is applied by the store on every append, so an event that reaches
// durable history cannot carry a credential even if a caller passes one.
func RedactFields(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if isSensitiveKey(k) {
			out[k] = Redacted
			continue
		}
		switch nested := v.(type) {
		case map[string]any:
			out[k] = RedactFields(nested)
		default:
			out[k] = v
		}
	}
	return out
}

func isSensitiveKey(k string) bool {
	lower := strings.ToLower(k)
	for _, frag := range sensitiveFragments {
		if strings.Contains(lower, frag) {
			return true
		}
	}
	return false
}

// EncodeFields serialises event fields for storage, redacting first.
func EncodeFields(in map[string]any) (string, error) {
	redacted := RedactFields(in)
	if redacted == nil {
		return "{}", nil
	}
	b, err := json.Marshal(redacted)
	if err != nil {
		return "", fmt.Errorf("encode event fields: %w", err)
	}
	return string(b), nil
}

// DecodeFields deserialises stored event fields.
func DecodeFields(s string) (map[string]any, error) {
	if s == "" || s == "{}" {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("decode event fields: %w", err)
	}
	return out, nil
}
