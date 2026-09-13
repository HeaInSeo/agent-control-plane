package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

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
//
// Every entry here is long enough that a substring match does not collide
// with ordinary field names. Short abbreviations belong in
// sensitiveTokens instead.
var sensitiveFragments = []string{
	"token", "secret", "password", "passwd", "passphrase", "credential",
	"authorization", "bearer", "cookie", "private_key", "privatekey",
	"api_key", "apikey", "access_key", "secret_key", "session_key",
	"ssh_key", "sshkey", "deploy_key", "signing_key", "encryption_key",
	"signature",
}

// sensitiveTokens mark a field name as sensitive only when they appear as a
// whole word within it.
//
// "pat" (personal access token) cannot be substring-matched: it occurs inside
// path, patch, compat, pattern and dispatch. Because redaction runs inside
// AppendEvent and the event table is append-only, a substring match there
// would permanently destroy ordinary operational data — the workspace path of
// every attempt, for one — from the history this control plane exists to
// preserve.
// A standalone word here is treated as sensitive wherever it appears, which
// errs toward redaction: a field named "pass-through-count" or "auth-mode"
// will be redacted even though it holds no credential. That is accepted
// deliberately. These words are not part of any field name this control
// plane writes (the event vocabulary is lane, intent, path, patch, attempt,
// epoch and the like, all verified unaffected), and for a name that genuinely
// reads as a bare credential word the safe reading is that it holds one.
var sensitiveTokens = []string{
	"pat", "pats",
	// "auth" cannot be a substring: it occurs in author, authored_at,
	// authority. As a whole word it still catches auth and basic_auth.
	"auth",
	// "pass" and "pw" likewise: passing, passenger, password (already a
	// fragment) and pwd-adjacent names must survive.
	"pass", "pw", "pwd",
	"cred", "creds",
	"jwt", "otp", "totp",
}

// RedactFields returns a copy of in with sensitive values replaced.
//
// Redaction is applied by the store on every append, so an event that reaches
// durable history cannot carry a credential even if a caller passes one.
//
// Redaction descends through every container, not just maps: a sensitive key
// nested inside a list, or inside a list of lists, is redacted too. A
// structure like
//
//	{"items": [{"authorization": "..."}]}
//
// must not reach durable history with its credential intact.
func RedactFields(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if isSensitiveKey(k) {
			// The key is sensitive, so the whole value goes, whatever shape
			// it has. Descending into it could leak the parts.
			out[k] = Redacted
			continue
		}
		out[k] = redactValue(v, 1)
	}
	return out
}

// maxRedactionDepth bounds how deep redaction descends.
//
// Field values come from callers, and a self-referential structure would
// otherwise recurse until the stack gave out — crashing the scheduler inside
// AppendEvent. At the limit the value is replaced rather than stored: if it
// cannot be inspected, it is not written.
const maxRedactionDepth = 64

// redactValue redacts inside an arbitrary field value.
//
// Only the shapes JSON decoding produces are handled, because EncodeFields
// normalises values through JSON before redacting them. That normalisation is
// what makes this total: a struct, a named map type or a json.Marshaler all
// arrive here as plain maps, slices and scalars.
func redactValue(v any, depth int) any {
	if depth > maxRedactionDepth {
		return Redacted
	}
	switch typed := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, elem := range typed {
			if isSensitiveKey(k) {
				out[k] = Redacted
				continue
			}
			out[k] = redactValue(elem, depth+1)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, elem := range typed {
			out[i] = redactValue(elem, depth+1)
		}
		return out
	default:
		// Scalars carry no keys, so there is nothing to match on. A secret
		// stored under an innocuous key, or as a bare list element, is a
		// documented limitation of key-name redaction.
		return v
	}
}

func isSensitiveKey(k string) bool {
	words := keyWords(k)
	// Fragments are matched against both the raw name and its
	// separator-normalised form, so every spelling of a multi-word credential
	// name is covered: api_key, apiKey, api-key, API.KEY and the canonical
	// HTTP header form x-api-key all normalise to a name containing api_key.
	// Matching only the raw string would let the hyphenated spellings — the
	// ones that actually appear in headers and config files — through.
	candidates := [2]string{strings.ToLower(k), strings.Join(words, "_")}
	for _, frag := range sensitiveFragments {
		for _, candidate := range candidates {
			if strings.Contains(candidate, frag) {
				return true
			}
		}
	}
	for _, word := range words {
		for _, token := range sensitiveTokens {
			if word == token {
				return true
			}
		}
	}
	return false
}

// keyWords splits a field name into its lowercased words, so a short
// sensitive token can be matched as a whole word.
//
// Separators and camelCase boundaries both split, so "githubPat",
// "github_pat", "github.pat" and "GITHUB-PAT" all yield a "pat" word, while
// "root_path" and "patch" do not. The split runs on the original spelling
// because lowercasing first would destroy the camelCase boundary.
func keyWords(key string) []string {
	var (
		words   []string
		current []rune
	)
	flush := func() {
		if len(current) > 0 {
			words = append(words, strings.ToLower(string(current)))
			current = current[:0]
		}
	}
	runes := []rune(key)
	for i, r := range runes {
		switch {
		case !unicode.IsLetter(r) && !unicode.IsDigit(r):
			flush()
		case unicode.IsUpper(r) && i > 0 && (unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1])):
			// lower-to-upper transition: githubPat -> github, Pat
			flush()
			current = append(current, r)
		case unicode.IsUpper(r) && i > 0 && unicode.IsUpper(runes[i-1]) &&
			i+1 < len(runes) && unicode.IsLower(runes[i+1]):
			// end of an acronym run: PATValue -> PAT, Value. Without this the
			// whole name is one word and a short sensitive token inside an
			// acronym escapes the word match.
			flush()
			current = append(current, r)
		default:
			current = append(current, r)
		}
	}
	flush()
	return words
}

// EncodeFields serialises event fields for storage, redacting first.
//
// Values are normalised through JSON before redaction, so redaction runs on
// exactly the shape that will be stored. Without that step a struct value
// would pass through untouched and then be marshalled with its json-tagged
// credential field intact — key-name redaction never saw the key, because in
// Go it was a field name rather than a map key. Normalising first also turns
// a self-referential value into a clean error here instead of a crash deeper
// in the walk.
func EncodeFields(in map[string]any) (string, error) {
	if len(in) == 0 {
		return "{}", nil
	}

	normalised, err := normaliseFields(in)
	if err != nil {
		return "", err
	}
	redacted := RedactFields(normalised)
	if redacted == nil {
		return "{}", nil
	}
	b, err := json.Marshal(redacted)
	if err != nil {
		return "", fmt.Errorf("encode event fields: %w", err)
	}
	return string(b), nil
}

// normaliseFields round-trips fields through JSON so that every value is a
// plain map, slice or scalar before redaction inspects it.
//
// Numbers are decoded as json.Number, not float64. Decoding into float64
// would silently corrupt any integer beyond 2^53 — a nanosecond timestamp, a
// byte count, a numeric external id — and event history is append-only, so a
// number mangled on the way in can never be corrected.
func normaliseFields(in map[string]any) (map[string]any, error) {
	encoded, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("encode event fields: %w", err)
	}
	out, err := decodeJSONObject(encoded)
	if err != nil {
		return nil, fmt.Errorf("normalise event fields: %w", err)
	}
	return out, nil
}

// decodeJSONObject decodes a JSON object, preserving numeric literals exactly.
func decodeJSONObject(encoded []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// DecodeFields deserialises stored event fields.
//
// Numbers come back as json.Number so that a value read out of history is the
// value that was stored, rather than a float64 approximation of it.
func DecodeFields(s string) (map[string]any, error) {
	if s == "" || s == "{}" {
		return nil, nil
	}
	out, err := decodeJSONObject([]byte(s))
	if err != nil {
		return nil, fmt.Errorf("decode event fields: %w", err)
	}
	return out, nil
}
