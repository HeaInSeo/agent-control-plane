package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
)

func TestRedactFields(t *testing.T) {
	in := map[string]any{
		"GITHUB_TOKEN":    "fake-not-a-real-token",
		"github_pat":      "fake-not-a-real-pat",
		"password":        "hunter2",
		"Authorization":   "Bearer x",
		"session_cookie":  "c",
		"ssh_private_key": "-----BEGIN-----",
		"api_key":         "fake-not-a-real-key",
		"repository":      "HeaInSeo/agent-control-plane",
		"attempt":         7,
		"nested":          map[string]any{"secret_value": "s", "lane": "operator"},
		"deeply":          map[string]any{"inner": map[string]any{"access_key": "a", "ok": true}},
	}
	out := domain.RedactFields(in)

	for _, key := range []string{
		"GITHUB_TOKEN", "github_pat", "password", "Authorization",
		"session_cookie", "ssh_private_key", "api_key",
	} {
		if out[key] != domain.Redacted {
			t.Fatalf("%q survived redaction as %v", key, out[key])
		}
	}
	if out["repository"] != "HeaInSeo/agent-control-plane" || out["attempt"] != 7 {
		t.Fatalf("non-sensitive fields were altered: %v", out)
	}
	nested, ok := out["nested"].(map[string]any)
	if !ok || nested["secret_value"] != domain.Redacted || nested["lane"] != "operator" {
		t.Fatalf("nested redaction failed: %v", out["nested"])
	}
	deeply := out["deeply"].(map[string]any)["inner"].(map[string]any)
	if deeply["access_key"] != domain.Redacted || deeply["ok"] != true {
		t.Fatalf("deep redaction failed: %v", deeply)
	}

	// Redaction must not mutate the caller's map.
	if in["password"] != "hunter2" {
		t.Fatal("RedactFields mutated its input")
	}
	if domain.RedactFields(nil) != nil {
		t.Fatal("nil input should produce nil output")
	}
}

func TestEventFieldsRoundtripThroughRedaction(t *testing.T) {
	encoded, err := domain.EncodeFields(map[string]any{"token": "t", "lane": "operator"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := domain.DecodeFields(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded["token"] != domain.Redacted {
		t.Fatalf("token was not redacted before storage: %v", decoded["token"])
	}
	if decoded["lane"] != "operator" {
		t.Fatalf("lane was mangled: %v", decoded["lane"])
	}

	empty, err := domain.EncodeFields(nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	if empty != "{}" {
		t.Fatalf("nil fields encoded as %q, want {}", empty)
	}
}

func TestEventValidation(t *testing.T) {
	valid := domain.Event{
		SchedulerEpoch: 1,
		EventID:        ids.NewEventID(),
		OccurredAt:     now,
		EventType:      "task.admitted",
		SubjectKind:    domain.SubjectTaskRun,
		SubjectID:      "task_x",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	cases := map[string]func(*domain.Event){
		"zero epoch":      func(e *domain.Event) { e.SchedulerEpoch = 0 },
		"no event id":     func(e *domain.Event) { e.EventID = "" },
		"no timestamp":    func(e *domain.Event) { e.OccurredAt = time.Time{} },
		"no type":         func(e *domain.Event) { e.EventType = "" },
		"unknown subject": func(e *domain.Event) { e.SubjectKind = domain.SubjectKind("MYSTERY") },
		"no subject id":   func(e *domain.Event) { e.SubjectID = "" },
		"bad task id":     func(e *domain.Event) { id := ids.TaskID("nope"); e.TaskID = &id },
		"bad attempt id":  func(e *domain.Event) { id := ids.AttemptID("nope"); e.AttemptID = &id },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := valid
			mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatal("invalid event accepted")
			} else if !errors.Is(err, domain.ErrEventInvalid) {
				t.Fatalf("want ErrEventInvalid, got %v", err)
			}
		})
	}
}
