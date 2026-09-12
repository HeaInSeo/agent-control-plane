package domain_test

import (
	"errors"
	"strings"
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

// The short "pat" token must match only as a whole word: substring matching
// would destroy ordinary field names in append-only history.
func TestSensitiveTokenMatchingIsWordBounded(t *testing.T) {
	mustSurvive := []string{
		"root_path", "path", "patch", "compat", "pattern", "dispatch",
		"patches", "compatibility", "workspace_path", "filePath", "PATH",
	}
	for _, key := range mustSurvive {
		out := domain.RedactFields(map[string]any{key: "keep-me"})
		if out[key] != "keep-me" {
			t.Fatalf("ordinary field %q was destroyed", key)
		}
	}

	mustRedact := []string{
		"pat", "github_pat", "githubPat", "GITHUB_PAT", "pat.value",
		"gh-pat", "pats", "token", "github_token", "authToken",
		"password", "api_key", "ssh_key", "authorization", "session_cookie",
	}
	for _, key := range mustRedact {
		out := domain.RedactFields(map[string]any{key: "fake-not-a-real-secret"})
		if out[key] != domain.Redacted {
			t.Fatalf("sensitive field %q survived as %v", key, out[key])
		}
	}
}

// Redaction must not recurse without bound, and must not be bypassable by a
// value shape the type switch does not name.
func TestEncodeFieldsHandlesHostileShapes(t *testing.T) {
	t.Run("cycle is a clean error", func(t *testing.T) {
		cyclic := map[string]any{}
		cyclic["self"] = cyclic
		if _, err := domain.EncodeFields(cyclic); err == nil {
			t.Fatal("a cyclic structure was encoded")
		}
	})

	t.Run("struct json tag is redacted", func(t *testing.T) {
		type creds struct {
			Token string `json:"github_token"`
			Lane  string `json:"lane"`
		}
		encoded, err := domain.EncodeFields(map[string]any{
			"a": creds{Token: "fake-not-a-real-token", Lane: "operator"},
			"b": []creds{{Token: "fake-not-a-real-token"}},
			"c": map[string]creds{"inner": {Token: "fake-not-a-real-token"}},
			"d": &creds{Token: "fake-not-a-real-token"},
		})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if strings.Contains(encoded, "fake-not-a-real-token") {
			t.Fatalf("a credential survived encoding: %s", encoded)
		}
		if !strings.Contains(encoded, `"lane":"operator"`) {
			t.Fatalf("non-sensitive struct field was lost: %s", encoded)
		}
	})

	t.Run("deep nesting is bounded", func(t *testing.T) {
		// Deeper than the redaction depth limit.
		deep := map[string]any{"leaf": "value"}
		for range 200 {
			deep = map[string]any{"next": deep}
		}
		out, err := domain.EncodeFields(deep)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if out == "" {
			t.Fatal("deeply nested fields produced no output")
		}
	})
}

// Finding 6: redaction missed several obvious credential key names, and
// event history cannot be corrected after the fact.
func TestRedactionCoversCommonCredentialNames(t *testing.T) {
	mustRedact := []string{
		"auth", "basic_auth", "Authorization", "authToken",
		"pass", "pw", "pwd", "password", "passwd", "passphrase",
		"ssh_passphrase", "deploy_key", "signing_key", "encryption_key",
		"secret_key", "client_secret", "refresh_token", "id_token",
		"cred", "creds", "credentials", "jwt", "otp", "totp",
		"session_cookie", "bearer_value", "api_key", "access_key",
	}
	for _, key := range mustRedact {
		out := domain.RedactFields(map[string]any{key: "fake-not-a-real-secret"})
		if out[key] != domain.Redacted {
			t.Fatalf("sensitive field %q survived as %v", key, out[key])
		}
	}

	// Widening must not start destroying ordinary operational data: these are
	// the false positives the substring approach would have produced.
	mustSurvive := []string{
		"author", "authored_at", "authority", "co_author",
		"passing", "passenger", "bypass_count", "compat",
		"root_path", "patch", "pattern", "dispatch",
		"keyword", "monkey", "key_count", "pwned_check_url",
		"credit", "accredited", "otpsomething",
	}
	for _, key := range mustSurvive {
		out := domain.RedactFields(map[string]any{key: "keep-me"})
		if out[key] != "keep-me" {
			t.Fatalf("ordinary field %q was destroyed", key)
		}
	}
}
