package domain_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

var now = time.Date(2026, 9, 12, 9, 0, 0, 0, time.UTC)

func digestOf(s string) domain.Digest {
	sum := sha256.Sum256([]byte(s))
	return domain.Digest(hex.EncodeToString(sum[:]))
}

func validPacket() domain.ExecutionPacket {
	return domain.ExecutionPacket{
		PacketID:            ids.NewPacketID(),
		TaskID:              ids.NewTaskID(),
		Lane:                state.LaneOperator,
		Intent:              state.IntentModifying,
		RepositorySubjectID: ids.NewRepositorySubjectID(),
		SourceRevision:      "notion:rev-1",
		SourceDigest:        digestOf("source"),
		PacketDigest:        digestOf("packet"),
		ApprovedAt:          now,
		ExpiresAt:           now.Add(24 * time.Hour),
		AllowedScope:        []string{"edit:internal/store", "run:go test ./..."},
		ForbiddenScope:      []string{"edit:.github/workflows", "push:origin"},
		StopConditions:      []string{"architecture contradiction"},
		AcceptanceContract:  "tests pass and the diff stays in scope",
		Status:              state.PacketApproved,
	}
}

func TestValidPacketValidates(t *testing.T) {
	if err := validPacket().Validate(); err != nil {
		t.Fatalf("valid packet rejected: %v", err)
	}
}

func TestPacketValidationRejectsBadShapes(t *testing.T) {
	cases := map[string]func(*domain.ExecutionPacket){
		"no packet id":            func(p *domain.ExecutionPacket) { p.PacketID = "" },
		"malformed packet id":     func(p *domain.ExecutionPacket) { p.PacketID = "pkt_short" },
		"task id of another kind": func(p *domain.ExecutionPacket) { p.TaskID = ids.TaskID(ids.NewAttemptID()) },
		"no source revision":      func(p *domain.ExecutionPacket) { p.SourceRevision = "" },
		"short source digest":     func(p *domain.ExecutionPacket) { p.SourceDigest = "abc" },
		"uppercase digest":        func(p *domain.ExecutionPacket) { p.PacketDigest = domain.Digest("A" + string(digestOf("x"))[1:]) },
		"zero approval time":      func(p *domain.ExecutionPacket) { p.ApprovedAt = time.Time{} },
		"expiry before approval":  func(p *domain.ExecutionPacket) { p.ExpiresAt = p.ApprovedAt.Add(-time.Hour) },
		"empty allowed scope":     func(p *domain.ExecutionPacket) { p.AllowedScope = nil },
		"blank allowed entry":     func(p *domain.ExecutionPacket) { p.AllowedScope = []string{""} },
		"contradictory scope":     func(p *domain.ExecutionPacket) { p.ForbiddenScope = []string{p.AllowedScope[0]} },
		"no acceptance contract":  func(p *domain.ExecutionPacket) { p.AcceptanceContract = "" },
		"unknown lane":            func(p *domain.ExecutionPacket) { p.Lane = state.Lane("mystery") },
		"unknown intent":          func(p *domain.ExecutionPacket) { p.Intent = state.Intent("MAYBE") },
		"unknown status":          func(p *domain.ExecutionPacket) { p.Status = state.PacketStatus("FINE") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := validPacket()
			mutate(&p)
			if err := p.Validate(); err == nil {
				t.Fatal("invalid packet was accepted")
			} else if !errors.Is(err, domain.ErrPacketInvalid) {
				t.Fatalf("want ErrPacketInvalid, got %v", err)
			}
		})
	}
}

// An empty or invalid scope must never read as permissive.
func TestUnspecifiedScopeIsDenied(t *testing.T) {
	p := validPacket()

	if d := p.DecideScope("edit:internal/store"); !d.Allowed {
		t.Fatalf("an explicitly allowed action was denied: %s", d.Reason)
	}
	for _, action := range []string{
		"edit:.github/workflows", // explicitly forbidden
		"push:origin",            // explicitly forbidden
		"edit:cmd/launcher",      // simply unspecified
		"git push",
		"notion:update",
		"",
	} {
		if d := p.DecideScope(action); d.Allowed {
			t.Fatalf("action %q was allowed: %s", action, d.Reason)
		}
	}

	// A packet whose forbidden list is empty is not thereby permissive.
	open := validPacket()
	open.ForbiddenScope = nil
	if err := open.Validate(); err != nil {
		t.Fatalf("packet with no forbidden scope should still validate: %v", err)
	}
	if d := open.DecideScope("git push"); d.Allowed {
		t.Fatalf("omission from forbidden_scope became permission: %s", d.Reason)
	}

	// A packet with an empty allowed list authorises nothing at all.
	empty := validPacket()
	empty.AllowedScope = nil
	for _, action := range []string{"edit:internal/store", "anything", ""} {
		if d := empty.DecideScope(action); d.Allowed {
			t.Fatalf("empty allowed_scope authorised %q", action)
		}
	}
}

func TestSourceBindingMismatchFailsClosed(t *testing.T) {
	p := validPacket()
	good := domain.SourceBinding{Revision: p.SourceRevision, Digest: p.SourceDigest}

	if err := p.Authorize(now, good); err != nil {
		t.Fatalf("matching source binding rejected: %v", err)
	}

	t.Run("revision drifted", func(t *testing.T) {
		err := p.Authorize(now, domain.SourceBinding{Revision: "notion:rev-2", Digest: p.SourceDigest})
		if !errors.Is(err, domain.ErrPacketStale) {
			t.Fatalf("want ErrPacketStale, got %v", err)
		}
	})
	t.Run("digest drifted", func(t *testing.T) {
		err := p.Authorize(now, domain.SourceBinding{Revision: p.SourceRevision, Digest: digestOf("edited")})
		if !errors.Is(err, domain.ErrPacketStale) {
			t.Fatalf("want ErrPacketStale, got %v", err)
		}
	})
	t.Run("binding unresolvable", func(t *testing.T) {
		if err := p.Authorize(now, domain.SourceBinding{}); !errors.Is(err, domain.ErrPacketStale) {
			t.Fatalf("want ErrPacketStale, got %v", err)
		}
	})
	t.Run("expired", func(t *testing.T) {
		if err := p.Authorize(p.ExpiresAt, good); !errors.Is(err, domain.ErrPacketExpired) {
			t.Fatalf("want ErrPacketExpired at the expiry instant, got %v", err)
		}
	})
	t.Run("stale status", func(t *testing.T) {
		stale := p
		stale.Status = state.PacketStale
		if err := stale.Authorize(now, good); !errors.Is(err, domain.ErrPacketNoAuthority) {
			t.Fatalf("want ErrPacketNoAuthority, got %v", err)
		}
	})
	t.Run("superseded status", func(t *testing.T) {
		superseded := p
		superseded.Status = state.PacketSuperseded
		if err := superseded.Authorize(now, good); !errors.Is(err, domain.ErrPacketNoAuthority) {
			t.Fatalf("want ErrPacketNoAuthority, got %v", err)
		}
	})
}

func TestCommitSHAValidation(t *testing.T) {
	good := domain.CommitSHA("829777a860de1f3c4a2a3f6d19d7919ca52a7c89")
	if err := good.Validate(); err != nil {
		t.Fatalf("a full sha was rejected: %v", err)
	}
	for _, bad := range []domain.CommitSHA{
		"", "main", "HEAD", "origin/main", "829777a",
		"829777A860DE1F3C4A2A3F6D19D7919CA52A7C89",
		"829777a860de1f3c4a2a3f6d19d7919ca52a7c8",
		"829777a860de1f3c4a2a3f6d19d7919ca52a7c89a",
		"zzz777a860de1f3c4a2a3f6d19d7919ca52a7c89",
	} {
		if err := bad.Validate(); !errors.Is(err, domain.ErrNotImmutableCommit) {
			t.Fatalf("%q: want ErrNotImmutableCommit, got %v", string(bad), err)
		}
	}
}

func TestTargetRefValidation(t *testing.T) {
	for _, good := range []string{
		"refs/heads/m0/bootstrap-contract",
		"refs/heads/main",
		"refs/pull/12/head",
	} {
		if err := domain.ValidateTargetRef(good); err != nil {
			t.Fatalf("%q rejected: %v", good, err)
		}
	}
	for _, bad := range []string{
		"", "main", "HEAD", "refs/heads/HEAD", "heads/main",
		"refs/heads/*", "refs/heads/a..b", "refs/heads/with space",
		"refs/heads/x~1", "refs/heads/x^", "refs/heads/x:y",
		"refs/heads/x/", "refs/heads/x.lock",
	} {
		if err := domain.ValidateTargetRef(bad); !errors.Is(err, domain.ErrPublishTargetInvalid) {
			t.Fatalf("%q: want ErrPublishTargetInvalid, got %v", bad, err)
		}
	}
}

func TestIdempotencyKeyDistinguishesIntents(t *testing.T) {
	base := domain.PublishAttempt{
		PublishAttemptID:    ids.NewPublishAttemptID(),
		TaskID:              ids.NewTaskID(),
		AttemptID:           ids.NewAttemptID(),
		SchedulerEpoch:      1,
		FenceEpoch:          1,
		WorkspaceID:         ids.NewWorkspaceID(),
		RepositorySubjectID: ids.NewRepositorySubjectID(),
		BaseSHA:             domain.CommitSHA("829777a860de1f3c4a2a3f6d19d7919ca52a7c89"),
		SourceCommitSHA:     domain.CommitSHA("aaaaaaa860de1f3c4a2a3f6d19d7919ca52a7c89"),
		TargetRef:           "refs/heads/m0/work",
		Status:              state.PublishPending,
		CreatedAt:           now,
	}
	key := domain.DeriveIdempotencyKey(base)

	// The row id is not part of the identity: the same intent retried is the
	// same publication.
	retry := base
	retry.PublishAttemptID = ids.NewPublishAttemptID()
	retry.CreatedAt = now.Add(time.Hour)
	retry.Status = state.PublishUnknown
	if domain.DeriveIdempotencyKey(retry) != key {
		t.Fatal("retrying the same intent produced a different identity")
	}

	mutations := map[string]func(domain.PublishAttempt) domain.PublishAttempt{
		"different attempt":   func(p domain.PublishAttempt) domain.PublishAttempt { p.AttemptID = ids.NewAttemptID(); return p },
		"different task":      func(p domain.PublishAttempt) domain.PublishAttempt { p.TaskID = ids.NewTaskID(); return p },
		"different epoch":     func(p domain.PublishAttempt) domain.PublishAttempt { p.SchedulerEpoch = 2; return p },
		"different fence":     func(p domain.PublishAttempt) domain.PublishAttempt { p.FenceEpoch = 2; return p },
		"different workspace": func(p domain.PublishAttempt) domain.PublishAttempt { p.WorkspaceID = ids.NewWorkspaceID(); return p },
		"different repository": func(p domain.PublishAttempt) domain.PublishAttempt {
			p.RepositorySubjectID = ids.NewRepositorySubjectID()
			return p
		},
		"different base": func(p domain.PublishAttempt) domain.PublishAttempt {
			p.BaseSHA = domain.CommitSHA("bbbbbbb860de1f3c4a2a3f6d19d7919ca52a7c89")
			return p
		},
		"different commit": func(p domain.PublishAttempt) domain.PublishAttempt {
			p.SourceCommitSHA = domain.CommitSHA("ccccccc860de1f3c4a2a3f6d19d7919ca52a7c89")
			return p
		},
		"different ref": func(p domain.PublishAttempt) domain.PublishAttempt { p.TargetRef = "refs/heads/other"; return p },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			if domain.DeriveIdempotencyKey(mutate(base)) == key {
				t.Fatal("a different publication intent collided onto the same identity")
			}
		})
	}
}

// Only a whole trailing HEAD component is symbolic. Rejecting any ref merely
// containing the substring would make an ordinary branch permanently
// unpublishable, since the schema mirrors these rules.
func TestTargetRefHeadMatchingIsComponentWise(t *testing.T) {
	for _, ok := range []string{
		"refs/heads/fix-HEADER-parsing",
		"refs/heads/HEADER",
		"refs/heads/subject-HEAD-line",
		"refs/heads/HEADless",
	} {
		if err := domain.ValidateTargetRef(ok); err != nil {
			t.Fatalf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"HEAD", "refs/HEAD", "refs/heads/HEAD", "refs/remotes/origin/HEAD"} {
		if err := domain.ValidateTargetRef(bad); !errors.Is(err, domain.ErrPublishTargetInvalid) {
			t.Fatalf("%q accepted: %v", bad, err)
		}
	}
}

// The ref-name rules must match what Git itself accepts, per component.
func TestTargetRefFollowsGitCheckRefFormat(t *testing.T) {
	for _, bad := range []string{
		"refs/heads/.hidden",     // component starting with a dot
		"refs/heads/a.",          // component ending with a dot
		"refs/heads//double",     // empty component
		"refs/heads/@{upstream}", // @{ sequence
		"refs/heads/a\x01b",      // control character
		"refs/heads/a\x7fb",      // DEL
		"refs/heads/x.lock/y",    // .lock on an inner component
		"refs/heads/x.lock",      // .lock on the last component
		"refs/heads/@",           // a component that is just @
		"refs/heads/a:b",
		"refs/heads/a?b",
		"refs/heads/a[b",
		"refs/heads/a\\b",
		"refs/heads/a~b",
		"refs/heads/a^b",
		"refs/heads/a b",
		"refs/heads/a..b",
		"refs/heads/",
	} {
		if err := domain.ValidateTargetRef(bad); !errors.Is(err, domain.ErrPublishTargetInvalid) {
			t.Fatalf("%q accepted by ValidateTargetRef", bad)
		}
	}
	for _, ok := range []string{
		"refs/heads/main",
		"refs/heads/m0/bootstrap-contract",
		"refs/heads/feature/a.b.c",
		"refs/heads/user@example",
		"refs/pull/12/head",
		"refs/tags/v1.0.0",
	} {
		if err := domain.ValidateTargetRef(ok); err != nil {
			t.Fatalf("%q rejected: %v", ok, err)
		}
	}
}

// Finding 10.3: a zero clock is before every real expiry, so an unset time
// would silently skip the expiry check in the primary launch/resume gate.
func TestAuthorizeRejectsAnUnsetClock(t *testing.T) {
	p := validPacket()
	binding := domain.SourceBinding{Revision: p.SourceRevision, Digest: p.SourceDigest}

	if err := p.Authorize(time.Time{}, binding); !errors.Is(err, domain.ErrPacketInvalid) {
		t.Fatalf("want ErrPacketInvalid for an unset clock, got %v", err)
	}
	if err := p.Authorize(now, binding); err != nil {
		t.Fatalf("a real clock was rejected: %v", err)
	}
}

// Finding 10.6: "/" and "/etc" are absolute and canonical, and neither is a
// workspace. The allocator materialises and later releases these trees, and
// UNIQUE(root_path) burns whatever is recorded for good.
func TestWorkspaceRootMustNotBeShallow(t *testing.T) {
	base := domain.Workspace{
		WorkspaceID:         ids.NewWorkspaceID(),
		AttemptID:           ids.NewAttemptID(),
		TaskID:              ids.NewTaskID(),
		RepositorySubjectID: ids.NewRepositorySubjectID(),
		BaseSHA:             domain.CommitSHA("829777a860de1f3c4a2a3f6d19d7919ca52a7c89"),
		IsolationKind:       domain.IsolationIsolatedClone,
		CreatedAt:           now,
	}
	for _, shallow := range []string{"/", "/etc", "/tmp", "/srv"} {
		ws := base
		ws.RootPath = shallow
		if err := ws.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
			t.Fatalf("root_path %q was accepted", shallow)
		}
	}
	for _, ok := range []string{"/tmp/ws-1", "/var/lib/acp/workspaces/attempt-17"} {
		ws := base
		ws.RootPath = ok
		if err := ws.Validate(); err != nil {
			t.Fatalf("root_path %q was rejected: %v", ok, err)
		}
	}
}
