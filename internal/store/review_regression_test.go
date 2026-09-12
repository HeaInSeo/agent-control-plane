package store_test

// Regressions for the findings of the independent review of head 6a4ffc2.
// Each test reproduces the reported defect and pins the corrected behaviour.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1: a caller-chosen idempotency key could create a second
// publication identity for one intent.
func TestCallerSuppliedIdempotencyKeyCannotSplitOneIntent(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-idempotency", state.IntentModifying, state.LaneOperator)

	commit := sha("rr-idempotency-commit")
	first := publishFor(f, commit, "refs/heads/m0/rr-idem")
	first.IdempotencyKey = "caller-chosen-1"

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, first)
	}); !errors.Is(err, domain.ErrPublishBindingInvalid) {
		t.Fatalf("a caller-chosen key was accepted: %v", err)
	}

	// The derived key is accepted and is what gets stored.
	honest := publishFor(f, commit, "refs/heads/m0/rr-idem")
	honest.IdempotencyKey = ""
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, honest)
	}); err != nil {
		t.Fatalf("recording with a derived key: %v", err)
	}
	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.PublishAttempt(ctx, honest.PublishAttemptID)
		if err != nil {
			return err
		}
		if want := domain.DeriveIdempotencyKey(honest); stored.IdempotencyKey != want {
			t.Fatalf("stored key %q, want the derived %q", stored.IdempotencyKey, want)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// The same intent under a second row id and a second invented key must
	// not produce a second publication identity.
	second := publishFor(f, commit, "refs/heads/m0/rr-idem")
	second.PublishAttemptID = ids.NewPublishAttemptID()
	second.IdempotencyKey = "caller-chosen-2"
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, second)
	}); err == nil {
		t.Fatal("one intent obtained two publication identities")
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		n, err := tx.QueryIntForTest(ctx,
			`SELECT count(*) FROM publish_attempt WHERE source_commit_sha = ?`, string(commit))
		if err != nil {
			return err
		}
		if n != 1 {
			t.Fatalf("%d publication rows for one intent", n)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 2: a fenced-out attempt's evidence completed a task that a
// successor was actively running.
func TestFencedOutAttemptEvidenceCannotCompleteTask(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-fenced", state.IntentModifying, state.LaneOperator)

	commit := sha("rr-fenced-commit")
	pub := publishFor(f, commit, "refs/heads/m0/rr-fenced")
	ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied); err != nil {
			return err
		}
		return tx.RecordEvidence(ctx, ev)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	// Attempt 1 is fenced out and a successor takes over.
	successor := f.Attempt
	successor.AttemptID = ids.NewAttemptID()
	successor.FenceEpoch = f.Attempt.FenceEpoch + 1
	successor.Status = state.AttemptRunning
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptAbandoned); err != nil {
			return err
		}
		if err := tx.CreateWorkerAttempt(ctx, successor); err != nil {
			return err
		}
		return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, successor.AttemptID)
	}); err != nil {
		t.Fatalf("hand over to successor: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	})
	if !errors.Is(err, store.ErrCompletionNotDerivable) {
		t.Fatalf("want ErrCompletionNotDerivable, got %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status == state.TaskCompleted {
			t.Fatal("a fenced-out attempt's evidence completed the task")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// A terminal current attempt must not complete either, even when it is still
// the task's current attempt.
func TestTerminalCurrentAttemptCannotCompleteTask(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-terminal", state.IntentModifying, state.LaneOperator)

	commit := sha("rr-terminal-commit")
	pub := publishFor(f, commit, "refs/heads/m0/rr-terminal")
	ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishObserved); err != nil {
			return err
		}
		if err := tx.RecordEvidence(ctx, ev); err != nil {
			return err
		}
		return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptEvidenceUnknown)
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	}); !errors.Is(err, store.ErrCompletionNotDerivable) {
		t.Fatalf("want ErrCompletionNotDerivable, got %v", err)
	}
}

// A released workspace must not complete a task, mirroring the publication
// precondition.
func TestReleasedWorkspaceCannotCompleteTask(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-released", state.IntentModifying, state.LaneOperator)

	commit := sha("rr-released-commit")
	pub := publishFor(f, commit, "refs/heads/m0/rr-released")
	ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishObserved); err != nil {
			return err
		}
		if err := tx.RecordEvidence(ctx, ev); err != nil {
			return err
		}
		return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	}); !errors.Is(err, store.ErrCompletionNotDerivable) {
		t.Fatalf("want ErrCompletionNotDerivable, got %v", err)
	}
}

// Finding 3: the "pat" fragment substring-matched ordinary field names and
// destroyed operational data in append-only history.
func TestRedactionDoesNotDestroyOrdinaryFieldNames(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-redaction", state.IntentReadOnly, state.LaneReview)

	e := eventFor(f.Epoch, "workspace.created")
	e.Fields = map[string]any{
		// Must survive: none of these is a credential.
		"root_path":     "/var/lib/acp/workspaces/ws-1",
		"patch":         "diff --git a/x b/x",
		"compat":        "v0.1",
		"pattern":       "refs/heads/*",
		"dispatch":      "manual",
		"path_segments": 4,
		// Must be redacted: "pat" as a whole word, in several spellings.
		"github_pat": "fake-not-a-real-pat",
		"githubPat":  "fake-not-a-real-pat",
		"pat":        "fake-not-a-real-pat",
		"pat.value":  "fake-not-a-real-pat",
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.AppendEvent(ctx, e)
		return err
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		var stored map[string]any
		for _, ev := range events {
			if ev.EventType == "workspace.created" {
				stored = ev.Fields
			}
		}
		if stored == nil {
			t.Fatal("appended event not found")
		}

		preserved := map[string]string{
			"root_path":     "/var/lib/acp/workspaces/ws-1",
			"patch":         "diff --git a/x b/x",
			"compat":        "v0.1",
			"pattern":       "refs/heads/*",
			"dispatch":      "manual",
			"path_segments": "4",
		}
		for key, want := range preserved {
			if got := fmt.Sprint(stored[key]); got != want {
				t.Fatalf("field %q was destroyed: got %v, want %v", key, got, want)
			}
		}
		for _, key := range []string{"github_pat", "githubPat", "pat", "pat.value"} {
			if stored[key] != domain.Redacted {
				t.Fatalf("field %q reached history as %v", key, stored[key])
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 4: RootPath was string-unique, not path-unique, so two attempts
// could be aliased onto one directory.
func TestWorkspacePathMustBeAbsoluteAndCanonical(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	a := seed(t, db, "rr-path-a", state.IntentModifying, state.LaneOperator)
	b := seed(t, db, "rr-path-b", state.IntentReadOnly, state.LaneReview)

	// Every one of these names the same directory as a's workspace, or is
	// otherwise non-canonical, and must be refused.
	for _, alias := range []string{
		a.Workspace.RootPath + "/.",
		a.Workspace.RootPath + "/",
		filepath.Dir(a.Workspace.RootPath) + "/./" + filepath.Base(a.Workspace.RootPath),
		filepath.Dir(a.Workspace.RootPath) + "/sub/../" + filepath.Base(a.Workspace.RootPath),
		"relative/workspace",
		"",
	} {
		t.Run(alias, func(t *testing.T) {
			ws := b.Workspace
			ws.WorkspaceID = ids.NewWorkspaceID()
			ws.RootPath = alias
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.CreateWorkspace(ctx, ws)
			}); !errors.Is(err, domain.ErrInvalidEntity) {
				t.Fatalf("want ErrInvalidEntity for %q, got %v", alias, err)
			}
		})
	}

	// The canonical duplicate is still caught by the uniqueness constraint.
	t.Run("exact canonical duplicate", func(t *testing.T) {
		ws := b.Workspace
		ws.WorkspaceID = ids.NewWorkspaceID()
		ws.RootPath = a.Workspace.RootPath
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); err == nil {
			t.Fatal("two attempts were pointed at the same directory")
		}
	})
}

// Findings 5: a zero-length file must not bypass the create gates.
func TestZeroLengthFileDoesNotBypassCreateGates(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	for _, tc := range []struct {
		name string
		cfg  store.Config
	}{
		{"read-write without AllowCreate", store.Config{Mode: store.ModeReadWrite}},
		{"read-only", store.Config{Mode: store.ModeReadOnly}},
		{"read-only with AllowCreate", store.Config{Mode: store.ModeReadOnly, AllowCreate: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "-")+".db")
			// The stale-empty file a typo or an abandoned run leaves behind.
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := tc.cfg
			cfg.Path = path
			if _, err := store.Open(ctx, cfg); !errors.Is(err, store.ErrDatabaseNotFound) {
				t.Fatalf("want ErrDatabaseNotFound, got %v", err)
			}
		})
	}

	// With AllowCreate, a read-write open still bootstraps over the empty file.
	path := filepath.Join(dir, "explicit.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("explicit create over an empty file: %v", err)
	}
	defer db.Close()
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

// Finding 6: per-connection pragmas must survive the pool replacing its
// connection, so they belong in the DSN rather than in a one-off Exec.
func TestConnectionPragmasSurviveConnectionChurn(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pragmas.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.VerifyConnectionPragmas(ctx); err != nil {
		t.Fatalf("pragmas after open: %v", err)
	}

	// Force the pool to discard and re-establish its connection, then check
	// the replacement carries the same settings.
	for i := range 3 {
		db.RecycleConnectionsForTest()
		if err := db.VerifyConnectionPragmas(ctx); err != nil {
			t.Fatalf("pragmas after churn %d: %v", i, err)
		}
		// Foreign key enforcement must really be in force, not just
		// reported. execution_packet has a foreign key to
		// repository_subject and no BEFORE INSERT trigger, so a dangling
		// reference here fails on the foreign key itself rather than on a
		// trigger that would fire first.
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`INSERT INTO execution_packet (packet_id, task_id, lane, intent, repository_subject_id,
				                               source_revision, source_digest, packet_digest,
				                               approved_at, expires_at, allowed_scope, forbidden_scope,
				                               stop_conditions, acceptance_contract, status)
				 VALUES (?, ?, 'operator', 'MODIFYING', 'rsub_00000000000000000000000000000000',
				         'rev', ?, ?,
				         '2026-09-12T09:00:00.000000000Z', '2026-09-13T09:00:00.000000000Z',
				         '["edit"]', '[]', '[]', 'contract', 'APPROVED')`,
				string(ids.NewPacketID()), string(ids.NewTaskID()),
				string(digestOf("churn-source")), string(digestOf("churn-packet")))
		})
		if err == nil {
			t.Fatalf("churn %d: foreign key enforcement was lost", i)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
			t.Fatalf("churn %d: expected a foreign key failure, got: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadOnly})
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}
	defer ro.Close()
	for i := range 3 {
		ro.RecycleConnectionsForTest()
		if err := ro.VerifyConnectionPragmas(ctx); err != nil {
			t.Fatalf("read-only pragmas after churn %d: %v", i, err)
		}
	}
}

// Findings 7 and 8: the store must be able to stamp CreatedAt, rather than
// rejecting a zero value it was about to fill in itself.
func TestStoreStampsCreatedAtWhenAbsent(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-createdat", state.IntentModifying, state.LaneOperator)

	// The fixture already created a's workspace, so use a successor attempt.
	successor := f.Attempt
	successor.AttemptID = ids.NewAttemptID()
	successor.FenceEpoch = f.Attempt.FenceEpoch + 1
	successor.Status = state.AttemptClaimed

	ws := domain.Workspace{
		WorkspaceID:         ids.NewWorkspaceID(),
		AttemptID:           successor.AttemptID,
		TaskID:              f.Task.TaskID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		BaseSHA:             sha("rr-createdat-base"),
		IsolationKind:       domain.IsolationIsolatedClone,
		RootPath:            "/var/lib/acp/workspaces/rr-createdat",
		// CreatedAt deliberately left zero.
	}
	pub := publishFor(f, sha("rr-createdat-commit"), "refs/heads/m0/rr-createdat")
	pub.CreatedAt = time.Time{}
	pub.IdempotencyKey = ""

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptAbandoned); err != nil {
			return err
		}
		if err := tx.CreateWorkerAttempt(ctx, successor); err != nil {
			return err
		}
		if err := tx.CreateWorkspace(ctx, ws); err != nil {
			return err
		}
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("store should stamp created_at itself: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.Workspace(ctx, ws.WorkspaceID)
		if err != nil {
			return err
		}
		if stored.CreatedAt.IsZero() {
			t.Fatal("workspace created_at was not stamped")
		}
		storedPub, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if storedPub.CreatedAt.IsZero() {
			t.Fatal("publish created_at was not stamped")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 9: a publication must not regain authority after being rejected.
func TestPublishStatusTransitions(t *testing.T) {
	ctx := context.Background()

	forbidden := []struct{ from, to state.PublishStatus }{
		{state.PublishRejected, state.PublishApplied},
		{state.PublishRejected, state.PublishObserved},
		{state.PublishRejected, state.PublishPending},
		{state.PublishObserved, state.PublishApplied},
		{state.PublishObserved, state.PublishRejected},
		{state.PublishApplied, state.PublishPending},
		{state.PublishApplied, state.PublishRejected},
	}
	for _, tc := range forbidden {
		t.Run("forbidden "+string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			db := newDB(t)
			f := seed(t, db, "rr-pubstatus", state.IntentModifying, state.LaneOperator)
			pub := publishFor(f, sha("rr-pubstatus-commit"), "refs/heads/m0/rr-pubstatus")
			if err := db.Write(ctx, func(tx *store.Tx) error {
				if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
					return err
				}
				return tx.SetPublishStatus(ctx, pub.PublishAttemptID, tc.from)
			}); err != nil {
				t.Fatalf("reaching %s: %v", string(tc.from), err)
			}

			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetPublishStatus(ctx, pub.PublishAttemptID, tc.to)
			}); !errors.Is(err, store.ErrForbiddenPublishTransition) {
				t.Fatalf("want ErrForbiddenPublishTransition, got %v", err)
			}

			// The schema must refuse it too.
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx,
					`UPDATE publish_attempt SET status = ? WHERE publish_attempt_id = ?`,
					string(tc.to), string(pub.PublishAttemptID))
			})
			if err == nil {
				t.Fatalf("schema accepted %s -> %s", string(tc.from), string(tc.to))
			}
			if !strings.Contains(err.Error(), "forbidden publish status transition") {
				t.Fatalf("expected the transition trigger to fire, got: %v", err)
			}
		})
	}

	// A rejected publication cannot be laundered into a completion.
	t.Run("rejected publication cannot complete a task", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "rr-rejected", state.IntentModifying, state.LaneOperator)
		commit := sha("rr-rejected-commit")
		pub := publishFor(f, commit, "refs/heads/m0/rr-rejected")
		ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)

		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
				return err
			}
			if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishRejected); err != nil {
				return err
			}
			return tx.RecordEvidence(ctx, ev)
		}); err != nil {
			t.Fatalf("setup: %v", err)
		}

		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
		}); !errors.Is(err, domain.ErrEvidenceInsufficient) {
			t.Fatalf("want ErrEvidenceInsufficient, got %v", err)
		}
	})
}

// Finding 10: the task_run ownership rules must also guard the INSERT path.
func TestTaskRunOwnershipGuardedOnInsert(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	owner := seed(t, db, "rr-ins-owner", state.IntentModifying, state.LaneOperator)
	other := seed(t, db, "rr-ins-other", state.IntentReadOnly, state.LaneReview)

	commit := sha("rr-ins-commit")
	pub := publishFor(owner, commit, "refs/heads/m0/rr-ins")
	ev := evidenceFor(owner, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishObserved); err != nil {
			return err
		}
		return tx.RecordEvidence(ctx, ev)
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	insert := `INSERT INTO task_run (task_id, repository_subject_id, packet_id, lane, intent,
	                                 status, current_attempt_id, completed_evidence_id,
	                                 created_at, updated_at)
	           VALUES (?, ?, ?, ?, ?, ?, ?, ?,
	                   '2026-09-12T09:00:00.000000000Z', '2026-09-12T09:00:00.000000000Z')`

	t.Run("inserted already COMPLETED on another task's evidence", func(t *testing.T) {
		// A packet must exist for this task, or the packet-coherence trigger
		// fires first and the test would pass for the wrong reason.
		taskID := ids.NewTaskID()
		packet := newPacket(taskID, other.Subject.RepositorySubjectID, state.IntentReadOnly, state.LaneReview)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ApprovePacket(ctx, packet)
		}); err != nil {
			t.Fatalf("approve packet: %v", err)
		}

		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx, insert,
				string(taskID), string(other.Subject.RepositorySubjectID), string(packet.PacketID),
				"review", "READ_ONLY", "COMPLETED", nil, string(ev.EvidenceID))
		})
		if err == nil {
			t.Fatal("a task was inserted COMPLETED on another task's evidence")
		}
		if !strings.Contains(err.Error(), "attributed to this task") {
			t.Fatalf("expected the insert-path trigger to fire, got: %v", err)
		}
	})

	t.Run("inserted pointing at another task's attempt", func(t *testing.T) {
		taskID := ids.NewTaskID()
		packet := newPacket(taskID, other.Subject.RepositorySubjectID, state.IntentReadOnly, state.LaneReview)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ApprovePacket(ctx, packet)
		}); err != nil {
			t.Fatalf("approve packet: %v", err)
		}

		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx, insert,
				string(taskID), string(other.Subject.RepositorySubjectID), string(packet.PacketID),
				"review", "READ_ONLY", "READY", string(owner.Attempt.AttemptID), nil)
		})
		if err == nil {
			t.Fatal("a task was inserted pointing at another task's attempt")
		}
		if !strings.Contains(err.Error(), "must belong to this task") {
			t.Fatalf("expected the insert-path trigger to fire, got: %v", err)
		}
	})
}

// Finding 11 and the struct-shaped leak: redaction must survive a
// self-referential value, and must not be bypassable by a Go struct whose
// json tag names a credential.
func TestRedactionHandlesHostileFieldShapes(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-hostile", state.IntentReadOnly, state.LaneReview)

	t.Run("self-referential value is an error, not a crash", func(t *testing.T) {
		cyclic := map[string]any{"name": "loop"}
		cyclic["self"] = cyclic

		e := eventFor(f.Epoch, "hostile.cycle")
		e.Fields = cyclic
		err := db.Write(ctx, func(tx *store.Tx) error {
			_, err := tx.AppendEvent(ctx, e)
			return err
		})
		if err == nil {
			t.Fatal("a cyclic field structure was accepted")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "cycle") {
			t.Fatalf("expected a clean cycle error, got: %v", err)
		}
	})

	t.Run("struct with a json-tagged credential field is redacted", func(t *testing.T) {
		type workerEnv struct {
			GitHubToken string `json:"github_token"`
			Lane        string `json:"lane"`
		}
		e := eventFor(f.Epoch, "hostile.struct")
		e.Fields = map[string]any{
			"env":  workerEnv{GitHubToken: "fake-not-a-real-token", Lane: "review"},
			"envs": []workerEnv{{GitHubToken: "fake-not-a-real-token", Lane: "review"}},
		}
		if err := db.Write(ctx, func(tx *store.Tx) error {
			_, err := tx.AppendEvent(ctx, e)
			return err
		}); err != nil {
			t.Fatalf("append: %v", err)
		}

		if err := db.Read(ctx, func(tx *store.Tx) error {
			events, err := tx.EventsInEpoch(ctx, f.Epoch)
			if err != nil {
				return err
			}
			for _, ev := range events {
				if ev.EventType != "hostile.struct" {
					continue
				}
				dumped := dumpFields(t, ev.Fields)
				if strings.Contains(dumped, "fake-not-a-real-token") {
					t.Fatalf("a struct field carried a credential into history: %s", dumped)
				}
				env := ev.Fields["env"].(map[string]any)
				if env["github_token"] != domain.Redacted {
					t.Fatalf("struct credential field not redacted: %v", env)
				}
				if env["lane"] != "review" {
					t.Fatalf("struct non-sensitive field mangled: %v", env["lane"])
				}
				envs := ev.Fields["envs"].([]any)[0].(map[string]any)
				if envs["github_token"] != domain.Redacted {
					t.Fatalf("struct inside a slice not redacted: %v", envs)
				}
				return nil
			}
			t.Fatal("appended event not found")
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})
}

// Finding 12: a stale repository observation must not revert the alias or
// write a backwards rename into append-only history.
func TestStaleRepositoryObservationIsIgnored(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-rename", state.IntentReadOnly, state.LaneReview)

	renamed := f.Subject
	renamed.CurrentFullName = "HeaInSeo/rr-renamed"
	renamed.ObservedAt = fixedNow.Add(time.Hour)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.ObserveRepositorySubject(ctx, renamed)
		return err
	}); err != nil {
		t.Fatalf("rename: %v", err)
	}

	// A retried reconciliation carrying the pre-rename snapshot.
	stale := f.Subject
	stale.ObservedAt = fixedNow.Add(-time.Hour)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		got, err := tx.ObserveRepositorySubject(ctx, stale)
		if err != nil {
			return err
		}
		if got.CurrentFullName != renamed.CurrentFullName {
			t.Fatalf("a stale observation reverted the alias to %q", got.CurrentFullName)
		}
		return nil
	}); err != nil {
		t.Fatalf("stale observation: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		current, err := tx.RepositorySubjectByNodeID(ctx, f.Subject.GitHubNodeID)
		if err != nil {
			return err
		}
		if current.CurrentFullName != renamed.CurrentFullName {
			t.Fatalf("stored alias is %q, want %q", current.CurrentFullName, renamed.CurrentFullName)
		}
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		renames := 0
		for _, e := range events {
			if e.EventType == "repository_subject.renamed" {
				renames++
			}
		}
		if renames != 1 {
			t.Fatalf("%d rename events in history, want exactly 1", renames)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// BC2 hardening: an approved packet cannot be deleted, so it cannot be
// re-inserted with different content under the same identity.
func TestApprovedPacketCannotBeDeleted(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "rr-pkt-delete", state.IntentModifying, state.LaneOperator)

	// Even a packet that no task references yet.
	orphan := newPacket(ids.NewTaskID(), f.Subject.RepositorySubjectID, state.IntentModifying, state.LaneOperator)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ApprovePacket(ctx, orphan)
	}); err != nil {
		t.Fatalf("approve orphan packet: %v", err)
	}

	for _, id := range []ids.PacketID{f.Packet.PacketID, orphan.PacketID} {
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx, `DELETE FROM execution_packet WHERE packet_id = ?`, string(id))
		})
		if err == nil {
			t.Fatalf("approved packet %s was deleted", string(id))
		}
		if !strings.Contains(err.Error(), "cannot be deleted") {
			t.Fatalf("expected the delete trigger to fire, got: %v", err)
		}
	}
}
