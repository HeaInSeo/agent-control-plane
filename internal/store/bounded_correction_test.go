package store_test

// Tests for the four M0 bounded corrections required by central adjudication
// of PR #1 at head 7cbfdbf:
//
//	BC1  READ_ONLY completion must bind READ_ONLY_REVIEW evidence to a fixed
//	     reviewed SHA and an immutable artifact digest.
//	BC2  An approved ExecutionPacket's authority-bearing content is immutable;
//	     only a permitted status transition is allowed.
//	BC3  Event redaction descends through lists as well as maps.
//	BC4  Fresh-database migration bootstrap is crash/retry safe.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// --- BC1 ------------------------------------------------------------------

func TestBC1ReadOnlyTaskCannotCompleteOnRepositoryEffectEvidence(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "bc1-generic", state.IntentReadOnly, state.LaneReview)

	reviewed := sha("bc1-reviewed")

	for _, kind := range []domain.EvidenceKind{domain.EvidenceBranchHead, domain.EvidencePullRequestHead} {
		t.Run(string(kind), func(t *testing.T) {
			// Equal published/observed SHAs, which for read-only work the
			// worker effectively selects itself.
			ev := evidenceFor(f, reviewed, reviewed, nil, kind)
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.RecordEvidence(ctx, ev)
			}); err != nil {
				t.Fatalf("recording a plain observation should still be allowed: %v", err)
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
					t.Fatalf("%s evidence completed a read-only task", string(kind))
				}
				return nil
			}); err != nil {
				t.Fatalf("read: %v", err)
			}
		})
	}
}

func TestBC1ReadOnlyTaskCompletesOnBoundReviewEvidence(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "bc1-review", state.IntentReadOnly, state.LaneReview)

	reviewed := sha("bc1-good-reviewed")
	ev := reviewEvidenceFor(f, reviewed, "bc1-artifact")

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordEvidence(ctx, ev); err != nil {
			return err
		}
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	}); err != nil {
		t.Fatalf("bound review evidence was refused: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status != state.TaskCompleted {
			t.Fatalf("task status is %q, want COMPLETED", string(run.Status))
		}
		if run.CompletedEvidenceID == nil || *run.CompletedEvidenceID != ev.EvidenceID {
			t.Fatalf("completion not bound to its evidence: %+v", run)
		}
		// The binding must survive the roundtrip: without it, a later
		// re-derivation could not tell review evidence from an observation.
		stored, err := tx.Evidence(ctx, ev.EvidenceID)
		if err != nil {
			return err
		}
		if stored.ReviewedSHA != reviewed {
			t.Fatalf("reviewed_sha roundtrip: got %q want %q", string(stored.ReviewedSHA), string(reviewed))
		}
		if stored.ArtifactDigest != ev.ArtifactDigest {
			t.Fatalf("artifact_digest roundtrip: got %q want %q",
				string(stored.ArtifactDigest), string(ev.ArtifactDigest))
		}
		if !stored.IsReadOnlyReview() {
			t.Fatal("stored evidence lost its READ_ONLY_REVIEW kind")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// The schema must hold the review binding even when the Go validation is
// bypassed entirely.
func TestBC1ReviewBindingEnforcedAtSchemaLevel(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "bc1-schema", state.IntentReadOnly, state.LaneReview)

	reviewed := string(sha("bc1-schema-reviewed"))
	other := string(sha("bc1-schema-other"))
	digest := string(digestOf("bc1-schema-artifact"))

	insert := `INSERT INTO evidence_observation (evidence_id, task_id, attempt_id, scheduler_epoch,
	                                             fence_epoch, repository_subject_id, workspace_id,
	                                             publish_attempt_id, published_sha, observed_sha,
	                                             reviewed_sha, artifact_digest,
	                                             observed_at, evidence_kind)
	           VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)`

	cases := []struct {
		name                        string
		published, observed         string
		reviewedSHA, artifactDigest any
		kind                        string
	}{
		{"review without reviewed_sha", reviewed, reviewed, nil, digest, "READ_ONLY_REVIEW"},
		{"review without artifact_digest", reviewed, reviewed, reviewed, nil, "READ_ONLY_REVIEW"},
		{"review with neither", reviewed, reviewed, nil, nil, "READ_ONLY_REVIEW"},
		{"review whose reviewed_sha differs from observed", reviewed, other, reviewed, digest, "READ_ONLY_REVIEW"},
		{"review whose reviewed_sha differs from published", other, reviewed, reviewed, digest, "READ_ONLY_REVIEW"},
		{"branch head carrying a reviewed_sha", reviewed, reviewed, reviewed, nil, "BRANCH_HEAD"},
		{"branch head carrying an artifact_digest", reviewed, reviewed, nil, digest, "BRANCH_HEAD"},
		{"pull request head carrying a full review binding", reviewed, reviewed, reviewed, digest, "PULL_REQUEST_HEAD"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, insert,
					string(ids.NewEvidenceID()), string(f.Task.TaskID), string(f.Attempt.AttemptID),
					int64(f.Attempt.SchedulerEpoch), int64(f.Attempt.FenceEpoch),
					string(f.Subject.RepositorySubjectID), string(f.Workspace.WorkspaceID),
					tc.published, tc.observed, tc.reviewedSHA, tc.artifactDigest,
					"2026-09-12T09:00:00.000000000Z", tc.kind)
			})
			if err == nil {
				t.Fatalf("schema accepted %q", tc.name)
			}
			if !strings.Contains(strings.ToLower(err.Error()), "constraint") {
				t.Fatalf("expected a CHECK constraint to reject it, got: %v", err)
			}
		})
	}
}

// A review must not borrow a publication to look like a repository effect.
func TestBC1ReviewCannotReferenceAPublication(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	// A modifying fixture is needed to own a real publication row.
	mod := seed(t, db, "bc1-pub-owner", state.IntentModifying, state.LaneOperator)
	commit := sha("bc1-pub-commit")
	pub := publishFor(mod, commit, "refs/heads/m0/bc1")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record publish: %v", err)
	}

	ev := reviewEvidenceFor(mod, commit, "bc1-borrowed")
	ev.PublishAttemptID = &pub.PublishAttemptID

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordEvidence(ctx, ev)
	}); !errors.Is(err, domain.ErrEvidenceInvalid) {
		t.Fatalf("want ErrEvidenceInvalid, got %v", err)
	}

	// And at schema level.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO evidence_observation (evidence_id, task_id, attempt_id, scheduler_epoch,
			                                   fence_epoch, repository_subject_id, workspace_id,
			                                   publish_attempt_id, published_sha, observed_sha,
			                                   reviewed_sha, artifact_digest,
			                                   observed_at, evidence_kind)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'READ_ONLY_REVIEW')`,
			string(ids.NewEvidenceID()), string(mod.Task.TaskID), string(mod.Attempt.AttemptID),
			int64(mod.Attempt.SchedulerEpoch), int64(mod.Attempt.FenceEpoch),
			string(mod.Subject.RepositorySubjectID), string(mod.Workspace.WorkspaceID),
			string(pub.PublishAttemptID), string(commit), string(commit),
			string(commit), string(digestOf("bc1-borrowed")),
			"2026-09-12T09:00:00.000000000Z")
	})
	if err == nil {
		t.Fatal("schema accepted review evidence referencing a publication")
	}
}

// --- BC2 ------------------------------------------------------------------

func TestBC2ApprovedPacketContentIsImmutable(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "bc2-immutable", state.IntentModifying, state.LaneOperator)
	other := seed(t, db, "bc2-other", state.IntentReadOnly, state.LaneReview)

	// Every authority-bearing column, including the ones the first
	// implementation of this trigger left out.
	mutations := map[string]string{
		"packet_id":             `UPDATE execution_packet SET packet_id = '` + string(ids.NewPacketID()) + `' WHERE packet_id = ?`,
		"task_id":               `UPDATE execution_packet SET task_id = '` + string(ids.NewTaskID()) + `' WHERE packet_id = ?`,
		"lane":                  `UPDATE execution_packet SET lane = 'guardrail' WHERE packet_id = ?`,
		"intent":                `UPDATE execution_packet SET intent = 'READ_ONLY' WHERE packet_id = ?`,
		"repository_subject_id": `UPDATE execution_packet SET repository_subject_id = '` + string(other.Subject.RepositorySubjectID) + `' WHERE packet_id = ?`,
		"source_revision":       `UPDATE execution_packet SET source_revision = 'notion:rev-2' WHERE packet_id = ?`,
		"source_digest":         `UPDATE execution_packet SET source_digest = '` + string(digestOf("other-source")) + `' WHERE packet_id = ?`,
		"packet_digest":         `UPDATE execution_packet SET packet_digest = '` + string(digestOf("other-packet")) + `' WHERE packet_id = ?`,
		"approved_at":           `UPDATE execution_packet SET approved_at = '2026-09-11T09:00:00.000000000Z' WHERE packet_id = ?`,
		"expires_at":            `UPDATE execution_packet SET expires_at = '2027-09-12T09:00:00.000000000Z' WHERE packet_id = ?`,
		"allowed_scope":         `UPDATE execution_packet SET allowed_scope = '["anything"]' WHERE packet_id = ?`,
		"forbidden_scope":       `UPDATE execution_packet SET forbidden_scope = '[]' WHERE packet_id = ?`,
		"stop_conditions":       `UPDATE execution_packet SET stop_conditions = '[]' WHERE packet_id = ?`,
		"acceptance_contract":   `UPDATE execution_packet SET acceptance_contract = 'anything goes' WHERE packet_id = ?`,
	}

	for column, stmt := range mutations {
		t.Run(column, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, stmt, string(f.Packet.PacketID))
			})
			if err == nil {
				t.Fatalf("approved packet accepted a change to %s", column)
			}
			if !strings.Contains(err.Error(), "immutable") {
				t.Fatalf("expected the immutability trigger to fire, got: %v", err)
			}
		})
	}

	// The stored packet must be byte-for-byte what was approved.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.Packet(ctx, f.Packet.PacketID)
		if err != nil {
			return err
		}
		if stored.Lane != f.Packet.Lane || stored.Intent != f.Packet.Intent ||
			stored.AcceptanceContract != f.Packet.AcceptanceContract ||
			!stored.ApprovedAt.Equal(f.Packet.ApprovedAt) ||
			!stored.ExpiresAt.Equal(f.Packet.ExpiresAt) ||
			len(stored.StopConditions) != len(f.Packet.StopConditions) {
			t.Fatalf("packet drifted from approval: %+v", stored)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestBC2OnlyPermittedPacketStatusTransitions(t *testing.T) {
	ctx := context.Background()

	permitted := []struct {
		from, to state.PacketStatus
	}{
		{state.PacketApproved, state.PacketStale},
		{state.PacketApproved, state.PacketSuperseded},
		{state.PacketStale, state.PacketSuperseded},
	}
	for _, tc := range permitted {
		t.Run("permitted "+string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			db := newDB(t)
			f := seed(t, db, "bc2-ok", state.IntentModifying, state.LaneOperator)
			if tc.from != state.PacketApproved {
				if err := db.Write(ctx, func(tx *store.Tx) error {
					return tx.SetPacketStatus(ctx, f.Packet.PacketID, tc.from)
				}); err != nil {
					t.Fatalf("reaching %s: %v", string(tc.from), err)
				}
			}
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetPacketStatus(ctx, f.Packet.PacketID, tc.to)
			}); err != nil {
				t.Fatalf("permitted transition refused: %v", err)
			}
		})
	}

	// Authority is never regained.
	forbidden := []struct {
		from, to state.PacketStatus
	}{
		{state.PacketStale, state.PacketApproved},
		{state.PacketSuperseded, state.PacketApproved},
		{state.PacketSuperseded, state.PacketStale},
	}
	for _, tc := range forbidden {
		t.Run("forbidden "+string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			db := newDB(t)
			f := seed(t, db, "bc2-bad", state.IntentModifying, state.LaneOperator)
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetPacketStatus(ctx, f.Packet.PacketID, tc.from)
			}); err != nil {
				t.Fatalf("reaching %s: %v", string(tc.from), err)
			}

			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetPacketStatus(ctx, f.Packet.PacketID, tc.to)
			})
			if !errors.Is(err, store.ErrForbiddenPacketTransition) {
				t.Fatalf("want ErrForbiddenPacketTransition, got %v", err)
			}

			// The schema must refuse it too, not only the Go guard.
			err = db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx,
					`UPDATE execution_packet SET status = ? WHERE packet_id = ?`,
					string(tc.to), string(f.Packet.PacketID))
			})
			if err == nil {
				t.Fatalf("schema accepted %s -> %s", string(tc.from), string(tc.to))
			}
			if !strings.Contains(err.Error(), "forbidden packet status transition") {
				t.Fatalf("expected the transition trigger to fire, got: %v", err)
			}

			// A packet that lost authority must not get it back.
			if err := db.Read(ctx, func(tx *store.Tx) error {
				stored, err := tx.Packet(ctx, f.Packet.PacketID)
				if err != nil {
					return err
				}
				if stored.Status.GrantsExecutionAuthority() {
					t.Fatalf("packet regained execution authority: %q", string(stored.Status))
				}
				return nil
			}); err != nil {
				t.Fatalf("read: %v", err)
			}
		})
	}
}

// --- BC3 ------------------------------------------------------------------

func TestBC3RedactionDescendsThroughLists(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "bc3-redaction", state.IntentReadOnly, state.LaneReview)

	e := eventFor(f.Epoch, "worker.spawn")
	e.Fields = map[string]any{
		// The exact shape named in the adjudication.
		"items": []any{
			map[string]any{"authorization": "fake-not-a-real-secret"},
		},
		// Map inside list inside map inside list.
		"stages": []any{
			map[string]any{
				"name": "clone",
				"env": []any{
					map[string]any{"github_token": "fake-not-a-real-token", "lane": "review"},
				},
			},
		},
		// Nested lists with no maps at all.
		"matrix": []any{[]any{"a", "b"}, []any{"c"}},
		// A typed Go slice of maps, not a decoded []any.
		"typed": []map[string]any{{"api_key": "fake-not-a-real-key", "ok": true}},
		// A sensitive key whose value is a container: the whole value goes.
		"credentials": []any{map[string]any{"user": "someone"}},
		// A non-sensitive scalar survives.
		"attempt": 3,
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
		var stored *domain.Event
		for i := range events {
			if events[i].EventType == "worker.spawn" {
				stored = &events[i]
			}
		}
		if stored == nil {
			t.Fatal("appended event not found")
		}

		// No credential may appear anywhere in the durable record.
		for _, leaked := range []string{
			"fake-not-a-real-secret", "fake-not-a-real-token", "fake-not-a-real-key",
		} {
			if strings.Contains(dumpFields(t, stored.Fields), leaked) {
				t.Fatalf("credential %q reached durable history: %v", leaked, stored.Fields)
			}
		}

		items, ok := stored.Fields["items"].([]any)
		if !ok || len(items) != 1 {
			t.Fatalf("items lost its shape: %v", stored.Fields["items"])
		}
		first, ok := items[0].(map[string]any)
		if !ok || first["authorization"] != domain.Redacted {
			t.Fatalf("authorization inside a list was not redacted: %v", items[0])
		}

		stages := stored.Fields["stages"].([]any)
		stage := stages[0].(map[string]any)
		if stage["name"] != "clone" {
			t.Fatalf("non-sensitive nested field was mangled: %v", stage["name"])
		}
		env := stage["env"].([]any)[0].(map[string]any)
		if env["github_token"] != domain.Redacted {
			t.Fatalf("token nested map-in-list-in-map-in-list survived: %v", env)
		}
		if env["lane"] != "review" {
			t.Fatalf("non-sensitive sibling was redacted: %v", env["lane"])
		}

		matrix := stored.Fields["matrix"].([]any)
		if len(matrix) != 2 || len(matrix[0].([]any)) != 2 {
			t.Fatalf("nested lists lost their shape: %v", matrix)
		}

		typed := stored.Fields["typed"].([]any)[0].(map[string]any)
		if typed["api_key"] != domain.Redacted {
			t.Fatalf("typed slice of maps was not redacted: %v", typed)
		}
		if typed["ok"] != true {
			t.Fatalf("non-sensitive field in a typed slice was mangled: %v", typed["ok"])
		}

		if stored.Fields["credentials"] != domain.Redacted {
			t.Fatalf("a sensitive key holding a container was descended into: %v", stored.Fields["credentials"])
		}
		if fmt.Sprint(stored.Fields["attempt"]) != "3" {
			t.Fatalf("non-sensitive scalar was altered: %v", stored.Fields["attempt"])
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// dumpFields renders stored fields as their durable JSON form, so a leak can
// be searched for regardless of where in the structure it hides.
func dumpFields(t *testing.T, fields map[string]any) string {
	t.Helper()
	encoded, err := domain.EncodeFields(fields)
	if err != nil {
		t.Fatalf("encode fields: %v", err)
	}
	return encoded
}

// --- BC4 ------------------------------------------------------------------

// A crash between creating the bookkeeping tables and applying the first
// migration must leave a database that is recognisably ours and safely
// retryable — not one that fails as foreign and cannot be recovered.
func TestBC4PartialBootstrapIsSafelyRetryable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control-plane.db")

	// Fresh database, bootstrap only, then "crash": close without migrating.
	first, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := first.BootstrapForTest(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	version, err := first.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("schema version: %v", err)
	}
	if version != 0 {
		t.Fatalf("half-bootstrapped database reports version %d, want 0", version)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: must not be mistaken for a foreign database.
	second, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err != nil {
		t.Fatalf("reopening a half-bootstrapped database failed: %v", err)
	}
	if errors.Is(err, store.ErrForeignDatabase) {
		t.Fatal("half-bootstrapped database was rejected as foreign")
	}

	// Retry must complete the migration.
	if err := second.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("retry migration: %v", err)
	}
	if err := second.VerifySchema(ctx); err != nil {
		t.Fatalf("verify schema after retry: %v", err)
	}

	// And the completed database must work.
	if _, err := second.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "post-recovery activation"); err != nil {
		t.Fatalf("activate after recovery: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}

	// Bootstrap is idempotent: repeating it changes nothing.
	third, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer third.Close()
	if err := third.BootstrapForTest(ctx); err != nil {
		t.Fatalf("repeat bootstrap: %v", err)
	}
	if err := third.VerifySchema(ctx); err != nil {
		t.Fatalf("verify schema after repeat bootstrap: %v", err)
	}
	epoch, err := third.CurrentEpoch(ctx)
	if err != nil {
		t.Fatalf("current epoch: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("bootstrap retry disturbed the epoch: %d", int64(epoch))
	}
}

// A crash partway through the very first migration must also be retryable:
// the migration runs in one transaction, so it either applied or it did not.
func TestBC4CrashDuringFirstMigrationIsRetryable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control-plane.db")

	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.BootstrapForTest(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// A migration set whose first step fails halfway through.
	set, err := store.Migrations()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}
	broken := make([]store.Migration, len(set))
	copy(broken, set)
	broken[0] = store.Migration{
		Version: 1,
		Name:    set[0].Name,
		SQL:     "CREATE TABLE half_applied (id INTEGER PRIMARY KEY);\nTHIS IS NOT SQL;",
	}
	if err := db.Migrate(ctx, broken); err == nil {
		t.Fatal("a broken migration was reported as applied")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Nothing from the failed migration may survive, and the retry with the
	// real set must succeed.
	reopened, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err != nil {
		t.Fatalf("reopen after failed migration: %v", err)
	}
	defer reopened.Close()

	if err := reopened.Read(ctx, func(tx *store.Tx) error {
		n, err := tx.QueryIntForTest(ctx,
			`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'half_applied'`)
		if err != nil {
			return err
		}
		if n != 0 {
			t.Fatal("a failed migration left a partially applied table behind")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	if err := reopened.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("retry with the real migration set: %v", err)
	}
	if err := reopened.VerifySchema(ctx); err != nil {
		t.Fatalf("verify schema: %v", err)
	}
}

// The recovery allowance must not weaken foreign-database rejection.
func TestBC4ForeignDatabaseIsStillRejected(t *testing.T) {
	ctx := context.Background()

	cases := map[string]func(t *testing.T, tx *store.Tx) error{
		"unrelated tables only": func(_ *testing.T, tx *store.Tx) error {
			return tx.ExecForTest(ctx, `CREATE TABLE unrelated_app (id INTEGER PRIMARY KEY)`)
		},
		"a schema_migration table alongside unrelated tables": func(_ *testing.T, tx *store.Tx) error {
			if err := tx.ExecForTest(ctx, `CREATE TABLE unrelated_app (id INTEGER PRIMARY KEY)`); err != nil {
				return err
			}
			return tx.ExecForTest(ctx, `CREATE TABLE schema_migration (version INTEGER PRIMARY KEY)`)
		},
		"a populated schema_migration with no marker": func(_ *testing.T, tx *store.Tx) error {
			if err := tx.ExecForTest(ctx,
				`CREATE TABLE schema_migration (version INTEGER PRIMARY KEY, name TEXT, checksum TEXT, applied_at TEXT)`,
			); err != nil {
				return err
			}
			return tx.ExecForTest(ctx,
				`INSERT INTO schema_migration VALUES (1, 'someone_elses', 'x', '2026-01-01T00:00:00.000000000Z')`)
		},
		"a control_plane_meta table declaring another owner": func(_ *testing.T, tx *store.Tx) error {
			if err := tx.ExecForTest(ctx,
				`CREATE TABLE control_plane_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
				return err
			}
			return tx.ExecForTest(ctx,
				`INSERT INTO control_plane_meta (key, value) VALUES ('db_kind', 'some-other-system')`)
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "foreign.db")
			seedDB, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := seedDB.Write(ctx, func(tx *store.Tx) error { return build(t, tx) }); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if err := seedDB.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite}); !errors.Is(err, store.ErrForeignDatabase) {
				t.Fatalf("want ErrForeignDatabase, got %v", err)
			}

			// And the file must be left alone.
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("refused open disturbed the file: %v", err)
			}
		})
	}
}
