package store_test

// Regressions for the findings of the eleventh independent review, of head
// 448b127. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1: the staleness guard used a strict Before, so a replayed
// observation carrying the same timestamp reverted the alias. GitHub
// timestamps are second-precision, so ties are ordinary.
func TestTiedObservationDoesNotRevertTheAlias(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r11-tie", state.IntentReadOnly, state.LaneReview)

	sameInstant := f.Subject.ObservedAt.Add(time.Hour)

	renamed := f.Subject
	renamed.CurrentFullName = "HeaInSeo/r11-renamed"
	renamed.ObservedAt = sameInstant
	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.ObserveRepositorySubject(ctx, renamed)
		return err
	}); err != nil {
		t.Fatalf("rename: %v", err)
	}

	// A replay of the pre-rename snapshot, from the very same second.
	replay := f.Subject
	replay.ObservedAt = sameInstant
	if err := db.Write(ctx, func(tx *store.Tx) error {
		got, err := tx.ObserveRepositorySubject(ctx, replay)
		if err != nil {
			return err
		}
		if got.CurrentFullName != renamed.CurrentFullName {
			t.Fatalf("a tied replay reverted the alias to %q", got.CurrentFullName)
		}
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		current, err := tx.RepositorySubjectByNodeID(ctx, f.Subject.GitHubNodeID)
		if err != nil {
			return err
		}
		if current.CurrentFullName != renamed.CurrentFullName {
			t.Fatalf("stored alias is %q", current.CurrentFullName)
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
			t.Fatalf("%d rename events in append-only history, want exactly 1", renames)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 2: the optional attempt timestamps were neither validated nor
// normalised, so a dereferenced unset field stored a lease expired in year one.
func TestOptionalAttemptTimestampsMustBeAbsentOrReal(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r11-lease", state.IntentModifying, state.LaneOperator)

	zero := time.Time{}
	for name, mutate := range map[string]func(*domain.WorkerAttempt){
		"lease_expires_at":   func(a *domain.WorkerAttempt) { a.LeaseExpiresAt = &zero },
		"last_checkpoint_at": func(a *domain.WorkerAttempt) { a.LastCheckpointAt = &zero },
	} {
		t.Run(name, func(t *testing.T) {
			successor := f.Attempt
			successor.AttemptID = ids.NewAttemptID()
			successor.FenceEpoch = f.Attempt.FenceEpoch + 1
			successor.Status = state.AttemptClaimed
			mutate(&successor)
			if err := successor.Validate(); !errors.Is(err, domain.ErrInvalidEntity) {
				t.Fatalf("want ErrInvalidEntity, got %v", err)
			}
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.CreateWorkerAttempt(ctx, successor)
			}); !errors.Is(err, domain.ErrInvalidEntity) {
				t.Fatalf("store: want ErrInvalidEntity, got %v", err)
			}
		})
	}

	// Absent is fine, and so is a real timestamp.
	lease := fixedNow.Add(time.Hour)
	successor := f.Attempt
	successor.AttemptID = ids.NewAttemptID()
	successor.FenceEpoch = f.Attempt.FenceEpoch + 1
	successor.Status = state.AttemptClaimed
	successor.LeaseExpiresAt = &lease
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed); err != nil {
			return err
		}
		return tx.CreateWorkerAttempt(ctx, successor)
	}); err != nil {
		t.Fatalf("a real lease was refused: %v", err)
	}
}

// Finding 3: a publication recorded nothing about when its status moved, on
// the entity whose recovery path most needs it.
func TestPublicationRecordsWhenItMoved(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r11-updated-at", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("r11-updated-commit"), "refs/heads/m0/r11-updated")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	later := fixedNow.Add(90 * time.Minute)
	moved := db.WithClock(func() time.Time { return later })
	if err := moved.Write(ctx, func(tx *store.Tx) error {
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied)
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if !stored.CreatedAt.Equal(fixedNow) {
			t.Fatalf("created_at moved to %v", stored.CreatedAt)
		}
		if !stored.UpdatedAt.Equal(later) {
			t.Fatalf("updated_at is %v, want %v", stored.UpdatedAt, later)
		}

		// And the transition left a trace in the history a resuming
		// publisher replays.
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.EventType != "publish_attempt.status_changed" {
				continue
			}
			if e.SubjectID != string(pub.PublishAttemptID) {
				continue
			}
			if e.Fields["previous_status"] != string(state.PublishPending) ||
				e.Fields["current_status"] != string(state.PublishApplied) {
				t.Fatalf("transition recorded as %v -> %v",
					e.Fields["previous_status"], e.Fields["current_status"])
			}
			if e.TaskID == nil || *e.TaskID != f.Task.TaskID {
				t.Fatalf("event is not bound to its task: %v", e.TaskID)
			}
			return nil
		}
		t.Fatal("the publication transition left no trace in history")
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// created_at stays frozen even against raw SQL.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE publish_attempt SET created_at = '2020-01-01T00:00:00.000000000Z' WHERE publish_attempt_id = ?`,
			string(pub.PublishAttemptID))
	}); err == nil {
		t.Fatal("created_at was rewritten")
	}
}

// Finding 4: three rationale comment blocks were orphaned by triggers
// inserted beneath them, so each documented the wrong trigger. In a schema
// whose comments carry the invariant reasoning, that misleads a maintainer
// into relaxing the wrong rule.
func TestMigrationCommentsHeadTheirOwnTrigger(t *testing.T) {
	set, err := store.Migrations()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Each rationale block must be immediately followed by the trigger it
	// describes, with no other CREATE TRIGGER in between.
	expected := map[string]string{
		"CC3's rule is a fresh isolated clone":                   "trg_workspace_isolation_matches_intent",
		"Every identity field must agree with the attempt":       "trg_publish_attempt_binding_coherence",
		"Evidence must be about the attempt it names":            "trg_evidence_attribution_coherence",
		"A finished task takes on no new durable state":          "trg_workspace_task_not_terminal",
		"Publication is the irreversible step, so the task-live": "trg_publish_attempt_task_not_terminal",
		"An observation filed against a finished task":           "trg_evidence_task_not_terminal",
	}

	for _, m := range set {
		lines := strings.Split(m.SQL, "\n")
		for marker, trigger := range expected {
			idx := -1
			for i, line := range lines {
				if strings.Contains(line, marker) {
					idx = i
					break
				}
			}
			if idx < 0 {
				continue
			}
			found := ""
			for i := idx; i < len(lines); i++ {
				if strings.HasPrefix(lines[i], "CREATE TRIGGER ") {
					found = strings.TrimSpace(strings.TrimPrefix(lines[i], "CREATE TRIGGER "))
					break
				}
			}
			if found != trigger {
				t.Fatalf("the %q rationale heads %q, but describes %q", marker, found, trigger)
			}
		}
	}
}
