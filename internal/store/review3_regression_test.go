package store_test

// Regressions for the findings of the third independent review, of head
// f55abb6. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"
)

// Finding 1: APPLIED -> UNKNOWN -> REJECTED let a landed publication be
// recorded permanently as "nothing was published".
func TestAppliedPublicationCannotBeLaunderedIntoRejected(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r3-launder", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("r3-launder-commit"), "refs/heads/m0/r3-launder")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied)
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// The laundering step is the one that must be refused.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishUnknown)
	}); !errors.Is(err, store.ErrForbiddenPublishTransition) {
		t.Fatalf("want ErrForbiddenPublishTransition, got %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishRejected)
	}); !errors.Is(err, store.ErrForbiddenPublishTransition) {
		t.Fatalf("want ErrForbiddenPublishTransition, got %v", err)
	}

	// And at schema level.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE publish_attempt SET status = 'UNKNOWN' WHERE publish_attempt_id = ?`,
			string(pub.PublishAttemptID))
	})
	if err == nil {
		t.Fatal("an applied publication regressed to UNKNOWN")
	}
	if !strings.Contains(err.Error(), "forbidden publish status transition") {
		t.Fatalf("expected the transition trigger to fire, got: %v", err)
	}

	// An unknown publication may still be resolved either way, which is what
	// UNKNOWN is for.
	t.Run("unknown from pending still resolves", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r3-unknown", state.IntentModifying, state.LaneOperator)
		p := publishFor(f, sha("r3-unknown-commit"), "refs/heads/m0/r3-unknown")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.RecordPublishAttempt(ctx, p); err != nil {
				return err
			}
			if err := tx.SetPublishStatus(ctx, p.PublishAttemptID, state.PublishUnknown); err != nil {
				return err
			}
			return tx.SetPublishStatus(ctx, p.PublishAttemptID, state.PublishRejected)
		}); err != nil {
			t.Fatalf("PENDING -> UNKNOWN -> REJECTED should remain possible: %v", err)
		}
	})
}

// Finding 2: SetTaskRunStatus had no transition rule, so ABANDONED -> READY
// resurrected a withdrawn task.
func TestTaskRunStatusTransitions(t *testing.T) {
	ctx := context.Background()

	t.Run("abandoned task cannot be resurrected", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r3-abandoned", state.IntentModifying, state.LaneOperator)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
		}); err != nil {
			t.Fatalf("abandon: %v", err)
		}

		for _, to := range []state.TaskRunStatus{state.TaskReady, state.TaskRunning, state.TaskBlockedDesign} {
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetTaskRunStatus(ctx, f.Task.TaskID, to)
			}); !errors.Is(err, store.ErrForbiddenTaskTransition) {
				t.Fatalf("ABANDONED -> %s: want ErrForbiddenTaskTransition, got %v", string(to), err)
			}
		}

		// And at schema level.
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`UPDATE task_run SET status = 'READY' WHERE task_id = ?`, string(f.Task.TaskID))
		})
		if err == nil {
			t.Fatal("an abandoned task was resurrected")
		}
		if !strings.Contains(err.Error(), "forbidden task run status transition") {
			t.Fatalf("expected the transition trigger to fire, got: %v", err)
		}
	})

	t.Run("completed task cannot be reopened", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r3-reopen", state.IntentReadOnly, state.LaneReview)
		reviewed := sha("r3-reopen-reviewed")
		ev := reviewEvidenceFor(f, reviewed, "r3-reopen-artifact")
		if err := db.Write(ctx, func(tx *store.Tx) error {
			if err := tx.RecordEvidence(ctx, ev); err != nil {
				return err
			}
			return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
		}); err != nil {
			t.Fatalf("complete: %v", err)
		}

		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskReady)
		}); !errors.Is(err, store.ErrForbiddenTaskTransition) {
			t.Fatalf("want ErrForbiddenTaskTransition, got %v", err)
		}
	})

	t.Run("normal cycling remains possible", func(t *testing.T) {
		db := newDB(t)
		f := seed(t, db, "r3-cycle", state.IntentModifying, state.LaneOperator)
		for _, to := range []state.TaskRunStatus{
			state.TaskRunning, state.TaskBlockedDesign, state.TaskReady,
			state.TaskRunning, state.TaskAbandoned,
		} {
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetTaskRunStatus(ctx, f.Task.TaskID, to)
			}); err != nil {
				t.Fatalf("transition to %s refused: %v", string(to), err)
			}
		}
	})
}

// Finding 3: a terminal task could be given fresh live modifying work,
// occupying the repository's slot and desyncing the completion binding.
func TestTerminalTaskCannotTakeNewWork(t *testing.T) {
	ctx := context.Background()

	for _, terminal := range []state.TaskRunStatus{state.TaskCompleted, state.TaskAbandoned} {
		t.Run(string(terminal), func(t *testing.T) {
			db := newDB(t)
			f := seed(t, db, "r3-terminal-task", state.IntentModifying, state.LaneOperator)

			if terminal == state.TaskCompleted {
				commit := sha("r3-terminal-commit")
				pub := appliedPublication(t, db, f, commit, "refs/heads/m0/r3-terminal")
				ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)
				if err := db.Write(ctx, func(tx *store.Tx) error {
					if err := tx.RecordEvidence(ctx, ev); err != nil {
						return err
					}
					return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
				}); err != nil {
					t.Fatalf("complete: %v", err)
				}
			} else if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
			}); err != nil {
				t.Fatalf("abandon: %v", err)
			}

			// Free the modifying slot, as a real handover would.
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptAbandoned)
			}); err != nil {
				t.Fatalf("abandon attempt: %v", err)
			}

			successor := f.Attempt
			successor.AttemptID = ids.NewAttemptID()
			successor.FenceEpoch = f.Attempt.FenceEpoch + 1
			successor.Status = state.AttemptRunning

			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.CreateWorkerAttempt(ctx, successor)
			}); !errors.Is(err, store.ErrTaskNotLive) {
				t.Fatalf("want ErrTaskNotLive, got %v", err)
			}

			// And at schema level.
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx,
					`INSERT INTO worker_attempt (attempt_id, task_id, packet_id, repository_subject_id,
					                             lane, intent, scheduler_epoch, fence_epoch, status,
					                             created_at, updated_at)
					 VALUES (?, ?, ?, ?, 'operator', 'MODIFYING', ?, ?, 'CLAIMED',
					         '2026-09-12T09:00:00.000000000Z', '2026-09-12T09:00:00.000000000Z')`,
					string(successor.AttemptID), string(f.Task.TaskID), string(f.Packet.PacketID),
					string(f.Subject.RepositorySubjectID), int64(f.Epoch), int64(successor.FenceEpoch))
			})
			if err == nil {
				t.Fatalf("a %s task admitted a new attempt", string(terminal))
			}
			if !strings.Contains(err.Error(), "terminal task cannot admit a new attempt") {
				t.Fatalf("expected the terminal-task trigger to fire, got: %v", err)
			}

			// The completion binding must still agree with the pointer.
			if terminal == state.TaskCompleted {
				if err := db.Read(ctx, func(tx *store.Tx) error {
					run, err := tx.TaskRun(ctx, f.Task.TaskID)
					if err != nil {
						return err
					}
					if run.CurrentAttemptID == nil || *run.CurrentAttemptID != f.Attempt.AttemptID {
						t.Fatalf("current attempt drifted from the completing attempt: %v", run.CurrentAttemptID)
					}
					return nil
				}); err != nil {
					t.Fatalf("read: %v", err)
				}
			}
		})
	}
}

// Finding 4: JSON normalisation turned int64 into float64, silently
// corrupting large integers in append-only history.
func TestLargeIntegersSurviveEventHistory(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r3-integers", state.IntentReadOnly, state.LaneReview)

	e := eventFor(f.Epoch, "worker.metrics")
	e.Fields = map[string]any{
		"bytes":        int64(9007199254740993),
		"nanos":        int64(1789200000123456789),
		"max_int64":    int64(9223372036854775807),
		"negative":     int64(-9007199254740993),
		"small":        int64(42),
		"nested":       map[string]any{"inner": int64(9007199254740993)},
		"list":         []any{int64(9007199254740993), int64(7)},
		"uint":         uint64(18446744073709551615),
		"real":         1.5,
		"string_field": "unchanged",
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
			if ev.EventType == "worker.metrics" {
				stored = ev.Fields
			}
		}
		if stored == nil {
			t.Fatal("appended event not found")
		}

		exact := map[string]string{
			"bytes":     "9007199254740993",
			"nanos":     "1789200000123456789",
			"max_int64": "9223372036854775807",
			"negative":  "-9007199254740993",
			"small":     "42",
			"uint":      "18446744073709551615",
			"real":      "1.5",
		}
		for key, want := range exact {
			if got := fmt.Sprint(stored[key]); got != want {
				t.Fatalf("field %q was corrupted: stored %s, want %s", key, got, want)
			}
		}
		if got := fmt.Sprint(stored["nested"].(map[string]any)["inner"]); got != "9007199254740993" {
			t.Fatalf("nested integer corrupted: %s", got)
		}
		if got := fmt.Sprint(stored["list"].([]any)[0]); got != "9007199254740993" {
			t.Fatalf("integer in a list corrupted: %s", got)
		}
		if stored["string_field"] != "unchanged" {
			t.Fatalf("string field altered: %v", stored["string_field"])
		}

		// The durable text itself must carry the exact literals, not
		// exponent-form approximations.
		raw, err := tx.QueryStringForTest(ctx,
			`SELECT fields FROM event WHERE event_type = 'worker.metrics'`)
		if err != nil {
			return err
		}
		for _, literal := range []string{
			"9007199254740993", "1789200000123456789", "9223372036854775807",
		} {
			if !strings.Contains(raw, literal) {
				t.Fatalf("durable record lost the literal %s: %s", literal, raw)
			}
		}
		if strings.Contains(raw, "e+") {
			t.Fatalf("durable record stored a number in exponent form: %s", raw)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 5: a retired generation's closed history could still be appended to.
func TestRetiredEpochHistoryIsClosed(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r3-history", state.IntentReadOnly, state.LaneReview)
	oldEpoch := f.Epoch

	before := 0
	if err := db.Read(ctx, func(tx *store.Tx) error {
		events, err := tx.EventsInEpoch(ctx, oldEpoch)
		before = len(events)
		return err
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "successor"); err != nil {
		t.Fatalf("activate successor: %v", err)
	}

	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.AppendEvent(ctx, eventFor(oldEpoch, "backdated"))
		return err
	}); !errors.Is(err, store.ErrStaleEpoch) {
		t.Fatalf("want ErrStaleEpoch, got %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		events, err := tx.EventsInEpoch(ctx, oldEpoch)
		if err != nil {
			return err
		}
		if len(events) != before {
			t.Fatalf("retired epoch history grew from %d to %d records", before, len(events))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 6: the publish gate could not refuse a publication made under a
// packet that had gone stale, been superseded, or expired mid-run.
func TestPublishGateChecksPacketAuthority(t *testing.T) {
	f := fixtureForPreconditions()

	if err := domain.CheckPublishPreconditions(f.pub, f.live); err != nil {
		t.Fatalf("a coherent publication was rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(domain.PublishPreconditions) domain.PublishPreconditions
		want   error
	}{
		{"stale packet", func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.PacketStatus = state.PacketStale
			return p
		}, domain.ErrPacketNoAuthority},
		{"superseded packet", func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.PacketStatus = state.PacketSuperseded
			return p
		}, domain.ErrPacketNoAuthority},
		{"expired packet", func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.Now = p.PacketExpiresAt
			return p
		}, domain.ErrPacketExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := domain.CheckPublishPreconditions(f.pub, tc.mutate(f.live)); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

// Finding 7: a publication could be recorded already terminal, bypassing its
// whole lifecycle and immediately supporting completion.
func TestPublicationMustStartPending(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r3-initial-status", state.IntentModifying, state.LaneOperator)

	for _, status := range []state.PublishStatus{
		state.PublishApplied, state.PublishObserved, state.PublishRejected, state.PublishUnknown,
	} {
		t.Run(string(status), func(t *testing.T) {
			pub := publishFor(f, sha("r3-initial-"+string(status)), "refs/heads/m0/r3-"+strings.ToLower(string(status)))
			pub.Status = status
			pub.IdempotencyKey = ""
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.RecordPublishAttempt(ctx, pub)
			}); !errors.Is(err, domain.ErrPublishBindingInvalid) {
				t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
			}
		})
	}

	// An unset status is filled in as PENDING.
	pub := publishFor(f, sha("r3-initial-unset"), "refs/heads/m0/r3-unset")
	pub.Status = ""
	pub.IdempotencyKey = ""
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("unset status should default to PENDING: %v", err)
	}
	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if stored.Status != state.PublishPending {
			t.Fatalf("stored status is %q, want PENDING", string(stored.Status))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 8: a released workspace could be reset to live, defeating both
// released-workspace guards.
func TestWorkspaceReleaseIsFinal(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r3-release", state.IntentModifying, state.LaneOperator)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
	}); err != nil {
		t.Fatalf("release: %v", err)
	}
	released := time.Time{}
	if err := db.Read(ctx, func(tx *store.Tx) error {
		ws, err := tx.Workspace(ctx, f.Workspace.WorkspaceID)
		if err != nil {
			return err
		}
		if ws.ReleasedAt == nil {
			t.Fatal("release was not recorded")
		}
		released = *ws.ReleasedAt
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// Un-releasing must be refused.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE workspace SET released_at = NULL WHERE workspace_id = ?`,
			string(f.Workspace.WorkspaceID))
	})
	if err == nil {
		t.Fatal("a released workspace was reset to live")
	}
	if !strings.Contains(err.Error(), "release is final") {
		t.Fatalf("expected the release-finality trigger to fire, got: %v", err)
	}

	// Overwriting the release time must be refused too.
	err = db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE workspace SET released_at = '2027-01-01T00:00:00.000000000Z' WHERE workspace_id = ?`,
			string(f.Workspace.WorkspaceID))
	})
	if err == nil {
		t.Fatal("the release timestamp was overwritten")
	}

	// Releasing again is a no-op that preserves the original timestamp.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
	}); err != nil {
		t.Fatalf("re-releasing should be a no-op: %v", err)
	}
	if err := db.Read(ctx, func(tx *store.Tx) error {
		ws, err := tx.Workspace(ctx, f.Workspace.WorkspaceID)
		if err != nil {
			return err
		}
		if !ws.ReleasedAt.Equal(released) {
			t.Fatalf("release timestamp changed from %v to %v", released, *ws.ReleasedAt)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}
