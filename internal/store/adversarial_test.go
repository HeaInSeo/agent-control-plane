package store_test

// These tests attack the M0 foundation rather than exercise it. Each one
// encodes an invariant from the approved M0 packet and tries to violate it
// through the store API and, where the API would not permit the shape at all,
// through raw SQL — so that the schema's own constraints are shown to be
// load-bearing.

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

// --- CC7: evidence attribution -------------------------------------------

func TestEvidenceWithForeignAttemptIsRejected(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	victim := seed(t, db, "victim", state.IntentModifying, state.LaneOperator)
	attacker := seed(t, db, "attacker", state.IntentModifying, state.LaneGuardrail)

	commit := sha("foreign-commit")

	cases := []struct {
		name  string
		build func() domain.EvidenceObservation
	}{
		{
			name: "foreign attempt id",
			build: func() domain.EvidenceObservation {
				e := evidenceFor(victim, commit, commit, nil, domain.EvidenceBranchHead)
				e.AttemptID = attacker.Attempt.AttemptID
				return e
			},
		},
		{
			name: "foreign task id",
			build: func() domain.EvidenceObservation {
				e := evidenceFor(victim, commit, commit, nil, domain.EvidenceBranchHead)
				e.TaskID = attacker.Task.TaskID
				return e
			},
		},
		{
			name: "foreign repository subject",
			build: func() domain.EvidenceObservation {
				e := evidenceFor(victim, commit, commit, nil, domain.EvidenceBranchHead)
				e.RepositorySubjectID = attacker.Subject.RepositorySubjectID
				return e
			},
		},
		{
			name: "foreign workspace",
			build: func() domain.EvidenceObservation {
				e := evidenceFor(victim, commit, commit, nil, domain.EvidenceBranchHead)
				e.WorkspaceID = attacker.Workspace.WorkspaceID
				return e
			},
		},
		{
			name: "wrong scheduler epoch",
			build: func() domain.EvidenceObservation {
				e := evidenceFor(victim, commit, commit, nil, domain.EvidenceBranchHead)
				e.SchedulerEpoch = victim.Attempt.SchedulerEpoch + 7
				return e
			},
		},
		{
			name: "wrong fence epoch",
			build: func() domain.EvidenceObservation {
				e := evidenceFor(victim, commit, commit, nil, domain.EvidenceBranchHead)
				e.FenceEpoch = victim.Attempt.FenceEpoch + 3
				return e
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := tc.build()
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.RecordEvidence(ctx, ev)
			})
			if err == nil {
				t.Fatal("misattributed evidence was accepted")
			}
			if err := db.Read(ctx, func(tx *store.Tx) error {
				if _, err := tx.Evidence(ctx, ev.EvidenceID); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("misattributed evidence was persisted: %v", err)
				}
				return nil
			}); err != nil {
				t.Fatalf("read: %v", err)
			}
		})
	}
}

// The schema must reject misattribution even when the Go-level check is
// bypassed entirely.
func TestEvidenceMisattributionRejectedAtSchemaLevel(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	victim := seed(t, db, "schema-victim", state.IntentModifying, state.LaneOperator)
	attacker := seed(t, db, "schema-attacker", state.IntentModifying, state.LaneGuardrail)

	commit := sha("raw-commit")
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO evidence_observation (evidence_id, task_id, attempt_id, scheduler_epoch,
			                                   fence_epoch, repository_subject_id, workspace_id,
			                                   publish_attempt_id, published_sha, observed_sha,
			                                   observed_at, evidence_kind)
			 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, 'BRANCH_HEAD')`,
			string(ids.NewEvidenceID()),
			string(victim.Task.TaskID),
			string(victim.Attempt.AttemptID),
			int64(victim.Attempt.SchedulerEpoch),
			int64(victim.Attempt.FenceEpoch),
			string(victim.Subject.RepositorySubjectID),
			// Another attempt's workspace.
			string(attacker.Workspace.WorkspaceID),
			string(commit), string(commit),
			"2026-09-12T09:00:00.000000000Z")
	})
	if err == nil {
		t.Fatal("raw insert of evidence naming a foreign workspace was accepted")
	}
	if !strings.Contains(err.Error(), "attributed to its own attempt identity") {
		t.Fatalf("expected the attribution trigger to fire, got: %v", err)
	}
}

func TestAncestorOrEqualIsNotAcceptedAsCompletion(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "advanced-ref", state.IntentModifying, state.LaneOperator)

	published := sha("our-commit")
	// An unrelated human commit advanced the branch after our publication.
	humanCommit := sha("someone-elses-commit")

	pub := publishFor(f, published, "refs/heads/m0/advanced")
	ev := evidenceFor(f, published, humanCommit, &pub.PublishAttemptID, domain.EvidenceBranchHead)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil {
			return err
		}
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied); err != nil {
			return err
		}
		// Recording the observation is fine: it is a true observation.
		return tx.RecordEvidence(ctx, ev)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	})
	if !errors.Is(err, domain.ErrEvidenceInsufficient) {
		t.Fatalf("want ErrEvidenceInsufficient for an advanced ref, got %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status == state.TaskCompleted {
			t.Fatal("task completed on ancestor-or-equal evidence")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// --- CC1: publication identity -------------------------------------------

func TestDuplicatePublishIdentityIsRejected(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "idempotency", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("dup-commit"), "refs/heads/m0/dup")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("first publish record: %v", err)
	}

	// A retry of the very same intent under a fresh publish_attempt_id must
	// still be refused: the identity is the intent, not the row id.
	retry := pub
	retry.PublishAttemptID = ids.NewPublishAttemptID()
	retry.IdempotencyKey = domain.DeriveIdempotencyKey(retry)
	if retry.IdempotencyKey != pub.IdempotencyKey {
		t.Fatalf("idempotency key is not derived from the intent: %s vs %s",
			retry.IdempotencyKey, pub.IdempotencyKey)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, retry)
	})
	if !errors.Is(err, store.ErrDuplicatePublishIdentity) {
		t.Fatalf("want ErrDuplicatePublishIdentity, got %v", err)
	}
}

func TestForeignOrStaleAttemptCannotReusePublishIdentity(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "publish-owner", state.IntentModifying, state.LaneOperator)
	other := seed(t, db, "publish-thief", state.IntentModifying, state.LaneGuardrail)

	commit := sha("owned-commit")
	pub := publishFor(f, commit, "refs/heads/m0/owned")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	t.Run("another attempt cannot borrow the idempotency key", func(t *testing.T) {
		thief := publishFor(other, commit, "refs/heads/m0/owned")
		thief.IdempotencyKey = pub.IdempotencyKey
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, thief)
		})
		// Rejected before it can reach the uniqueness constraint: the key is
		// the intent, so a key that is not this intent's is not a key.
		if !errors.Is(err, domain.ErrPublishBindingInvalid) {
			t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
		}
	})

	t.Run("another attempt cannot publish from this workspace", func(t *testing.T) {
		thief := publishFor(other, commit, "refs/heads/m0/stolen")
		thief.WorkspaceID = f.Workspace.WorkspaceID // another attempt's workspace
		thief.IdempotencyKey = domain.DeriveIdempotencyKey(thief)
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, thief)
		})
		// Refused as a typed binding error before it reaches the trigger.
		if !errors.Is(err, domain.ErrPublishBindingInvalid) {
			t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
		}

		// The trigger is still the backstop, proven by bypassing the Go gate.
		err = db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`INSERT INTO publish_attempt (publish_attempt_id, task_id, attempt_id,
				                              scheduler_epoch, fence_epoch, workspace_id,
				                              repository_subject_id, base_sha, source_commit_sha,
				                              target_ref, idempotency_key, status, created_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING',
				         '2026-09-12T09:00:00.000000000Z')`,
				string(ids.NewPublishAttemptID()), string(other.Task.TaskID),
				string(other.Attempt.AttemptID), int64(other.Attempt.SchedulerEpoch),
				int64(other.Attempt.FenceEpoch), string(f.Workspace.WorkspaceID),
				string(other.Subject.RepositorySubjectID), string(other.Workspace.BaseSHA),
				string(commit), "refs/heads/m0/stolen-raw", "raw-key-"+string(commit))
		})
		if err == nil {
			t.Fatal("publication from a foreign workspace was accepted")
		}
		if !strings.Contains(err.Error(), "must match its attempt and workspace") {
			t.Fatalf("expected the binding trigger to fire, got: %v", err)
		}
	})

	t.Run("publication cannot claim a foreign base commit", func(t *testing.T) {
		forged := publishFor(f, commit, "refs/heads/m0/forged")
		forged.BaseSHA = other.Workspace.BaseSHA // not this workspace's base
		forged.IdempotencyKey = domain.DeriveIdempotencyKey(forged)
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, forged)
		})
		if err == nil {
			t.Fatal("publication with foreign base commit metadata was accepted")
		}
	})

	t.Run("read-only attempt cannot publish", func(t *testing.T) {
		review := seed(t, db, "reviewer", state.IntentReadOnly, state.LaneReview)
		p := publishFor(review, commit, "refs/heads/m0/review")
		p.IdempotencyKey = domain.DeriveIdempotencyKey(p)
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, p)
		})
		if err == nil {
			t.Fatal("a read-only attempt was allowed to record a publication")
		}
	})
}

func TestPublishRequiresExactImmutableCommit(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "exact-commit", state.IntentModifying, state.LaneOperator)

	for _, bad := range []struct {
		name   string
		source domain.CommitSHA
	}{
		{"branch name", "main"},
		{"symbolic head", "HEAD"},
		{"remote branch", "origin/main"},
		{"abbreviated sha", "829777a"},
		{"empty", ""},
	} {
		t.Run(bad.name, func(t *testing.T) {
			p := publishFor(f, bad.source, "refs/heads/m0/exact")
			p.IdempotencyKey = domain.DeriveIdempotencyKey(p)
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.RecordPublishAttempt(ctx, p)
			})
			if !errors.Is(err, domain.ErrPublishBindingInvalid) {
				t.Fatalf("want ErrPublishBindingInvalid for %q, got %v", string(bad.source), err)
			}
		})
	}

	for _, ref := range []string{"main", "HEAD", "refs/heads/HEAD", "refs/heads/*", "refs/heads/a..b", "refs/heads/with space"} {
		t.Run("target "+ref, func(t *testing.T) {
			p := publishFor(f, sha("ok-commit"), ref)
			p.IdempotencyKey = domain.DeriveIdempotencyKey(p)
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.RecordPublishAttempt(ctx, p)
			})
			if !errors.Is(err, domain.ErrPublishTargetInvalid) {
				t.Fatalf("want ErrPublishTargetInvalid for %q, got %v", ref, err)
			}
		})
	}
}

// --- CC4: scheduler epoch -------------------------------------------------

func TestOldEpochRecordsAreReadableButCannotActUnderCurrentOwnership(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "epoch-handover", state.IntentModifying, state.LaneOperator)
	oldEpoch := f.Epoch

	// A new scheduler generation takes over.
	activated, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "successor activation")
	if err != nil {
		t.Fatalf("activate successor: %v", err)
	}
	if activated.Epoch != oldEpoch+1 {
		t.Fatalf("successor epoch is %d, want %d", int64(activated.Epoch), int64(oldEpoch+1))
	}

	// Old records remain readable.
	if err := db.Read(ctx, func(tx *store.Tx) error {
		attempt, err := tx.WorkerAttempt(ctx, f.Attempt.AttemptID)
		if err != nil {
			return err
		}
		if attempt.SchedulerEpoch != oldEpoch {
			t.Fatalf("old attempt epoch changed to %d", int64(attempt.SchedulerEpoch))
		}
		epochRecord, err := tx.SchedulerEpoch(ctx, oldEpoch)
		if err != nil {
			return err
		}
		if epochRecord.Epoch != oldEpoch {
			t.Fatalf("old epoch record unreadable: %+v", epochRecord)
		}
		events, err := tx.EventsInEpoch(ctx, oldEpoch)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			t.Fatal("old epoch history disappeared")
		}
		return nil
	}); err != nil {
		t.Fatalf("read old epoch: %v", err)
	}

	// But they cannot act.
	t.Run("stale epoch cannot admit an attempt", func(t *testing.T) {
		stale := f.Attempt
		stale.AttemptID = ids.NewAttemptID()
		stale.FenceEpoch = f.Attempt.FenceEpoch + 1
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkerAttempt(ctx, stale)
		})
		if !errors.Is(err, store.ErrStaleEpoch) {
			t.Fatalf("want ErrStaleEpoch, got %v", err)
		}
	})

	t.Run("stale epoch cannot publish", func(t *testing.T) {
		p := publishFor(f, sha("stale-commit"), "refs/heads/m0/stale")
		p.IdempotencyKey = domain.DeriveIdempotencyKey(p)
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, p)
		})
		if !errors.Is(err, store.ErrStaleEpoch) {
			t.Fatalf("want ErrStaleEpoch, got %v", err)
		}
	})

	t.Run("schema rejects a stale-epoch attempt inserted raw", func(t *testing.T) {
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`INSERT INTO worker_attempt (attempt_id, task_id, packet_id, repository_subject_id,
				                             lane, intent, scheduler_epoch, fence_epoch, status,
				                             created_at, updated_at)
				 VALUES (?, ?, ?, ?, 'operator', 'MODIFYING', ?, 99, 'CLAIMED',
				         '2026-09-12T09:00:00.000000000Z', '2026-09-12T09:00:00.000000000Z')`,
				string(ids.NewAttemptID()), string(f.Task.TaskID), string(f.Packet.PacketID),
				string(f.Subject.RepositorySubjectID), int64(oldEpoch))
		})
		if err == nil {
			t.Fatal("raw insert under a stale epoch was accepted")
		}
		if !strings.Contains(err.Error(), "current scheduler epoch") {
			t.Fatalf("expected the epoch trigger to fire, got: %v", err)
		}
	})

	t.Run("ownership cannot regress to an older generation", func(t *testing.T) {
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`UPDATE scheduler_ownership SET current_epoch = ? WHERE id = 1`, int64(oldEpoch))
		})
		if err == nil {
			t.Fatal("scheduler ownership was allowed to regress")
		}
		if !strings.Contains(err.Error(), "must move forward") {
			t.Fatalf("expected the ownership trigger to fire, got: %v", err)
		}
	})
}

func TestAttemptRequiresAnActivatedScheduler(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	// No scheduler has been activated on this database.
	if _, err := db.CurrentEpoch(ctx); !errors.Is(err, store.ErrNoSchedulerEpoch) {
		t.Fatalf("want ErrNoSchedulerEpoch, got %v", err)
	}
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, domain.WorkerAttempt{
			AttemptID:           ids.NewAttemptID(),
			TaskID:              ids.NewTaskID(),
			PacketID:            ids.NewPacketID(),
			RepositorySubjectID: ids.NewRepositorySubjectID(),
			Lane:                state.LaneOperator,
			Intent:              state.IntentModifying,
			SchedulerEpoch:      1,
			FenceEpoch:          1,
			Status:              state.AttemptClaimed,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	})
	if !errors.Is(err, store.ErrNoSchedulerEpoch) {
		t.Fatalf("want ErrNoSchedulerEpoch, got %v", err)
	}
}

// --- CC9: lane-agnostic modifying exclusion -------------------------------

func TestSameRepositoryModifyingExclusionIsLaneAgnostic(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "shared-repo", state.IntentModifying, state.LaneOperator)

	// A second task on the SAME repository subject, in a DIFFERENT lane.
	guardrailTask := ids.NewTaskID()
	guardrailPacket := newPacket(guardrailTask, f.Subject.RepositorySubjectID, state.IntentModifying, state.LaneGuardrail)
	guardrailAttempt := domain.WorkerAttempt{
		AttemptID:           ids.NewAttemptID(),
		TaskID:              guardrailTask,
		PacketID:            guardrailPacket.PacketID,
		RepositorySubjectID: f.Subject.RepositorySubjectID,
		Lane:                state.LaneGuardrail,
		Intent:              state.IntentModifying,
		SchedulerEpoch:      f.Epoch,
		FenceEpoch:          1,
		Status:              state.AttemptClaimed,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
	}

	// Commit the guardrail packet and task on their own, so the attempt
	// insert below is the only thing that can fail.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.ApprovePacket(ctx, guardrailPacket); err != nil {
			return err
		}
		return tx.CreateTaskRun(ctx, domain.TaskRun{
			TaskID:              guardrailTask,
			RepositorySubjectID: f.Subject.RepositorySubjectID,
			PacketID:            guardrailPacket.PacketID,
			Lane:                state.LaneGuardrail,
			Intent:              state.IntentModifying,
			Status:              state.TaskReady,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	}); err != nil {
		t.Fatalf("seed guardrail task: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, guardrailAttempt)
	})
	if err == nil {
		t.Fatal("a guardrail-lane modifying attempt was admitted while an operator-lane one was live")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Fatalf("expected the modifying-slot index to reject it, got: %v", err)
	}

	// A read-only attempt on the same repository is not excluded.
	review := seed(t, db, "shared-repo-review", state.IntentReadOnly, state.LaneReview)
	if review.Attempt.AttemptID == "" {
		t.Fatal("read-only attempt should be admissible alongside a modifying one")
	}

	// Once the modifying attempt reaches a terminal state, the slot frees up.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptAbandoned)
	}); err != nil {
		t.Fatalf("abandon attempt: %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, guardrailAttempt)
	}); err != nil {
		t.Fatalf("modifying slot was not released after abandonment: %v", err)
	}
}

// --- CC3: workspace isolation ---------------------------------------------

func TestWorkspaceCannotBeSharedOrRebound(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	a := seed(t, db, "ws-a", state.IntentModifying, state.LaneOperator)
	b := seed(t, db, "ws-b", state.IntentModifying, state.LaneGuardrail)

	t.Run("second workspace for the same attempt is rejected", func(t *testing.T) {
		second := a.Workspace
		second.WorkspaceID = ids.NewWorkspaceID()
		second.RootPath = a.Workspace.RootPath + "-second"
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, second)
		})
		if err == nil {
			t.Fatal("an attempt was given a second workspace")
		}
	})

	t.Run("two attempts cannot share a root path", func(t *testing.T) {
		shared := b.Workspace
		shared.WorkspaceID = ids.NewWorkspaceID()
		shared.AttemptID = b.Attempt.AttemptID
		shared.RootPath = a.Workspace.RootPath
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, shared)
		})
		if err == nil {
			t.Fatal("two attempts were pointed at the same workspace directory")
		}
	})

	t.Run("workspace cannot be rebound to another attempt", func(t *testing.T) {
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx,
				`UPDATE workspace SET attempt_id = ? WHERE workspace_id = ?`,
				string(b.Attempt.AttemptID), string(a.Workspace.WorkspaceID))
		})
		if err == nil {
			t.Fatal("a workspace was rebound to a different attempt")
		}
		if !strings.Contains(err.Error(), "ownership is immutable") {
			t.Fatalf("expected the ownership trigger to fire, got: %v", err)
		}
	})

	t.Run("lookup by attempt never returns another attempt's workspace", func(t *testing.T) {
		if err := db.Read(ctx, func(tx *store.Tx) error {
			got, err := tx.WorkspaceForAttempt(ctx, a.Attempt.AttemptID)
			if err != nil {
				return err
			}
			if got.WorkspaceID != a.Workspace.WorkspaceID {
				t.Fatalf("attempt %s got workspace %s", string(a.Attempt.AttemptID), string(got.WorkspaceID))
			}
			return nil
		}); err != nil {
			t.Fatalf("read: %v", err)
		}
	})
}

// --- CC6: state domains cannot be conflated -------------------------------

func TestStateDomainsCannotBeConflated(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "state-domains", state.IntentModifying, state.LaneOperator)

	// A real publication row is needed, or the cross-domain UPDATE below would
	// match nothing and pass for the wrong reason.
	pub := publishFor(f, sha("state-domain-commit"), "refs/heads/m0/state-domains")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record publish: %v", err)
	}

	// Cross-domain values are compile errors at the Go level, so what is left
	// to test is that the stored columns reject them too.
	conflations := []struct {
		name string
		stmt string
		args []any
	}{
		{
			name: "PACKET_STALE as a worker attempt state",
			stmt: `UPDATE worker_attempt SET status = 'STALE' WHERE attempt_id = ?`,
			args: []any{string(f.Attempt.AttemptID)},
		},
		{
			name: "COMPLETED as a worker attempt state",
			stmt: `UPDATE worker_attempt SET status = 'COMPLETED' WHERE attempt_id = ?`,
			args: []any{string(f.Attempt.AttemptID)},
		},
		{
			name: "a publish state on a task run",
			stmt: `UPDATE task_run SET status = 'OBSERVED' WHERE task_id = ?`,
			args: []any{string(f.Task.TaskID)},
		},
		{
			name: "a task state on a packet",
			stmt: `UPDATE execution_packet SET status = 'READY' WHERE packet_id = ?`,
			args: []any{string(f.Packet.PacketID)},
		},
		{
			name: "an attempt state on a publish attempt",
			stmt: `UPDATE publish_attempt SET status = 'RUNNING' WHERE publish_attempt_id = ?`,
			args: []any{string(pub.PublishAttemptID)},
		},
	}

	for _, tc := range conflations {
		t.Run(tc.name, func(t *testing.T) {
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, tc.stmt, tc.args...)
			})
			if err == nil {
				t.Fatalf("cross-domain state %q was accepted", tc.name)
			}
		})
	}
}

func TestWorkerExitCannotCompleteATask(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "exit-zero", state.IntentModifying, state.LaneOperator)

	// The API refuses to write COMPLETED at all.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskCompleted)
	})
	if !errors.Is(err, store.ErrCompletionNotDerivable) {
		t.Fatalf("want ErrCompletionNotDerivable, got %v", err)
	}

	// And the schema refuses COMPLETED without bound evidence.
	err = db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE task_run SET status = 'COMPLETED' WHERE task_id = ?`, string(f.Task.TaskID))
	})
	if err == nil {
		t.Fatal("a task reached COMPLETED without evidence")
	}

	// Nor can a task be created already complete.
	err = db.Write(ctx, func(tx *store.Tx) error {
		run := f.Task
		run.TaskID = ids.NewTaskID()
		run.Status = state.TaskCompleted
		return tx.CreateTaskRun(ctx, run)
	})
	if err == nil {
		t.Fatal("a task was created already COMPLETED")
	}
}

func TestCompletionCannotBorrowAnotherTasksEvidence(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	owner := seed(t, db, "evidence-owner", state.IntentModifying, state.LaneOperator)
	borrower := seed(t, db, "evidence-borrower", state.IntentModifying, state.LaneGuardrail)

	commit := sha("owner-commit")
	pub := publishFor(owner, commit, "refs/heads/m0/owner")
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
		t.Fatalf("record: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, borrower.Task.TaskID, ev.EvidenceID)
	})
	if !errors.Is(err, store.ErrCompletionNotDerivable) {
		t.Fatalf("want ErrCompletionNotDerivable, got %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		run, err := tx.TaskRun(ctx, borrower.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status == state.TaskCompleted {
			t.Fatal("a task completed on another task's evidence")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestModifyingCompletionRequiresAnAppliedPublication(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "unapplied-publish", state.IntentModifying, state.LaneOperator)

	commit := sha("pending-commit")
	pub := publishFor(f, commit, "refs/heads/m0/pending")
	ev := evidenceFor(f, commit, commit, &pub.PublishAttemptID, domain.EvidenceBranchHead)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, pub); err != nil { // stays PENDING
			return err
		}
		return tx.RecordEvidence(ctx, ev)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ev.EvidenceID)
	})
	if !errors.Is(err, domain.ErrEvidenceInsufficient) {
		t.Fatalf("want ErrEvidenceInsufficient for a PENDING publication, got %v", err)
	}
}

// --- CC10: event identity -------------------------------------------------

func TestEventIdentityCollisionIsRejected(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "event-collision", state.IntentReadOnly, state.LaneReview)

	var seq int64
	if err := db.Write(ctx, func(tx *store.Tx) error {
		var err error
		seq, err = tx.AppendEvent(ctx, eventFor(f.Epoch, "first"))
		return err
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.AppendEventAt(ctx, eventFor(f.Epoch, "collider"), seq)
	})
	if !errors.Is(err, store.ErrDuplicateEventIdentity) {
		t.Fatalf("want ErrDuplicateEventIdentity, got %v", err)
	}

	// The same seq in a different epoch is a different identity and is fine.
	next, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "second generation")
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.AppendEventAt(ctx, eventFor(next.Epoch, "same seq, new epoch"), seq)
	}); err != nil {
		t.Fatalf("same seq in a new epoch should be accepted: %v", err)
	}
}

func TestEventHistoryIsAppendOnly(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "append-only", state.IntentReadOnly, state.LaneReview)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.AppendEvent(ctx, eventFor(f.Epoch, "immutable"))
		return err
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	for _, stmt := range []string{
		`UPDATE event SET event_type = 'rewritten' WHERE scheduler_epoch = ?`,
		`DELETE FROM event WHERE scheduler_epoch = ?`,
	} {
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx, stmt, int64(f.Epoch))
		})
		if err == nil {
			t.Fatalf("event history accepted %q", stmt)
		}
		if !strings.Contains(err.Error(), "append-only") {
			t.Fatalf("expected the append-only trigger to fire, got: %v", err)
		}
	}
}

func TestEventFieldsAreRedactedOnAppend(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "redaction", state.IntentReadOnly, state.LaneReview)

	e := eventFor(f.Epoch, "worker.env")
	e.Fields = map[string]any{
		"github_token":   "fake-not-a-real-token",
		"ssh_key_path":   "/home/user/.ssh/id_ed25519",
		"authorization":  "Bearer abc",
		"nested":         map[string]any{"api_key": "fake-not-a-real-key", "repo": "HeaInSeo/x"},
		"repository":     "HeaInSeo/agent-control-plane",
		"attempt_number": 2,
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
		var found bool
		for _, stored := range events {
			if stored.EventType != "worker.env" {
				continue
			}
			found = true
			for _, key := range []string{"github_token", "ssh_key_path", "authorization"} {
				if stored.Fields[key] != domain.Redacted {
					t.Fatalf("field %q reached history as %v", key, stored.Fields[key])
				}
			}
			nested, ok := stored.Fields["nested"].(map[string]any)
			if !ok {
				t.Fatalf("nested field lost its shape: %v", stored.Fields["nested"])
			}
			if nested["api_key"] != domain.Redacted {
				t.Fatalf("nested api_key reached history as %v", nested["api_key"])
			}
			if nested["repo"] != "HeaInSeo/x" {
				t.Fatalf("nested non-sensitive field was mangled: %v", nested["repo"])
			}
			if stored.Fields["repository"] != "HeaInSeo/agent-control-plane" {
				t.Fatalf("non-sensitive field was redacted: %v", stored.Fields["repository"])
			}
		}
		if !found {
			t.Fatal("appended event not found")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// --- CC5: repository identity --------------------------------------------

func TestAmbiguousRepositoryAliasFailsClosed(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	// Observing a repository is a scheduler-owned write, so the handle needs
	// ownership before it can record anything.
	if _, err := db.ActivateScheduler(ctx, ids.NewSchedulerOwnerID(), "alias test"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	// Two distinct repositories transiently observed under the same alias,
	// which is what a rename looks like before reconciliation catches up.
	first := newSubject("alias-a")
	second := newSubject("alias-b")
	second.CurrentFullName = first.CurrentFullName

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if _, err := tx.ObserveRepositorySubject(ctx, first); err != nil {
			return err
		}
		_, err := tx.ObserveRepositorySubject(ctx, second)
		return err
	}); err != nil {
		t.Fatalf("observe: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		if _, err := tx.RepositorySubjectByFullName(ctx, first.CurrentFullName); !errors.Is(err, store.ErrAmbiguous) {
			t.Fatalf("want ErrAmbiguous, got %v", err)
		}
		// Identity lookups stay unambiguous.
		a, err := tx.RepositorySubjectByNodeID(ctx, first.GitHubNodeID)
		if err != nil {
			return err
		}
		b, err := tx.RepositorySubjectByNodeID(ctx, second.GitHubNodeID)
		if err != nil {
			return err
		}
		if a.RepositorySubjectID == b.RepositorySubjectID {
			t.Fatal("two node ids collapsed onto one subject")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestRepositorySubjectNodeIDIsImmutable(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "immutable-node", state.IntentReadOnly, state.LaneReview)

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE repository_subject SET github_node_id = 'R_node_other' WHERE repository_subject_id = ?`,
			string(f.Subject.RepositorySubjectID))
	})
	if err == nil {
		t.Fatal("a repository subject's stable identity was reassigned")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected the immutability trigger to fire, got: %v", err)
	}
}

// --- CC8: packet source binding ------------------------------------------

func TestStoredPacketSourceBindingIsImmutable(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "packet-binding", state.IntentModifying, state.LaneOperator)

	for _, stmt := range []string{
		`UPDATE execution_packet SET source_revision = 'notion:rev-2' WHERE packet_id = ?`,
		`UPDATE execution_packet SET allowed_scope = '["anything"]' WHERE packet_id = ?`,
		`UPDATE execution_packet SET forbidden_scope = '[]' WHERE packet_id = ?`,
	} {
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.ExecForTest(ctx, stmt, string(f.Packet.PacketID))
		})
		if err == nil {
			t.Fatalf("approved packet accepted %q", stmt)
		}
		if !strings.Contains(err.Error(), "immutable") {
			t.Fatalf("expected the binding trigger to fire, got: %v", err)
		}
	}

	// Status may still move: staleness is a status change, not a rebinding.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetPacketStatus(ctx, f.Packet.PacketID, state.PacketStale)
	}); err != nil {
		t.Fatalf("marking a packet stale should be allowed: %v", err)
	}
}

func TestEmptyAllowedScopeIsRejectedByTheSchema(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "empty-scope", state.IntentReadOnly, state.LaneReview)

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`INSERT INTO execution_packet (packet_id, task_id, lane, intent, repository_subject_id,
			                               source_revision, source_digest, packet_digest,
			                               approved_at, expires_at, allowed_scope, forbidden_scope,
			                               stop_conditions, acceptance_contract, status)
			 VALUES (?, ?, 'review', 'READ_ONLY', ?, 'rev', ?, ?,
			         '2026-09-12T09:00:00.000000000Z', '2026-09-13T09:00:00.000000000Z',
			         '[]', '[]', '[]', 'contract', 'APPROVED')`,
			string(ids.NewPacketID()), string(ids.NewTaskID()),
			string(f.Subject.RepositorySubjectID),
			string(digestOf("s")), string(digestOf("p")))
	})
	if err == nil {
		t.Fatal("a packet with an empty allowed scope was stored")
	}
}

func TestTaskRunMustMatchItsPacket(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "packet-coherence", state.IntentModifying, state.LaneOperator)
	other := seed(t, db, "packet-coherence-other", state.IntentModifying, state.LaneGuardrail)

	// A task pointing at a packet approved for a different task.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateTaskRun(ctx, domain.TaskRun{
			TaskID:              ids.NewTaskID(),
			RepositorySubjectID: f.Subject.RepositorySubjectID,
			PacketID:            other.Packet.PacketID,
			Lane:                state.LaneOperator,
			Intent:              state.IntentModifying,
			Status:              state.TaskReady,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	})
	if err == nil {
		t.Fatal("a task run was bound to another task's packet")
	}
	if !strings.Contains(err.Error(), "must match its execution_packet") {
		t.Fatalf("expected the coherence trigger to fire, got: %v", err)
	}
}

func TestAttemptMustMatchItsTask(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "attempt-coherence", state.IntentModifying, state.LaneOperator)
	other := seed(t, db, "attempt-coherence-other", state.IntentReadOnly, state.LaneReview)

	bad := f.Attempt
	bad.AttemptID = ids.NewAttemptID()
	bad.FenceEpoch = 2
	bad.RepositorySubjectID = other.Subject.RepositorySubjectID

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, bad)
	})
	if err == nil {
		t.Fatal("an attempt was admitted for a repository its task does not own")
	}
}

func TestFenceEpochIsMonotonicWithinATask(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "fencing", state.IntentModifying, state.LaneOperator)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed)
	}); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}

	// Reusing an already-issued fencing token must be refused.
	replay := f.Attempt
	replay.AttemptID = ids.NewAttemptID()
	replay.FenceEpoch = f.Attempt.FenceEpoch
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, replay)
	}); err == nil {
		t.Fatal("a fencing token was reissued within one task")
	}

	// The next token is accepted.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		next, err := tx.NextFenceEpoch(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if next != f.Attempt.FenceEpoch+1 {
			t.Fatalf("next fence epoch is %d, want %d", int64(next), int64(f.Attempt.FenceEpoch+1))
		}
		retry := f.Attempt
		retry.AttemptID = ids.NewAttemptID()
		retry.FenceEpoch = next
		retry.Status = state.AttemptClaimed
		return tx.CreateWorkerAttempt(ctx, retry)
	}); err != nil {
		t.Fatalf("successor attempt rejected: %v", err)
	}
}

func TestPublishPreconditionsRejectStaleBinding(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "preconditions", state.IntentModifying, state.LaneOperator)

	commit := sha("precondition-commit")
	pub := publishFor(f, commit, "refs/heads/m0/preconditions")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	live := domain.PublishPreconditions{
		CurrentSchedulerEpoch: f.Epoch,
		TaskCurrentAttemptID:  f.Attempt.AttemptID,
		AttemptStatus:         state.AttemptVerifying,
		AttemptFenceEpoch:     f.Attempt.FenceEpoch,
		WorkspaceAttemptID:    f.Attempt.AttemptID,
		WorkspaceReleased:     false,
		RepositorySubjectID:   f.Subject.RepositorySubjectID,
		CommitInWorkspace:     true,
		TaskStatus:            state.TaskRunning,
		PacketStatus:          state.PacketApproved,
		PacketExpiresAt:       f.Packet.ExpiresAt,
		Now:                   fixedNow,
	}
	if err := domain.CheckPublishPreconditions(pub, live); err != nil {
		t.Fatalf("a coherent publication was rejected: %v", err)
	}

	mutations := map[string]func(domain.PublishPreconditions) domain.PublishPreconditions{
		"scheduler epoch advanced": func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.CurrentSchedulerEpoch++
			return p
		},
		"task moved to another attempt": func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.TaskCurrentAttemptID = ids.NewAttemptID()
			return p
		},
		"attempt was fenced out": func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.AttemptFenceEpoch++
			return p
		},
		"attempt is terminal": func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.AttemptStatus = state.AttemptAbandoned
			return p
		},
		"workspace belongs to another attempt": func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.WorkspaceAttemptID = ids.NewAttemptID()
			return p
		},
		"workspace was released": func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.WorkspaceReleased = true
			return p
		},
		"repository subject changed": func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.RepositorySubjectID = ids.NewRepositorySubjectID()
			return p
		},
		"commit is not in the workspace": func(p domain.PublishPreconditions) domain.PublishPreconditions {
			p.CommitInWorkspace = false
			return p
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			if err := domain.CheckPublishPreconditions(pub, mutate(live)); !errors.Is(err, domain.ErrPublishBindingInvalid) {
				t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
			}
		})
	}

	// The packet must still carry authority at the moment of publication, not
	// only at launch and resume.
	t.Run("packet went stale mid-run", func(t *testing.T) {
		stale := live
		stale.PacketStatus = state.PacketStale
		if err := domain.CheckPublishPreconditions(pub, stale); !errors.Is(err, domain.ErrPacketNoAuthority) {
			t.Fatalf("want ErrPacketNoAuthority, got %v", err)
		}
	})
	t.Run("packet was superseded mid-run", func(t *testing.T) {
		superseded := live
		superseded.PacketStatus = state.PacketSuperseded
		if err := domain.CheckPublishPreconditions(pub, superseded); !errors.Is(err, domain.ErrPacketNoAuthority) {
			t.Fatalf("want ErrPacketNoAuthority, got %v", err)
		}
	})
	t.Run("packet expired mid-run", func(t *testing.T) {
		expired := live
		expired.Now = f.Packet.ExpiresAt
		if err := domain.CheckPublishPreconditions(pub, expired); !errors.Is(err, domain.ErrPacketExpired) {
			t.Fatalf("want ErrPacketExpired, got %v", err)
		}
	})
	t.Run("withdrawn task", func(t *testing.T) {
		for _, status := range []state.TaskRunStatus{state.TaskAbandoned, state.TaskCompleted} {
			withdrawn := live
			withdrawn.TaskStatus = status
			if err := domain.CheckPublishPreconditions(pub, withdrawn); !errors.Is(err, domain.ErrPublishBindingInvalid) {
				t.Fatalf("%s: want ErrPublishBindingInvalid, got %v", string(status), err)
			}
		}
	})
	t.Run("packet authority unset fails closed", func(t *testing.T) {
		for _, mutate := range []func(domain.PublishPreconditions) domain.PublishPreconditions{
			func(p domain.PublishPreconditions) domain.PublishPreconditions { p.PacketStatus = ""; return p },
			func(p domain.PublishPreconditions) domain.PublishPreconditions { p.TaskStatus = ""; return p },
			func(p domain.PublishPreconditions) domain.PublishPreconditions { p.Now = time.Time{}; return p },
			func(p domain.PublishPreconditions) domain.PublishPreconditions {
				p.PacketExpiresAt = time.Time{}
				return p
			},
		} {
			if err := domain.CheckPublishPreconditions(pub, mutate(live)); err == nil {
				t.Fatal("an unset precondition was accepted")
			}
		}
	})
}

func TestExpiredPacketCannotAuthorizeExecution(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "expiry", state.IntentModifying, state.LaneOperator)

	if err := db.Read(ctx, func(tx *store.Tx) error {
		p, err := tx.Packet(ctx, f.Packet.PacketID)
		if err != nil {
			return err
		}
		observed := domain.SourceBinding{Revision: p.SourceRevision, Digest: p.SourceDigest}
		if err := p.Authorize(fixedNow, observed); err != nil {
			t.Fatalf("a valid packet was refused: %v", err)
		}
		if err := p.Authorize(p.ExpiresAt.Add(time.Second), observed); !errors.Is(err, domain.ErrPacketExpired) {
			t.Fatalf("want ErrPacketExpired, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestTaskCannotPointAtAnotherTasksAttempt(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	a := seed(t, db, "pointer-a", state.IntentModifying, state.LaneOperator)
	b := seed(t, db, "pointer-b", state.IntentReadOnly, state.LaneReview)

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskCurrentAttempt(ctx, a.Task.TaskID, b.Attempt.AttemptID)
	})
	if !errors.Is(err, store.ErrInvalidTaskRun) {
		t.Fatalf("want ErrInvalidTaskRun, got %v", err)
	}

	// The schema must refuse it too, not only the Go guard.
	err = db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE task_run SET current_attempt_id = ? WHERE task_id = ?`,
			string(b.Attempt.AttemptID), string(a.Task.TaskID))
	})
	if err == nil {
		t.Fatal("a task was pointed at another task's attempt")
	}
	if !strings.Contains(err.Error(), "must belong to this task") {
		t.Fatalf("expected the current-attempt trigger to fire, got: %v", err)
	}
}

func TestEvidenceCannotClaimAForeignPublication(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	owner := seed(t, db, "pub-owner", state.IntentModifying, state.LaneOperator)
	other := seed(t, db, "pub-other", state.IntentModifying, state.LaneGuardrail)

	ownerCommit := sha("owner-published")
	otherCommit := sha("other-published")
	ownerPub := publishFor(owner, ownerCommit, "refs/heads/m0/pub-owner")
	otherPub := publishFor(other, otherCommit, "refs/heads/m0/pub-other")

	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.RecordPublishAttempt(ctx, ownerPub); err != nil {
			return err
		}
		return tx.RecordPublishAttempt(ctx, otherPub)
	}); err != nil {
		t.Fatalf("record publications: %v", err)
	}

	t.Run("naming another attempt's publication", func(t *testing.T) {
		// Identity fields all describe `owner`, but the publication belongs
		// to `other`.
		ev := evidenceFor(owner, otherCommit, otherCommit, &otherPub.PublishAttemptID, domain.EvidenceBranchHead)
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		})
		if err == nil {
			t.Fatal("evidence claimed a foreign publication")
		}
		if !strings.Contains(err.Error(), "source_commit_sha") {
			t.Fatalf("expected the publish-coherence trigger to fire, got: %v", err)
		}
	})

	t.Run("published sha disagreeing with the publication", func(t *testing.T) {
		// Its own publication, but a different commit than was published.
		ev := evidenceFor(owner, otherCommit, otherCommit, &ownerPub.PublishAttemptID, domain.EvidenceBranchHead)
		err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		})
		if err == nil {
			t.Fatal("evidence published_sha disagreed with its publication and was accepted")
		}
	})

	t.Run("its own publication and commit is accepted", func(t *testing.T) {
		ev := evidenceFor(owner, ownerCommit, ownerCommit, &ownerPub.PublishAttemptID, domain.EvidenceBranchHead)
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, ev)
		}); err != nil {
			t.Fatalf("coherent evidence rejected: %v", err)
		}
	})
}

func TestTerminalAttemptCannotBeRevivedIntoAHeldModifyingSlot(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "revival", state.IntentModifying, state.LaneOperator)

	// The first attempt fails, freeing the repository's modifying slot.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptFailed)
	}); err != nil {
		t.Fatalf("fail first attempt: %v", err)
	}

	// A successor takes the slot.
	successor := f.Attempt
	successor.AttemptID = ids.NewAttemptID()
	successor.FenceEpoch = f.Attempt.FenceEpoch + 1
	successor.Status = state.AttemptRunning
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, successor)
	}); err != nil {
		t.Fatalf("create successor: %v", err)
	}

	// The fenced-out attempt must not be able to make itself live again.
	// Revival is refused outright by the transition rule, which is stronger
	// than relying on the modifying-slot index to happen to be occupied.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptRunning)
	})
	if !errors.Is(err, store.ErrForbiddenAttemptTransition) {
		t.Fatalf("want ErrForbiddenAttemptTransition, got %v", err)
	}

	// And at schema level.
	err = db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE worker_attempt SET status = 'RUNNING' WHERE attempt_id = ?`,
			string(f.Attempt.AttemptID))
	})
	if err == nil {
		t.Fatal("a fenced-out attempt reclaimed the modifying slot")
	}
	if !strings.Contains(err.Error(), "forbidden worker attempt status transition") {
		t.Fatalf("expected the transition trigger to fire, got: %v", err)
	}
}

func TestReleasingAWorkspaceIsAllowedButDoesNotRebindIt(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "release", state.IntentModifying, state.LaneOperator)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
	}); err != nil {
		t.Fatalf("release workspace: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		ws, err := tx.Workspace(ctx, f.Workspace.WorkspaceID)
		if err != nil {
			return err
		}
		if ws.ReleasedAt == nil {
			t.Fatal("release was not recorded")
		}
		if ws.AttemptID != f.Attempt.AttemptID {
			t.Fatalf("release changed workspace ownership to %s", string(ws.AttemptID))
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}

	// A released workspace is rejected by the publish preconditions.
	pub := publishFor(f, sha("released-commit"), "refs/heads/m0/released")
	live := domain.PublishPreconditions{
		CurrentSchedulerEpoch: f.Epoch,
		TaskCurrentAttemptID:  f.Attempt.AttemptID,
		AttemptStatus:         state.AttemptVerifying,
		AttemptFenceEpoch:     f.Attempt.FenceEpoch,
		WorkspaceAttemptID:    f.Attempt.AttemptID,
		WorkspaceReleased:     true,
		RepositorySubjectID:   f.Subject.RepositorySubjectID,
		CommitInWorkspace:     true,
	}
	if err := domain.CheckPublishPreconditions(pub, live); !errors.Is(err, domain.ErrPublishBindingInvalid) {
		t.Fatalf("a released workspace was accepted for publication: %v", err)
	}
}
