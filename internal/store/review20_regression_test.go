package store_test

// Regressions for the findings of the twentieth independent review, of head
// 7595781.

import (
	"context"
	"errors"
	"math"
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

// Finding 1: observed_at is the only ordering guard on a rename and had no
// upper bound, so one skewed observation silently dropped every correctly
// timestamped rename that followed — no update, no event, no error.
func TestFutureDatedObservationCannotFreezeAnAlias(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r20-freeze", state.IntentReadOnly, state.LaneReview)

	skewed := f.Subject
	skewed.CurrentFullName = "HeaInSeo/r20-skewed"
	skewed.ObservedAt = fixedNow.AddDate(100, 0, 0)

	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.ObserveRepositorySubject(ctx, skewed)
		return err
	}); !errors.Is(err, store.ErrFutureTimestamp) {
		t.Fatalf("want ErrFutureTimestamp, got %v", err)
	}

	// The alias is untouched, so a correctly-timestamped rename still lands.
	renamed := f.Subject
	renamed.CurrentFullName = "HeaInSeo/r20-renamed"
	renamed.ObservedAt = fixedNow.Add(time.Hour)
	later := db.WithClock(func() time.Time { return renamed.ObservedAt })
	if err := later.Write(ctx, func(tx *store.Tx) error {
		got, err := tx.ObserveRepositorySubject(ctx, renamed)
		if err != nil {
			return err
		}
		if got.CurrentFullName != renamed.CurrentFullName {
			t.Fatalf("the rename was dropped: alias is %q", got.CurrentFullName)
		}
		return nil
	}); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

// Finding 8: the other writer taking a caller-supplied observation time.
func TestFutureDatedEvidenceIsRefused(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r20-evidence-time", state.IntentModifying, state.LaneOperator)

	commit := sha("r20-evidence-commit")
	ev := evidenceFor(f, commit, commit, nil, domain.EvidenceBranchHead)
	ev.ObservedAt = fixedNow.Add(time.Hour)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordEvidence(ctx, ev)
	}); !errors.Is(err, store.ErrFutureTimestamp) {
		t.Fatalf("want ErrFutureTimestamp, got %v", err)
	}
}

// Finding 2: a file with a valid SQLite header but no tables — which is what
// Open itself leaves behind, since enabling WAL writes the header before any
// migration — bypassed the AllowCreate gate.
func TestHeaderOnlyDatabaseStillRequiresAllowCreate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "header-only.db")

	// Produce exactly the state a first Open leaves before migrating.
	first, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Skip("this platform leaves a zero-length file, which the size gate already covers")
	}

	for _, cfg := range []store.Config{
		{Path: path, Mode: store.ModeReadWrite},
		{Path: path, Mode: store.ModeReadOnly},
		{Path: path, Mode: store.ModeReadOnly, AllowCreate: true},
	} {
		if _, err := store.Open(ctx, cfg); !errors.Is(err, store.ErrDatabaseNotFound) {
			t.Fatalf("%s: want ErrDatabaseNotFound, got %v", cfg.Mode, err)
		}
	}

	// With AllowCreate it bootstraps, and once populated it opens normally.
	db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("explicit create: %v", err)
	}
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err != nil {
		t.Fatalf("a populated database must open without AllowCreate: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

// Finding 3: four of the six fields the publish coherence trigger covers were
// left to a raw trigger abort, against this file's own stated convention.
func TestEveryPublishBindingFieldIsTyped(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r20-binding", state.IntentModifying, state.LaneOperator)
	other := seed(t, db, "r20-binding-other", state.IntentModifying, state.LaneGuardrail)

	commit := sha("r20-binding-commit")
	base := publishFor(f, commit, "refs/heads/m0/r20-binding")

	for name, mutate := range map[string]func(*domain.PublishAttempt){
		"task": func(p *domain.PublishAttempt) { p.TaskID = other.Task.TaskID },
		"scheduler epoch": func(p *domain.PublishAttempt) {
			p.SchedulerEpoch = f.Attempt.SchedulerEpoch + 1
		},
		"fence epoch":        func(p *domain.PublishAttempt) { p.FenceEpoch = f.Attempt.FenceEpoch + 5 },
		"repository subject": func(p *domain.PublishAttempt) { p.RepositorySubjectID = other.Subject.RepositorySubjectID },
		"base commit":        func(p *domain.PublishAttempt) { p.BaseSHA = other.Workspace.BaseSHA },
		"workspace":          func(p *domain.PublishAttempt) { p.WorkspaceID = other.Workspace.WorkspaceID },
	} {
		t.Run(name, func(t *testing.T) {
			p := base
			p.PublishAttemptID = ids.NewPublishAttemptID()
			mutate(&p)
			p.IdempotencyKey = ""
			err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.RecordPublishAttempt(ctx, p)
			})
			if !errors.Is(err, domain.ErrPublishBindingInvalid) &&
				!errors.Is(err, store.ErrStaleEpoch) {
				t.Fatalf("want a typed binding refusal, got %v", err)
			}
			if strings.Contains(err.Error(), "constraint failed") {
				t.Fatalf("a binding refusal surfaced as a driver error: %v", err)
			}
		})
	}

	// The coherent publication still works.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, base)
	}); err != nil {
		t.Fatalf("a coherent publication was refused: %v", err)
	}
}

// Finding 4: the (epoch, seq) mapping was written twice, so the production
// copy went unverified. One shared mapping means the test covers the code.
func TestEventIdentityMappingIsShared(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r20-event-map", state.IntentReadOnly, state.LaneReview)

	// The production path: a reused event_id.
	e := eventFor(f.Epoch, "r20.dup")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.AppendEvent(ctx, e)
		return err
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	prodErr := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.AppendEvent(ctx, e)
		return err
	})
	if !errors.Is(prodErr, store.ErrDuplicateEventIdentity) {
		t.Fatalf("production path: want ErrDuplicateEventIdentity, got %v", prodErr)
	}
	if !strings.Contains(prodErr.Error(), "event_id") {
		t.Fatalf("production path did not name the constraint: %v", prodErr)
	}

	// The explicit-sequence path goes through the same mapping, and the
	// driver's message really does name both key columns.
	var seq int64
	if err := db.Write(ctx, func(tx *store.Tx) error {
		var err error
		seq, err = tx.AppendEvent(ctx, eventFor(f.Epoch, "r20.seq"))
		return err
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	seqErr := db.Write(ctx, func(tx *store.Tx) error {
		return tx.AppendEventAt(ctx, eventFor(f.Epoch, "r20.collide"), seq)
	})
	if !errors.Is(seqErr, store.ErrDuplicateEventIdentity) {
		t.Fatalf("sequence path: want ErrDuplicateEventIdentity, got %v", seqErr)
	}
	if !strings.Contains(seqErr.Error(), "epoch") {
		t.Fatalf("sequence path did not name the position: %v", seqErr)
	}
}

// Finding 5: the lease guard caught only the exact zero value, though any
// past timestamp has the identical effect and could never be corrected.
func TestOptionalAttemptTimestampsCannotPredateCreation(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r20-lease", state.IntentModifying, state.LaneOperator)

	past := fixedNow.AddDate(-100, 0, 0)
	for name, mutate := range map[string]func(*domain.WorkerAttempt){
		"lease expired a century ago": func(a *domain.WorkerAttempt) { a.LeaseExpiresAt = &past },
		"checkpoint a century ago":    func(a *domain.WorkerAttempt) { a.LastCheckpointAt = &past },
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

	// The schema refuses it too.
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.ExecForTest(ctx,
			`UPDATE worker_attempt SET lease_expires_at = '1900-01-01T00:00:00.000000000Z'
			   WHERE attempt_id = ?`, string(f.Attempt.AttemptID))
	})
	if err == nil {
		t.Fatal("the schema accepted a lease predating the attempt")
	}

	// A future lease is fine, which is the normal case.
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
		t.Fatalf("a future lease was refused: %v", err)
	}
}

// Finding 6: an unencodable field value aborted the enclosing transaction, so
// a telemetry value cost the state transition itself.
func TestUnencodableFieldDoesNotCostTheTransition(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r20-degrade", state.IntentModifying, state.LaneOperator)

	pub := publishFor(f, sha("r20-degrade-commit"), "refs/heads/m0/r20-degrade")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, pub)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	// An event carrying a NaN, appended in the same transaction as a status
	// transition: the transition must survive.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.SetPublishStatus(ctx, pub.PublishAttemptID, state.PublishApplied); err != nil {
			return err
		}
		e := eventFor(f.Epoch, "r20.telemetry")
		e.Fields = map[string]any{"ratio": math.NaN(), "note": "ordinary"}
		_, err := tx.AppendEvent(ctx, e)
		return err
	}); err != nil {
		t.Fatalf("an unencodable telemetry value cost the transition: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		stored, err := tx.PublishAttempt(ctx, pub.PublishAttemptID)
		if err != nil {
			return err
		}
		if stored.Status != state.PublishApplied {
			t.Fatalf("the status transition was rolled back: %q", string(stored.Status))
		}
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.EventType != "r20.telemetry" {
				continue
			}
			if e.Fields["ratio"] != domain.Unencodable {
				t.Fatalf("the unencodable value was stored as %v", e.Fields["ratio"])
			}
			if e.Fields["note"] != "ordinary" {
				t.Fatalf("a sibling field was lost: %v", e.Fields["note"])
			}
			return nil
		}
		t.Fatal("the event did not land")
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 7: the exported Migrate accepted an arbitrary set that would brick
// the database with no in-tree recovery, so it is test-only now.
func TestOnlyTheEmbeddedSetIsProductionSurface(t *testing.T) {
	// Compile-time: store.DB has no exported Migrate taking a custom set.
	// MigrateEmbedded is the production entry point, and MigrateForTest is
	// available only to this test build.
	ctx := context.Background()
	db := newDB(t)
	if err := db.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	set, err := store.Migrations()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}
	if err := db.MigrateForTest(ctx, set); err != nil {
		t.Fatalf("test-only migrate with the embedded set: %v", err)
	}
}
