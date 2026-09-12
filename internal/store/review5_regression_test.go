package store_test

// Regressions for the findings of the fifth independent review, of head
// 088497a. Each test reproduces the reported defect and pins the fix.

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
	"github.com/HeaInSeo/agent-control-plane/internal/store"

	_ "modernc.org/sqlite"
)

// journalModeOf reads a database's journal mode without going through the
// store, so the test can observe what a refused Open left behind.
func journalModeOf(t *testing.T, path string) string {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open for inspection: %v", err)
	}
	defer raw.Close()
	var mode string
	if err := raw.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	return mode
}

// buildForeignDatabase creates an ordinary SQLite database that is not a
// control plane database, in its default (non-WAL) journal mode.
func buildForeignDatabase(t *testing.T, path string) {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("create foreign database: %v", err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`CREATE TABLE unrelated_service (id INTEGER PRIMARY KEY, payload TEXT)`); err != nil {
		t.Fatalf("seed foreign database: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO unrelated_service (payload) VALUES ('do not touch')`); err != nil {
		t.Fatalf("seed foreign database: %v", err)
	}
}

// Finding 1: Open set WAL before checking the identity marker, so a refused
// open had already rewritten an unrelated service's database.
func TestRefusedOpenDoesNotMutateForeignDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "someone-elses.db")
	buildForeignDatabase(t, path)

	before := journalModeOf(t, path)
	if before == "wal" {
		t.Fatal("test is vacuous: the foreign database is already in WAL mode")
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite}); !errors.Is(err, store.ErrForeignDatabase) {
		t.Fatalf("want ErrForeignDatabase, got %v", err)
	}

	if after := journalModeOf(t, path); after != before {
		t.Fatalf("a refused open converted the file's journal mode from %q to %q", before, after)
	}
	if now, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else if now.Size() != stat.Size() {
		t.Fatalf("a refused open changed the file size from %d to %d", stat.Size(), now.Size())
	}
	for _, sidecar := range []string{path + "-wal", path + "-shm"} {
		if _, err := os.Stat(sidecar); err == nil {
			t.Fatalf("a refused open left %s behind", filepath.Base(sidecar))
		}
	}

	// The foreign data is intact.
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var payload string
	if err := raw.QueryRow(`SELECT payload FROM unrelated_service`).Scan(&payload); err != nil {
		t.Fatalf("foreign data unreadable after refused open: %v", err)
	}
	if payload != "do not touch" {
		t.Fatalf("foreign data changed to %q", payload)
	}
}

// Finding 4: a tampered or divergent migration history opened cleanly,
// although README and the threat boundary both promise it fails closed.
func TestOpenRejectsDivergentMigrationHistory(t *testing.T) {
	ctx := context.Background()

	tampers := map[string]string{
		"tampered checksum": `UPDATE schema_migration SET checksum = 'deadbeef' WHERE version = 1`,
		"unknown name":      `UPDATE schema_migration SET name = 'genesis' WHERE version = 1`,
		"newer version": `INSERT INTO schema_migration (version, name, checksum, applied_at)
		                  VALUES (99, 'from_the_future', 'deadbeef', '2027-01-01T00:00:00.000000000Z')`,
	}

	for name, tamper := range tampers {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "diverged.db")
			db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := db.MigrateEmbedded(ctx); err != nil {
				t.Fatalf("migrate: %v", err)
			}
			if err := db.Write(ctx, func(tx *store.Tx) error {
				return tx.ExecForTest(ctx, tamper)
			}); err != nil {
				t.Fatalf("tamper: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			for _, mode := range []store.Mode{store.ModeReadWrite, store.ModeReadOnly} {
				_, err := store.Open(ctx, store.Config{Path: path, Mode: mode})
				if err == nil {
					t.Fatalf("%s open accepted a divergent history", mode)
				}
				if !errors.Is(err, store.ErrMigrationChecksumMismatch) &&
					!errors.Is(err, store.ErrMigrationHistoryDivergent) &&
					!errors.Is(err, store.ErrSchemaVersionUnsupported) {
					t.Fatalf("%s open: unexpected error %v", mode, err)
				}
			}
		})
	}

	// A partially migrated database is still openable: it is one this process
	// may be about to migrate forward.
	t.Run("partially migrated database opens", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "partial.db")
		db, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if err := db.BootstrapForTest(ctx); err != nil {
			t.Fatalf("bootstrap: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
		if err != nil {
			t.Fatalf("a half-bootstrapped database must still open: %v", err)
		}
		defer reopened.Close()
		if err := reopened.MigrateEmbedded(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	})
}

// Finding 3: ObserveRepositorySubject was the last mutator without the
// ownership fence.
func TestEveryMutatorRequiresOwnership(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fence.db")

	owner, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite, AllowCreate: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer owner.Close()
	if err := owner.MigrateEmbedded(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	clocked := owner.WithClock(func() time.Time { return fixedNow })
	f := seed(t, clocked, "r5-fence", state.IntentModifying, state.LaneOperator)

	bystander, err := store.Open(ctx, store.Config{Path: path, Mode: store.ModeReadWrite})
	if err != nil {
		t.Fatalf("open bystander: %v", err)
	}
	defer bystander.Close()

	commit := sha("r5-fence-commit")
	mutators := map[string]func(tx *store.Tx) error{
		"ObserveRepositorySubject": func(tx *store.Tx) error {
			renamed := f.Subject
			renamed.CurrentFullName = "HeaInSeo/should-not-happen"
			renamed.ObservedAt = fixedNow.Add(time.Hour)
			_, err := tx.ObserveRepositorySubject(ctx, renamed)
			return err
		},
		"ApprovePacket": func(tx *store.Tx) error {
			return tx.ApprovePacket(ctx, newPacket(ids.NewTaskID(),
				f.Subject.RepositorySubjectID, state.IntentModifying, state.LaneOperator))
		},
		"SetPacketStatus": func(tx *store.Tx) error {
			return tx.SetPacketStatus(ctx, f.Packet.PacketID, state.PacketStale)
		},
		"CreateTaskRun": func(tx *store.Tx) error {
			return tx.CreateTaskRun(ctx, f.Task)
		},
		"SetTaskRunStatus": func(tx *store.Tx) error {
			return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskAbandoned)
		},
		"SetTaskCurrentAttempt": func(tx *store.Tx) error {
			return tx.SetTaskCurrentAttempt(ctx, f.Task.TaskID, f.Attempt.AttemptID)
		},
		"CreateWorkerAttempt": func(tx *store.Tx) error {
			return tx.CreateWorkerAttempt(ctx, f.Attempt)
		},
		"SetWorkerAttemptStatus": func(tx *store.Tx) error {
			return tx.SetWorkerAttemptStatus(ctx, f.Attempt.AttemptID, state.AttemptAbandoned)
		},
		"CreateWorkspace": func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, f.Workspace)
		},
		"ReleaseWorkspace": func(tx *store.Tx) error {
			return tx.ReleaseWorkspace(ctx, f.Workspace.WorkspaceID)
		},
		"RecordPublishAttempt": func(tx *store.Tx) error {
			return tx.RecordPublishAttempt(ctx, publishFor(f, commit, "refs/heads/m0/r5-fence"))
		},
		"RecordEvidence": func(tx *store.Tx) error {
			return tx.RecordEvidence(ctx, evidenceFor(f, commit, commit, nil, domain.EvidenceBranchHead))
		},
		"AppendEvent": func(tx *store.Tx) error {
			_, err := tx.AppendEvent(ctx, eventFor(f.Epoch, "unowned"))
			return err
		},
		"AppendEventAt": func(tx *store.Tx) error {
			return tx.AppendEventAt(ctx, eventFor(f.Epoch, "unowned.forged"), 500)
		},
		"CompleteTaskRunFromEvidence": func(tx *store.Tx) error {
			return tx.CompleteTaskRunFromEvidence(ctx, f.Task.TaskID, ids.NewEvidenceID())
		},
	}

	for name, mutate := range mutators {
		t.Run(name, func(t *testing.T) {
			if err := bystander.Write(ctx, mutate); !errors.Is(err, store.ErrNoSchedulerOwnership) {
				t.Fatalf("want ErrNoSchedulerOwnership, got %v", err)
			}
		})
	}

	// Nothing leaked through: the alias, the task and the history are intact.
	if err := clocked.Read(ctx, func(tx *store.Tx) error {
		subject, err := tx.RepositorySubject(ctx, f.Subject.RepositorySubjectID)
		if err != nil {
			return err
		}
		if subject.CurrentFullName != f.Subject.CurrentFullName {
			t.Fatalf("alias changed to %q", subject.CurrentFullName)
		}
		run, err := tx.TaskRun(ctx, f.Task.TaskID)
		if err != nil {
			return err
		}
		if run.Status != state.TaskReady {
			t.Fatalf("task status changed to %q", string(run.Status))
		}
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		for _, e := range events {
			if strings.HasPrefix(e.EventType, "unowned") {
				t.Fatalf("an unowned handle wrote %q into history", e.EventType)
			}
			if e.Seq > int64(len(events)) {
				t.Fatalf("history has a hole: event at seq %d among %d records", e.Seq, len(events))
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 7: a rename must always record the alias it replaced, since the
// event is the only place the previous owner/name survives.
func TestRenameAlwaysRecordsItsPreviousAlias(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r5-alias", state.IntentReadOnly, state.LaneReview)

	renamed := f.Subject
	renamed.CurrentFullName = "HeaInSeo/r5-renamed"
	renamed.ObservedAt = fixedNow.Add(time.Hour)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		_, err := tx.ObserveRepositorySubject(ctx, renamed)
		return err
	}); err != nil {
		t.Fatalf("rename: %v", err)
	}

	if err := db.Read(ctx, func(tx *store.Tx) error {
		events, err := tx.EventsInEpoch(ctx, f.Epoch)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.EventType != "repository_subject.renamed" {
				continue
			}
			if e.Fields["previous_name"] != f.Subject.CurrentFullName {
				t.Fatalf("previous alias recorded as %v, want %q",
					e.Fields["previous_name"], f.Subject.CurrentFullName)
			}
			if e.Fields["current_name"] != renamed.CurrentFullName {
				t.Fatalf("new alias recorded as %v", e.Fields["current_name"])
			}
			return nil
		}
		t.Fatal("the rename left no record of the alias it replaced")
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// Finding 5: the most common legitimate scheduling refusal must be typed.
func TestContentionErrorsAreTyped(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	first := seed(t, db, "r5-slot-a", state.IntentModifying, state.LaneOperator)

	// A second task on the same repository, different lane.
	secondTask := ids.NewTaskID()
	secondPacket := newPacket(secondTask, first.Subject.RepositorySubjectID, state.IntentModifying, state.LaneGuardrail)
	if err := db.Write(ctx, func(tx *store.Tx) error {
		if err := tx.ApprovePacket(ctx, secondPacket); err != nil {
			return err
		}
		return tx.CreateTaskRun(ctx, domain.TaskRun{
			TaskID:              secondTask,
			RepositorySubjectID: first.Subject.RepositorySubjectID,
			PacketID:            secondPacket.PacketID,
			Lane:                state.LaneGuardrail,
			Intent:              state.IntentModifying,
			Status:              state.TaskReady,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	}); err != nil {
		t.Fatalf("seed second task: %v", err)
	}

	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.CreateWorkerAttempt(ctx, domain.WorkerAttempt{
			AttemptID:           ids.NewAttemptID(),
			TaskID:              secondTask,
			PacketID:            secondPacket.PacketID,
			RepositorySubjectID: first.Subject.RepositorySubjectID,
			Lane:                state.LaneGuardrail,
			Intent:              state.IntentModifying,
			SchedulerEpoch:      first.Epoch,
			FenceEpoch:          1,
			Status:              state.AttemptClaimed,
			CreatedAt:           fixedNow,
			UpdatedAt:           fixedNow,
		})
	})
	if !errors.Is(err, store.ErrModifyingSlotBusy) {
		t.Fatalf("want ErrModifyingSlotBusy, got %v", err)
	}

	// Workspace collisions are typed too.
	second := seed(t, db, "r5-ws-b", state.IntentReadOnly, state.LaneReview)
	t.Run("duplicate root path", func(t *testing.T) {
		ws := second.Workspace
		ws.WorkspaceID = ids.NewWorkspaceID()
		ws.AttemptID = second.Attempt.AttemptID
		ws.RootPath = first.Workspace.RootPath
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); !errors.Is(err, store.ErrWorkspaceConflict) {
			t.Fatalf("want ErrWorkspaceConflict, got %v", err)
		}
	})
	t.Run("second workspace for one attempt", func(t *testing.T) {
		ws := second.Workspace
		ws.WorkspaceID = ids.NewWorkspaceID()
		ws.RootPath = "/var/lib/acp/workspaces/r5-ws-b-second"
		if err := db.Write(ctx, func(tx *store.Tx) error {
			return tx.CreateWorkspace(ctx, ws)
		}); !errors.Is(err, store.ErrWorkspaceConflict) {
			t.Fatalf("want ErrWorkspaceConflict, got %v", err)
		}
	})
}

// Finding 9: a primary-key collision must not be reported as a duplicate
// idempotency key, which would send an operator after a non-existent intent.
func TestPublishConstraintErrorsNameTheRightConstraint(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r5-constraint", state.IntentModifying, state.LaneOperator)

	first := publishFor(f, sha("r5-constraint-one"), "refs/heads/m0/r5-one")
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, first)
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	// A different intent that reuses the row identifier.
	reused := publishFor(f, sha("r5-constraint-two"), "refs/heads/m0/r5-two")
	reused.PublishAttemptID = first.PublishAttemptID
	reused.IdempotencyKey = ""
	err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, reused)
	})
	if err == nil {
		t.Fatal("a reused publish_attempt_id was accepted")
	}
	if errors.Is(err, store.ErrDuplicatePublishIdentity) {
		t.Fatalf("a primary-key collision was reported as a duplicate idempotency key: %v", err)
	}

	// While a genuine duplicate intent still is.
	duplicate := publishFor(f, first.SourceCommitSHA, first.TargetRef)
	duplicate.PublishAttemptID = ids.NewPublishAttemptID()
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.RecordPublishAttempt(ctx, duplicate)
	}); !errors.Is(err, store.ErrDuplicatePublishIdentity) {
		t.Fatalf("want ErrDuplicatePublishIdentity, got %v", err)
	}
}

// Finding 8: the publish gate must refuse an already-decided publication,
// which is what the crash-recovery path hands it.
func TestPublishGateRefusesADecidedPublication(t *testing.T) {
	f := fixtureForPreconditions()

	if err := domain.CheckPublishPreconditions(f.pub, f.live); err != nil {
		t.Fatalf("a pending publication was rejected: %v", err)
	}
	for _, decided := range []state.PublishStatus{
		state.PublishApplied, state.PublishObserved, state.PublishRejected, state.PublishUnknown,
	} {
		t.Run(string(decided), func(t *testing.T) {
			pub := f.pub
			pub.Status = decided
			if err := domain.CheckPublishPreconditions(pub, f.live); !errors.Is(err, domain.ErrPublishBindingInvalid) {
				t.Fatalf("want ErrPublishBindingInvalid, got %v", err)
			}
		})
	}
}

// Finding 10: a nested transaction must fail fast rather than deadlock on the
// single connection.
func TestNestedTransactionFailsFast(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	f := seed(t, db, "r5-nested", state.IntentReadOnly, state.LaneReview)

	nestings := map[string]func() error{
		"read inside write": func() error {
			return db.Write(ctx, func(*store.Tx) error {
				return db.Read(ctx, func(*store.Tx) error { return nil })
			})
		},
		"write inside write": func() error {
			return db.Write(ctx, func(*store.Tx) error {
				return db.Write(ctx, func(*store.Tx) error { return nil })
			})
		},
		"read inside read": func() error {
			return db.Read(ctx, func(*store.Tx) error {
				return db.Read(ctx, func(*store.Tx) error { return nil })
			})
		},
		"CurrentEpoch inside write": func() error {
			return db.Write(ctx, func(*store.Tx) error {
				_, err := db.CurrentEpoch(ctx)
				return err
			})
		},
	}

	for name, nest := range nestings {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- nest() }()
			select {
			case err := <-done:
				if !errors.Is(err, store.ErrNestedTransaction) {
					t.Fatalf("want ErrNestedTransaction, got %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a nested transaction deadlocked instead of failing fast")
			}
		})
	}

	// The handle is still usable afterwards.
	if err := db.Write(ctx, func(tx *store.Tx) error {
		return tx.SetTaskRunStatus(ctx, f.Task.TaskID, state.TaskBlockedDesign)
	}); err != nil {
		t.Fatalf("the handle must remain usable: %v", err)
	}
}
