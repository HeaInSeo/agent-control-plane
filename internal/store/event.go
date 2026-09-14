package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
)

// AppendEvent appends one record to the history and returns its sequence
// number within the scheduler epoch.
//
// Order identity is (scheduler_epoch, seq), and seq is allocated here inside
// the caller's transaction rather than chosen by the caller. Two concurrent
// appends cannot land on the same (epoch, seq): the allocation and the insert
// share one transaction, the primary key rejects a collision, and the store
// runs a single write connection.
func (t *Tx) AppendEvent(ctx context.Context, e domain.Event) (int64, error) {
	if err := e.Validate(); err != nil {
		return 0, err
	}
	if err := t.RequireOwnership(ctx); err != nil {
		return 0, err
	}
	// History belongs to the generation that wrote it. A retired epoch's
	// stream is closed: allowing appends to it would let a superseded
	// scheduler add records to the replay stream EventsInEpoch reconstructs,
	// which is the same objection that keeps attempts, publications, evidence
	// and completions bound to the current epoch.
	if err := t.RequireCurrentEpoch(ctx, e.SchedulerEpoch); err != nil {
		return 0, err
	}

	var last int64
	if err := t.tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM event WHERE scheduler_epoch = ?`, int64(e.SchedulerEpoch),
	).Scan(&last); err != nil {
		return 0, fmt.Errorf("read event sequence: %w", err)
	}
	seq := last + 1

	// Fields are redacted on the way in, so a credential cannot reach durable
	// history even if a caller passes one.
	fields, err := domain.EncodeFields(e.Fields)
	if err != nil {
		return 0, err
	}

	if _, err := t.exec(ctx,
		`INSERT INTO event (scheduler_epoch, seq, event_id, occurred_at, event_type,
		                    subject_kind, subject_id, task_id, attempt_id, fields)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		int64(e.SchedulerEpoch), seq, string(e.EventID), formatTime(e.OccurredAt),
		e.EventType, string(e.SubjectKind), e.SubjectID,
		taskIDArg(e.TaskID), attemptIDArg(e.AttemptID), fields,
	); err != nil {
		return 0, mapEventInsertError(err, e, seq)
	}
	return seq, nil
}

func taskIDArg(id *ids.TaskID) any {
	if id == nil {
		return nil
	}
	return string(*id)
}

func attemptIDArg(id *ids.AttemptID) any {
	if id == nil {
		return nil
	}
	return string(*id)
}

// maxEventsPerRead bounds a single history read.
//
// An epoch lasts as long as a scheduler generation and the event table can
// never be pruned, so an unbounded read would eventually pull a whole
// generation's history into memory. The cap is far above any plausible test
// or inspection and exists so that growth surfaces as an error naming the
// paginated accessor rather than as memory pressure.
const maxEventsPerRead = 100_000

// ErrHistoryTooLarge is returned when a single history read would exceed
// maxEventsPerRead.
var ErrHistoryTooLarge = errors.New("history read exceeds the per-read limit")

// EventsInEpoch returns the history of one scheduler generation, in sequence
// order. Events from earlier epochs remain readable for ever.
//
// This reads the whole epoch, bounded by maxEventsPerRead. Replay and any
// other caller that may face a long-lived generation should use
// EventsInEpochPage instead.
func (t *Tx) EventsInEpoch(ctx context.Context, epoch domain.Epoch) ([]domain.Event, error) {
	events, err := t.EventsInEpochPage(ctx, epoch, 0, maxEventsPerRead+1)
	if err != nil {
		return nil, err
	}
	if len(events) > maxEventsPerRead {
		return nil, fmt.Errorf("%w: epoch %d has more than %d events; use EventsInEpochPage",
			ErrHistoryTooLarge, int64(epoch), maxEventsPerRead)
	}
	return events, nil
}

// EventsInEpochPage returns up to limit events of one epoch with a sequence
// number greater than afterSeq, in sequence order.
//
// Paging on (epoch, seq) rather than an offset means a page boundary cannot
// shift under a concurrent append: seq is monotonic within an epoch and
// history is append-only, so the next page always starts exactly where the
// last one ended.
func (t *Tx) EventsInEpochPage(ctx context.Context, epoch domain.Epoch, afterSeq int64, limit int) ([]domain.Event, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("read events: limit must be positive, got %d", limit)
	}
	rows, err := t.tx.QueryContext(ctx,
		`SELECT scheduler_epoch, seq, event_id, occurred_at, event_type,
		        subject_kind, subject_id, task_id, attempt_id, fields
		   FROM event WHERE scheduler_epoch = ? AND seq > ? ORDER BY seq LIMIT ?`,
		int64(epoch), afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	defer rows.Close()

	var out []domain.Event
	for rows.Next() {
		var (
			schedEpoch, seq                int64
			eventID, occurredAt, eventType string
			subjectKind, subjectID, fields string
			taskID, attemptID              sql.NullString
		)
		if err := rows.Scan(&schedEpoch, &seq, &eventID, &occurredAt, &eventType,
			&subjectKind, &subjectID, &taskID, &attemptID, &fields); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		at, err := parseTime(occurredAt)
		if err != nil {
			return nil, err
		}
		decoded, err := domain.DecodeFields(fields)
		if err != nil {
			return nil, err
		}
		ev := domain.Event{
			SchedulerEpoch: domain.Epoch(schedEpoch),
			Seq:            seq,
			EventID:        ids.EventID(eventID),
			OccurredAt:     at,
			EventType:      eventType,
			SubjectKind:    domain.SubjectKind(subjectKind),
			SubjectID:      subjectID,
			Fields:         decoded,
		}
		if taskID.Valid {
			id := ids.TaskID(taskID.String)
			ev.TaskID = &id
		}
		if attemptID.Valid {
			id := ids.AttemptID(attemptID.String)
			ev.AttemptID = &id
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}

// mapEventInsertError turns an event insert failure into a typed error.
//
// Shared with the test-only explicit-sequence append, so the mapping the
// production path relies on is the one the tests exercise. Writing it twice
// let the (epoch, seq) branch go unverified — including whether the driver's
// message names both columns.
func mapEventInsertError(err error, e domain.Event, seq int64) error {
	if isUniqueViolationOn(err, "event.event_id") {
		return fmt.Errorf("%w: event_id %s is already recorded: %w",
			ErrDuplicateEventIdentity, string(e.EventID), err)
	}
	if isUniqueViolationOn(err, "event.scheduler_epoch", "event.seq") {
		return fmt.Errorf("%w: (epoch %d, seq %d) is already recorded: %w",
			ErrDuplicateEventIdentity, int64(e.SchedulerEpoch), seq, err)
	}
	return fmt.Errorf("append event: %w", err)
}

// ErrDuplicateEventIdentity is returned when an event's identity is already
// recorded — either its event_id or its (scheduler_epoch, seq) position.
//
// Both are mapped, because a caller matching on this sentinel to decide
// whether an append already landed cannot know which constraint fired.
var ErrDuplicateEventIdentity = errors.New("duplicate event identity")

// isUniqueViolation reports whether err is a SQLite uniqueness failure.
//
// The driver does not export a typed constraint error, so the message is
// matched. Both spellings SQLite uses are covered.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint failed") ||
		strings.Contains(msg, "constraint failed: unique")
}

// isTriggerAbort reports whether err carries a RAISE(ABORT) message.
//
// The schema expresses several invariants as BEFORE INSERT triggers, which
// abort before any table constraint is evaluated, so matching on the trigger's
// own message is the only way to give those refusals a typed error.
func isTriggerAbort(err error, message string) bool {
	return err != nil && strings.Contains(err.Error(), message)
}

// isUniqueViolationOn reports whether err is a uniqueness failure naming all
// of the given columns.
//
// Matching the column matters: without it any uniqueness failure — a reused
// primary key, say — would be reported as whichever constraint the caller
// happened to guess, sending an operator after a problem that does not exist.
func isUniqueViolationOn(err error, columns ...string) bool {
	if !isUniqueViolation(err) {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, column := range columns {
		if !strings.Contains(msg, strings.ToLower(column)) {
			return false
		}
	}
	return true
}
