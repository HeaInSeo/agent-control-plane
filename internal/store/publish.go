package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

// ErrDuplicatePublishIdentity is returned when a publication intent with the
// same idempotency identity already exists.
var ErrDuplicatePublishIdentity = errors.New("duplicate publish identity")

// ErrEvidenceNotRecordable is returned when an observation cannot be recorded
// because the attempt or workspace it names is no longer live.
var ErrEvidenceNotRecordable = errors.New("evidence is not recordable")

// ---------------------------------------------------------------------------
// PublishAttempt (CC1)
// ---------------------------------------------------------------------------

// RecordPublishAttempt stores a publication intent bound to an exact immutable
// commit.
//
// M0 records intent only. No push, no GitHub API mutation and no credential
// handling exists in this milestone; the publisher that consumes these rows is
// later work. Recording the binding now means the future publisher cannot be
// written without one.
func (t *Tx) RecordPublishAttempt(ctx context.Context, p domain.PublishAttempt) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	// The idempotency key is the intent, so it is always recomputed here. A
	// caller-chosen key would defeat the uniqueness constraint entirely: two
	// keys for one intent are two publication identities, which is exactly
	// what the constraint exists to prevent. A supplied key is accepted only
	// if it is the derived one.
	derived := domain.DeriveIdempotencyKey(p)
	if p.IdempotencyKey != "" && p.IdempotencyKey != derived {
		return fmt.Errorf("%w: idempotency_key %q is not derived from this intent (want %q)",
			domain.ErrPublishBindingInvalid, p.IdempotencyKey, derived)
	}
	p.IdempotencyKey = derived

	// A publication intent starts at the beginning of its lifecycle. The
	// transition triggers only constrain UPDATE, so without this a row could
	// be inserted already OBSERVED — a durable record claiming the effect was
	// independently observed although no lifecycle was ever traversed, and
	// immediately usable to complete a modifying task.
	if p.Status == "" {
		p.Status = state.PublishPending
	}
	if p.Status != state.PublishPending {
		return fmt.Errorf("%w: a publication must be recorded as %q, not %q",
			domain.ErrPublishBindingInvalid, string(state.PublishPending), string(p.Status))
	}

	if p.CreatedAt.IsZero() {
		p.CreatedAt = t.Now()
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if err := t.RequireCurrentEpoch(ctx, p.SchedulerEpoch); err != nil {
		return err
	}

	// The same liveness fence completion applies. Without it a fenced-out
	// attempt could mint a publication intent for itself, and the row is
	// undeletable and immutable except for status — so the pending queue
	// would carry an intent that can only ever resolve to REJECTED.
	attempt, err := t.WorkerAttempt(ctx, p.AttemptID)
	if err != nil {
		return err
	}
	if attempt.Status.IsTerminal() {
		return fmt.Errorf("%w: attempt %s is terminal (%s)",
			domain.ErrPublishBindingInvalid, string(p.AttemptID), string(attempt.Status))
	}
	workspace, err := t.Workspace(ctx, p.WorkspaceID)
	if err != nil {
		return err
	}
	if workspace.ReleasedAt != nil {
		return fmt.Errorf("%w: workspace %s is released",
			domain.ErrPublishBindingInvalid, string(p.WorkspaceID))
	}
	// Publication is the third moment packet authority has to hold, alongside
	// admission and launch/resume. Recording an intent mints durable,
	// authority-bearing state that is undeletable and immutable except for
	// status, so a packet whose binding was already found not to hold would
	// otherwise leave a PENDING row in the queue for ever, resolvable only to
	// REJECTED.
	if err := t.requirePacketAuthority(ctx, attempt.PacketID); err != nil {
		return err
	}
	// And the task itself must still be live. Publication is the irreversible
	// step: a cancelled task whose push still went out is the worst outcome
	// this control plane can produce.
	if err := t.requireLiveTask(ctx, p.TaskID); err != nil {
		return err
	}

	_, err = t.exec(ctx,
		`INSERT INTO publish_attempt (publish_attempt_id, task_id, attempt_id,
		                              scheduler_epoch, fence_epoch, workspace_id,
		                              repository_subject_id, base_sha, source_commit_sha,
		                              target_ref, idempotency_key, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(p.PublishAttemptID), string(p.TaskID), string(p.AttemptID),
		int64(p.SchedulerEpoch), int64(p.FenceEpoch), string(p.WorkspaceID),
		string(p.RepositorySubjectID), string(p.BaseSHA), string(p.SourceCommitSHA),
		p.TargetRef, p.IdempotencyKey, string(p.Status), formatTime(p.CreatedAt))
	if err != nil {
		if isUniqueViolationOn(err, "publish_attempt.idempotency_key") {
			return fmt.Errorf("%w: idempotency_key %s is already recorded; "+
				"resolve it with PublishAttemptByIdempotencyKey: %w",
				ErrDuplicatePublishIdentity, p.IdempotencyKey, err)
		}
		return fmt.Errorf("insert publish attempt: %w", err)
	}
	return nil
}

// PublishAttempt loads a publication intent.
func (t *Tx) PublishAttempt(ctx context.Context, id ids.PublishAttemptID) (domain.PublishAttempt, error) {
	var (
		out                                   domain.PublishAttempt
		pubID, taskID, attemptID, workspaceID string
		repoSubject, baseSHA, sourceSHA       string
		targetRef, idemKey, status, createdAt string
		schedEpoch, fenceEpoch                int64
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT publish_attempt_id, task_id, attempt_id, scheduler_epoch, fence_epoch,
		        workspace_id, repository_subject_id, base_sha, source_commit_sha,
		        target_ref, idempotency_key, status, created_at
		   FROM publish_attempt WHERE publish_attempt_id = ?`, string(id),
	).Scan(&pubID, &taskID, &attemptID, &schedEpoch, &fenceEpoch,
		&workspaceID, &repoSubject, &baseSHA, &sourceSHA,
		&targetRef, &idemKey, &status, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("%w: publish attempt %s", ErrNotFound, string(id))
	}
	if err != nil {
		return out, fmt.Errorf("read publish attempt: %w", err)
	}
	created, err := parseTime(createdAt)
	if err != nil {
		return out, err
	}
	return domain.PublishAttempt{
		PublishAttemptID:    ids.PublishAttemptID(pubID),
		TaskID:              ids.TaskID(taskID),
		AttemptID:           ids.AttemptID(attemptID),
		SchedulerEpoch:      domain.Epoch(schedEpoch),
		FenceEpoch:          domain.FenceEpoch(fenceEpoch),
		WorkspaceID:         ids.WorkspaceID(workspaceID),
		RepositorySubjectID: ids.RepositorySubjectID(repoSubject),
		BaseSHA:             domain.CommitSHA(baseSHA),
		SourceCommitSHA:     domain.CommitSHA(sourceSHA),
		TargetRef:           targetRef,
		IdempotencyKey:      idemKey,
		Status:              state.PublishStatus(status),
		CreatedAt:           created,
	}, nil
}

// ErrForbiddenPublishTransition is returned when a publication status change
// is not a permitted transition.
var ErrForbiddenPublishTransition = errors.New("forbidden publish status transition")

// requirePacketAuthority rejects work authorised by a packet that no longer
// grants execution.
//
// Note the deliberate asymmetry with completion, which does NOT require this.
// Completion is derived from an effect that was externally observed: if the
// work landed and a packet went stale afterwards, the change exists in the
// repository, and refusing to complete would leave the task permanently open
// while the effect stands. Publication is the opposite case — it creates the
// effect — so it must not proceed on authority that has lapsed.
func (t *Tx) requirePacketAuthority(ctx context.Context, id ids.PacketID) error {
	packet, err := t.Packet(ctx, id)
	if err != nil {
		return err
	}
	if !packet.Status.GrantsExecutionAuthority() {
		return fmt.Errorf("%w: packet %s status is %q",
			domain.ErrPacketNoAuthority, string(id), string(packet.Status))
	}
	if !t.Now().Before(packet.ExpiresAt) {
		return fmt.Errorf("%w: packet %s expired at %s",
			domain.ErrPacketExpired, string(id), packet.ExpiresAt.UTC())
	}
	return nil
}

// PublishAttemptByIdempotencyKey resolves a publication by its identity.
//
// This is the resolution half of the idempotency contract. Without it a
// publisher restarting after a crash gets ErrDuplicatePublishIdentity with no
// way to reach the intent that already exists and learn whether it was
// applied — which is precisely the situation idempotency is for.
func (t *Tx) PublishAttemptByIdempotencyKey(ctx context.Context, key string) (domain.PublishAttempt, error) {
	if key == "" {
		return domain.PublishAttempt{}, errors.New("idempotency key is empty")
	}
	var id string
	err := t.tx.QueryRowContext(ctx,
		`SELECT publish_attempt_id FROM publish_attempt WHERE idempotency_key = ?`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.PublishAttempt{}, fmt.Errorf("%w: publication with idempotency key %s", ErrNotFound, key)
	}
	if err != nil {
		return domain.PublishAttempt{}, fmt.Errorf("read publication by idempotency key: %w", err)
	}
	return t.PublishAttempt(ctx, ids.PublishAttemptID(id))
}

// PublishAttemptsForAttempt lists an attempt's publications, oldest first.
func (t *Tx) PublishAttemptsForAttempt(ctx context.Context, attempt ids.AttemptID) ([]domain.PublishAttempt, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT publish_attempt_id FROM publish_attempt WHERE attempt_id = ? ORDER BY created_at, publish_attempt_id`,
		string(attempt))
	if err != nil {
		return nil, fmt.Errorf("read publications for attempt: %w", err)
	}
	var identifiers []ids.PublishAttemptID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan publication id: %w", err)
		}
		identifiers = append(identifiers, ids.PublishAttemptID(id))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate publications: %w", err)
	}
	rows.Close()

	out := make([]domain.PublishAttempt, 0, len(identifiers))
	for _, id := range identifiers {
		pub, err := t.PublishAttempt(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, pub)
	}
	return out, nil
}

// SetPublishStatus moves a publication within PublishStatus.
//
// Only a permitted transition is accepted. Without this a publication
// recorded REJECTED could be flipped to APPLIED and then used to complete a
// task — the same "authority regained" move the packet transitions forbid.
// The same rule is enforced by a schema trigger.
func (t *Tx) SetPublishStatus(ctx context.Context, id ids.PublishAttemptID, status state.PublishStatus) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	if err := status.Validate(); err != nil {
		return err
	}
	current, err := t.PublishAttempt(ctx, id)
	if err != nil {
		return err
	}
	if !current.Status.CanTransitionTo(status) {
		return fmt.Errorf("%w: publication %s cannot move from %q to %q",
			ErrForbiddenPublishTransition, string(id), string(current.Status), string(status))
	}
	return t.exactlyOne(ctx, "publish attempt", string(id),
		`UPDATE publish_attempt SET status = ? WHERE publish_attempt_id = ?`,
		string(status), string(id))
}

// ---------------------------------------------------------------------------
// EvidenceObservation (CC7)
// ---------------------------------------------------------------------------

// RecordEvidence stores an externally observed repository effect.
//
// The observation's identity fields are checked against the attempt it names
// before the insert, and again by a schema trigger. Evidence about one attempt
// can therefore never be filed under another.
func (t *Tx) RecordEvidence(ctx context.Context, e domain.EvidenceObservation) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return err
	}
	attempt, err := t.WorkerAttempt(ctx, e.AttemptID)
	if err != nil {
		return err
	}
	// Appending evidence under a retired generation would let a superseded
	// scheduler manufacture the input that completion is derived from. The
	// schema trigger only requires the observation's epoch to match its
	// attempt's, which an old-epoch attempt satisfies trivially.
	if err := t.RequireCurrentEpoch(ctx, attempt.SchedulerEpoch); err != nil {
		return err
	}
	// The same liveness fence the sibling writers apply. An observation about
	// a dead attempt or a released workspace has no consumer: completion
	// rejects both, and the row is immutable and undeletable, so it would be
	// a permanent record that can never be acted on. A later milestone that
	// genuinely needs post-mortem observations should add an explicit path
	// rather than weaken this one.
	if attempt.Status.IsTerminal() {
		return fmt.Errorf("%w: attempt %s is terminal (%s)",
			ErrEvidenceNotRecordable, string(e.AttemptID), string(attempt.Status))
	}
	workspace, err := t.WorkspaceForAttempt(ctx, e.AttemptID)
	if err != nil {
		return err
	}
	if workspace.ReleasedAt != nil {
		return fmt.Errorf("%w: workspace %s is released",
			ErrEvidenceNotRecordable, string(workspace.WorkspaceID))
	}
	if err := t.requireLiveTask(ctx, e.TaskID); err != nil {
		return err
	}
	if err := e.CheckAttribution(domain.AttemptIdentity{
		AttemptID:           attempt.AttemptID,
		TaskID:              attempt.TaskID,
		SchedulerEpoch:      attempt.SchedulerEpoch,
		FenceEpoch:          attempt.FenceEpoch,
		RepositorySubjectID: attempt.RepositorySubjectID,
		WorkspaceID:         workspace.WorkspaceID,
	}); err != nil {
		return err
	}

	if _, err := t.exec(ctx,
		`INSERT INTO evidence_observation (evidence_id, task_id, attempt_id, scheduler_epoch,
		                                   fence_epoch, repository_subject_id, workspace_id,
		                                   publish_attempt_id, published_sha, observed_sha,
		                                   reviewed_sha, artifact_digest,
		                                   observed_at, evidence_kind)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(e.EvidenceID), string(e.TaskID), string(e.AttemptID), int64(e.SchedulerEpoch),
		int64(e.FenceEpoch), string(e.RepositorySubjectID), string(e.WorkspaceID),
		publishIDArg(e.PublishAttemptID), string(e.PublishedSHA), string(e.ObservedSHA),
		nullIfEmpty(string(e.ReviewedSHA)), nullIfEmpty(string(e.ArtifactDigest)),
		formatTime(e.ObservedAt), string(e.EvidenceKind),
	); err != nil {
		return fmt.Errorf("insert evidence observation: %w", err)
	}
	return nil
}

// nullIfEmpty stores an absent optional string as SQL NULL, so the schema's
// biconditional CHECK on the review binding sees a real absence rather than
// an empty string.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func publishIDArg(id *ids.PublishAttemptID) any {
	if id == nil {
		return nil
	}
	return string(*id)
}

// Evidence loads an observation.
func (t *Tx) Evidence(ctx context.Context, id ids.EvidenceID) (domain.EvidenceObservation, error) {
	var (
		out                                   domain.EvidenceObservation
		evID, taskID, attemptID               string
		repoSubject, workspaceID              string
		publishedSHA, observedSHA, observedAt string
		evidenceKind                          string
		schedEpoch, fenceEpoch                int64
		publishID                             sql.NullString
		reviewedSHA, artifactDigest           sql.NullString
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT evidence_id, task_id, attempt_id, scheduler_epoch, fence_epoch,
		        repository_subject_id, workspace_id, publish_attempt_id,
		        published_sha, observed_sha, reviewed_sha, artifact_digest,
		        observed_at, evidence_kind
		   FROM evidence_observation WHERE evidence_id = ?`, string(id),
	).Scan(&evID, &taskID, &attemptID, &schedEpoch, &fenceEpoch,
		&repoSubject, &workspaceID, &publishID,
		&publishedSHA, &observedSHA, &reviewedSHA, &artifactDigest,
		&observedAt, &evidenceKind)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("%w: evidence observation %s", ErrNotFound, string(id))
	}
	if err != nil {
		return out, fmt.Errorf("read evidence observation: %w", err)
	}
	at, err := parseTime(observedAt)
	if err != nil {
		return out, err
	}
	out = domain.EvidenceObservation{
		EvidenceID:          ids.EvidenceID(evID),
		TaskID:              ids.TaskID(taskID),
		AttemptID:           ids.AttemptID(attemptID),
		SchedulerEpoch:      domain.Epoch(schedEpoch),
		FenceEpoch:          domain.FenceEpoch(fenceEpoch),
		RepositorySubjectID: ids.RepositorySubjectID(repoSubject),
		WorkspaceID:         ids.WorkspaceID(workspaceID),
		PublishedSHA:        domain.CommitSHA(publishedSHA),
		ObservedSHA:         domain.CommitSHA(observedSHA),
		ReviewedSHA:         domain.CommitSHA(reviewedSHA.String),
		ArtifactDigest:      domain.Digest(artifactDigest.String),
		ObservedAt:          at,
		EvidenceKind:        domain.EvidenceKind(evidenceKind),
	}
	if publishID.Valid {
		pid := ids.PublishAttemptID(publishID.String)
		out.PublishAttemptID = &pid
	}
	return out, nil
}
