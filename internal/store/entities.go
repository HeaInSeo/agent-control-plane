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

// ErrForbiddenPacketTransition is returned when a packet status change is not
// a permitted transition.
var ErrForbiddenPacketTransition = errors.New("forbidden packet status transition")

// ---------------------------------------------------------------------------
// RepositorySubject (CC5)
// ---------------------------------------------------------------------------

// ObserveRepositorySubject records an observation of a repository's identity.
//
// The key is GitHubNodeID. Observing the same node id under a new owner/name
// renames the existing subject in place, because owner/name is an alias and a
// rename is not a new repository. A rename is recorded as an event so the
// alias history is reconstructible without an alias-history column.
func (t *Tx) ObserveRepositorySubject(ctx context.Context, subject domain.RepositorySubject) (domain.RepositorySubject, error) {
	if err := subject.Validate(); err != nil {
		return domain.RepositorySubject{}, err
	}

	existing, err := t.RepositorySubjectByNodeID(ctx, subject.GitHubNodeID)
	switch {
	case err == nil:
		if existing.CurrentFullName == subject.CurrentFullName {
			// Record that the alias was confirmed at this later time. Leaving
			// observed_at behind would make the staleness guard below compare
			// against a timestamp that lags reality, so an observation older
			// than this confirmation could still overwrite the alias.
			if subject.ObservedAt.After(existing.ObservedAt) {
				if _, err := t.exec(ctx,
					`UPDATE repository_subject SET observed_at = ? WHERE github_node_id = ?`,
					formatTime(subject.ObservedAt), subject.GitHubNodeID,
				); err != nil {
					return domain.RepositorySubject{}, fmt.Errorf("refresh repository subject observation: %w", err)
				}
				existing.ObservedAt = subject.ObservedAt
			}
			return existing, nil
		}
		// An observation older than what is already recorded is ignored. A
		// retried or queued reconciliation carrying a pre-rename snapshot
		// would otherwise revert the alias, write a backwards rename into
		// append-only history, and — because aliases are not unique — could
		// make a legitimate alias lookup start failing as ambiguous.
		if subject.ObservedAt.Before(existing.ObservedAt) {
			return existing, nil
		}
		if _, err := t.exec(ctx,
			`UPDATE repository_subject SET current_full_name = ?, observed_at = ? WHERE github_node_id = ?`,
			subject.CurrentFullName, formatTime(subject.ObservedAt), subject.GitHubNodeID,
		); err != nil {
			return domain.RepositorySubject{}, fmt.Errorf("rename repository subject: %w", err)
		}
		epoch, err := t.CurrentEpoch(ctx)
		if err == nil {
			if _, err := t.AppendEvent(ctx, domain.Event{
				SchedulerEpoch: epoch,
				EventID:        ids.NewEventID(),
				OccurredAt:     subject.ObservedAt,
				EventType:      "repository_subject.renamed",
				SubjectKind:    domain.SubjectRepositorySubject,
				SubjectID:      string(existing.RepositorySubjectID),
				Fields: map[string]any{
					"github_node_id": subject.GitHubNodeID,
					"previous_name":  existing.CurrentFullName,
					"current_name":   subject.CurrentFullName,
				},
			}); err != nil {
				return domain.RepositorySubject{}, err
			}
		} else if !errors.Is(err, ErrNoSchedulerEpoch) {
			return domain.RepositorySubject{}, err
		}
		existing.CurrentFullName = subject.CurrentFullName
		existing.ObservedAt = subject.ObservedAt
		return existing, nil

	case errors.Is(err, ErrNotFound):
		if _, err := t.exec(ctx,
			`INSERT INTO repository_subject (repository_subject_id, github_node_id, current_full_name, observed_at)
			 VALUES (?, ?, ?, ?)`,
			string(subject.RepositorySubjectID), subject.GitHubNodeID,
			subject.CurrentFullName, formatTime(subject.ObservedAt),
		); err != nil {
			return domain.RepositorySubject{}, fmt.Errorf("insert repository subject: %w", err)
		}
		return subject, nil

	default:
		return domain.RepositorySubject{}, err
	}
}

// RepositorySubjectByNodeID looks a subject up by its stable identity.
func (t *Tx) RepositorySubjectByNodeID(ctx context.Context, nodeID string) (domain.RepositorySubject, error) {
	return t.scanRepositorySubject(t.tx.QueryRowContext(ctx,
		`SELECT repository_subject_id, github_node_id, current_full_name, observed_at
		   FROM repository_subject WHERE github_node_id = ?`, nodeID),
		fmt.Sprintf("repository subject with node id %q", nodeID))
}

// RepositorySubject looks a subject up by its control-plane identifier.
func (t *Tx) RepositorySubject(ctx context.Context, id ids.RepositorySubjectID) (domain.RepositorySubject, error) {
	return t.scanRepositorySubject(t.tx.QueryRowContext(ctx,
		`SELECT repository_subject_id, github_node_id, current_full_name, observed_at
		   FROM repository_subject WHERE repository_subject_id = ?`, string(id)),
		fmt.Sprintf("repository subject %s", string(id)))
}

// RepositorySubjectByFullName resolves the mutable owner/name alias.
//
// This lookup fails closed when the alias matches more than one subject. Our
// observations lag GitHub, so after a rename two subjects can transiently
// carry the same alias, and guessing which one was meant is exactly the kind
// of decision that must not be made silently.
func (t *Tx) RepositorySubjectByFullName(ctx context.Context, fullName string) (domain.RepositorySubject, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT repository_subject_id, github_node_id, current_full_name, observed_at
		   FROM repository_subject WHERE current_full_name = ? ORDER BY repository_subject_id`, fullName)
	if err != nil {
		return domain.RepositorySubject{}, fmt.Errorf("read repository subject by name: %w", err)
	}
	defer rows.Close()

	var found []domain.RepositorySubject
	for rows.Next() {
		var (
			s          domain.RepositorySubject
			id, node   string
			name, seen string
		)
		if err := rows.Scan(&id, &node, &name, &seen); err != nil {
			return domain.RepositorySubject{}, fmt.Errorf("scan repository subject: %w", err)
		}
		at, err := parseTime(seen)
		if err != nil {
			return domain.RepositorySubject{}, err
		}
		s = domain.RepositorySubject{
			RepositorySubjectID: ids.RepositorySubjectID(id),
			GitHubNodeID:        node,
			CurrentFullName:     name,
			ObservedAt:          at,
		}
		found = append(found, s)
	}
	if err := rows.Err(); err != nil {
		return domain.RepositorySubject{}, fmt.Errorf("iterate repository subjects: %w", err)
	}
	switch len(found) {
	case 0:
		return domain.RepositorySubject{}, fmt.Errorf("%w: repository subject with name %q", ErrNotFound, fullName)
	case 1:
		return found[0], nil
	default:
		return domain.RepositorySubject{}, fmt.Errorf(
			"%w: alias %q currently matches %d repository subjects; resolve by github_node_id",
			ErrAmbiguous, fullName, len(found))
	}
}

func (t *Tx) scanRepositorySubject(row *sql.Row, what string) (domain.RepositorySubject, error) {
	var id, node, name, seen string
	err := row.Scan(&id, &node, &name, &seen)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RepositorySubject{}, fmt.Errorf("%w: %s", ErrNotFound, what)
	}
	if err != nil {
		return domain.RepositorySubject{}, fmt.Errorf("read %s: %w", what, err)
	}
	at, err := parseTime(seen)
	if err != nil {
		return domain.RepositorySubject{}, err
	}
	return domain.RepositorySubject{
		RepositorySubjectID: ids.RepositorySubjectID(id),
		GitHubNodeID:        node,
		CurrentFullName:     name,
		ObservedAt:          at,
	}, nil
}

// ---------------------------------------------------------------------------
// ExecutionPacket (CC8)
// ---------------------------------------------------------------------------

// ApprovePacket stores an approved execution packet.
func (t *Tx) ApprovePacket(ctx context.Context, p domain.ExecutionPacket) error {
	if err := p.Validate(); err != nil {
		return err
	}
	allowed, err := domain.EncodeStringList(p.AllowedScope)
	if err != nil {
		return fmt.Errorf("encode allowed_scope: %w", err)
	}
	forbidden, err := domain.EncodeStringList(p.ForbiddenScope)
	if err != nil {
		return fmt.Errorf("encode forbidden_scope: %w", err)
	}
	stop, err := domain.EncodeStringList(p.StopConditions)
	if err != nil {
		return fmt.Errorf("encode stop_conditions: %w", err)
	}

	if _, err := t.exec(ctx,
		`INSERT INTO execution_packet (
		    packet_id, task_id, lane, intent, repository_subject_id,
		    source_revision, source_digest, packet_digest,
		    approved_at, expires_at,
		    allowed_scope, forbidden_scope, stop_conditions, acceptance_contract, status)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(p.PacketID), string(p.TaskID), string(p.Lane), string(p.Intent),
		string(p.RepositorySubjectID),
		p.SourceRevision, string(p.SourceDigest), string(p.PacketDigest),
		formatTime(p.ApprovedAt), formatTime(p.ExpiresAt),
		allowed, forbidden, stop, p.AcceptanceContract, string(p.Status),
	); err != nil {
		return fmt.Errorf("insert execution packet: %w", err)
	}
	return nil
}

// Packet loads an execution packet.
func (t *Tx) Packet(ctx context.Context, id ids.PacketID) (domain.ExecutionPacket, error) {
	var (
		p                              domain.ExecutionPacket
		packetID, taskID, lane, intent string
		repoSubject, sourceRev         string
		sourceDigest, packetDigest     string
		approvedAt, expiresAt          string
		allowed, forbidden, stop       string
		acceptance, status             string
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT packet_id, task_id, lane, intent, repository_subject_id,
		        source_revision, source_digest, packet_digest,
		        approved_at, expires_at,
		        allowed_scope, forbidden_scope, stop_conditions, acceptance_contract, status
		   FROM execution_packet WHERE packet_id = ?`, string(id),
	).Scan(&packetID, &taskID, &lane, &intent, &repoSubject,
		&sourceRev, &sourceDigest, &packetDigest,
		&approvedAt, &expiresAt,
		&allowed, &forbidden, &stop, &acceptance, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return p, fmt.Errorf("%w: execution packet %s", ErrNotFound, string(id))
	}
	if err != nil {
		return p, fmt.Errorf("read execution packet: %w", err)
	}

	approved, err := parseTime(approvedAt)
	if err != nil {
		return p, err
	}
	expires, err := parseTime(expiresAt)
	if err != nil {
		return p, err
	}
	allowedScope, err := domain.DecodeStringList(allowed)
	if err != nil {
		return p, err
	}
	forbiddenScope, err := domain.DecodeStringList(forbidden)
	if err != nil {
		return p, err
	}
	stopConditions, err := domain.DecodeStringList(stop)
	if err != nil {
		return p, err
	}

	return domain.ExecutionPacket{
		PacketID:            ids.PacketID(packetID),
		TaskID:              ids.TaskID(taskID),
		Lane:                state.Lane(lane),
		Intent:              state.Intent(intent),
		RepositorySubjectID: ids.RepositorySubjectID(repoSubject),
		SourceRevision:      sourceRev,
		SourceDigest:        domain.Digest(sourceDigest),
		PacketDigest:        domain.Digest(packetDigest),
		ApprovedAt:          approved,
		ExpiresAt:           expires,
		AllowedScope:        allowedScope,
		ForbiddenScope:      forbiddenScope,
		StopConditions:      stopConditions,
		AcceptanceContract:  acceptance,
		Status:              state.PacketStatus(status),
	}, nil
}

// SetPacketStatus moves a packet within PacketStatus only. It cannot be passed
// a value from another state domain: the parameter type makes that a compile
// error rather than a runtime surprise.
//
// Only a permitted transition is accepted. The packet's content is immutable
// after approval, and its status never moves back towards authority, so this
// is the only mutation an approved packet ever accepts. The same rule is
// enforced by a schema trigger.
func (t *Tx) SetPacketStatus(ctx context.Context, id ids.PacketID, status state.PacketStatus) error {
	if err := status.Validate(); err != nil {
		return err
	}
	current, err := t.Packet(ctx, id)
	if err != nil {
		return err
	}
	if !current.Status.CanTransitionTo(status) {
		return fmt.Errorf("%w: packet %s cannot move from %q to %q",
			ErrForbiddenPacketTransition, string(id), string(current.Status), string(status))
	}
	return t.exactlyOne(ctx, "execution packet", string(id),
		`UPDATE execution_packet SET status = ? WHERE packet_id = ?`, string(status), string(id))
}

// exactlyOne runs an update that must affect exactly one row.
func (t *Tx) exactlyOne(ctx context.Context, what, id, query string, args ...any) error {
	res, err := t.exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update %s %s: %w", what, id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update %s %s: rows affected: %w", what, id, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %s %s", ErrNotFound, what, id)
	}
	return nil
}
