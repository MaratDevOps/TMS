package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/outbox"
)

func (s *Store) Claim(ctx context.Context, p outbox.ClaimParams) ([]outbox.Claimed, error) {
	if p.Limit < 1 {
		return nil, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (event_id)
				event_id,
				id AS attempt_id,
				attempt_number,
				status,
				next_attempt_at,
				locked_until
			FROM outbox_delivery_attempts
			ORDER BY event_id, attempt_number DESC
		)
		SELECT
			o.event_id,
			o.aggregate_id,
			o.aggregate_type,
			o.event_type,
			o.event_version,
			o.topic,
			o.message_key,
			o.payload,
			o.occurred_at,
			o.created_at,
			l.attempt_id,
			l.attempt_number,
			l.status,
			l.locked_until
		FROM outbox o
		LEFT JOIN latest l ON l.event_id = o.event_id
		WHERE (
			l.event_id IS NULL
			OR (
				l.status = 'FAILED'
				AND l.next_attempt_at IS NOT NULL
				AND l.next_attempt_at <= $1
				AND l.attempt_number < $2
			)
			OR (
				l.status = 'INTERRUPTED'
				AND l.attempt_number < $2
			)
			OR (
				l.status = 'PROCESSING'
				AND l.locked_until <= $1
			)
		)
		AND NOT EXISTS (
			SELECT 1
			FROM outbox earlier
			LEFT JOIN latest el ON el.event_id = earlier.event_id
			WHERE earlier.message_key = o.message_key
			  AND (
					earlier.created_at < o.created_at
					OR (earlier.created_at = o.created_at AND earlier.event_id < o.event_id)
			  )
			  AND (el.event_id IS NULL OR el.status IS DISTINCT FROM 'PUBLISHED')
		)
		ORDER BY o.created_at, o.event_id
		FOR UPDATE OF o SKIP LOCKED
		LIMIT $3
	`, p.Now, p.MaxAttempts, p.Limit)
	if err != nil {
		return nil, fmt.Errorf("claim select: %w", err)
	}

	type selected struct {
		event           domain.OutboxEvent
		prevAttemptID   *uuid.UUID
		prevNumber      *int
		prevStatus      *string
		prevLockedUntil *time.Time
	}

	var selectedRows []selected
	for rows.Next() {
		var row selected
		var payload []byte
		if err := rows.Scan(
			&row.event.EventID,
			&row.event.AggregateID,
			&row.event.AggregateType,
			&row.event.EventType,
			&row.event.EventVersion,
			&row.event.Topic,
			&row.event.MessageKey,
			&payload,
			&row.event.OccurredAt,
			&row.event.CreatedAt,
			&row.prevAttemptID,
			&row.prevNumber,
			&row.prevStatus,
			&row.prevLockedUntil,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("claim scan: %w", err)
		}
		row.event.Payload = payload
		selectedRows = append(selectedRows, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim rows: %w", err)
	}

	claimed := make([]outbox.Claimed, 0, len(selectedRows))
	lockedUntil := p.Now.Add(p.LockTimeout)

	for _, row := range selectedRows {
		if row.prevStatus != nil && *row.prevStatus == string(domain.AttemptProcessing) {
			tag, err := tx.Exec(ctx, `
				UPDATE outbox_delivery_attempts
				SET status = 'INTERRUPTED',
				    finished_at = $2,
				    error_message = 'lock expired'
				WHERE id = $1
				  AND status = 'PROCESSING'
			`, row.prevAttemptID, p.Now)
			if err != nil {
				return nil, fmt.Errorf("interrupt attempt: %w", err)
			}
			if tag.RowsAffected() > 0 {
				s.metrics.OutboxInterrupted()
			}
		}

		prev := 0
		if row.prevNumber != nil {
			prev = *row.prevNumber
		}
		nextNumber := prev + 1
		if nextNumber > p.MaxAttempts {
			continue
		}

		attemptID := uuid.New()
		_, err := tx.Exec(ctx, `
			INSERT INTO outbox_delivery_attempts (
				id, event_id, attempt_number, status, started_at, finished_at,
				next_attempt_at, publisher_instance, locked_until, partition,
				offset_value, error_message
			) VALUES (
				$1, $2, $3, 'PROCESSING', $4, NULL,
				NULL, $5, $6, NULL,
				NULL, NULL
			)
		`, attemptID, row.event.EventID, nextNumber, p.Now, p.Instance, lockedUntil)
		if err != nil {
			return nil, fmt.Errorf("insert attempt: %w", err)
		}

		claimed = append(claimed, outbox.Claimed{
			Event:         row.event,
			AttemptID:     attemptID,
			AttemptNumber: nextNumber,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim commit: %w", err)
	}
	return claimed, nil
}

func (s *Store) MarkPublished(ctx context.Context, attemptID uuid.UUID, instance string, partition int32, offset int64, finishedAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_delivery_attempts
		SET status = 'PUBLISHED',
		    finished_at = $2,
		    partition = $3,
		    offset_value = $4,
		    error_message = NULL,
		    next_attempt_at = NULL
		WHERE id = $1
		  AND status = 'PROCESSING'
		  AND publisher_instance = $5
	`, attemptID, finishedAt, partition, offset, instance)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark published: ownership conflict, rows=%d", tag.RowsAffected())
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, attemptID uuid.UUID, instance string, errMsg string, nextAttemptAt *time.Time, finishedAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_delivery_attempts
		SET status = 'FAILED',
		    finished_at = $2,
		    error_message = $3,
		    next_attempt_at = $4
		WHERE id = $1
		  AND status = 'PROCESSING'
		  AND publisher_instance = $5
	`, attemptID, finishedAt, errMsg, nextAttemptAt, instance)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark failed: ownership conflict, rows=%d", tag.RowsAffected())
	}
	return nil
}

func (s *Store) LatestAttempts(ctx context.Context, eventIDs []uuid.UUID) ([]outbox.LatestAttempt, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			o.event_id,
			l.status,
			l.attempt_number,
			l.next_attempt_at
		FROM outbox o
		LEFT JOIN LATERAL (
			SELECT status, attempt_number, next_attempt_at
			FROM outbox_delivery_attempts a
			WHERE a.event_id = o.event_id
			ORDER BY a.attempt_number DESC
			LIMIT 1
		) l ON true
		WHERE o.event_id = ANY($1)
	`, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("latest attempts: %w", err)
	}
	defer rows.Close()

	out := make([]outbox.LatestAttempt, 0, len(eventIDs))
	for rows.Next() {
		var row outbox.LatestAttempt
		var status *string
		var number *int
		if err := rows.Scan(&row.EventID, &status, &number, &row.NextAttemptAt); err != nil {
			return nil, fmt.Errorf("latest attempts scan: %w", err)
		}
		if status != nil && number != nil {
			row.Exists = true
			row.Status = domain.AttemptStatus(*status)
			row.AttemptNumber = *number
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) QueueSnapshot(ctx context.Context) (outbox.QueueSnapshot, error) {
	var snap outbox.QueueSnapshot
	err := s.pool.QueryRow(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (event_id)
				event_id,
				status,
				locked_until
			FROM outbox_delivery_attempts
			ORDER BY event_id, attempt_number DESC
		)
		SELECT
			COUNT(*) FILTER (
				WHERE l.status IS DISTINCT FROM 'PUBLISHED'
				  AND NOT (l.status = 'PROCESSING' AND l.locked_until > now())
			),
			COUNT(*) FILTER (
				WHERE l.status = 'PROCESSING' AND l.locked_until > now()
			),
			COALESCE(
				EXTRACT(EPOCH FROM (now() - MIN(o.created_at) FILTER (
					WHERE l.status IS DISTINCT FROM 'PUBLISHED'
				))),
				0
			)
		FROM outbox o
		LEFT JOIN latest l ON l.event_id = o.event_id
	`).Scan(&snap.Pending, &snap.Processing, &snap.OldestAgeSeconds)
	if err != nil {
		return outbox.QueueSnapshot{}, fmt.Errorf("outbox queue snapshot: %w", err)
	}
	return snap, nil
}
