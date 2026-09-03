package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

type Claimed struct {
	Event         domain.OutboxEvent
	AttemptID     uuid.UUID
	AttemptNumber int
}

type ClaimParams struct {
	Limit       int
	Instance    string
	Now         time.Time
	MaxAttempts int
	LockTimeout time.Duration
}

type Claimer interface {
	Claim(ctx context.Context, p ClaimParams) ([]Claimed, error)
}

type Attempts interface {
	MarkPublished(ctx context.Context, attemptID uuid.UUID, instance string, partition int32, offset int64, finishedAt time.Time) error
	MarkFailed(ctx context.Context, attemptID uuid.UUID, instance string, errMsg string, nextAttemptAt *time.Time, finishedAt time.Time) error
}

type Message struct {
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

type Kafka interface {
	Publish(ctx context.Context, msg Message) (partition int32, offset int64, err error)
}

type LatestAttempt struct {
	EventID       uuid.UUID
	Exists        bool
	Status        domain.AttemptStatus
	AttemptNumber int
	NextAttemptAt *time.Time
}

type StatusReader interface {
	LatestAttempts(ctx context.Context, eventIDs []uuid.UUID) ([]LatestAttempt, error)
}

type QueueSnapshot struct {
	Pending          int
	Processing       int
	OldestAgeSeconds float64
}

type QueueReader interface {
	QueueSnapshot(ctx context.Context) (QueueSnapshot, error)
}
