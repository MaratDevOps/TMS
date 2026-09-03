package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type AttemptStatus string

const (
	AttemptProcessing  AttemptStatus = "PROCESSING"
	AttemptPublished   AttemptStatus = "PUBLISHED"
	AttemptFailed      AttemptStatus = "FAILED"
	AttemptInterrupted AttemptStatus = "INTERRUPTED"
)

type OutboxEvent struct {
	EventID       uuid.UUID
	AggregateID   uuid.UUID
	AggregateType string
	EventType     string
	EventVersion  string
	Topic         string
	MessageKey    string
	Payload       json.RawMessage
	OccurredAt    time.Time
	CreatedAt     time.Time
}

type DeliveryAttempt struct {
	ID                uuid.UUID
	EventID           uuid.UUID
	AttemptNumber     int
	Status            AttemptStatus
	StartedAt         time.Time
	FinishedAt        *time.Time
	NextAttemptAt     *time.Time
	PublisherInstance string
	LockedUntil       time.Time
	Partition         *int32
	Offset            *int64
	ErrorMessage      *string
}
