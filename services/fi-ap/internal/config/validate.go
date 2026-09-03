package config

import (
	"fmt"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

func (c Config) Validate() error {
	if c.Postgres.DSN == "" {
		return fmt.Errorf("postgres.dsn is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must not be empty")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka.topic is required")
	}
	if c.Kafka.ProducerSystem == "" {
		return fmt.Errorf("kafka.producerSystem is required")
	}
	if err := c.Generation.Validate(); err != nil {
		return err
	}
	return c.OutboxPublisher.Validate()
}

func (g GenerationConfig) Validate() error {
	if len(g.Scenarios) == 0 {
		return fmt.Errorf("generation.scenarios must not be empty")
	}
	ref := g.ReferenceData
	switch {
	case len(ref.CompanyCodes) == 0:
		return fmt.Errorf("referenceData.companyCodes must not be empty")
	case len(ref.CounterpartyIDs) == 0:
		return fmt.Errorf("referenceData.counterpartyIds must not be empty")
	case len(ref.Currencies) == 0:
		return fmt.Errorf("referenceData.currencies must not be empty")
	case len(ref.PaymentMethods) == 0:
		return fmt.Errorf("referenceData.paymentMethods must not be empty")
	case len(ref.Statuses) == 0:
		return fmt.Errorf("referenceData.statuses must not be empty")
	case len(ref.OriginDocumentTypes) == 0:
		return fmt.Errorf("referenceData.originDocumentTypes must not be empty")
	}
	return nil
}

func (s Scenario) Validate() error {
	switch {
	case s.PositionCount < 1:
		return fmt.Errorf("positionCount must be >= 1")
	case s.ParallelThreads < 1:
		return fmt.Errorf("parallelThreads must be >= 1")
	case s.ParallelThreads > s.PositionCount:
		return fmt.Errorf("parallelThreads must be <= positionCount")
	case s.PositionsPerDocument < 1 || s.PositionsPerDocument > 999:
		return fmt.Errorf("positionsPerDocument must be in 1..999")
	case s.BaseDocumentDate.IsZero():
		return fmt.Errorf("baseDocumentDate is required")
	case s.OriginDocumentShare < 0 || s.OriginDocumentShare > 1:
		return fmt.Errorf("originDocumentShare must be in 0.0..1.0")
	case s.DueDays.Min < 0:
		return fmt.Errorf("dueDays.min must be >= 0")
	case s.DueDays.Max < s.DueDays.Min:
		return fmt.Errorf("dueDays.max must be >= dueDays.min")
	case s.PaymentBlockProbability < 0 || s.PaymentBlockProbability > 1:
		return fmt.Errorf("paymentBlockProbability must be in 0.0..1.0")
	case s.PaymentPurposeTemplate == "":
		return fmt.Errorf("paymentPurposeTemplate is required")
	}
	minAmount, err := domain.NewAmount(string(s.Amount.Min))
	if err != nil {
		return fmt.Errorf("amount.min: %w", err)
	}
	maxAmount, err := domain.NewAmount(string(s.Amount.Max))
	if err != nil {
		return fmt.Errorf("amount.max: %w", err)
	}
	if maxAmount.Decimal().LessThan(minAmount.Decimal()) {
		return fmt.Errorf("amount.max must be >= amount.min")
	}
	return nil
}

func (o OutboxPublisherConfig) Validate() error {
	if !o.Enabled {
		return fmt.Errorf("outboxPublisher.enabled must be true")
	}
	switch {
	case o.Workers < 1:
		return fmt.Errorf("outboxPublisher.workers must be >= 1")
	case o.BatchSize < 1:
		return fmt.Errorf("outboxPublisher.batchSize must be >= 1")
	case o.PollInterval.Duration() <= 0:
		return fmt.Errorf("outboxPublisher.pollInterval must be > 0")
	case o.ClaimTransactionTimeout.Duration() <= 0:
		return fmt.Errorf("outboxPublisher.claimTransactionTimeout must be > 0")
	case o.LockTimeout.Duration() <= o.KafkaSendTimeout.Duration():
		return fmt.Errorf("outboxPublisher.lockTimeout must be > kafkaSendTimeout")
	case o.MaxAttempts < 1:
		return fmt.Errorf("outboxPublisher.maxAttempts must be >= 1")
	case o.Retry.InitialInterval.Duration() <= 0:
		return fmt.Errorf("outboxPublisher.retry.initialInterval must be > 0")
	case o.Retry.Multiplier < 1:
		return fmt.Errorf("outboxPublisher.retry.multiplier must be >= 1")
	case o.Retry.MaxInterval.Duration() < o.Retry.InitialInterval.Duration():
		return fmt.Errorf("outboxPublisher.retry.maxInterval must be >= initialInterval")
	case o.Retry.Jitter < 0 || o.Retry.Jitter > 1:
		return fmt.Errorf("outboxPublisher.retry.jitter must be in 0.0..1.0")
	case o.ShutdownTimeout.Duration() <= 0:
		return fmt.Errorf("outboxPublisher.shutdownTimeout must be > 0")
	}
	return nil
}
