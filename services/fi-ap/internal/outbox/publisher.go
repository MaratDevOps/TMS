package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/config"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/metrics"
)

type Publisher struct {
	Log            *slog.Logger
	Claimer        Claimer
	Attempts       Attempts
	Queue          QueueReader
	Kafka          Kafka
	Metrics        *metrics.Registry
	Config         config.OutboxPublisherConfig
	Instance       string
	ProducerSystem string
	Now            func() time.Time
	RandFloat      func() float64
}

func NewInstance() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("hostname: %w", err)
	}
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.NewString()), nil
}

func (p *Publisher) logger() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}

func (p *Publisher) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now().UTC()
}

func (p *Publisher) randFloat() float64 {
	if p.RandFloat != nil {
		return p.RandFloat()
	}
	return rand.Float64()
}

func (p *Publisher) refreshQueue(ctx context.Context) {
	if p.Queue == nil {
		return
	}
	snap, err := p.Queue.QueueSnapshot(ctx)
	if err != nil {
		if ctx.Err() == nil {
			p.logger().Error("outbox queue snapshot failed", "err", err)
		}
		return
	}
	p.Metrics.SetOutboxQueue(snap.Pending, snap.Processing, snap.OldestAgeSeconds)
}

func (p *Publisher) Run(ctx context.Context) error {
	if p.Instance == "" {
		inst, err := NewInstance()
		if err != nil {
			return err
		}
		p.Instance = inst
	}

	sem := make(chan struct{}, p.Config.Workers)
	var wg sync.WaitGroup

loop:
	for {
		if ctx.Err() != nil {
			break
		}

		free := p.Config.Workers - len(sem)
		if free < 1 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}

		limit := p.Config.BatchSize
		if free < limit {
			limit = free
		}

		claimCtx, cancel := context.WithTimeout(ctx, p.Config.ClaimTransactionTimeout.Duration())
		claimed, err := p.Claimer.Claim(claimCtx, ClaimParams{
			Limit:       limit,
			Instance:    p.Instance,
			Now:         p.now(),
			MaxAttempts: p.Config.MaxAttempts,
			LockTimeout: p.Config.LockTimeout.Duration(),
		})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			p.logger().Error("outbox claim failed", "publisherInstance", p.Instance, "err", err)
			p.refreshQueue(ctx)
			select {
			case <-ctx.Done():
			case <-time.After(p.Config.PollInterval.Duration()):
			}
			continue
		}
		p.refreshQueue(ctx)
		if len(claimed) == 0 {
			select {
			case <-ctx.Done():
			case <-time.After(p.Config.PollInterval.Duration()):
			}
			continue
		}

		for _, item := range claimed {
			sem <- struct{}{}
			wg.Add(1)
			go func(item Claimed) {
				defer wg.Done()
				defer func() { <-sem }()
				p.publishOne(ctx, item)
			}(item)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(p.Config.ShutdownTimeout.Duration()):
		p.logger().Error("outbox shutdown timeout", "publisherInstance", p.Instance)
	}
	return nil
}

func (p *Publisher) publishOne(ctx context.Context, item Claimed) {
	log := p.logger()
	started := p.now()
	msg := Message{
		Topic:   item.Event.Topic,
		Key:     item.Event.MessageKey,
		Payload: item.Event.Payload,
		Headers: map[string]string{
			"eventId":        item.Event.EventID.String(),
			"eventType":      item.Event.EventType,
			"eventVersion":   item.Event.EventVersion,
			"producerSystem": p.ProducerSystem,
		},
	}

	log.Info("outbox attempt started",
		"eventId", item.Event.EventID.String(),
		"attemptId", item.AttemptID.String(),
		"attemptNumber", item.AttemptNumber,
		"publisherInstance", p.Instance,
		"messageKey", item.Event.MessageKey,
		"status", string(domain.AttemptProcessing),
	)

	if err := ValidateRecord(msg); err != nil {
		p.fail(ctx, item, started, err)
		return
	}

	sendCtx, cancel := context.WithTimeout(ctx, p.Config.KafkaSendTimeout.Duration())
	partition, offset, err := p.Kafka.Publish(sendCtx, msg)
	cancel()
	finished := p.now()
	duration := finished.Sub(started)

	if err != nil {
		if ctx.Err() != nil {
			log.Info("outbox attempt left processing",
				"eventId", item.Event.EventID.String(),
				"attemptId", item.AttemptID.String(),
				"attemptNumber", item.AttemptNumber,
				"publisherInstance", p.Instance,
				"messageKey", item.Event.MessageKey,
				"err", err,
			)
			return
		}
		p.fail(ctx, item, finished, err)
		return
	}

	if err := p.Attempts.MarkPublished(ctx, item.AttemptID, p.Instance, partition, offset, finished); err != nil {
		log.Error("outbox mark published failed",
			"eventId", item.Event.EventID.String(),
			"attemptId", item.AttemptID.String(),
			"attemptNumber", item.AttemptNumber,
			"publisherInstance", p.Instance,
			"messageKey", item.Event.MessageKey,
			"err", err,
		)
		return
	}
	p.Metrics.OutboxPublished(duration)
	log.Info("outbox attempt published",
		"eventId", item.Event.EventID.String(),
		"attemptId", item.AttemptID.String(),
		"attemptNumber", item.AttemptNumber,
		"publisherInstance", p.Instance,
		"messageKey", item.Event.MessageKey,
		"status", string(domain.AttemptPublished),
		"durationMs", duration.Milliseconds(),
		"partition", partition,
		"offset", offset,
	)
}

func (p *Publisher) fail(ctx context.Context, item Claimed, finished time.Time, pubErr error) {
	next := (*time.Time)(nil)
	if Retryable(pubErr) && item.AttemptNumber < p.Config.MaxAttempts {
		delay := WithJitter(
			BaseDelay(
				item.AttemptNumber,
				p.Config.Retry.InitialInterval.Duration(),
				p.Config.Retry.MaxInterval.Duration(),
				p.Config.Retry.Multiplier,
			),
			p.Config.Retry.Jitter,
			p.randFloat,
		)
		at := finished.Add(delay)
		next = &at
	}

	msg := pubErr.Error()
	if err := p.Attempts.MarkFailed(ctx, item.AttemptID, p.Instance, msg, next, finished); err != nil {
		p.logger().Error("outbox mark failed failed",
			"eventId", item.Event.EventID.String(),
			"attemptId", item.AttemptID.String(),
			"publisherInstance", p.Instance,
			"err", err,
		)
		return
	}

	p.Metrics.OutboxFailed()
	status := string(domain.AttemptFailed)
	p.logger().Info("outbox attempt failed",
		"eventId", item.Event.EventID.String(),
		"attemptId", item.AttemptID.String(),
		"attemptNumber", item.AttemptNumber,
		"publisherInstance", p.Instance,
		"messageKey", item.Event.MessageKey,
		"status", status,
		"errorCategory", errorCategory(pubErr),
		"retryable", Retryable(pubErr) && next != nil,
		"err", pubErr,
	)
}

func errorCategory(err error) string {
	if errors.Is(err, ErrPermanent) {
		return "permanent"
	}
	return "transient"
}
