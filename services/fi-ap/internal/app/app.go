package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/config"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/generate"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/metrics"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/outbox"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/result"
)

type App struct {
	Log         *slog.Logger
	Loaded      config.Loaded
	Coordinator *generate.Coordinator
	Publisher   *outbox.Publisher
	Status      outbox.StatusReader
	Metrics     *metrics.Registry
}

func (a *App) Run(ctx context.Context, runID uuid.UUID, startedAt time.Time) result.Result {
	scenarioName := a.Loaded.Name
	scenario := a.Loaded.Scenario
	log := a.Log
	if log == nil {
		log = slog.Default()
	}

	pubCtx, stopPub := context.WithCancel(ctx)
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		if err := a.Publisher.Run(pubCtx); err != nil {
			log.Error("outbox publisher stopped", "runId", runID.String(), "err", err)
		}
	}()
	defer func() {
		stopPub()
		<-pubDone
	}()

	genStarted := time.Now().UTC()
	stats, err := a.Coordinator.Run(ctx, scenario, runID)
	genFinished := time.Now().UTC()
	a.Metrics.GenerationRun(genFinished.Sub(genStarted))
	if err != nil {
		log.Error("generation failed", "runId", runID.String(), "err", err)
		published, pubFailed, waitInterrupted := a.waitDelivery(ctx, log, stats.EventIDs)
		res := result.FromSummary(result.Summary{
			RunID:              runID,
			Scenario:           scenarioName,
			StartedAt:          startedAt,
			GenerationStarted:  genStarted,
			GenerationFinished: genFinished,
			Requested:          scenario.PositionCount,
			Created:            stats.Created,
			ParallelThreads:    scenario.ParallelThreads,
			Interrupted:        ctx.Err() != nil || waitInterrupted,
			GenerationErrors:   max(1, stats.Errors),
			PublishedEvents:    published,
			PublicationErrors:  pubFailed,
		})
		if res.Status == result.StatusCompleted {
			res.Status = result.StatusFailed
			res.ExitCode = result.ExitError
			res.ErrorCodes = []string{result.CodeGenerationError}
		}
		return res
	}

	log.Info("generation finished",
		"runId", runID.String(),
		"created", stats.Created,
		"errors", stats.Errors,
		"interrupted", stats.Interrupted,
	)

	published, pubFailed, waitInterrupted := a.waitDelivery(ctx, log, stats.EventIDs)
	return result.FromSummary(result.Summary{
		RunID:              runID,
		Scenario:           scenarioName,
		StartedAt:          startedAt,
		GenerationStarted:  genStarted,
		GenerationFinished: genFinished,
		Requested:          scenario.PositionCount,
		Created:            stats.Created,
		GenerationErrors:   stats.Errors,
		PublishedEvents:    published,
		PublicationErrors:  pubFailed,
		ParallelThreads:    scenario.ParallelThreads,
		Interrupted:        stats.Interrupted || waitInterrupted,
	})
}

func (a *App) waitDelivery(ctx context.Context, log *slog.Logger, eventIDs []uuid.UUID) (published, failed int, interrupted bool) {
	if len(eventIDs) == 0 {
		return 0, 0, ctx.Err() != nil
	}
	maxAttempts := a.Loaded.Config.OutboxPublisher.MaxAttempts
	poll := a.Loaded.Config.OutboxPublisher.PollInterval.Duration()
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}

	read := func(readCtx context.Context) (int, int, int, error) {
		rows, err := a.Status.LatestAttempts(readCtx, eventIDs)
		if err != nil {
			return 0, 0, 0, err
		}
		p, f, n := outbox.CountDelivery(eventIDs, rows, maxAttempts)
		return p, f, n, nil
	}

	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		p, f, pending, err := read(ctx)
		if err == nil && pending == 0 {
			return p, f, false
		}
		if err != nil && ctx.Err() == nil {
			log.Error("outbox status read failed", "err", err)
		}
		select {
		case <-ctx.Done():
			snapCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			p, f, _, _ = read(snapCtx)
			cancel()
			return p, f, true
		case <-ticker.C:
		}
	}
}
