package generate

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/config"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/metrics"
)

type job struct {
	number         int
	documentNumber string
	positionNumber string
}

type Stats struct {
	Created     int
	Errors      int
	Interrupted bool
	EventIDs    []uuid.UUID
}

type Coordinator struct {
	Log       *slog.Logger
	Store     Store
	Numbers   DocumentNumbers
	Generator Generator
	Metrics   *metrics.Registry
}

func (c *Coordinator) logger() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.Default()
}

func (c *Coordinator) Run(ctx context.Context, scenario config.Scenario, runID uuid.UUID) (Stats, error) {
	docCount := DocumentCount(scenario.PositionCount, scenario.PositionsPerDocument)
	numbers, err := c.Numbers.NextBatch(ctx, docCount)
	if err != nil {
		return Stats{}, fmt.Errorf("document numbers: %w", err)
	}
	if len(numbers) != docCount {
		return Stats{}, fmt.Errorf("document numbers: got %d want %d", len(numbers), docCount)
	}

	jobs := make([]job, 0, scenario.PositionCount)
	for n := 1; n <= scenario.PositionCount; n++ {
		idx, pos := Assign(n, scenario.PositionsPerDocument)
		positionNumber, err := domain.FormatPositionNumber(pos)
		if err != nil {
			return Stats{}, err
		}
		jobs = append(jobs, job{
			number:         n,
			documentNumber: numbers[idx],
			positionNumber: positionNumber,
		})
	}

	jobsCh := make(chan job)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var stats Stats
	var stopIssuing atomic.Bool

	workers := scenario.ParallelThreads
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobsCh {
				if stopIssuing.Load() || ctx.Err() != nil {
					continue
				}
				if eventID, err := c.process(ctx, runID, j); err != nil {
					c.logger().Error("position failed",
						"runId", runID.String(),
						"jobNumber", j.number,
						"err", err,
					)
					c.Metrics.PositionFailed()
					mu.Lock()
					stats.Errors++
					mu.Unlock()
					if scenario.StopOnError {
						stopIssuing.Store(true)
					}
					continue
				} else {
					mu.Lock()
					stats.Created++
					stats.EventIDs = append(stats.EventIDs, eventID)
					mu.Unlock()
				}
			}
		}()
	}

	for _, j := range jobs {
		if stopIssuing.Load() {
			break
		}
		if ctx.Err() != nil {
			stats.Interrupted = true
			break
		}
		select {
		case <-ctx.Done():
			stats.Interrupted = true
		case jobsCh <- j:
		}
		if stats.Interrupted {
			break
		}
	}
	close(jobsCh)
	wg.Wait()

	if ctx.Err() != nil {
		stats.Interrupted = true
	}
	return stats, nil
}

func (c *Coordinator) process(ctx context.Context, runID uuid.UUID, j job) (uuid.UUID, error) {
	started := time.Now()
	built, err := c.Generator.Build(j.number, j.documentNumber, j.positionNumber)
	if err != nil {
		return uuid.Nil, err
	}
	if err := c.Store.Save(ctx, built.Origin, built.Item, built.Event); err != nil {
		return uuid.Nil, err
	}
	c.Metrics.PositionCreated(time.Since(started))
	c.logger().Info("position created",
		"runId", runID.String(),
		"jobNumber", j.number,
		"eventId", built.Event.EventID.String(),
		"sourceLineItemId", built.Item.SourceLineItemID,
	)
	return built.Event.EventID, nil
}
