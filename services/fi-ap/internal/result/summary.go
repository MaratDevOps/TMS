package result

import (
	"time"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

type Summary struct {
	RunID              uuid.UUID
	Scenario           string
	StartedAt          time.Time
	GenerationStarted  time.Time
	GenerationFinished time.Time
	Requested          int
	Created            int
	GenerationErrors   int
	PublishedEvents    int
	PublicationErrors  int
	ParallelThreads    int
	Interrupted        bool
}

func FromSummary(s Summary) Result {
	finished := time.Now().UTC()
	started := s.StartedAt.UTC()
	if started.IsZero() {
		started = finished
	}
	genStarted := s.GenerationStarted.UTC()
	if genStarted.IsZero() {
		genStarted = started
	}
	genFinished := s.GenerationFinished.UTC()
	if genFinished.IsZero() {
		genFinished = finished
	}
	genMs := genFinished.Sub(genStarted).Milliseconds()
	if genMs < 0 {
		genMs = 0
	}
	var pps float64
	if genMs > 0 {
		pps = float64(s.Created) / float64(genMs) * 1000
	}

	scenario := s.Scenario
	var scenarioPtr *string
	if scenario != "" {
		scenarioPtr = &scenario
	}

	res := Result{
		ResultVersion:        domain.ResultVersion,
		RunID:                s.RunID.String(),
		Scenario:             scenarioPtr,
		RequestedPositions:   s.Requested,
		CreatedPositions:     s.Created,
		GenerationErrors:     s.GenerationErrors,
		PublishedEvents:      s.PublishedEvents,
		PublicationErrors:    s.PublicationErrors,
		ParallelThreads:      s.ParallelThreads,
		StartedAt:            started.Format(time.RFC3339),
		FinishedAt:           finished.Format(time.RFC3339),
		DurationMs:           finished.Sub(started).Milliseconds(),
		GenerationDurationMs: genMs,
		PositionsPerSecond:   pps,
		ErrorCodes:           []string{},
		Status:               StatusCompleted,
		ExitCode:             ExitOK,
	}

	switch {
	case s.Interrupted:
		res.Status = StatusInterrupted
		res.ExitCode = ExitError
		res.ErrorCodes = append(res.ErrorCodes, CodeInterrupted)
	case s.GenerationErrors > 0 && s.Created == 0:
		res.Status = StatusFailed
		res.ExitCode = ExitError
		res.ErrorCodes = append(res.ErrorCodes, CodeGenerationError)
	case s.GenerationErrors > 0 || s.PublicationErrors > 0:
		res.Status = StatusCompletedWithErrors
		res.ExitCode = ExitError
		if s.GenerationErrors > 0 {
			res.ErrorCodes = append(res.ErrorCodes, CodeGenerationError)
		}
		if s.PublicationErrors > 0 {
			res.ErrorCodes = append(res.ErrorCodes, CodePublicationError)
		}
	}
	return res
}
