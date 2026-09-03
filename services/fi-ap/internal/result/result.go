package result

import (
	"io"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

const (
	StatusCompleted           = "COMPLETED"
	StatusCompletedWithErrors = "COMPLETED_WITH_ERRORS"
	StatusInterrupted         = "INTERRUPTED"
	StatusFailed              = "FAILED"

	ExitOK            = 0
	ExitError         = 1
	ExitConfiguration = 2
	ExitDependency    = 3

	CodeConfigurationError    = "CONFIGURATION_ERROR"
	CodeDependencyUnavailable = "DEPENDENCY_UNAVAILABLE"
	CodeGenerationError       = "GENERATION_ERROR"
	CodePublicationError      = "PUBLICATION_ERROR"
	CodeInterrupted           = "INTERRUPTED"
)

type Result struct {
	ResultVersion        string   `yaml:"resultVersion"`
	RunID                string   `yaml:"runId"`
	Scenario             *string  `yaml:"scenario"`
	Status               string   `yaml:"status"`
	RequestedPositions   int      `yaml:"requestedPositions"`
	CreatedPositions     int      `yaml:"createdPositions"`
	GenerationErrors     int      `yaml:"generationErrors"`
	PublishedEvents      int      `yaml:"publishedEvents"`
	PublicationErrors    int      `yaml:"publicationErrors"`
	ParallelThreads      int      `yaml:"parallelThreads"`
	StartedAt            string   `yaml:"startedAt"`
	FinishedAt           string   `yaml:"finishedAt"`
	DurationMs           int64    `yaml:"durationMs"`
	GenerationDurationMs int64    `yaml:"generationDurationMs"`
	PositionsPerSecond   float64  `yaml:"positionsPerSecond"`
	ErrorCodes           []string `yaml:"errorCodes"`
	ExitCode             int      `yaml:"exitCode"`
}

func Failed(runID uuid.UUID, scenario *string, startedAt time.Time, exitCode int, codes ...string) Result {
	finished := time.Now().UTC()
	if startedAt.IsZero() {
		startedAt = finished
	}
	if codes == nil {
		codes = []string{}
	}
	return Result{
		ResultVersion: domain.ResultVersion,
		RunID:         runID.String(),
		Scenario:      scenario,
		Status:        StatusFailed,
		StartedAt:     startedAt.UTC().Format(time.RFC3339),
		FinishedAt:    finished.Format(time.RFC3339),
		DurationMs:    finished.Sub(startedAt.UTC()).Milliseconds(),
		ErrorCodes:    codes,
		ExitCode:      exitCode,
	}
}

func (r Result) Write(w io.Writer) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	defer enc.Close()
	return enc.Encode(r)
}
