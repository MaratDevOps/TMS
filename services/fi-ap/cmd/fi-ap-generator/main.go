package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/app"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/config"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/generate"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/kafka"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/metrics"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/outbox"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/postgres"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/result"
	"github.com/MaratDevOps/TMS/services/fi-ap/migrations"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("fi-ap-generator", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "configs/generation.yaml", "path to YAML configuration")
	scenario := fs.String("scenario", "", "generation scenario; overrides generation.activeScenario")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return result.ExitConfiguration
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	runID := uuid.New()
	startedAt := time.Now().UTC()
	writeResult := func(res result.Result) int {
		if err := res.Write(os.Stdout); err != nil {
			logger.Error("write result failed", "err", err)
		}
		return res.ExitCode
	}

	loaded, err := config.Load(*configPath, *scenario)
	if err != nil {
		logger.Error("configuration failed", "runId", runID.String(), "err", err)
		return writeResult(result.Failed(runID, nil, startedAt, result.ExitConfiguration, result.CodeConfigurationError))
	}

	logger.Info("configuration loaded",
		"runId", runID.String(),
		"scenario", loaded.Name,
		"positionCount", loaded.Scenario.PositionCount,
		"parallelThreads", loaded.Scenario.ParallelThreads,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Connect(ctx, loaded.Config.Postgres.DSN)
	if err != nil {
		logger.Error("postgres unavailable", "runId", runID.String(), "err", err)
		return writeResult(dependencyFailed(runID, loaded, startedAt))
	}
	defer pool.Close()

	if err := postgres.Migrate(ctx, pool, migrations.FS); err != nil {
		logger.Error("migrate failed", "runId", runID.String(), "err", err)
		return writeResult(dependencyFailed(runID, loaded, startedAt))
	}

	producer, err := kafka.NewProducer(loaded.Config.Kafka.Brokers)
	if err != nil {
		logger.Error("kafka client failed", "runId", runID.String(), "err", err)
		return writeResult(dependencyFailed(runID, loaded, startedAt))
	}
	defer producer.Close()

	if err := producer.Ping(ctx); err != nil {
		logger.Error("kafka unavailable", "runId", runID.String(), "err", err)
		return writeResult(dependencyFailed(runID, loaded, startedAt))
	}

	reg := metrics.New()
	if loaded.Config.Metrics.Listen != "" {
		go func() {
			if err := reg.Listen(ctx, loaded.Config.Metrics.Listen); err != nil {
				logger.Error("metrics server failed", "runId", runID.String(), "err", err)
			}
		}()
		logger.Info("metrics listening", "runId", runID.String(), "addr", loaded.Config.Metrics.Listen)
	}

	instance, err := outbox.NewInstance()
	if err != nil {
		logger.Error("publisher instance failed", "runId", runID.String(), "err", err)
		name := loaded.Name
		return writeResult(result.Failed(runID, &name, startedAt, result.ExitError, result.CodeGenerationError))
	}

	store := postgres.NewStore(pool, reg)
	application := &app.App{
		Log:    logger,
		Loaded: loaded,
		Coordinator: &generate.Coordinator{
			Log:     logger,
			Store:   store,
			Numbers: store,
			Metrics: reg,
			Generator: generate.Generator{
				Scenario:       loaded.Scenario,
				Reference:      loaded.Config.Generation.ReferenceData,
				Topic:          loaded.Config.Kafka.Topic,
				ProducerSystem: loaded.Config.Kafka.ProducerSystem,
			},
		},
		Publisher: &outbox.Publisher{
			Log:            logger,
			Claimer:        store,
			Attempts:       store,
			Queue:          store,
			Kafka:          &kafkaBus{producer: producer, metrics: reg},
			Metrics:        reg,
			Config:         loaded.Config.OutboxPublisher,
			Instance:       instance,
			ProducerSystem: loaded.Config.Kafka.ProducerSystem,
		},
		Status:  store,
		Metrics: reg,
	}

	res := application.Run(ctx, runID, startedAt)
	return writeResult(res)
}

func dependencyFailed(runID uuid.UUID, loaded config.Loaded, startedAt time.Time) result.Result {
	name := loaded.Name
	res := result.Failed(runID, &name, startedAt, result.ExitDependency, result.CodeDependencyUnavailable)
	res.RequestedPositions = loaded.Scenario.PositionCount
	res.ParallelThreads = loaded.Scenario.ParallelThreads
	return res
}

type kafkaBus struct {
	producer *kafka.Producer
	metrics  *metrics.Registry
}

func (k *kafkaBus) Publish(ctx context.Context, msg outbox.Message) (int32, int64, error) {
	started := time.Now()
	partition, offset, err := k.producer.Publish(ctx, kafka.Message{
		Topic:   msg.Topic,
		Key:     msg.Key,
		Payload: msg.Payload,
		Headers: msg.Headers,
	})
	k.metrics.KafkaPublish(time.Since(started), err)
	return partition, offset, err
}

var (
	_ generate.Store           = (*postgres.Store)(nil)
	_ generate.DocumentNumbers = (*postgres.Store)(nil)
	_ outbox.Claimer           = (*postgres.Store)(nil)
	_ outbox.Attempts          = (*postgres.Store)(nil)
	_ outbox.StatusReader      = (*postgres.Store)(nil)
	_ outbox.QueueReader       = (*postgres.Store)(nil)
	_ outbox.Kafka             = (*kafkaBus)(nil)
)
