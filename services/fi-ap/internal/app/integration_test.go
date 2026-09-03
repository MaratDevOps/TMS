package app

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/config"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/generate"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/kafka"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/metrics"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/outbox"
	pgstore "github.com/MaratDevOps/TMS/services/fi-ap/internal/postgres"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/result"
	"github.com/MaratDevOps/TMS/services/fi-ap/migrations"
)

func TestGenerateAndPublishPaymentDemand(t *testing.T) {
	if os.Getenv("SKIP_ITEST") != "" {
		t.Skip("SKIP_ITEST is set")
	}
	if runtime.GOOS == "windows" {
		t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
		if os.Getenv("DOCKER_HOST") == "" {
			t.Setenv("DOCKER_HOST", "npipe:////./pipe/dockerDesktopLinuxEngine")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	pgC, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("fiap"),
		postgres.WithUsername("fiap"),
		postgres.WithPassword("fiap"),
		postgres.BasicWaitStrategies(),
	)
	if skipOrFatal(t, err, "postgres") {
		return
	}
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })

	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	kafkaC, err := tckafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("fi-ap-itest"),
	)
	if skipOrFatal(t, err, "kafka") {
		return
	}
	t.Cleanup(func() { _ = kafkaC.Terminate(context.Background()) })

	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pool, err := pgstore.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	if err := pgstore.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatal(err)
	}

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	if err := producer.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	scenario := config.Scenario{
		PositionCount:           3,
		ParallelThreads:         2,
		RandomSeed:              1001,
		PositionsPerDocument:    10,
		BaseDocumentDate:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		OriginDocumentShare:     1,
		Amount:                  config.AmountRange{Min: "100.00", Max: "1000.00"},
		DueDays:                 config.IntRange{Min: 1, Max: 30},
		PaymentBlockProbability: 0,
		PaymentPurposeTemplate:  "Оплата по FI-позиции %s",
		StopOnError:             true,
	}
	reference := config.ReferenceData{
		CompanyCodes:        []string{"1000"},
		CounterpartyIDs:     []string{"0001007788"},
		Currencies:          []string{"RUB"},
		PaymentMethods:      []string{"BANK_TRANSFER"},
		Statuses:            []string{"CREATED"},
		OriginDocumentTypes: []string{"MIRO"},
	}
	outboxCfg := config.OutboxPublisherConfig{
		Enabled:                 true,
		Workers:                 2,
		BatchSize:               10,
		PollInterval:            config.Duration(50 * time.Millisecond),
		ClaimTransactionTimeout: config.Duration(5 * time.Second),
		LockTimeout:             config.Duration(15 * time.Second),
		KafkaSendTimeout:        config.Duration(10 * time.Second),
		MaxAttempts:             5,
		Retry: config.RetryConfig{
			InitialInterval: config.Duration(100 * time.Millisecond),
			Multiplier:      2,
			MaxInterval:     config.Duration(time.Second),
			Jitter:          0,
		},
		ShutdownTimeout: config.Duration(10 * time.Second),
	}

	reg := metrics.New()
	store := pgstore.NewStore(pool, reg)
	instance, err := outbox.NewInstance()
	if err != nil {
		t.Fatal(err)
	}

	application := &App{
		Metrics: reg,
		Loaded: config.Loaded{
			Config: config.Config{
				Kafka: config.KafkaConfig{
					Brokers:        brokers,
					Topic:          domain.DefaultTopic,
					ProducerSystem: domain.ProducerSystemSAPFI,
				},
				OutboxPublisher: outboxCfg,
				Generation: config.GenerationConfig{
					ReferenceData: reference,
				},
			},
			Scenario: scenario,
			Name:     "itest",
		},
		Coordinator: &generate.Coordinator{
			Store:   store,
			Numbers: store,
			Metrics: reg,
			Generator: generate.Generator{
				Scenario:       scenario,
				Reference:      reference,
				Topic:          domain.DefaultTopic,
				ProducerSystem: domain.ProducerSystemSAPFI,
			},
		},
		Publisher: &outbox.Publisher{
			Claimer:        store,
			Attempts:       store,
			Queue:          store,
			Kafka:          &itestKafka{producer: producer, metrics: reg},
			Metrics:        reg,
			Config:         outboxCfg,
			Instance:       instance,
			ProducerSystem: domain.ProducerSystemSAPFI,
		},
		Status: store,
	}

	res := application.Run(ctx, uuid.New(), time.Now().UTC())
	if res.ExitCode != result.ExitOK {
		t.Fatalf("status=%s codes=%v created=%d published=%d genErr=%d pubErr=%d",
			res.Status, res.ErrorCodes, res.CreatedPositions, res.PublishedEvents, res.GenerationErrors, res.PublicationErrors)
	}
	if res.CreatedPositions != 3 || res.PublishedEvents != 3 || res.PublicationErrors != 0 {
		t.Fatalf("created=%d published=%d pubErr=%d", res.CreatedPositions, res.PublishedEvents, res.PublicationErrors)
	}

	var items, origins, published int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM open_vendor_items`).Scan(&items); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM origin_documents`).Scan(&origins); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox_delivery_attempts WHERE status = 'PUBLISHED'
	`).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if items != 3 || origins != 3 || published != 3 {
		t.Fatalf("db items=%d origins=%d published=%d", items, origins, published)
	}

	records := consumePaymentDemand(t, ctx, brokers, 3)
	seenKeys := map[string]struct{}{}
	for _, rec := range records {
		key := string(rec.Key)
		if key == "" {
			t.Fatal("empty kafka key")
		}
		if _, ok := seenKeys[key]; ok {
			t.Fatalf("duplicate key %s", key)
		}
		seenKeys[key] = struct{}{}
		var demand domain.PaymentDemand
		if err := json.Unmarshal(rec.Value, &demand); err != nil {
			t.Fatal(err)
		}
		if demand.EventType != domain.EventTypePaymentDemand {
			t.Fatalf("eventType %s", demand.EventType)
		}
		if demand.SourceLineItemID != key {
			t.Fatalf("key %s payload %s", key, demand.SourceLineItemID)
		}
		if header(rec, "eventId") != demand.EventID.String() ||
			header(rec, "eventType") != domain.EventTypePaymentDemand ||
			header(rec, "producerSystem") != domain.ProducerSystemSAPFI {
			t.Fatalf("headers %+v", rec.Headers)
		}
	}
}

type itestKafka struct {
	producer *kafka.Producer
	metrics  *metrics.Registry
}

func (k *itestKafka) Publish(ctx context.Context, msg outbox.Message) (int32, int64, error) {
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

func consumePaymentDemand(t *testing.T, ctx context.Context, brokers []string, want int) []*kgo.Record {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("fi-ap-itest-"+uuid.NewString()),
		kgo.ConsumeTopics(domain.DefaultTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	deadline := time.Now().Add(30 * time.Second)
	var out []*kgo.Record
	for time.Now().Before(deadline) && ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := client.PollFetches(pollCtx)
		cancel()
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			out = append(out, r)
		})
		if len(out) >= want {
			return out[:want]
		}
	}
	t.Fatalf("got %d kafka records, want %d", len(out), want)
	return nil
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func skipOrFatal(t *testing.T, err error, what string) bool {
	t.Helper()
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	dockerGone := strings.Contains(msg, "npipe") ||
		strings.Contains(msg, "dockerdesktop") ||
		strings.Contains(msg, "docker provider") ||
		strings.Contains(msg, "rootless docker") ||
		(strings.Contains(msg, "docker") &&
			(strings.Contains(msg, "cannot connect") ||
				strings.Contains(msg, "pipe") ||
				strings.Contains(msg, "daemon") ||
				strings.Contains(msg, "the system cannot find") ||
				strings.Contains(msg, "connection refused")))
	if dockerGone {
		t.Skipf("%s: docker unavailable: %v", what, err)
		return true
	}
	t.Fatalf("%s: %v", what, err)
	return true
}

var _ outbox.Kafka = (*itestKafka)(nil)
