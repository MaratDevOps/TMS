package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/config"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

type fakeKafka struct {
	mu      sync.Mutex
	err     error
	calls   int
	last    Message
	publish func(ctx context.Context, msg Message) (int32, int64, error)
}

func (f *fakeKafka) Publish(ctx context.Context, msg Message) (int32, int64, error) {
	f.mu.Lock()
	f.calls++
	f.last = msg
	fn := f.publish
	err := f.err
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, msg)
	}
	if err != nil {
		return 0, 0, err
	}
	return 1, 42, nil
}

type fakeStore struct {
	mu         sync.Mutex
	claim      []Claimed
	claimed    bool
	published  []uuid.UUID
	failed     []uuid.UUID
	failNext   *time.Time
	failErr    string
	publishErr error
}

func (f *fakeStore) Claim(_ context.Context, p ClaimParams) ([]Claimed, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimed {
		return nil, nil
	}
	f.claimed = true
	_ = p
	return f.claim, nil
}

func (f *fakeStore) MarkPublished(_ context.Context, attemptID uuid.UUID, _ string, _ int32, _ int64, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.publishErr != nil {
		return f.publishErr
	}
	f.published = append(f.published, attemptID)
	return nil
}

func (f *fakeStore) MarkFailed(_ context.Context, attemptID uuid.UUID, _ string, errMsg string, next *time.Time, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, attemptID)
	f.failErr = errMsg
	f.failNext = next
	return nil
}

func testPublisherConfig() config.OutboxPublisherConfig {
	return config.OutboxPublisherConfig{
		Enabled:                 true,
		Workers:                 2,
		BatchSize:               10,
		PollInterval:            config.Duration(20 * time.Millisecond),
		ClaimTransactionTimeout: config.Duration(time.Second),
		LockTimeout:             config.Duration(30 * time.Second),
		KafkaSendTimeout:        config.Duration(time.Second),
		MaxAttempts:             5,
		Retry: config.RetryConfig{
			InitialInterval: config.Duration(time.Second),
			Multiplier:      2,
			MaxInterval:     config.Duration(time.Minute),
			Jitter:          0,
		},
		ShutdownTimeout: config.Duration(time.Second),
	}
}

func sampleClaimed() Claimed {
	id := uuid.MustParse("0f8fad5b-d9cb-469f-a165-70867728950e")
	payload, _ := json.Marshal(map[string]string{"eventId": id.String()})
	return Claimed{
		Event: domain.OutboxEvent{
			EventID:      id,
			EventType:    domain.EventTypePaymentDemand,
			EventVersion: domain.EventVersion,
			Topic:        domain.DefaultTopic,
			MessageKey:   "k1",
			Payload:      payload,
		},
		AttemptID:     uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		AttemptNumber: 1,
	}
}

func TestPublisherMarksPublished(t *testing.T) {
	item := sampleClaimed()
	store := &fakeStore{claim: []Claimed{item}}
	bus := &fakeKafka{}
	p := &Publisher{
		Claimer:        store,
		Attempts:       store,
		Kafka:          bus,
		Config:         testPublisherConfig(),
		Instance:       "test:1:id",
		ProducerSystem: domain.ProducerSystemSAPFI,
		Now:            func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		store.mu.Lock()
		n := len(store.published)
		store.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for publish")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if bus.last.Headers["eventId"] != item.Event.EventID.String() {
		t.Fatalf("headers %+v", bus.last.Headers)
	}
	if bus.last.Key != "k1" {
		t.Fatalf("key %s", bus.last.Key)
	}
}

func TestPublisherRetryableFailureSetsNextAttempt(t *testing.T) {
	item := sampleClaimed()
	store := &fakeStore{claim: []Claimed{item}}
	bus := &fakeKafka{err: errors.New("broker down")}
	p := &Publisher{
		Claimer:        store,
		Attempts:       store,
		Kafka:          bus,
		Config:         testPublisherConfig(),
		Instance:       "test:1:id",
		ProducerSystem: domain.ProducerSystemSAPFI,
		Now:            func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) },
		RandFloat:      func() float64 { return 0.5 },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		store.mu.Lock()
		n := len(store.failed)
		store.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for fail")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if store.failNext == nil {
		t.Fatal("expected next_attempt_at")
	}
}

func TestPublisherPermanentFailureHasNoRetry(t *testing.T) {
	item := sampleClaimed()
	item.Event.Topic = ""
	store := &fakeStore{claim: []Claimed{item}}
	p := &Publisher{
		Claimer:        store,
		Attempts:       store,
		Kafka:          &fakeKafka{},
		Config:         testPublisherConfig(),
		Instance:       "test:1:id",
		ProducerSystem: domain.ProducerSystemSAPFI,
		Now:            func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		store.mu.Lock()
		n := len(store.failed)
		store.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if store.failNext != nil {
		t.Fatal("permanent error must not retry")
	}
}

func TestValidateRecord(t *testing.T) {
	valid, _ := json.Marshal(map[string]string{"a": "b"})
	if err := ValidateRecord(Message{Topic: "t", Key: "k", Payload: valid}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRecord(Message{Topic: "", Key: "k", Payload: valid}); !errors.Is(err, ErrPermanent) {
		t.Fatal(err)
	}
}

func TestClassifyLatest(t *testing.T) {
	id := uuid.New()
	pub, fail, pend := ClassifyLatest(LatestAttempt{EventID: id, Exists: false}, 5)
	if pub || fail || !pend {
		t.Fatal("no attempts should be pending")
	}
	pub, fail, pend = ClassifyLatest(LatestAttempt{EventID: id, Exists: true, Status: domain.AttemptPublished, AttemptNumber: 1}, 5)
	if !pub || fail || pend {
		t.Fatal("published")
	}
	next := time.Now()
	pub, fail, pend = ClassifyLatest(LatestAttempt{EventID: id, Exists: true, Status: domain.AttemptFailed, AttemptNumber: 1, NextAttemptAt: &next}, 5)
	if pub || fail || !pend {
		t.Fatal("failed with retry should be pending")
	}
	pub, fail, pend = ClassifyLatest(LatestAttempt{EventID: id, Exists: true, Status: domain.AttemptFailed, AttemptNumber: 5}, 5)
	if pub || !fail || pend {
		t.Fatal("exhausted should be final")
	}
}
