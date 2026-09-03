package generate

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

type memoryStore struct {
	mu      sync.Mutex
	items   []domain.OpenVendorItem
	events  []domain.OutboxEvent
	failAll bool
	failN   int
	saves   int
}

func (m *memoryStore) NextBatch(_ context.Context, count int) ([]string, error) {
	out := make([]string, count)
	for i := 0; i < count; i++ {
		out[i] = domain.FormatDocumentNumber(1900010000 + int64(i))
	}
	return out, nil
}

func (m *memoryStore) Save(_ context.Context, origin *domain.OriginDocument, item domain.OpenVendorItem, event domain.OutboxEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves++
	if m.failAll {
		return errors.New("db down")
	}
	if m.failN > 0 && m.saves == m.failN {
		return errors.New("injected failure")
	}
	m.items = append(m.items, item)
	m.events = append(m.events, event)
	_ = origin
	return nil
}

func TestCoordinatorCreatesAll(t *testing.T) {
	store := &memoryStore{}
	scenario := testScenario()
	scenario.ParallelThreads = 3
	c := &Coordinator{
		Store:     store,
		Numbers:   store,
		Generator: testGenerator(1),
	}
	stats, err := c.Run(context.Background(), scenario, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Created != 10 || stats.Errors != 0 {
		t.Fatalf("created=%d errors=%d", stats.Created, stats.Errors)
	}
	if len(stats.EventIDs) != 10 {
		t.Fatalf("eventIds %d", len(stats.EventIDs))
	}
	if len(store.items) != 10 {
		t.Fatalf("saved %d", len(store.items))
	}
	seen := map[string]struct{}{}
	for _, item := range store.items {
		if _, ok := seen[item.SourceLineItemID]; ok {
			t.Fatalf("duplicate sourceLineItemId %s", item.SourceLineItemID)
		}
		seen[item.SourceLineItemID] = struct{}{}
	}
}

func TestCoordinatorStopOnError(t *testing.T) {
	store := &memoryStore{failAll: true}
	scenario := testScenario()
	scenario.ParallelThreads = 1
	scenario.StopOnError = true
	c := &Coordinator{
		Store:     store,
		Numbers:   store,
		Generator: testGenerator(0),
	}
	stats, err := c.Run(context.Background(), scenario, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Created != 0 {
		t.Fatalf("created %d", stats.Created)
	}
	if stats.Errors != 1 {
		t.Fatalf("errors %d, saves %d", stats.Errors, store.saves)
	}
}

func TestCoordinatorContinueOnError(t *testing.T) {
	store := &memoryStore{failAll: true}
	scenario := testScenario()
	scenario.ParallelThreads = 1
	scenario.StopOnError = false
	c := &Coordinator{
		Store:     store,
		Numbers:   store,
		Generator: testGenerator(0),
	}
	stats, err := c.Run(context.Background(), scenario, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Errors != 10 || stats.Created != 0 {
		t.Fatalf("created=%d errors=%d", stats.Created, stats.Errors)
	}
}
