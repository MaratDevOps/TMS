package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlerExposesOutboxAndGeneration(t *testing.T) {
	reg := New()
	reg.PositionCreated(15 * time.Millisecond)
	reg.PositionFailed()
	reg.OutboxPublished(20 * time.Millisecond)
	reg.OutboxFailed()
	reg.OutboxInterrupted()
	reg.SetOutboxQueue(3, 1, 12.5)
	reg.KafkaPublish(5*time.Millisecond, nil)
	reg.GenerationRun(time.Second)

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, name := range []string{
		"outbox_pending_events",
		"outbox_processing_events",
		"outbox_oldest_pending_age_seconds",
		"outbox_published_total",
		"outbox_failed_attempts_total",
		"outbox_interrupted_attempts_total",
		"outbox_publish_duration_seconds",
		"generation_positions_created_total",
		"generation_errors_total",
		"generation_position_duration_seconds",
		"generation_run_duration_seconds",
		"kafka_publish_total",
		"kafka_publish_errors_total",
		"kafka_publish_duration_seconds",
	} {
		if !strings.Contains(text, name) {
			t.Fatalf("missing metric %s", name)
		}
	}
	if !strings.Contains(text, "outbox_published_total 1") {
		t.Fatalf("published counter:\n%s", text)
	}
}

func TestNilRegistryIsNoop(t *testing.T) {
	var r *Registry
	r.PositionCreated(time.Second)
	r.OutboxFailed()
	r.KafkaPublish(time.Millisecond, io.EOF)
	if err := r.Listen(t.Context(), ":0"); err != nil {
		t.Fatal(err)
	}
}
