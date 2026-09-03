package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Registry struct {
	gatherer prometheus.Gatherer

	outboxPending     prometheus.Gauge
	outboxProcessing  prometheus.Gauge
	outboxOldestAge   prometheus.Gauge
	outboxPublished   prometheus.Counter
	outboxFailed      prometheus.Counter
	outboxInterrupted prometheus.Counter
	outboxDuration    prometheus.Histogram

	generationCreated  prometheus.Counter
	generationErrors   prometheus.Counter
	generationDuration prometheus.Histogram
	generationRun      prometheus.Histogram

	kafkaPublished prometheus.Counter
	kafkaErrors    prometheus.Counter
	kafkaDuration  prometheus.Histogram
}

func New() *Registry {
	reg := prometheus.NewRegistry()
	r := &Registry{gatherer: reg}

	r.outboxPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_pending_events",
		Help: "Outbox events waiting to be published or retried.",
	})
	r.outboxProcessing = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_processing_events",
		Help: "Outbox events with an active PROCESSING lock.",
	})
	r.outboxOldestAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_oldest_pending_age_seconds",
		Help: "Age in seconds of the oldest unpublished outbox event.",
	})
	r.outboxPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_published_total",
		Help: "Outbox attempts confirmed published to Kafka.",
	})
	r.outboxFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_failed_attempts_total",
		Help: "Outbox attempts marked FAILED.",
	})
	r.outboxInterrupted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_interrupted_attempts_total",
		Help: "Outbox attempts marked INTERRUPTED after a lock timeout.",
	})
	r.outboxDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "outbox_publish_duration_seconds",
		Help:    "Duration of an outbox publish attempt including the Kafka send.",
		Buckets: prometheus.DefBuckets,
	})

	r.generationCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "generation_positions_created_total",
		Help: "Open vendor items saved successfully.",
	})
	r.generationErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "generation_errors_total",
		Help: "Position generation or save failures.",
	})
	r.generationDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "generation_position_duration_seconds",
		Help:    "Duration of generating and saving one position.",
		Buckets: prometheus.DefBuckets,
	})
	r.generationRun = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "generation_run_duration_seconds",
		Help:    "Duration of the parallel generation stage of one CLI run.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
	})

	r.kafkaPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_publish_total",
		Help: "Kafka ProduceSync calls that received a broker acknowledgement.",
	})
	r.kafkaErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_publish_errors_total",
		Help: "Kafka ProduceSync calls that failed.",
	})
	r.kafkaDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_publish_duration_seconds",
		Help:    "Duration of a Kafka ProduceSync call.",
		Buckets: prometheus.DefBuckets,
	})

	reg.MustRegister(
		r.outboxPending,
		r.outboxProcessing,
		r.outboxOldestAge,
		r.outboxPublished,
		r.outboxFailed,
		r.outboxInterrupted,
		r.outboxDuration,
		r.generationCreated,
		r.generationErrors,
		r.generationDuration,
		r.generationRun,
		r.kafkaPublished,
		r.kafkaErrors,
		r.kafkaDuration,
	)
	return r
}

func (r *Registry) Handler() http.Handler {
	if r == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(r.gatherer, promhttp.HandlerOpts{})
}

func (r *Registry) Listen(ctx context.Context, addr string) error {
	if r == nil || addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", r.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return <-errCh
	case err := <-errCh:
		return err
	}
}

func (r *Registry) SetOutboxQueue(pending, processing int, oldestAgeSeconds float64) {
	if r == nil {
		return
	}
	r.outboxPending.Set(float64(pending))
	r.outboxProcessing.Set(float64(processing))
	r.outboxOldestAge.Set(oldestAgeSeconds)
}

func (r *Registry) OutboxPublished(d time.Duration) {
	if r == nil {
		return
	}
	r.outboxPublished.Inc()
	r.outboxDuration.Observe(d.Seconds())
}

func (r *Registry) OutboxFailed() {
	if r == nil {
		return
	}
	r.outboxFailed.Inc()
}

func (r *Registry) OutboxInterrupted() {
	if r == nil {
		return
	}
	r.outboxInterrupted.Inc()
}

func (r *Registry) PositionCreated(d time.Duration) {
	if r == nil {
		return
	}
	r.generationCreated.Inc()
	r.generationDuration.Observe(d.Seconds())
}

func (r *Registry) PositionFailed() {
	if r == nil {
		return
	}
	r.generationErrors.Inc()
}

func (r *Registry) GenerationRun(d time.Duration) {
	if r == nil {
		return
	}
	r.generationRun.Observe(d.Seconds())
}

func (r *Registry) KafkaPublish(d time.Duration, err error) {
	if r == nil {
		return
	}
	r.kafkaDuration.Observe(d.Seconds())
	if err != nil {
		r.kafkaErrors.Inc()
		return
	}
	r.kafkaPublished.Inc()
}
