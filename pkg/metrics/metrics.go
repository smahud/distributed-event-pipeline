package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

var (
	// Consumer metrics
	ConsumerMessagesProcessedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_messages_processed_total",
		Help: "Total number of messages successfully processed",
	})
	ConsumerErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_errors_total",
		Help: "Total number of consumer errors",
	})
	ConsumerBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "consumer_batch_size",
		Help:    "Size of message batches processed",
		Buckets: prometheus.LinearBuckets(1, 10, 20),
	})
	ConsumerBatchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "consumer_batch_duration_seconds",
		Help:    "Time spent processing batches",
		Buckets: prometheus.DefBuckets,
	})
	ConsumerCommitErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_commit_errors_total",
		Help: "Total number of commit errors",
	})
	ConsumerDLQMessagesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_dlq_messages_total",
		Help: "Total number of messages sent to DLQ",
	})
	ConsumerDLQErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_dlq_errors_total",
		Help: "Total number of DLQ write errors",
	})
	ConsumerLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_lag",
		Help: "Current consumer lag",
	})

	// Processor metrics
	ProcessorEventsProcessedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_events_processed_total",
		Help: "Total number of events processed by processor",
	})
	ProcessorParseErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_parse_errors_total",
		Help: "Total number of event parse errors",
	})
	ProcessorEnrichmentErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_enrichment_errors_total",
		Help: "Total number of enrichment errors",
	})
	ProcessorStorageErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_storage_errors_total",
		Help: "Total number of storage errors",
	})

	// Producer metrics (for testing)
	ProducerMessagesSentTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "producer_messages_sent_total",
		Help: "Total number of messages sent by producer",
	})
	ProducerErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "producer_errors_total",
		Help: "Total number of producer errors",
	})
	ProducerLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "producer_latency_seconds",
		Help:    "Producer send latency",
		Buckets: prometheus.DefBuckets,
	})
)

func Init(port int, path string) {
	// Register all metrics
	prometheus.MustRegister(
		ConsumerMessagesProcessedTotal,
		ConsumerErrorsTotal,
		ConsumerBatchSize,
		ConsumerBatchDuration,
		ConsumerCommitErrorsTotal,
		ConsumerDLQMessagesTotal,
		ConsumerDLQErrorsTotal,
		ConsumerLag,
		ProcessorEventsProcessedTotal,
		ProcessorParseErrorsTotal,
		ProcessorEnrichmentErrorsTotal,
		ProcessorStorageErrorsTotal,
		ProducerMessagesSentTotal,
		ProducerErrorsTotal,
		ProducerLatency,
	)

	// Start metrics HTTP server
	go func() {
		http.Handle(path, promhttp.Handler())
		http.ListenAndServe(":9090", nil)
	}()
}