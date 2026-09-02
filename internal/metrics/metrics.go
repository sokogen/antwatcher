// Package metrics owns the Prometheus registry of the service and every metric
// family antwatcher exports about its own health. Components receive a
// *Metrics and record into shared instances, so the same family is never
// registered twice and every metric is visible on the admin listener.
//
// Naming follows the plan's "Service metrics" table: everything is prefixed
// antwatcher_, timestamps are unix seconds, durations are seconds.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace is the prefix of every antwatcher metric.
const Namespace = "antwatcher"

// Results recorded in antwatcher_webhooks_total{result}.
const (
	WebhookPublished         = "published"
	WebhookPing              = "ping"
	WebhookRejectedSignature = "rejected_signature"
	WebhookBadRequest        = "bad_request"
	WebhookPublishFailed     = "publish_failed"
	WebhookPublishTimeout    = "publish_timeout"
)

// Results recorded in antwatcher_sink_events_total{result}.
const (
	SinkOK        = "ok"
	SinkError     = "error"
	SinkPermanent = "permanent"
	SinkSkipped   = "skipped"
)

// Metrics is the set of metric families shared by all components. Every
// field is registered on Registry by New.
type Metrics struct {
	// Registry collects every antwatcher metric plus the Go runtime and process
	// collectors. Components that need a prometheus.Registerer (bus drivers,
	// the Watermill router metrics) use it directly.
	Registry *prometheus.Registry

	// BuildInfo is antwatcher_build_info{version,commit} = 1.
	BuildInfo *prometheus.GaugeVec

	// Webhook ingress (receiver).
	WebhooksTotal         *prometheus.CounterVec // {event,result}
	WebhookPublishSeconds prometheus.Histogram
	WebhooksInflight      prometheus.Gauge

	// Bus.
	BusConnected   prometheus.Gauge
	BusConsumerLag *prometheus.GaugeVec // {consumer}

	// Sinks (router and stalled handling).
	SinkEventsTotal              *prometheus.CounterVec   // {sink,class,result}
	SinkProcessSeconds           *prometheus.HistogramVec // {sink,class}
	SinkLastSuccessTimestamp     *prometheus.GaugeVec     // {sink}
	SinkStalled                  *prometheus.GaugeVec     // {sink}
	SinkStalledMessages          *prometheus.GaugeVec     // {sink}
	OTLPRejectedTotal            *prometheus.CounterVec   // {signal}
	RecoveryScansTotal           *prometheus.CounterVec   // {target,result}
	RecoveryRedeliveriesTotal    *prometheus.CounterVec   // {target}
	RecoveryPending              *prometheus.GaugeVec     // {target}
	RecoveryDegraded             *prometheus.GaugeVec     // {target}
	RecoveryLastScanTimestampSec *prometheus.GaugeVec     // {target}
}

// New creates a registry with the Go runtime and process collectors and every
// antwatcher metric family, and sets antwatcher_build_info for this build.
func New(version, commit string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	f := promauto.With(reg)

	m := &Metrics{
		Registry: reg,
		BuildInfo: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "build_info",
			Help: "Build information of the running antwatcher; always 1.",
		}, []string{"version", "commit"}),

		WebhooksTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "webhooks_total",
			Help: "Webhook deliveries received by event type and outcome.",
		}, []string{"event", "result"}),
		WebhookPublishSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "webhook_publish_seconds",
			Help:    "Time spent in Bus.Publish for one webhook delivery.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		WebhooksInflight: f.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "webhooks_inflight",
			Help: "Webhook requests currently being handled.",
		}),

		BusConnected: f.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "bus_connected",
			Help: "1 while the bus driver reports a live broker connection, 0 otherwise.",
		}),
		BusConsumerLag: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "bus_consumer_lag",
			Help: "Messages the consumer has not acknowledged yet (pending on the broker plus in flight); last value polled.",
		}, []string{"consumer"}),

		SinkEventsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "sink_events_total",
			Help: "Events processed per sink by result: ok, error (retryable), permanent, skipped.",
		}, []string{"sink", "class", "result"}),
		SinkProcessSeconds: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "sink_process_seconds",
			Help:    "Duration of one sink.Process call.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"sink", "class"}),
		SinkLastSuccessTimestamp: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "sink_last_success_timestamp_seconds",
			Help: "Unix time of the last event the sink processed successfully.",
		}, []string{"sink"}),
		SinkStalled: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "sink_stalled",
			Help: "1 while at least one message is stalled on a permanent error for the sink.",
		}, []string{"sink"}),
		SinkStalledMessages: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "sink_stalled_messages",
			Help: "Number of distinct messages currently stalled for the sink.",
		}, []string{"sink"}),

		OTLPRejectedTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "otlp_rejected_total",
			Help: "Spans or log records rejected by an OTLP partial-success response; not retried by protocol rule.",
		}, []string{"signal"}),

		RecoveryScansTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "recovery_scans_total",
			Help: "Webhook delivery scans per recovery target by result.",
		}, []string{"target", "result"}),
		RecoveryRedeliveriesTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "recovery_redeliveries_total",
			Help: "Redeliveries requested from GitHub per recovery target.",
		}, []string{"target"}),
		RecoveryPending: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "recovery_pending",
			Help: "Delivery GUIDs without a successful attempt tracked per recovery target.",
		}, []string{"target"}),
		RecoveryDegraded: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "recovery_degraded",
			Help: "1 while the recovery target cannot be scanned (auth, API, rate limit, hook discovery).",
		}, []string{"target"}),
		RecoveryLastScanTimestampSec: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "recovery_last_scan_timestamp_seconds",
			Help: "Unix time of the last completed scan per recovery target.",
		}, []string{"target"}),
	}
	m.BuildInfo.WithLabelValues(version, commit).Set(1)
	return m
}

// Handler serves the registry in the Prometheus exposition format and counts
// its own scrapes (promhttp_metric_handler_*). Mount it on the admin listener only.
func (m *Metrics) Handler() http.Handler {
	h := promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
		Registry:      m.Registry,
	})
	return promhttp.InstrumentMetricHandler(m.Registry, h)
}
