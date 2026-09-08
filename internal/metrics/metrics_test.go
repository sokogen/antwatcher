package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_BuildInfoAndCollectors(t *testing.T) {
	m := New("1.2.3", "abc123")

	expected := `
# HELP antwatcher_build_info Build information of the running antwatcher; always 1.
# TYPE antwatcher_build_info gauge
antwatcher_build_info{commit="abc123",version="1.2.3"} 1
`
	require.NoError(t, testutil.GatherAndCompare(m.Registry, strings.NewReader(expected), "antwatcher_build_info"))

	families, err := m.Registry.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	assert.True(t, names["go_goroutines"], "Go runtime collector registered")
	assert.True(t, names["process_start_time_seconds"], "process collector registered")
}

func TestNew_EveryFamilyIsRegisteredAndNamedPerPlan(t *testing.T) {
	m := New("v", "c")

	// touch every vector so it appears in the gather output
	m.WebhooksTotal.WithLabelValues("workflow_job", WebhookPublished).Inc()
	m.WebhookPublishSeconds.Observe(0.1)
	m.WebhooksInflight.Set(1)
	m.BusConnected.Set(1)
	m.BusConsumerLag.WithLabelValues("sink-a").Set(3)
	m.SinkEventsTotal.WithLabelValues("a", "trace", SinkOK).Inc()
	m.SinkProcessSeconds.WithLabelValues("a", "trace").Observe(0.2)
	m.SinkLastSuccessTimestamp.WithLabelValues("a").Set(1)
	m.SinkStalled.WithLabelValues("a").Set(0)
	m.SinkStalledMessages.WithLabelValues("a").Set(0)
	m.OTLPRejectedTotal.WithLabelValues("traces").Inc()
	m.RecoveryScansTotal.WithLabelValues("owner/repo", "ok").Inc()
	m.RecoveryRedeliveriesTotal.WithLabelValues("owner/repo").Inc()
	m.RecoveryPending.WithLabelValues("owner/repo").Set(2)
	m.RecoveryDegraded.WithLabelValues("owner/repo").Set(1)
	m.RecoveryLastScanTimestampSec.WithLabelValues("owner/repo").Set(1)

	families, err := m.Registry.Gather()
	require.NoError(t, err)
	got := map[string][]string{}
	for _, f := range families {
		var labels []string
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
		got[f.GetName()] = labels
	}

	want := map[string][]string{
		"antwatcher_build_info":                           {"commit", "version"},
		"antwatcher_webhooks_total":                       {"event", "result"},
		"antwatcher_webhook_publish_seconds":              nil,
		"antwatcher_webhooks_inflight":                    nil,
		"antwatcher_bus_connected":                        nil,
		"antwatcher_bus_consumer_lag":                     {"consumer"},
		"antwatcher_sink_events_total":                    {"class", "result", "sink"},
		"antwatcher_sink_process_seconds":                 {"class", "sink"},
		"antwatcher_sink_last_success_timestamp_seconds":  {"sink"},
		"antwatcher_sink_stalled":                         {"sink"},
		"antwatcher_sink_stalled_messages":                {"sink"},
		"antwatcher_otlp_rejected_total":                  {"signal"},
		"antwatcher_recovery_scans_total":                 {"result", "target"},
		"antwatcher_recovery_redeliveries_total":          {"target"},
		"antwatcher_recovery_pending":                     {"target"},
		"antwatcher_recovery_degraded":                    {"target"},
		"antwatcher_recovery_last_scan_timestamp_seconds": {"target"},
	}
	for name, labels := range want {
		gotLabels, ok := got[name]
		assert.True(t, ok, "family %s missing", name)
		assert.Equal(t, labels, gotLabels, "labels of %s", name)
	}
}

func TestNew_IndependentRegistries(t *testing.T) {
	// two instances must not collide on the global registry
	a := New("a", "1")
	b := New("b", "2")
	a.WebhooksInflight.Set(5)
	assert.InDelta(t, 5, testutil.ToFloat64(a.WebhooksInflight), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(b.WebhooksInflight), 0)
}

func TestHandler_ServesExposition(t *testing.T) {
	m := New("9.9.9", "deadbeef")
	m.WebhooksTotal.WithLabelValues("ping", WebhookPing).Inc()

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, `antwatcher_build_info{commit="deadbeef",version="9.9.9"} 1`)
	assert.Contains(t, body, `antwatcher_webhooks_total{event="ping",result="ping"} 1`)
	assert.Contains(t, body, "go_goroutines")
	assert.Contains(t, body, "promhttp_metric_handler_requests_total", "handler registers its own metrics on the registry")
}
