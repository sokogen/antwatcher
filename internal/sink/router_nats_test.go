package sink_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/bus/natsjs"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/metrics"
	"github.com/sokogen/antwatcher/internal/sink"
)

// TestRouter_JetStreamBacklogPerSink proves the retention contract on a real
// broker: a failing sink keeps its own backlog (lag) while the others drain,
// and once it recovers it catches up from its own durable position.
func TestRouter_JetStreamBacklogPerSink(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	cfg := ts.Config()
	cfg.AckWait = 15 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	b, err := natsjs.New(ctx, cfg, topic, nil, natsjs.WithInProcess(ts))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	healthy := newCaptureSink("healthy", sink.ClassLog)
	flaky := newCaptureSink("flaky", sink.ClassTrace)
	var down atomic.Bool
	down.Store(true)
	flaky.setProcess(func(context.Context, event.Envelope) error {
		if down.Load() {
			return errors.New("connection refused")
		}
		return nil
	})
	rc := routerCfg
	rc.ProcessTimeout = 2 * time.Second
	h := start(t, b, rc, bus.ModeFail,
		sink.Instance{Sink: healthy, StartFrom: bus.Earliest}, sink.Instance{Sink: flaky, StartFrom: bus.Earliest})

	lag := func(consumer string) int64 {
		n, err := b.Lag(ctx, consumer)
		require.NoError(t, err)
		return n
	}

	guids := []string{"g1", "g2", "g3", "g4", "g5"}
	for _, g := range guids {
		h.publish(envelope(t, g))
	}
	eventually(t, func() bool { return len(healthy.guids()) == 5 && lag("sink-healthy") == 0 }, "healthy sink drains")
	assert.Equal(t, guids, healthy.guids())
	eventually(t, func() bool { return flaky.count() >= 2 }, "flaky sink is being re-attempted")
	assert.Equal(t, int64(5), lag("sink-flaky"), "the failing sink keeps its whole backlog")
	assert.GreaterOrEqual(t, h.events("flaky", sink.ClassTrace, metrics.SinkError), 2.0)
	assert.InDelta(t, 5, h.events("healthy", sink.ClassLog, metrics.SinkOK), 0)

	// The backlog grows for the failing sink only.
	for _, g := range []string{"g6", "g7", "g8"} {
		h.publish(envelope(t, g))
	}
	eventually(t, func() bool { return len(healthy.guids()) == 8 && lag("sink-healthy") == 0 }, "healthy sink keeps up")
	eventually(t, func() bool { return lag("sink-flaky") == 8 }, "lag grows for the failing sink")

	// Recovery: the flaky sink drains its own backlog from its durable position.
	down.Store(false)
	eventually(t, func() bool { return lag("sink-flaky") == 0 }, "backlog drained after recovery")
	eventually(t, func() bool { return h.events("flaky", sink.ClassTrace, metrics.SinkOK) == 8 }, "every backlog message processed once")
	assert.ElementsMatch(t, []string{"g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8"}, flaky.guids())
	assert.InDelta(t, 8, h.events("healthy", sink.ClassLog, metrics.SinkOK), 0, "the healthy sink saw nothing twice")
	assert.Equal(t, 8, healthy.count())
	stalled, _ := h.stalledGauges("flaky")
	assert.InDelta(t, 0, stalled, 0, "retryable outages never stall")
}
