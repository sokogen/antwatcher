package sink

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/metrics"
)

func gauges(t *testing.T, m *metrics.Metrics, sinkName string) (stalled, count float64) {
	t.Helper()
	return testutil.ToFloat64(m.SinkStalled.WithLabelValues(sinkName)), testutil.ToFloat64(m.SinkStalledMessages.WithLabelValues(sinkName))
}

func TestStalledSet(t *testing.T) {
	m := metrics.New("v", "c")
	s := newStalledSet("tempo", m)
	t0 := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)

	stalled, count := gauges(t, m, "tempo")
	assert.InDelta(t, 0, stalled, 0, "gauges exist as zero from creation")
	assert.InDelta(t, 0, count, 0)
	assert.Empty(t, s.list())
	assert.Equal(t, 0, s.len())

	attempts, logNow := s.record("a", errors.New("invalid argument"), t0)
	assert.Equal(t, 1, attempts)
	assert.True(t, logNow, "first occurrence is logged")
	stalled, count = gauges(t, m, "tempo")
	assert.InDelta(t, 1, stalled, 0)
	assert.InDelta(t, 1, count, 0)

	for i := 2; i < stalledLogEvery; i++ {
		attempts, logNow = s.record("a", errors.New("invalid argument"), t0.Add(time.Duration(i)*time.Minute))
		assert.Equal(t, i, attempts)
		assert.False(t, logNow, "attempt %d is not logged", i)
	}
	attempts, logNow = s.record("a", errors.New("still invalid"), t0.Add(time.Hour))
	assert.Equal(t, stalledLogEvery, attempts)
	assert.True(t, logNow, "every %dth attempt is logged", stalledLogEvery)
	_, count = gauges(t, m, "tempo")
	assert.InDelta(t, 1, count, 0, "re-attempts of the same UUID do not add entries")

	_, _ = s.record("b", errors.New("schema"), t0.Add(2*time.Hour))
	stalled, count = gauges(t, m, "tempo")
	assert.InDelta(t, 1, stalled, 0)
	assert.InDelta(t, 2, count, 0)

	list := s.list()
	require.Len(t, list, 2)
	assert.Equal(t, "a", list[0].UUID, "ordered by first occurrence")
	assert.Equal(t, StalledInfo{Reason: "still invalid", FirstSeen: t0, LastSeen: t0.Add(time.Hour), Attempts: stalledLogEvery}, list[0].StalledInfo)
	assert.Equal(t, "b", list[1].UUID)
	assert.Equal(t, 1, list[1].Attempts)

	assert.False(t, s.clear("c"), "success of an unrelated UUID clears nothing")
	assert.Equal(t, 2, s.len())
	assert.True(t, s.clear("b"), "success of a different UUID does not clear the older one")
	assert.Equal(t, 1, s.len())
	assert.Equal(t, "a", s.list()[0].UUID)
	stalled, count = gauges(t, m, "tempo")
	assert.InDelta(t, 1, stalled, 0)
	assert.InDelta(t, 1, count, 0)

	assert.True(t, s.clear("a"), "success of the same UUID clears it")
	assert.False(t, s.clear("a"))
	assert.Equal(t, 0, s.len())
	stalled, count = gauges(t, m, "tempo")
	assert.InDelta(t, 0, stalled, 0)
	assert.InDelta(t, 0, count, 0)
}

func TestStalledSet_ListOrderTiesOnUUID(t *testing.T) {
	s := newStalledSet("x", nil)
	now := time.Now()
	_, _ = s.record("b", errors.New("e"), now)
	_, _ = s.record("a", errors.New("e"), now)
	list := s.list()
	require.Len(t, list, 2)
	assert.Equal(t, "a", list[0].UUID)
	assert.Equal(t, "b", list[1].UUID)
}

func TestStalledSet_NoMetrics(t *testing.T) {
	s := newStalledSet("x", nil)
	_, _ = s.record("a", errors.New("e"), time.Now())
	assert.Equal(t, 1, s.len())
	assert.True(t, s.clear("a"))
}

func TestStalledSetCapsTheTrackedMessages(t *testing.T) {
	m := metrics.New("v", "c")
	s := newStalledSet("tempo", m)
	now := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	boom := errors.New("invalid argument")

	for i := range maxStalledEntries {
		s.record(fmt.Sprintf("uuid-%d", i), boom, now)
	}
	require.Equal(t, maxStalledEntries, s.len())

	// A destination rejecting everything must not grow the set, nor the
	// /status document, without bound.
	attempts, logNow := s.record("over-1", boom, now)
	assert.Equal(t, 1, attempts)
	assert.True(t, logNow, "the first untracked failure is still logged")
	for i := 2; i < stalledLogEvery; i++ {
		_, logNow = s.record(fmt.Sprintf("over-%d", i), boom, now)
		assert.False(t, logNow, "untracked failures are logged at the same pace")
	}
	_, logNow = s.record("over-10", boom, now)
	assert.True(t, logNow, "every stalledLogEvery-th untracked failure is logged")

	assert.Equal(t, maxStalledEntries, s.len())
	assert.Len(t, s.list(), maxStalledEntries)
	_, count := gauges(t, m, "tempo")
	assert.InDelta(t, maxStalledEntries, count, 0)

	// The cap does not stop a tracked message from clearing.
	require.True(t, s.clear("uuid-0"))
	assert.Equal(t, maxStalledEntries-1, s.len())
	s.record("over-1", boom, now)
	assert.Equal(t, maxStalledEntries, s.len(), "the freed slot is taken by the next message")
}
