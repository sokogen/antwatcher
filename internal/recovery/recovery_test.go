package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/ghclient"
	"github.com/sokogen/antwatcher/internal/metrics"
)

var base = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return base.Add(d) }

func attempt(id int64, guid string, when time.Time, status int, redelivery bool) ghclient.Delivery {
	return ghclient.Delivery{ID: id, GUID: guid, DeliveredAt: when, StatusCode: status, Redelivery: redelivery, Event: "workflow_job", Action: "completed"}
}

// TestGroup covers the folding the recovery loop relies on in merge: one entry
// per GUID, the latest attempt won on timestamp then on id, a 2xx anywhere in
// the chain marking the GUID as delivered, and attempts without a GUID
// dropped. Grace, the per-scan cap and the redelivery order belong to the loop,
// not to group, and are covered by the TestScan_* cases.
func TestGroup(t *testing.T) {
	tests := []struct {
		name       string
		deliveries []ghclient.Delivery
		want       map[string]attempts
	}{
		{name: "empty", deliveries: nil, want: map[string]attempts{}},
		{
			name: "all failed keeps the latest attempt",
			deliveries: []ghclient.Delivery{
				attempt(3, "a", at(-20*time.Minute), 503, true),
				attempt(1, "a", at(-60*time.Minute), 503, false),
				attempt(2, "a", at(-40*time.Minute), 502, true),
			},
			want: map[string]attempts{"a": {latestID: 3, latestAt: at(-20 * time.Minute)}},
		},
		{
			name: "an original 2xx marks the guid delivered",
			deliveries: []ghclient.Delivery{
				attempt(1, "a", at(-60*time.Minute), 200, false),
				attempt(2, "a", at(-40*time.Minute), 503, true),
			},
			want: map[string]attempts{"a": {latestID: 2, latestAt: at(-40 * time.Minute), succeeded: true}},
		},
		{
			name: "a successful redelivery marks the guid delivered",
			deliveries: []ghclient.Delivery{
				attempt(1, "a", at(-60*time.Minute), 503, false),
				attempt(2, "a", at(-40*time.Minute), 202, true),
			},
			want: map[string]attempts{"a": {latestID: 2, latestAt: at(-40 * time.Minute), succeeded: true}},
		},
		{
			name: "equal timestamps prefer the higher id",
			deliveries: []ghclient.Delivery{
				attempt(21, "a", at(-30*time.Minute), 503, true),
				attempt(20, "a", at(-30*time.Minute), 503, false),
			},
			want: map[string]attempts{"a": {latestID: 21, latestAt: at(-30 * time.Minute)}},
		},
		{
			name: "attempts without a guid are dropped",
			deliveries: []ghclient.Delivery{
				attempt(15, "", at(-90*time.Minute), 500, false),
				attempt(16, "a", at(-90*time.Minute), 500, false),
			},
			want: map[string]attempts{"a": {latestID: 16, latestAt: at(-90 * time.Minute)}},
		},
		{
			name: "guids stay independent",
			deliveries: []ghclient.Delivery{
				attempt(11, "ok", at(-30*time.Minute), 200, false),
				attempt(12, "old", at(-70*time.Minute), 500, false),
				attempt(13, "old", at(-50*time.Minute), 500, true),
			},
			want: map[string]attempts{
				"ok":  {latestID: 11, latestAt: at(-30 * time.Minute), succeeded: true},
				"old": {latestID: 13, latestAt: at(-50 * time.Minute)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, group(tt.deliveries))
		})
	}
}

// fakeAPI scripts ListDeliveries, Redeliver, and DiscoverHookID and records
// every call.
type fakeAPI struct {
	mu sync.Mutex

	listFn     func(target ghclient.Target, since time.Time) ([]ghclient.Delivery, error)
	redeliver  func(target ghclient.Target, id int64) error
	discoverFn func(target ghclient.Target, url string) (int64, error)

	listSince   []time.Time
	listTargets []ghclient.Target
	redelivered []int64
	discovered  []string
}

func (f *fakeAPI) ListDeliveries(ctx context.Context, target ghclient.Target, since time.Time) ([]ghclient.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.listSince = append(f.listSince, since)
	f.listTargets = append(f.listTargets, target)
	fn := f.listFn
	f.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(target, since)
}

func (f *fakeAPI) Redeliver(ctx context.Context, target ghclient.Target, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.redelivered = append(f.redelivered, id)
	fn := f.redeliver
	f.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(target, id)
}

func (f *fakeAPI) DiscoverHookID(ctx context.Context, target ghclient.Target, url string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	f.discovered = append(f.discovered, url)
	fn := f.discoverFn
	f.mu.Unlock()
	if fn == nil {
		return 0, errors.New("no discovery scripted")
	}
	return fn(target, url)
}

func (f *fakeAPI) lists() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.listSince)
}

func (f *fakeAPI) redeliveries() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.redelivered...)
}

// clock is a fake time source advanced by tests.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var repoTarget = ghclient.Target{Owner: "acme", Repo: "widgets", HookID: 42}

func testOptions(api *fakeAPI, c *clock, m *metrics.Metrics) Options {
	return Options{
		Client:     api,
		Targets:    []ghclient.Target{repoTarget},
		WebhookURL: "https://antwatcher.example.com/webhook",
		AuthType:   config.AuthTypeToken,
		Interval:   10 * time.Minute,
		Lookback:   72 * time.Hour,
		Overlap:    15 * time.Minute,
		Grace:      5 * time.Minute,
		MaxPerScan: 100,
		Metrics:    m,
		Logger:     slog.New(slog.DiscardHandler),
		Now:        c.Now,
	}
}

func newLoop(t *testing.T, opts Options) *Loop {
	t.Helper()
	l, err := New(opts)
	require.NoError(t, err)
	return l
}

func gauge(t *testing.T, m *metrics.Metrics, name string, labels ...string) float64 {
	t.Helper()
	switch name {
	case "pending":
		return testutil.ToFloat64(m.RecoveryPending.WithLabelValues(labels...))
	case "degraded":
		return testutil.ToFloat64(m.RecoveryDegraded.WithLabelValues(labels...))
	case "last_scan":
		return testutil.ToFloat64(m.RecoveryLastScanTimestampSec.WithLabelValues(labels...))
	case "redeliveries":
		return testutil.ToFloat64(m.RecoveryRedeliveriesTotal.WithLabelValues(labels...))
	case "scans":
		return testutil.ToFloat64(m.RecoveryScansTotal.WithLabelValues(labels...))
	}
	t.Fatalf("unknown metric %q", name)
	return 0
}

func TestNew_ValidatesStaticConfig(t *testing.T) {
	c := &clock{now: base}
	good := testOptions(&fakeAPI{}, c, nil)

	t.Run("valid", func(t *testing.T) {
		_, err := New(good)
		require.NoError(t, err)
	})

	tests := []struct {
		name   string
		mutate func(o *Options)
		want   string
	}{
		{"nil client", func(o *Options) { o.Client = nil }, "client is required"},
		{"no targets", func(o *Options) { o.Targets = nil }, "at least one target"},
		{"target without hook id and url", func(o *Options) {
			o.Targets = []ghclient.Target{{Owner: "acme", Repo: "widgets"}}
			o.WebhookURL = ""
		}, "hook_id is not set and server.public_url is empty"},
		{"target with repo and org", func(o *Options) {
			o.Targets = []ghclient.Target{{Owner: "acme", Repo: "widgets", Org: "acme", HookID: 1}}
		}, "exactly one of repo or org"},
		{"target with neither", func(o *Options) { o.Targets = []ghclient.Target{{HookID: 1}} }, "exactly one of repo or org"},
		{"interval", func(o *Options) { o.Interval = 0 }, "interval must be > 0"},
		{"lookback zero", func(o *Options) { o.Lookback = 0 }, "lookback must be in"},
		{"lookback too long", func(o *Options) { o.Lookback = 73 * time.Hour }, "lookback must be in"},
		{"overlap", func(o *Options) { o.Overlap = -time.Second }, "overlap must be >= 0"},
		{"grace", func(o *Options) { o.Grace = -time.Second }, "grace must be >= 0"},
		{"max per scan", func(o *Options) { o.MaxPerScan = 0 }, "max_per_scan must be > 0"},
		{"redeliver delay", func(o *Options) { o.RedeliverDelay = -time.Second }, "redeliver delay must be >= 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := good
			tt.mutate(&o)
			_, err := New(o)
			require.ErrorContains(t, err, tt.want)
		})
	}

	t.Run("defaults logger and clock", func(t *testing.T) {
		o := good
		o.Logger, o.Now = nil, nil
		l, err := New(o)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), l.now(), time.Minute)
	})
}

func TestOptionsFromConfig(t *testing.T) {
	cfg := config.Default().Recovery
	cfg.Enabled = true
	cfg.Auth.Token = "ghp_secret"
	cfg.Targets = []config.RecoveryTarget{{Repo: "acme/widgets", HookID: 7}, {Org: "acme"}}

	opts, err := OptionsFromConfig(cfg, "https://antwatcher.example.com/webhook")
	require.NoError(t, err)
	assert.Equal(t, []ghclient.Target{{Owner: "acme", Repo: "widgets", HookID: 7}, {Org: "acme"}}, opts.Targets)
	assert.Equal(t, "https://antwatcher.example.com/webhook", opts.WebhookURL)
	assert.Equal(t, config.AuthTypeToken, opts.AuthType)
	assert.Equal(t, cfg.Interval, opts.Interval)
	assert.Equal(t, cfg.Lookback, opts.Lookback)
	assert.Equal(t, cfg.Overlap, opts.Overlap)
	assert.Equal(t, cfg.Grace, opts.Grace)
	assert.Equal(t, cfg.MaxPerScan, opts.MaxPerScan)
	assert.Equal(t, DefaultRedeliverDelay, opts.RedeliverDelay)

	cfg.Targets = []config.RecoveryTarget{{Repo: "not-a-repo"}}
	_, err = OptionsFromConfig(cfg, "")
	require.ErrorContains(t, err, "recovery.targets[0]")
}

func TestScan_WindowsAndRedelivery(t *testing.T) {
	c := &clock{now: base}
	m := metrics.New("test", "abc")
	api := &fakeAPI{}
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		return []ghclient.Delivery{
			attempt(1, "missed", at(-2*time.Hour), 503, false),
			attempt(2, "missed", at(-time.Hour), 502, true), // latest attempt of the guid
			attempt(3, "fine", at(-time.Hour), 200, false),
			attempt(4, "fresh", at(-time.Minute), 503, false), // inside grace
		}, nil
	}
	l := newLoop(t, testOptions(api, c, m))
	ctx := context.Background()

	l.ScanAll(ctx)
	require.Len(t, api.listSince, 1)
	assert.Equal(t, at(-72*time.Hour), api.listSince[0], "first scan covers the full lookback")
	assert.Equal(t, repoTarget, api.listTargets[0])
	assert.Equal(t, []int64{2}, api.redeliveries(), "exact latest id of the missed guid")

	target := repoTarget.Name()
	assert.InDelta(t, 2, gauge(t, m, "pending", target), 0, "missed and fresh are pending")
	assert.InDelta(t, 0, gauge(t, m, "degraded", target), 0)
	assert.InDelta(t, 1, gauge(t, m, "redeliveries", target), 0)
	assert.InDelta(t, 1, gauge(t, m, "scans", target, ScanOK), 0)
	assert.InDelta(t, float64(base.Unix()), gauge(t, m, "last_scan", target), 0)

	st := l.Status()
	require.Len(t, st.Targets, 1)
	assert.Equal(t, target, st.Targets[0].Target)
	assert.EqualValues(t, 42, st.Targets[0].HookID)
	assert.False(t, st.Targets[0].Degraded)
	assert.Equal(t, 2, st.Targets[0].Pending)
	assert.EqualValues(t, 1, st.Targets[0].Redeliveries)
	assert.Equal(t, ScanOK, st.Targets[0].LastResult)
	require.NotNil(t, st.Targets[0].LastScan)
	assert.Equal(t, base, *st.Targets[0].LastScan)

	// Second tick: incremental window, and no re-request inside grace.
	c.Advance(2 * time.Minute)
	l.ScanAll(ctx)
	require.Len(t, api.listSince, 2)
	assert.Equal(t, base.Add(-15*time.Minute), api.listSince[1], "second scan starts at lastScan - overlap")
	assert.Equal(t, []int64{2}, api.redeliveries(), "missed was requested within grace; fresh is still inside grace")

	// After grace without a 2xx the same delivery is requested again, and
	// fresh (now 7 minutes old) is due too; never-requested goes first.
	c.Advance(4 * time.Minute)
	l.ScanAll(ctx)
	assert.Equal(t, []int64{2, 4, 2}, api.redeliveries())

	// A successful redelivery clears the guid from pending.
	api.mu.Lock()
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		return []ghclient.Delivery{
			attempt(2, "missed", at(-time.Hour), 502, true),
			attempt(5, "missed", c.Now().Add(-time.Second), 200, true),
			attempt(4, "fresh", at(-time.Minute), 503, false),
		}, nil
	}
	api.mu.Unlock()
	c.Advance(10 * time.Minute)
	l.ScanAll(ctx)
	assert.InDelta(t, 1, gauge(t, m, "pending", target), 0, "only fresh remains")
	assert.Equal(t, []int64{2, 4, 2, 4}, api.redeliveries(), "fresh past grace is requested again, missed is not")
	assert.Equal(t, 1, l.Status().Targets[0].Pending)
}

// TestScan_GraceBoundary pins both comparisons in redeliver at exactly Grace.
// Statement coverage cannot see this: every other TestScan_ case sits minutes
// away from the boundary, so flipping either `<` to `<=` leaves them green
// while a delivery due on the tick is deferred a whole scan interval.
func TestScan_GraceBoundary(t *testing.T) {
	const grace = 5 * time.Minute

	t.Run("one nanosecond inside grace is not due", func(t *testing.T) {
		c := &clock{now: base}
		api := &fakeAPI{}
		api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
			return []ghclient.Delivery{attempt(1, "g", c.Now().Add(-grace+time.Nanosecond), 503, false)}, nil
		}
		l := newLoop(t, testOptions(api, c, nil))

		l.ScanAll(context.Background())
		assert.Empty(t, api.redeliveries(), "still inside grace by a nanosecond")
	})

	t.Run("exactly at grace is due", func(t *testing.T) {
		c := &clock{now: base}
		api := &fakeAPI{}
		api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
			return []ghclient.Delivery{attempt(1, "g", at(-time.Hour), 503, false)}, nil
		}
		l := newLoop(t, testOptions(api, c, nil))

		// The attempt is an hour old, so only the first comparison is at play
		// on this tick; make it exact by moving the clock to attempt + grace.
		c.Advance(-time.Hour + grace)
		l.ScanAll(context.Background())
		assert.Equal(t, []int64{1}, api.redeliveries(), "an attempt exactly grace old is due")

		// Now the second comparison: requestedAt is the tick above, so exactly
		// grace later the same GUID is requested again.
		c.Advance(grace - time.Nanosecond)
		l.ScanAll(context.Background())
		assert.Equal(t, []int64{1}, api.redeliveries(), "a request one nanosecond inside grace is not repeated")

		c.Advance(time.Nanosecond)
		l.ScanAll(context.Background())
		assert.Equal(t, []int64{1, 1}, api.redeliveries(), "a request exactly grace old is repeated")
	})
}

func TestScan_ClampsIncrementalWindowToLookback(t *testing.T) {
	c := &clock{now: base}
	api := &fakeAPI{}
	opts := testOptions(api, c, nil)
	opts.Lookback = 10 * time.Minute
	opts.Overlap = time.Hour
	l := newLoop(t, opts)

	l.ScanAll(context.Background())
	c.Advance(time.Minute)
	l.ScanAll(context.Background())
	require.Len(t, api.listSince, 2)
	assert.Equal(t, c.Now().Add(-10*time.Minute), api.listSince[1], "lastScan - overlap is older than the lookback and gets clamped")
}

func TestScan_CapAndDelay(t *testing.T) {
	c := &clock{now: base}
	api := &fakeAPI{}
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		return []ghclient.Delivery{
			attempt(30, "c", at(-30*time.Minute), 503, false),
			attempt(10, "a", at(-50*time.Minute), 503, false),
			attempt(20, "b", at(-40*time.Minute), 503, false),
		}, nil
	}
	opts := testOptions(api, c, nil)
	opts.MaxPerScan = 2
	opts.RedeliverDelay = time.Millisecond
	l := newLoop(t, opts)

	l.ScanAll(context.Background())
	assert.Equal(t, []int64{10, 20}, api.redeliveries(), "capped, oldest first")

	c.Advance(2 * time.Minute)
	l.ScanAll(context.Background())
	assert.Equal(t, []int64{10, 20, 30}, api.redeliveries(), "the rest follows on the next scan; requested ones wait for grace")

	c.Advance(10 * time.Minute)
	l.ScanAll(context.Background())
	assert.Equal(t, []int64{10, 20, 30, 10, 20}, api.redeliveries(), "past grace, the least recently requested go first")

	c.Advance(10 * time.Minute)
	l.ScanAll(context.Background())
	assert.Equal(t, []int64{10, 20, 30, 10, 20, 30, 10}, api.redeliveries(), "no delivery starves under the cap")
}

func TestScan_ListErrorsDegradeAndRecover(t *testing.T) {
	c := &clock{now: base}
	m := metrics.New("test", "abc")
	api := &fakeAPI{}
	var listErr error
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		if listErr != nil {
			return nil, listErr
		}
		return []ghclient.Delivery{attempt(1, "a", at(-time.Hour), 503, false)}, nil
	}
	l := newLoop(t, testOptions(api, c, m))
	ctx := context.Background()
	target := repoTarget.Name()

	listErr = errors.New("401 Bad credentials")
	l.ScanAll(ctx)
	assert.InDelta(t, 1, gauge(t, m, "degraded", target), 0)
	assert.InDelta(t, 1, gauge(t, m, "scans", target, ScanError), 0)
	assert.InDelta(t, 0, gauge(t, m, "last_scan", target), 0, "a failed listing does not count as a scan")
	st := l.Status().Targets[0]
	assert.True(t, st.Degraded)
	assert.Contains(t, st.DegradedReason, "list deliveries: 401 Bad credentials")
	assert.Nil(t, st.LastScan)
	assert.Empty(t, api.redeliveries())

	listErr = &ghclient.ErrRateLimited{RetryAfter: time.Minute}
	c.Advance(10 * time.Minute)
	l.ScanAll(ctx)
	assert.InDelta(t, 1, gauge(t, m, "degraded", target), 0)
	assert.InDelta(t, 1, gauge(t, m, "scans", target, ScanRateLimited), 0)
	assert.Equal(t, ScanRateLimited, l.Status().Targets[0].LastResult)

	// The next good tick still runs the full lookback (never scanned yet) and clears degraded.
	listErr = nil
	c.Advance(10 * time.Minute)
	l.ScanAll(ctx)
	assert.Equal(t, c.Now().Add(-72*time.Hour), api.listSince[2])
	assert.InDelta(t, 0, gauge(t, m, "degraded", target), 0)
	assert.False(t, l.Status().Targets[0].Degraded)
	assert.Equal(t, []int64{1}, api.redeliveries())
}

func TestScan_RedeliverErrors(t *testing.T) {
	c := &clock{now: base}
	m := metrics.New("test", "abc")
	api := &fakeAPI{}
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		return []ghclient.Delivery{
			attempt(1, "a", at(-50*time.Minute), 503, false),
			attempt(2, "b", at(-40*time.Minute), 503, false),
			attempt(3, "c", at(-30*time.Minute), 503, false),
		}, nil
	}
	target := repoTarget.Name()

	t.Run("rate limit stops the scan", func(t *testing.T) {
		api.mu.Lock()
		api.redelivered = nil
		api.redeliver = func(_ ghclient.Target, id int64) error {
			if id == 2 {
				return &ghclient.ErrRateLimited{RetryAfter: 30 * time.Second}
			}
			return nil
		}
		api.mu.Unlock()
		l := newLoop(t, testOptions(api, c, m))
		l.ScanAll(context.Background())
		assert.Equal(t, []int64{1, 2}, api.redeliveries(), "3 is not attempted after the rate limit")
		st := l.Status().Targets[0]
		assert.True(t, st.Degraded)
		assert.Contains(t, st.DegradedReason, "redeliver 2: ")
		assert.Contains(t, st.DegradedReason, "rate limited")
		assert.Equal(t, ScanRateLimited, st.LastResult)
		assert.EqualValues(t, 1, st.Redeliveries)
		assert.InDelta(t, 1, gauge(t, m, "degraded", target), 0)
		assert.InDelta(t, 3, gauge(t, m, "pending", target), 0)
		require.NotNil(t, st.LastScan, "the listing succeeded, so the window advances")

		// Next scan past grace: 1 was requested within grace, 2 and 3 are due.
		api.mu.Lock()
		api.redeliver = nil
		api.mu.Unlock()
		c.Advance(time.Minute)
		l.ScanAll(context.Background())
		assert.Equal(t, []int64{1, 2, 2, 3}, api.redeliveries())
		assert.False(t, l.Status().Targets[0].Degraded)
	})

	t.Run("other errors continue and degrade", func(t *testing.T) {
		api.mu.Lock()
		api.redelivered = nil
		api.redeliver = func(_ ghclient.Target, id int64) error {
			if id == 1 {
				return errors.New("404 Not Found")
			}
			return nil
		}
		api.mu.Unlock()
		l := newLoop(t, testOptions(api, c, nil))
		l.ScanAll(context.Background())
		assert.Equal(t, []int64{1, 2, 3}, api.redeliveries())
		st := l.Status().Targets[0]
		assert.True(t, st.Degraded)
		assert.Equal(t, "redeliver 1: 404 Not Found", st.DegradedReason)
		assert.Equal(t, ScanError, st.LastResult)
		assert.EqualValues(t, 2, st.Redeliveries)
	})
}

func TestScan_HookDiscovery(t *testing.T) {
	c := &clock{now: base}
	m := metrics.New("test", "abc")
	api := &fakeAPI{}
	discoverErr := errors.New("403 Resource not accessible by personal access token")
	api.discoverFn = func(_ ghclient.Target, _ string) (int64, error) {
		if discoverErr != nil {
			return 0, discoverErr
		}
		return 77, nil
	}
	api.listFn = func(target ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		assert.EqualValues(t, 77, target.HookID, "listing uses the discovered hook id")
		return nil, nil
	}
	opts := testOptions(api, c, m)
	opts.Targets = []ghclient.Target{{Org: "acme"}}
	l := newLoop(t, opts)
	ctx := context.Background()

	l.ScanAll(ctx)
	assert.Equal(t, []string{"https://antwatcher.example.com/webhook"}, api.discovered)
	assert.Equal(t, 0, api.lists(), "no listing without a hook id")
	st := l.Status().Targets[0]
	assert.Equal(t, "org:acme", st.Target)
	assert.Zero(t, st.HookID)
	assert.True(t, st.Degraded)
	assert.Contains(t, st.DegradedReason, "hook discovery: 403")
	assert.InDelta(t, 1, gauge(t, m, "degraded", "org:acme"), 0)

	discoverErr = nil
	c.Advance(10 * time.Minute)
	l.ScanAll(ctx)
	assert.Len(t, api.discovered, 2, "discovery retried on the next tick")
	assert.Equal(t, 1, api.lists())
	st = l.Status().Targets[0]
	assert.EqualValues(t, 77, st.HookID)
	assert.False(t, st.Degraded)
	assert.InDelta(t, 0, gauge(t, m, "degraded", "org:acme"), 0)

	c.Advance(10 * time.Minute)
	l.ScanAll(ctx)
	assert.Len(t, api.discovered, 2, "discovered once, remembered afterwards")
	assert.Equal(t, 2, api.lists())
}

func TestScan_DropsPendingOlderThanLookback(t *testing.T) {
	c := &clock{now: base}
	m := metrics.New("test", "abc")
	api := &fakeAPI{}
	first := true
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		if first {
			first = false
			return []ghclient.Delivery{attempt(1, "old", at(-71*time.Hour), 503, false)}, nil
		}
		return nil, nil
	}
	l := newLoop(t, testOptions(api, c, m))
	l.ScanAll(context.Background())
	assert.Equal(t, []int64{1}, api.redeliveries())
	assert.Equal(t, 1, l.Status().Targets[0].Pending)

	c.Advance(2 * time.Hour)
	l.ScanAll(context.Background())
	assert.Equal(t, 0, l.Status().Targets[0].Pending, "GitHub no longer keeps it; considered lost")
	assert.InDelta(t, 0, gauge(t, m, "pending", repoTarget.Name()), 0)
	assert.Equal(t, []int64{1}, api.redeliveries())
}

func TestScan_MultipleTargetsAreIndependent(t *testing.T) {
	c := &clock{now: base}
	m := metrics.New("test", "abc")
	api := &fakeAPI{}
	api.listFn = func(target ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		if target.IsOrg() {
			return nil, errors.New("boom")
		}
		return []ghclient.Delivery{attempt(1, "a", at(-time.Hour), 503, false)}, nil
	}
	opts := testOptions(api, c, m)
	opts.Targets = []ghclient.Target{repoTarget, {Org: "acme", HookID: 9}}
	l := newLoop(t, opts)
	l.ScanAll(context.Background())

	st := l.Status()
	require.Len(t, st.Targets, 2)
	assert.False(t, st.Targets[0].Degraded)
	assert.True(t, st.Targets[1].Degraded)
	assert.Equal(t, []int64{1}, api.redeliveries())
	assert.InDelta(t, 0, gauge(t, m, "degraded", "acme/widgets"), 0)
	assert.InDelta(t, 1, gauge(t, m, "degraded", "org:acme"), 0)
}

func TestRun_TicksAndStopsOnCancel(t *testing.T) {
	c := &clock{now: base}
	m := metrics.New("test", "abc")
	api := &fakeAPI{}
	opts := testOptions(api, c, m)
	opts.Interval = 5 * time.Millisecond
	l := newLoop(t, opts)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	require.Eventually(t, func() bool { return api.lists() >= 3 }, 5*time.Second, time.Millisecond, "first scan at start, then one per tick")
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "Run never fails")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestScan_CancelledContextRecordsNothing(t *testing.T) {
	c := &clock{now: base}
	m := metrics.New("test", "abc")
	api := &fakeAPI{}
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		return []ghclient.Delivery{attempt(1, "a", at(-time.Hour), 503, false)}, nil
	}
	l := newLoop(t, testOptions(api, c, m))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l.ScanAll(ctx)
	assert.Equal(t, 0, api.lists())
	assert.False(t, l.Status().Targets[0].Degraded)
	assert.InDelta(t, 0, gauge(t, m, "scans", repoTarget.Name(), ScanError), 0)

	// Cancel in the middle of the redelivery delay: no degraded mark, no stray requests.
	api.mu.Lock()
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		return []ghclient.Delivery{
			attempt(1, "a", at(-time.Hour), 503, false),
			attempt(2, "b", at(-time.Hour), 503, false),
		}, nil
	}
	api.mu.Unlock()
	opts := testOptions(api, c, m)
	opts.RedeliverDelay = time.Hour
	l = newLoop(t, opts)
	ctx, cancel = context.WithCancel(context.Background())
	api.mu.Lock()
	api.redeliver = func(_ ghclient.Target, _ int64) error { cancel(); return nil }
	api.mu.Unlock()
	l.ScanAll(ctx)
	assert.Equal(t, []int64{1}, api.redeliveries())
	assert.False(t, l.Status().Targets[0].Degraded)
	assert.InDelta(t, 0, gauge(t, m, "scans", repoTarget.Name(), ScanError), 0)
}

func TestStatus_HasNoSecretsAndRendersJSON(t *testing.T) {
	c := &clock{now: base}
	opts := testOptions(&fakeAPI{}, c, nil)
	l := newLoop(t, opts)
	body, err := json.Marshal(l.Status())
	require.NoError(t, err)
	assert.Contains(t, string(body), `"auth_type":"token"`)
	assert.Contains(t, string(body), `"interval":"10m0s"`)
	assert.Contains(t, string(body), `"target":"acme/widgets"`)
	assert.Contains(t, string(body), `"hook_id":42`)
	assert.NotContains(t, string(body), "last_scan", "never scanned")
	assert.NotContains(t, string(body), "ghp_")
}

func TestScan_NoMetricsIsFine(t *testing.T) {
	c := &clock{now: base}
	api := &fakeAPI{}
	api.listFn = func(_ ghclient.Target, _ time.Time) ([]ghclient.Delivery, error) {
		return []ghclient.Delivery{attempt(1, "a", at(-time.Hour), 503, false)}, nil
	}
	l := newLoop(t, testOptions(api, c, nil))
	l.ScanAll(context.Background())
	assert.Equal(t, []int64{1}, api.redeliveries())
}

func TestScanAll_IsSafeToCallConcurrently(t *testing.T) {
	// ScanAll is exported "so tests and operators can force a scan", so it
	// must not depend on Run being its only caller: scan reads st.target,
	// st.scanned and st.lastScan outside l.mu while writing them under it.
	c := &clock{now: base}
	api := &fakeAPI{}
	release := make(chan struct{})
	api.discoverFn = func(_ ghclient.Target, _ string) (int64, error) {
		<-release // hold the first scan inside discovery so the second overlaps
		return 77, nil
	}
	opts := testOptions(api, c, metrics.New("test", "abc"))
	opts.Targets = []ghclient.Target{{Org: "acme"}}
	l := newLoop(t, opts)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			l.ScanAll(ctx)
		}()
	}
	close(release)
	wg.Wait()

	assert.EqualValues(t, 77, l.Status().Targets[0].HookID)
}
