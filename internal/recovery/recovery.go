// Package recovery repairs missed ingress through the GitHub webhook
// deliveries API. For every configured hook it periodically lists the delivery
// attempts, groups them by GUID, and asks GitHub to redeliver each GUID that
// never got a 2xx from the receiver. A redelivery re-enters the receiver like
// any other webhook, so recovery never bypasses the bus.
//
// The first scan after start covers the whole lookback window (72h by
// default, the GitHub retention of deliveries); later scans are incremental
// from the previous scan minus an overlap. In-memory state per target (hook
// ID, last scan time, pending GUIDs) is a cache: a restart repeats the full
// scan and reaches the same decisions.
//
// Recovery is never fatal. A bad token, an unreachable API, a rate limit, or a
// failed hook discovery marks the target degraded
// (antwatcher_recovery_degraded{target}=1, reason in /status) and is retried
// on the next tick while the receiver keeps serving. Only invalid static
// configuration (no targets, bad intervals, a target without hook_id and
// without a webhook URL to discover it) is rejected by New.
//
// Metrics (all labelled by target, "owner/repo" or "org:name"):
//
//	antwatcher_recovery_scans_total{target,result}          scans by result: ok, error, rate_limited
//	antwatcher_recovery_redeliveries_total{target}          redeliveries requested from GitHub
//	antwatcher_recovery_pending{target}                     GUIDs tracked without a successful attempt
//	antwatcher_recovery_degraded{target}                    1 while the target cannot be scanned
//	antwatcher_recovery_last_scan_timestamp_seconds{target} unix time of the last completed listing
//
// Single replica: run recovery on exactly one instance. Two instances scanning
// the same hook would each request redeliveries of the same GUIDs; the bus
// dedup window and sink idempotency make that safe but wasteful.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/ghclient"
	"github.com/sokogen/antwatcher/internal/metrics"
)

// Scan results recorded in antwatcher_recovery_scans_total{result}.
const (
	ScanOK          = "ok"
	ScanError       = "error"
	ScanRateLimited = "rate_limited"
)

// DefaultRedeliverDelay spaces consecutive redelivery requests of one scan so
// a large backlog does not trip the secondary rate limit.
const DefaultRedeliverDelay = 200 * time.Millisecond

// Options configures the loop. Interval, Lookback, MaxPerScan, and at least
// one target are required; see New.
type Options struct {
	// Client talks to GitHub; a fake in tests.
	Client ghclient.API
	// Targets are the hooks to scan. A target with HookID 0 is discovered
	// through WebhookURL on its first scan.
	Targets []ghclient.Target
	// WebhookURL is the receiver URL GitHub posts to (server.public_url),
	// used to discover hook IDs.
	WebhookURL string
	// AuthType is shown in Status; the loop never sees the token.
	AuthType string

	// Interval between scans of every target.
	Interval time.Duration
	// Lookback bounds the first scan (and every scan) to now-Lookback.
	Lookback time.Duration
	// Overlap is subtracted from the previous scan time on incremental scans.
	Overlap time.Duration
	// Grace leaves alone attempts younger than this and GUIDs redelivered
	// within this.
	Grace time.Duration
	// MaxPerScan caps redelivery requests per target per scan.
	MaxPerScan int
	// RedeliverDelay is the pause between two redelivery requests; zero
	// means none (OptionsFromConfig sets DefaultRedeliverDelay).
	RedeliverDelay time.Duration

	// Metrics receives the recovery families; nil disables recording.
	Metrics *metrics.Metrics
	// Logger may be nil.
	Logger *slog.Logger
	// Now is the clock; time.Now when nil. Tests inject a fake.
	Now func() time.Time
}

// OptionsFromConfig maps the recovery block. webhookURL is server.public_url.
// Client, Metrics, and Logger are left for the caller.
func OptionsFromConfig(cfg config.Recovery, webhookURL string) (Options, error) {
	targets := make([]ghclient.Target, 0, len(cfg.Targets))
	for i, t := range cfg.Targets {
		target, err := ghclient.TargetFromConfig(t)
		if err != nil {
			return Options{}, fmt.Errorf("recovery.targets[%d]: %w", i, err)
		}
		targets = append(targets, target)
	}
	return Options{
		Targets:        targets,
		WebhookURL:     webhookURL,
		AuthType:       cfg.Auth.Type,
		Interval:       cfg.Interval,
		Lookback:       cfg.Lookback,
		Overlap:        cfg.Overlap,
		Grace:          cfg.Grace,
		MaxPerScan:     cfg.MaxPerScan,
		RedeliverDelay: DefaultRedeliverDelay,
	}, nil
}

// pendingInfo is what the loop remembers about a GUID without a 2xx attempt.
type pendingInfo struct {
	latestDeliveryID int64
	latestAt         time.Time
	requestedAt      time.Time // zero until this loop asked for a redelivery
}

// targetState is the per-target cache. Everything here is rebuilt by a full
// scan after a restart.
type targetState struct {
	target      ghclient.Target
	lastScan    time.Time
	pending     map[string]pendingInfo
	degraded    string
	redelivered int64
	lastResult  string
	scanned     bool
}

// Loop runs the scans. Build it with New, run it with Run, inspect it with
// Status.
type Loop struct {
	opts   Options
	client ghclient.API
	logger *slog.Logger
	now    func() time.Time

	// scanMu serialises ScanAll. mu below guards targetState against
	// concurrent readers (/status, metrics); it does not make two scans of
	// the same target safe against each other, because a scan reads
	// st.target, st.scanned and st.lastScan outside it. ScanAll is exported
	// for operators and tests, so it must not rely on Run being the only
	// caller.
	scanMu sync.Mutex

	mu      sync.Mutex
	targets []*targetState
}

// New validates the static configuration and builds the loop. It performs no
// network call: hook discovery and the first scan happen in Run.
func New(opts Options) (*Loop, error) {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if opts.Client == nil {
		add("recovery: client is required")
	}
	if len(opts.Targets) == 0 {
		add("recovery: at least one target is required")
	}
	for i, t := range opts.Targets {
		if t.IsOrg() == (t.Repo != "") || (t.Repo != "" && t.Owner == "") {
			add("recovery: targets[%d]: exactly one of repo or org is required", i)
		}
		if t.HookID == 0 && opts.WebhookURL == "" {
			add("recovery: targets[%d] (%s): hook_id is not set and server.public_url is empty, so it cannot be discovered", i, t.Name())
		}
	}
	if opts.Interval <= 0 {
		add("recovery: interval must be > 0")
	}
	if opts.Lookback <= 0 || opts.Lookback > config.MaxLookback {
		add("recovery: lookback must be in (0, %s]", config.MaxLookback)
	}
	if opts.Overlap < 0 {
		add("recovery: overlap must be >= 0")
	}
	if opts.Grace < 0 {
		add("recovery: grace must be >= 0")
	}
	if opts.MaxPerScan <= 0 {
		add("recovery: max_per_scan must be > 0")
	}
	if opts.RedeliverDelay < 0 {
		add("recovery: redeliver delay must be >= 0")
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	l := &Loop{opts: opts, client: opts.Client, logger: logger, now: now}
	for _, t := range opts.Targets {
		l.targets = append(l.targets, &targetState{target: t, pending: map[string]pendingInfo{}})
	}
	return l, nil
}

// Run scans every target once immediately, then every Interval, until ctx
// ends. It always returns nil: recovery problems degrade targets and are
// retried, they never stop the service.
func (l *Loop) Run(ctx context.Context) error {
	l.ScanAll(ctx)
	ticker := time.NewTicker(l.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			l.ScanAll(ctx)
		}
	}
}

// ScanAll scans every target once, in configuration order. It is what Run
// calls on each tick and is exported so tests and operators can force a scan.
func (l *Loop) ScanAll(ctx context.Context) {
	l.scanMu.Lock()
	defer l.scanMu.Unlock()
	for i := range l.targets {
		if ctx.Err() != nil {
			return
		}
		l.scan(ctx, i)
	}
}

// scan runs one scan of target i: discover the hook if needed, list the
// attempts since the window start, merge them into pending, and redeliver.
func (l *Loop) scan(ctx context.Context, i int) {
	st := l.targets[i]
	now := l.now()

	if st.target.HookID == 0 {
		id, err := l.client.DiscoverHookID(ctx, st.target, l.opts.WebhookURL)
		if err != nil {
			l.finish(ctx, st, ScanError, fmt.Sprintf("hook discovery: %v", err))
			return
		}
		l.mu.Lock()
		st.target.HookID = id
		l.mu.Unlock()
		l.logger.Info("recovery hook discovered", "target", st.target.String(), "hook_id", id)
	}

	since := now.Add(-l.opts.Lookback)
	if st.scanned {
		if inc := st.lastScan.Add(-l.opts.Overlap); inc.After(since) {
			since = inc
		}
	}
	deliveries, err := l.client.ListDeliveries(ctx, st.target, since)
	if err != nil {
		l.finish(ctx, st, resultOf(err), fmt.Sprintf("list deliveries: %v", err))
		return
	}

	l.mu.Lock()
	st.scanned = true
	st.lastScan = now
	l.merge(st, deliveries, now)
	l.mu.Unlock()
	if l.opts.Metrics != nil {
		l.opts.Metrics.RecoveryLastScanTimestampSec.WithLabelValues(st.target.Name()).Set(float64(now.Unix()))
	}

	result, reason := l.redeliver(ctx, st, now)
	l.finish(ctx, st, result, reason)
}

// merge applies the attempts of one scan to the pending set: a GUID with a
// 2xx attempt leaves, every other GUID enters or is refreshed with its latest
// attempt. Entries older than the lookback can no longer be redelivered and
// are dropped as lost.
func (l *Loop) merge(st *targetState, deliveries []ghclient.Delivery, now time.Time) {
	for guid, a := range group(deliveries) {
		if a.succeeded {
			delete(st.pending, guid)
			continue
		}
		p := st.pending[guid]
		if a.latestID != p.latestDeliveryID {
			p.latestDeliveryID, p.latestAt = a.latestID, a.latestAt
		}
		st.pending[guid] = p
	}
	horizon := now.Add(-l.opts.Lookback)
	for guid, p := range st.pending {
		if p.latestAt.Before(horizon) {
			l.logger.Warn("recovery gave up on a delivery older than the lookback window", "target", st.target.String(), "guid", guid, "delivery_id", p.latestDeliveryID)
			delete(st.pending, guid)
		}
	}
}

// redeliver asks GitHub for the pending GUIDs that are past grace and were
// not requested within grace, up to MaxPerScan. A rate limit
// ends the scan; any other redelivery error is logged, degrades the target,
// and does not stop the remaining requests.
func (l *Loop) redeliver(ctx context.Context, st *targetState, now time.Time) (result, reason string) {
	type due struct {
		guid string
		info pendingInfo
	}
	l.mu.Lock()
	var candidates []due
	for guid, p := range st.pending {
		if now.Sub(p.latestAt) < l.opts.Grace {
			continue
		}
		if !p.requestedAt.IsZero() && now.Sub(p.requestedAt) < l.opts.Grace {
			continue
		}
		candidates = append(candidates, due{guid: guid, info: p})
	}
	l.mu.Unlock()
	// Never-requested GUIDs go first, then the least recently requested, then
	// the oldest attempt: under MaxPerScan a persistent set of failing
	// deliveries must not starve the rest.
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i].info, candidates[j].info
		if !a.requestedAt.Equal(b.requestedAt) {
			return a.requestedAt.Before(b.requestedAt)
		}
		if !a.latestAt.Equal(b.latestAt) {
			return a.latestAt.Before(b.latestAt)
		}
		return a.latestDeliveryID < b.latestDeliveryID
	})
	if len(candidates) > l.opts.MaxPerScan {
		l.logger.Info("recovery capped redeliveries for this scan", "target", st.target.String(), "due", len(candidates), "max_per_scan", l.opts.MaxPerScan)
		candidates = candidates[:l.opts.MaxPerScan]
	}

	var firstErr error
	for n, c := range candidates {
		if n > 0 && l.opts.RedeliverDelay > 0 {
			if err := sleep(ctx, l.opts.RedeliverDelay); err != nil {
				return ScanError, "interrupted"
			}
		}
		err := l.client.Redeliver(ctx, st.target, c.info.latestDeliveryID)
		if err == nil {
			l.mu.Lock()
			if p, ok := st.pending[c.guid]; ok {
				p.requestedAt = now
				st.pending[c.guid] = p
			}
			st.redelivered++
			l.mu.Unlock()
			if l.opts.Metrics != nil {
				l.opts.Metrics.RecoveryRedeliveriesTotal.WithLabelValues(st.target.Name()).Inc()
			}
			l.logger.Info("recovery requested redelivery", "target", st.target.String(), "guid", c.guid, "delivery_id", c.info.latestDeliveryID)
			continue
		}
		if ghclient.IsRateLimited(err) || ctx.Err() != nil {
			return resultOf(err), fmt.Sprintf("redeliver %d: %v", c.info.latestDeliveryID, err)
		}
		l.logger.Warn("recovery redelivery failed", "target", st.target.String(), "guid", c.guid, "delivery_id", c.info.latestDeliveryID, "error", err.Error())
		if firstErr == nil {
			firstErr = fmt.Errorf("redeliver %d: %w", c.info.latestDeliveryID, err)
		}
	}
	if firstErr != nil {
		return ScanError, firstErr.Error()
	}
	return ScanOK, ""
}

// finish records the outcome of a scan: the result counter, the degraded
// gauge and reason, and the pending gauge. During shutdown nothing is
// recorded, since a cancelled call is not a GitHub problem.
func (l *Loop) finish(ctx context.Context, st *targetState, result, reason string) {
	if ctx.Err() != nil {
		return
	}
	l.mu.Lock()
	wasDegraded := st.degraded
	st.lastResult = result
	st.degraded = reason
	pending := len(st.pending)
	l.mu.Unlock()

	name := st.target.Name()
	if l.opts.Metrics != nil {
		m := l.opts.Metrics
		m.RecoveryScansTotal.WithLabelValues(name, result).Inc()
		m.RecoveryPending.WithLabelValues(name).Set(float64(pending))
		degraded := 0.0
		if reason != "" {
			degraded = 1
		}
		m.RecoveryDegraded.WithLabelValues(name).Set(degraded)
	}
	switch {
	case reason != "" && wasDegraded == "":
		l.logger.Warn("recovery target degraded", "target", st.target.String(), "result", result, "reason", reason)
	case reason != "":
		l.logger.Debug("recovery target still degraded", "target", st.target.String(), "result", result, "reason", reason)
	case wasDegraded != "":
		l.logger.Info("recovery target recovered", "target", st.target.String(), "pending", pending)
	default:
		l.logger.Debug("recovery scan done", "target", st.target.String(), "pending", pending)
	}
}

// resultOf maps a GitHub error to a scan result label.
func resultOf(err error) string {
	if ghclient.IsRateLimited(err) {
		return ScanRateLimited
	}
	return ScanError
}

// sleep waits d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// TargetStatus is the /status view of one target. It carries no secret.
type TargetStatus struct {
	Target         string     `json:"target"`
	HookID         int64      `json:"hook_id,omitempty"`
	Degraded       bool       `json:"degraded"`
	DegradedReason string     `json:"degraded_reason,omitempty"`
	LastScan       *time.Time `json:"last_scan,omitempty"`
	LastResult     string     `json:"last_result,omitempty"`
	Pending        int        `json:"pending"`
	Redeliveries   int64      `json:"redeliveries"`
}

// Status is the /status view of the loop.
type Status struct {
	AuthType   string         `json:"auth_type"`
	Interval   string         `json:"interval"`
	Lookback   string         `json:"lookback"`
	Overlap    string         `json:"overlap"`
	Grace      string         `json:"grace"`
	MaxPerScan int            `json:"max_per_scan"`
	Targets    []TargetStatus `json:"targets"`
}

// Status snapshots every target for the admin /status endpoint.
func (l *Loop) Status() Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := Status{
		AuthType:   l.opts.AuthType,
		Interval:   l.opts.Interval.String(),
		Lookback:   l.opts.Lookback.String(),
		Overlap:    l.opts.Overlap.String(),
		Grace:      l.opts.Grace.String(),
		MaxPerScan: l.opts.MaxPerScan,
		Targets:    make([]TargetStatus, 0, len(l.targets)),
	}
	for _, st := range l.targets {
		ts := TargetStatus{
			Target:         st.target.Name(),
			HookID:         st.target.HookID,
			Degraded:       st.degraded != "",
			DegradedReason: st.degraded,
			LastResult:     st.lastResult,
			Pending:        len(st.pending),
			Redeliveries:   st.redelivered,
		}
		if st.scanned {
			t := st.lastScan
			ts.LastScan = &t
		}
		s.Targets = append(s.Targets, ts)
	}
	return s
}
