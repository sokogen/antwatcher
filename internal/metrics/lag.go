package metrics

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/sokogen/antwatcher/internal/bus"
)

// LagPoller samples the backlog of every registered consumer from a bus that
// implements bus.LagReporter into antwatcher_bus_consumer_lag{consumer}.
//
// On a bus that does not report lag (capability ReportsLag false, or the
// driver lacks the interface) Run logs once and returns immediately: the
// gauge is simply absent. A failed sample keeps the previous value of the
// gauge, since a stale backlog number is more useful to an alert than a gap;
// the failure is logged at warn once per streak and at debug afterwards.
type LagPoller struct {
	reporter bus.LagReporter // nil when the bus does not report lag
	metrics  *Metrics
	interval time.Duration
	timeout  time.Duration
	logger   *slog.Logger

	mu        sync.Mutex
	consumers map[string]bool // consumer name -> last sample failed
}

// NewLagPoller creates a poller over b sampling every interval. interval must
// be positive (validated as admin.lag_interval). logger may be nil.
func NewLagPoller(b bus.Bus, m *Metrics, interval time.Duration, logger *slog.Logger) *LagPoller {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	p := &LagPoller{
		metrics:   m,
		interval:  interval,
		timeout:   interval,
		logger:    logger,
		consumers: map[string]bool{},
	}
	if b != nil && b.Capabilities().ReportsLag {
		if r, ok := b.(bus.LagReporter); ok {
			p.reporter = r
		}
	}
	return p
}

// Supported reports whether the bus provides lag; when false, Run is a no-op.
func (p *LagPoller) Supported() bool { return p.reporter != nil }

// Register adds a consumer to the set sampled on every tick. Registering the
// same name again is a no-op.
func (p *LagPoller) Register(consumer string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.consumers[consumer]; !ok {
		p.consumers[consumer] = false
	}
}

// Unregister stops sampling the consumer and removes its gauge series.
func (p *LagPoller) Unregister(consumer string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.consumers, consumer)
	p.metrics.BusConsumerLag.DeleteLabelValues(consumer)
}

// Run samples once immediately, then every interval, until ctx ends. It
// always returns nil so an errgroup never stops on it.
func (p *LagPoller) Run(ctx context.Context) error {
	if p.reporter == nil {
		p.logger.Info("bus does not report consumer lag; antwatcher_bus_consumer_lag is not exported")
		return nil
	}
	p.Poll(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.Poll(ctx)
		}
	}
}

// Poll samples every registered consumer once. It is what Run calls on each
// tick and is exported so tests and status handlers can force a sample.
func (p *LagPoller) Poll(ctx context.Context) {
	if p.reporter == nil {
		return
	}
	for _, consumer := range p.registered() {
		if ctx.Err() != nil {
			return
		}
		p.sample(ctx, consumer)
	}
}

func (p *LagPoller) registered() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	names := make([]string, 0, len(p.consumers))
	for name := range p.consumers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (p *LagPoller) sample(ctx context.Context, consumer string) {
	sampleCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	lag, err := p.reporter.Lag(sampleCtx, consumer)

	p.mu.Lock()
	defer p.mu.Unlock()
	wasFailing, tracked := p.consumers[consumer]
	if !tracked { // unregistered while sampling
		return
	}
	if err != nil {
		if ctx.Err() != nil {
			return // shutting down, not a broker problem
		}
		p.consumers[consumer] = true
		level := slog.LevelWarn
		if wasFailing {
			level = slog.LevelDebug
		}
		p.logger.Log(ctx, level, "consumer lag sample failed; keeping the last value", "consumer", consumer, "error", err.Error())
		return
	}
	if wasFailing {
		p.logger.Info("consumer lag sampling recovered", "consumer", consumer, "lag", lag)
	}
	p.consumers[consumer] = false
	p.metrics.BusConsumerLag.WithLabelValues(consumer).Set(float64(lag))
}
