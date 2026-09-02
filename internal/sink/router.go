package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	wmmetrics "github.com/ThreeDotsLabs/watermill/components/metrics"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/metrics"
)

// Router delivers every bus message to every sink through a Watermill router
// with one handler per sink, each bound to the sink's own consumer.
//
// Delivery semantics, per message and per sink:
//
//   - Process returns nil or ErrSkipped: the message is acked.
//   - Process returns a retryable error (anything not marked Permanent): the
//     handler returns the error, Watermill Nacks, the broker redelivers with
//     a growing delay while the message is inside retention.
//   - Process returns a permanent error, or the message does not decode into
//     an envelope: the message is recorded as stalled for this sink and
//     Nacked at once. The broker paces the re-attempts; the entry is cleared
//     when the same message succeeds (after a fix of config, code, or
//     destination, without restart). Nothing is discarded; there is no DLQ.
//
// Handlers never sleep and there is no retry or poison middleware: short
// transient retries belong to the destination client, long-term retry belongs
// to the broker. Each Process call runs under router.process_timeout, which
// the broker driver validates against its ack wait, so a slow destination
// times out and Nacks instead of racing a concurrent redelivery.
type Router struct {
	router   *message.Router
	handlers []*handler
	logger   *slog.Logger

	closeOnce sync.Once
	closeErr  error
}

// SinkStatus is the /status view of one sink.
type SinkStatus struct {
	Name               string            `json:"name"`
	Class              Class             `json:"class"`
	Driver             string            `json:"driver"`
	Consumer           string            `json:"consumer"`
	RequestedStartFrom bus.StartPosition `json:"requested_start_from"`
	EffectiveStartFrom bus.StartPosition `json:"effective_start_from"`
	Warnings           []bus.Warning     `json:"warnings,omitempty"`
	LastSuccess        *time.Time        `json:"last_success,omitempty"`
	Stalled            []StalledEntry    `json:"stalled,omitempty"`
}

// BuildRouter creates the router and one consumer per instance on b. For each
// sink the requested start position is resolved with bus.ResolveConsumer
// under mode: warnings are logged and kept for /status, errors abort startup.
// The consumers are created here (so a failure is reported before the
// receiver starts); delivery begins with Run. m must not be nil; logger may be.
func BuildRouter(ctx context.Context, b bus.Bus, cfg config.Router, mode bus.Mode, instances []Instance, m *metrics.Metrics, logger *slog.Logger) (*Router, error) {
	if b == nil {
		return nil, errors.New("sink: router needs a bus")
	}
	if m == nil {
		return nil, errors.New("sink: router needs metrics")
	}
	if cfg.ProcessTimeout <= 0 {
		return nil, fmt.Errorf("sink: router.process_timeout must be > 0 (got %s)", cfg.ProcessTimeout)
	}
	if cfg.CloseTimeout <= 0 {
		return nil, fmt.Errorf("sink: router.close_timeout must be > 0 (got %s)", cfg.CloseTimeout)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	wm, err := message.NewRouter(message.RouterConfig{CloseTimeout: cfg.CloseTimeout}, newWatermillLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("sink: create router: %w", err)
	}
	wm.AddMiddleware(middleware.Recoverer)
	wmmetrics.NewPrometheusMetricsBuilder(m.Registry, metrics.Namespace, "watermill").AddPrometheusRouterMetrics(wm)

	r := &Router{router: wm, logger: logger}
	caps := b.Capabilities()
	for _, inst := range instances {
		h, err := r.bind(ctx, b, caps, cfg, mode, inst, m)
		if err != nil {
			_ = r.closeSubscribers()
			return nil, err
		}
		r.handlers = append(r.handlers, h)
	}
	return r, nil
}

// bind resolves the consumer policy for one sink, creates its consumer, and
// registers its handler.
func (r *Router) bind(ctx context.Context, b bus.Bus, caps bus.Capabilities, cfg config.Router, mode bus.Mode, inst Instance, m *metrics.Metrics) (*handler, error) {
	name := inst.Name()
	logger := r.logger.With("sink", name, "class", inst.Class().String())
	effective, warnings, err := bus.ResolveConsumer(caps, inst.StartFrom, mode)
	if err != nil {
		return nil, fmt.Errorf("sink %q: %w", name, err)
	}
	for _, w := range warnings {
		logger.Log(ctx, warningLevel(w), "consumer policy: "+w.Message, "capability", w.Capability, "requested_start_from", inst.StartFrom.String(), "effective_start_from", effective.String())
	}

	consumer := ConsumerName(name)
	sub, err := b.Subscribe(ctx, consumer, bus.SubscribeOptions{StartFrom: effective})
	if err != nil {
		return nil, fmt.Errorf("sink %q: create consumer %q: %w", name, consumer, err)
	}

	h := &handler{
		inst:           inst,
		consumer:       consumer,
		effective:      effective,
		warnings:       warnings,
		subscriber:     sub,
		processTimeout: cfg.ProcessTimeout,
		metrics:        m,
		logger:         logger,
		stalled:        newStalledSet(name, m),
		now:            time.Now,
	}
	h.initMetrics()
	r.router.AddConsumerHandler(consumer, b.Topic(), sub, h.handle)
	logger.Info("sink bound", "consumer", consumer, "driver", inst.Driver,
		"requested_start_from", inst.StartFrom.String(), "effective_start_from", effective.String())
	return h, nil
}

func warningLevel(w bus.Warning) slog.Level {
	if w.Severity == bus.SeverityInfo {
		return slog.LevelInfo
	}
	return slog.LevelWarn
}

// Run starts delivering and blocks until ctx ends or Close is called. It
// returns nil on a clean stop.
func (r *Router) Run(ctx context.Context) error {
	if err := r.router.Run(ctx); err != nil {
		return fmt.Errorf("sink: router: %w", err)
	}
	return nil
}

// Running is closed once every handler is subscribed and delivering.
func (r *Router) Running() <-chan struct{} {
	return r.router.Running()
}

// Close stops delivery: in-flight Process calls may finish within
// router.close_timeout, then the consumers are closed. Messages not acked
// by then are redelivered later by the broker. Sinks are not closed here;
// the caller closes them after Close returns. Idempotent.
func (r *Router) Close() error {
	r.closeOnce.Do(func() {
		var errs []error
		if r.router.IsRunning() {
			if err := r.router.Close(); err != nil {
				errs = append(errs, fmt.Errorf("sink: close router: %w", err))
			}
		}
		if err := r.closeSubscribers(); err != nil {
			errs = append(errs, err)
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

// closeSubscribers closes every consumer; safe when the router already did.
func (r *Router) closeSubscribers() error {
	var errs []error
	for _, h := range r.handlers {
		if err := h.subscriber.Close(); err != nil {
			errs = append(errs, fmt.Errorf("sink %q: close consumer: %w", h.inst.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Consumers lists the consumer names bound by this router, in sink order,
// for the lag poller.
func (r *Router) Consumers() []string {
	out := make([]string, 0, len(r.handlers))
	for _, h := range r.handlers {
		out = append(out, h.consumer)
	}
	return out
}

// Status returns the /status view of every sink, in configuration order.
func (r *Router) Status() []SinkStatus {
	out := make([]SinkStatus, 0, len(r.handlers))
	for _, h := range r.handlers {
		out = append(out, h.status())
	}
	return out
}

// handler is the Watermill handler of one sink.
type handler struct {
	inst           Instance
	consumer       string
	effective      bus.StartPosition
	warnings       []bus.Warning
	subscriber     message.Subscriber
	processTimeout time.Duration
	metrics        *metrics.Metrics
	logger         *slog.Logger
	stalled        *stalledSet
	now            func() time.Time

	mu          sync.Mutex
	lastSuccess time.Time
}

// initMetrics creates every series of the sink so dashboards and alerts see
// zeros instead of gaps before the first event.
func (h *handler) initMetrics() {
	name, class := h.inst.Name(), h.inst.Class().String()
	for _, result := range []string{metrics.SinkOK, metrics.SinkError, metrics.SinkPermanent, metrics.SinkSkipped} {
		h.metrics.SinkEventsTotal.WithLabelValues(name, class, result)
	}
	h.metrics.SinkProcessSeconds.WithLabelValues(name, class)
}

// handle processes one message. Returning an error makes Watermill Nack it;
// returning nil acks it.
func (h *handler) handle(msg *message.Message) error {
	env, err := event.FromMessage(msg)
	if err != nil {
		// An undecodable message cannot become decodable by retrying, and it
		// must not be acked either: it stays on the bus, stalled for this
		// sink, until the schema or the code catches up.
		return h.stall(msg.UUID, Permanent(fmt.Errorf("decode message: %w", err)))
	}

	// The process deadline is the only bound on a Process call. The message
	// context is detached from cancellation on purpose: Watermill cancels it
	// when the router closes, and an in-flight call must be allowed to
	// finish within router.close_timeout (the broker validates ack wait
	// against process_timeout, so the deadline still holds) instead of being
	// aborted and redelivered after every restart.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(msg.Context()), h.processTimeout)
	defer cancel()
	start := h.now()
	err = h.inst.Process(ctx, env)
	elapsed := h.now().Sub(start)
	h.metrics.SinkProcessSeconds.WithLabelValues(h.inst.Name(), h.inst.Class().String()).Observe(elapsed.Seconds())

	fields := []any{"uuid", msg.UUID, "delivery", env.DeliveryGUID, "event", env.Event, "action", env.Action, "elapsed_ms", elapsed.Milliseconds()}
	switch {
	case err == nil:
		h.succeeded(msg.UUID, metrics.SinkOK)
		h.logger.Debug("event processed", fields...)
		return nil
	case errors.Is(err, ErrSkipped):
		h.succeeded(msg.UUID, metrics.SinkSkipped)
		h.logger.Debug("event skipped", fields...)
		return nil
	case IsPermanent(err):
		return h.stall(msg.UUID, err)
	default:
		h.count(metrics.SinkError)
		if ctx.Err() != nil {
			fields = append(fields, "process_timeout", h.processTimeout.String())
		}
		h.logger.Warn("event failed; the broker redelivers", append(fields, "err", err)...)
		return err
	}
}

// succeeded records an ok or skipped result: a skipped event is a successful
// decision by the projection, so it counts as success for the last-success
// age (a trace sink seeing only queued jobs is healthy) and clears a stalled
// entry of the same message.
func (h *handler) succeeded(uuid, result string) {
	h.count(result)
	now := h.now()
	h.mu.Lock()
	h.lastSuccess = now
	h.mu.Unlock()
	h.metrics.SinkLastSuccessTimestamp.WithLabelValues(h.inst.Name()).Set(float64(now.UnixNano()) / 1e9)
	if h.stalled.clear(uuid) {
		h.logger.Info("stalled message processed", "uuid", uuid, "result", result, "stalled_remaining", h.stalled.len())
	}
}

// stall records a permanent failure and returns the error so the message is
// Nacked at once. No delay is introduced here: Watermill stops waiting for
// the Ack or Nack at the broker's ack wait and sends no in-progress signal,
// so any handler-side delay would only race the redelivery. The broker's nak
// delay paces the re-attempts.
func (h *handler) stall(uuid string, err error) error {
	h.count(metrics.SinkPermanent)
	attempts, logNow := h.stalled.record(uuid, err, h.now())
	fields := []any{"uuid", uuid, "attempts", attempts, "stalled_total", h.stalled.len(), "err", err}
	if logNow {
		h.logger.Error("message stalled on a permanent error; fix the cause and it succeeds on its next redelivery", fields...)
	} else {
		h.logger.Debug("stalled message failed again", fields...)
	}
	return err
}

func (h *handler) count(result string) {
	h.metrics.SinkEventsTotal.WithLabelValues(h.inst.Name(), h.inst.Class().String(), result).Inc()
}

func (h *handler) status() SinkStatus {
	h.mu.Lock()
	last := h.lastSuccess
	h.mu.Unlock()
	st := SinkStatus{
		Name:               h.inst.Name(),
		Class:              h.inst.Class(),
		Driver:             h.inst.Driver,
		Consumer:           h.consumer,
		RequestedStartFrom: h.inst.StartFrom,
		EffectiveStartFrom: h.effective,
		Warnings:           h.warnings,
		Stalled:            h.stalled.list(),
	}
	if !last.IsZero() {
		st.LastSuccess = &last
	}
	return st
}
