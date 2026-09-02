package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/admin"
	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/ghclient"
	"github.com/sokogen/antwatcher/internal/metrics"
	"github.com/sokogen/antwatcher/internal/receiver"
	"github.com/sokogen/antwatcher/internal/recovery"
	"github.com/sokogen/antwatcher/internal/sink"
)

// serveOptions is what the wiring needs besides the configuration. Production
// uses the default registries and the build information; tests inject their
// own registries and observe the lifecycle through stage.
type serveOptions struct {
	busDrivers  *bus.Registry
	sinkDrivers *sink.Registry
	version     string
	commit      string
	// stage is called with the name of every lifecycle step as it starts
	// (see the stage* constants). nil disables it.
	stage func(string)
}

func defaultServeOptions() serveOptions {
	return serveOptions{
		busDrivers:  bus.DefaultRegistry,
		sinkDrivers: sink.DefaultRegistry,
		version:     version,
		commit:      commit,
	}
}

// describers exposes every registered driver block to config validation and
// redaction.
func (o serveOptions) describers() config.Describers {
	return config.Describers{Bus: o.busDrivers.Describers(), Sinks: o.sinkDrivers.Describers()}
}

// Lifecycle stages reported through serveOptions.stage, in the order they
// happen. Startup goes top to bottom; shutdown is the exact reverse of the
// dependencies: the receiver stops accepting first so nothing new enters,
// the router finishes in-flight handlers, the sinks close, the bus (and the
// embedded server) closes, and the admin listener is the last to go so
// /readyz reports the shutdown while it lasts.
const (
	stageBusOpen      = "bus.open"
	stageAdminStart   = "admin.start"
	stageRouterStart  = "router.start"
	stageReceiverOpen = "receiver.start"
	stageReceiverStop = "receiver.stop"
	stageRouterClose  = "router.close"
	stageSinksClose   = "sinks.close"
	stageBusClose     = "bus.close"
	stageAdminStop    = "admin.stop"
)

// service is the wired process: everything newService built, run and torn
// down in order by run.
type service struct {
	cfg      config.Config
	redacted config.RedactedConfig
	opts     serveOptions
	logger   *slog.Logger

	metrics     *metrics.Metrics
	bus         bus.Bus
	mode        bus.Mode
	busWarnings []bus.Warning
	admin       *admin.Server
	sinks       []sink.Instance
	router      *sink.Router
	lag         *metrics.LagPoller
	recovery    *recovery.Loop // nil when disabled
	receiver    *receiver.Server

	startedAt time.Time
	closing   atomic.Bool
}

// newService builds the process from a validated configuration in dependency
// order and binds both listeners, so every static problem is reported before
// run: an unknown driver, a policy error in fail mode, a sink constructor
// rejecting its block, a recovery block that cannot be scanned, or a port in
// use. The ingress bus must open here: it is the durability boundary behind
// every 2xx. Nothing else may fail startup. An unreachable destination shows
// up as a degraded sink and a GitHub API problem as a degraded recovery
// target while the webhook path serves.
//
// On error everything built so far is closed.
func newService(ctx context.Context, cfg config.Config, logger *slog.Logger, opts serveOptions) (svc *service, err error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	redacted, err := config.Redacted(cfg, opts.describers())
	if err != nil {
		return nil, err
	}
	s := &service{cfg: cfg, redacted: redacted, opts: opts, logger: logger, startedAt: time.Now()}
	defer func() {
		if err != nil {
			s.closeBuilt()
		}
	}()

	s.metrics = metrics.New(opts.version, opts.commit)

	mode, err := bus.ParseMode(cfg.Bus.OnMissingCapability)
	if err != nil {
		return nil, fmt.Errorf("bus: %w", err)
	}
	s.mode = mode

	s.stage(stageBusOpen)
	b, err := opts.busDrivers.Open(ctx, cfg.Bus, logger, s.metrics.Registry)
	if err != nil {
		return nil, err
	}
	s.bus = b
	s.metrics.BusConnected.Set(1)
	caps := b.Capabilities()
	s.busWarnings, err = bus.ResolveIngress(caps, mode)
	if err != nil {
		return nil, fmt.Errorf("bus %q: %w", cfg.Bus.Driver, err)
	}
	logger.Info("bus open", "driver", cfg.Bus.Driver, "topic", b.Topic(), "capabilities", caps.String())
	for _, w := range s.busWarnings {
		logger.Warn("ingress policy: "+w.Message, "capability", w.Capability)
	}

	s.admin = admin.New(admin.Options{
		Listen:    cfg.Admin.Listen,
		Metrics:   s.metrics.Handler(),
		Readiness: s.readiness,
		Status:    s.status,
		Logger:    logger.With("component", "admin"),
	})

	s.sinks, err = opts.sinkDrivers.Build(ctx, cfg.Sinks, sink.Deps{
		Logger:     logger,
		Metrics:    s.metrics,
		BusDrivers: opts.busDrivers,
		Ingress:    cfg.Bus,
		Version:    opts.version,
	})
	if err != nil {
		return nil, err
	}
	s.router, err = sink.BuildRouter(ctx, b, cfg.Router, mode, s.sinks, s.metrics, logger.With("component", "router"))
	if err != nil {
		return nil, err
	}

	s.lag = metrics.NewLagPoller(b, s.metrics, cfg.Admin.LagInterval, logger.With("component", "lag"))
	for _, consumer := range s.router.Consumers() {
		s.lag.Register(consumer)
	}

	if cfg.Recovery.Enabled {
		s.recovery, err = buildRecovery(cfg, s.metrics, logger.With("component", "recovery"), opts.version)
		if err != nil {
			return nil, err
		}
	}

	handler := receiver.New(cfg.Server, b, s.metrics, logger.With("component", "receiver"))
	s.receiver = receiver.NewServer(cfg.Server, handler, logger.With("component", "receiver"))

	if err := s.admin.Listen(); err != nil {
		return nil, err
	}
	if err := s.receiver.Listen(); err != nil {
		return nil, err
	}
	return s, nil
}

// buildRecovery wires the GitHub client and the loop. Only static problems
// are errors: the client never dials here and the loop discovers hooks and
// scans in Run, where every failure degrades the target instead.
func buildRecovery(cfg config.Config, m *metrics.Metrics, logger *slog.Logger, version string) (*recovery.Loop, error) {
	client, err := ghclient.New(ghclient.AuthFromConfig(cfg.Recovery.Auth), ghclient.Options{
		APIBaseURL: cfg.Recovery.APIBaseURL,
		UserAgent:  "antwatcher/" + version,
	})
	if err != nil {
		return nil, fmt.Errorf("recovery: %w", err)
	}
	opts, err := recovery.OptionsFromConfig(cfg.Recovery, cfg.Server.PublicURL)
	if err != nil {
		return nil, err
	}
	opts.Client = client
	opts.Metrics = m
	opts.Logger = logger
	return recovery.New(opts)
}

// closeBuilt releases whatever newService managed to build, in shutdown order.
func (s *service) closeBuilt() {
	if s.router != nil {
		_ = s.router.Close()
	}
	if len(s.sinks) > 0 {
		_ = sink.CloseAll(s.sinks)
	}
	if s.bus != nil {
		_ = s.bus.Close()
	}
}

func (s *service) stage(name string) {
	if s.opts.stage != nil {
		s.opts.stage(name)
	}
}

// task is one component running in its own goroutine under its own context,
// so run can stop components one at a time in a chosen order.
type task struct {
	name   string
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// startTask runs fn until stop is called. The task's context is detached
// from the root context on purpose: a signal must not cancel every component
// at once, it triggers the ordered shutdown in run, which stops the tasks
// one by one. failures receives a non-nil error once, when fn fails; a nil
// return (a component that finished on its own, like the lag poller on a
// bus without lag) is not a failure.
func startTask(ctx context.Context, name string, fn func(context.Context) error, failures chan<- error) *task {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t := &task{name: name, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(t.done)
		if err := fn(ctx); err != nil {
			t.err = fmt.Errorf("%s: %w", name, err)
			failures <- t.err
		}
	}()
	return t
}

// stop cancels the task and waits for it. It returns the task's error, if
// any, after the wait.
func (t *task) stop() error {
	if t == nil {
		return nil
	}
	t.cancel()
	<-t.done
	return t.err
}

// run serves until ctx ends or a component fails, then shuts everything
// down in order. It returns nil after a clean, signal-driven stop, and the
// failure that triggered the shutdown otherwise (joined with any shutdown
// errors).
func (s *service) run(ctx context.Context) error {
	failures := make(chan error, 8)

	s.stage(stageAdminStart)
	adminTask := startTask(ctx, "admin", s.admin.Run, failures)

	s.stage(stageRouterStart)
	routerTask := startTask(ctx, "router", s.router.Run, failures)
	lagTask := startTask(ctx, "lag", s.lag.Run, failures)
	healthTask := startTask(ctx, "bus-health", s.sampleBusConnected, failures)
	var recoveryTask *task
	if s.recovery != nil {
		recoveryTask = startTask(ctx, "recovery", s.recovery.Run, failures)
	}

	// The receiver accepts webhooks only once every sink consumer is
	// delivering, so a delivery accepted on a non-durable bus is not lost
	// between the two.
	var receiverTask *task
	var cause error
	select {
	case <-s.router.Running():
		s.stage(stageReceiverOpen)
		receiverTask = startTask(ctx, "receiver", s.receiver.Run, failures)
		s.logger.Info("antwatcher started", "version", s.opts.version, "webhook", s.receiver.Addr().String(), "admin", s.admin.Addr().String())
		select {
		case <-ctx.Done():
		case cause = <-failures:
		}
	case <-ctx.Done():
	case cause = <-failures:
	}

	if cause != nil {
		s.logger.Error("component failed; shutting down", "error", cause.Error())
	} else {
		s.logger.Info("shutdown requested")
	}
	s.closing.Store(true)

	var errs []error
	s.stage(stageReceiverStop)
	errs = append(errs, receiverTask.stop())
	s.stage(stageRouterClose)
	errs = append(errs, s.router.Close(), routerTask.stop(), recoveryTask.stop(), lagTask.stop(), healthTask.stop())
	s.stage(stageSinksClose)
	errs = append(errs, sink.CloseAll(s.sinks))
	s.stage(stageBusClose)
	errs = append(errs, s.bus.Close())
	s.metrics.BusConnected.Set(0)
	s.stage(stageAdminStop)
	errs = append(errs, adminTask.stop())

	// The cause is already in errs through its task; report it first.
	shutdownErr := errors.Join(dropCause(errs, cause)...)
	if cause == nil && shutdownErr == nil {
		s.logger.Info("antwatcher stopped")
		return nil
	}
	if shutdownErr != nil {
		s.logger.Error("shutdown finished with errors", "error", shutdownErr.Error())
	}
	return errors.Join(cause, shutdownErr)
}

func dropCause(errs []error, cause error) []error {
	out := errs[:0]
	for _, err := range errs {
		if err != nil && err != cause { // identity: the cause's own task error is reported once
			out = append(out, err)
		}
	}
	return out
}

// connectionReporter is implemented by bus drivers that know whether their
// broker connection is live (the JetStream driver). Others count as
// connected while open.
type connectionReporter interface {
	Connected() bool
}

// sampleBusConnected keeps antwatcher_bus_connected in step with the
// driver's connection state at the lag interval.
func (s *service) sampleBusConnected(ctx context.Context) error {
	reporter, ok := s.bus.(connectionReporter)
	if !ok {
		return nil
	}
	ticker := time.NewTicker(s.cfg.Admin.LagInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			v := 0.0
			if reporter.Connected() {
				v = 1
			}
			s.metrics.BusConnected.Set(v)
		}
	}
}

// readiness backs /readyz: the service accepts webhooks while the bus is
// open and connected and no shutdown has started. Sinks never affect it.
func (s *service) readiness() error {
	if s.closing.Load() {
		return errors.New("shutting down")
	}
	if r, ok := s.bus.(connectionReporter); ok && !r.Connected() {
		return errors.New("bus disconnected")
	}
	return nil
}

// statusView is the /status document. Everything in it comes from typed
// values or config.Redacted, so no secret can appear.
type statusView struct {
	Version        string            `json:"version"`
	Commit         string            `json:"commit"`
	StartedAt      time.Time         `json:"started_at"`
	Uptime         string            `json:"uptime"`
	Ready          bool              `json:"ready"`
	NotReadyReason string            `json:"not_ready_reason,omitempty"`
	Server         serverStatus      `json:"server"`
	Bus            busStatus         `json:"bus"`
	Router         any               `json:"router"`
	Sinks          []sink.SinkStatus `json:"sinks"`
	Recovery       recoveryStatus    `json:"recovery"`
}

type serverStatus struct {
	Listen      string `json:"listen"`
	WebhookPath string `json:"webhook_path"`
	PublicURL   string `json:"public_url,omitempty"`
}

type busStatus struct {
	Driver              string           `json:"driver"`
	Topic               string           `json:"topic"`
	OnMissingCapability string           `json:"on_missing_capability"`
	Capabilities        bus.Capabilities `json:"capabilities"`
	Connected           *bool            `json:"connected,omitempty"`
	// Config is the driver block as rendered by config.Redacted: the
	// retention window and the like, with credentials masked.
	Config   any           `json:"config,omitempty"`
	Warnings []bus.Warning `json:"warnings,omitempty"`
}

type recoveryStatus struct {
	Enabled bool `json:"enabled"`
	*recovery.Status
}

// status renders the /status document.
func (s *service) status() any {
	now := time.Now()
	v := statusView{
		Version:   s.opts.version,
		Commit:    s.opts.commit,
		StartedAt: s.startedAt.UTC(),
		Uptime:    now.Sub(s.startedAt).Round(time.Second).String(),
		Ready:     true,
		Server: serverStatus{
			Listen:      s.cfg.Server.Listen,
			WebhookPath: s.cfg.Server.WebhookPath,
			PublicURL:   s.cfg.Server.PublicURL,
		},
		Bus: busStatus{
			Driver:              s.cfg.Bus.Driver,
			Topic:               s.bus.Topic(),
			OnMissingCapability: s.mode.String(),
			Capabilities:        s.bus.Capabilities(),
			Config:              readable(s.redacted.Bus.Drivers[s.cfg.Bus.Driver]),
			Warnings:            s.busWarnings,
		},
		Router:   readable(s.cfg.Router),
		Sinks:    s.router.Status(),
		Recovery: recoveryStatus{Enabled: s.recovery != nil},
	}
	if err := s.readiness(); err != nil {
		v.Ready = false
		v.NotReadyReason = err.Error()
	}
	if r, ok := s.bus.(connectionReporter); ok {
		connected := r.Connected()
		v.Bus.Connected = &connected
	}
	if s.recovery != nil {
		st := s.recovery.Status()
		v.Recovery.Status = &st
	}
	return v
}

// readable renders a configuration value the way `-check` prints it, so
// durations read "168h0m0s" instead of nanoseconds and Secret fields mask
// themselves, then hands it to the JSON encoder as plain maps and scalars.
// A value that cannot be rendered is reported in place instead of hiding
// the rest of the document.
func readable(v any) any {
	if v == nil {
		return nil
	}
	text, err := yaml.Marshal(v)
	if err != nil {
		return map[string]string{"error": "render: " + err.Error()}
	}
	var out any
	if err := yaml.Unmarshal(text, &out); err != nil {
		return map[string]string{"error": "render: " + err.Error()}
	}
	return out
}
