// Package bus is the "bus" driver of the forward class: it re-publishes every
// event on a bus opened through the bus driver registry, so any bus driver
// antwatcher can ingest from (NATS JetStream today, others as they are added)
// is also a forward target without forward-specific code.
//
// The driver registers itself as forward/bus on import. Its configuration
// block is Config: driver and topic of the target bus, max_hops, and the
// target driver's own block keyed by its name, exactly as under the top-level
// `bus:` mapping. Validation rejects a target with the same driver and topic
// as the ingress bus: forwarding into the topic the sinks consume from is a
// loop.
//
// Building the sink validates the block and performs no network round trip:
// the target bus is opened on the first Process call (see Publisher). A
// target that cannot be reached then makes Process fail with a retryable
// error, so the sink starts degraded and catches up once the target is back;
// the bus is published to only, no consumer is ever created on it. Publish
// returns under the target driver's own durability guarantee: the source
// message is acked only once a durable target has stored the copy.
package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/forward"
)

// Name is the driver name in configuration (sinks[].driver).
const Name = "bus"

// Keys of the block that are not driver blocks.
const (
	keyDriver  = "driver"
	keyTopic   = "topic"
	keyMaxHops = "max_hops"
)

// Config is the sinks[].config block of the driver as rendered by `-check`
// and /status: the typed fields plus every target driver block decoded
// through the bus driver's own Describer, so secrets in them mask
// themselves. It implements config.DriverValidator and
// config.IngressValidator.
type Config struct {
	// Driver is the bus driver of the target, one of the registered bus
	// drivers.
	Driver string `yaml:"driver" json:"driver"`
	// Topic is the target topic.
	Topic string `yaml:"topic" json:"topic"`
	// MaxHops is the forward chain limit: a copy that already hopped this
	// many times is refused (default 3).
	MaxHops int `yaml:"max_hops" json:"max_hops"`
	// Drivers holds every target driver block found in the config, keyed by
	// driver name, in the driver's described form. The selected driver's
	// block is always present (defaults when it was absent).
	Drivers map[string]any `yaml:",inline" json:"drivers,omitempty"`
}

// DefaultConfig is the block with every default applied.
func DefaultConfig() Config {
	return Config{MaxHops: forward.DefaultMaxHops}
}

// rawConfig is the block as it is in the file: typed fields plus the opaque
// driver blocks the bus registry decodes.
type rawConfig struct {
	Driver  string
	Topic   string
	MaxHops int
	Drivers map[string]yaml.Node
}

// parse decodes raw without a registry: typed fields are checked for type,
// every other key must hold a mapping (a driver block).
func parse(raw yaml.Node) (rawConfig, error) {
	c := rawConfig{MaxHops: forward.DefaultMaxHops, Drivers: map[string]yaml.Node{}}
	switch {
	case raw.Kind == 0, raw.Kind == yaml.ScalarNode && raw.Tag == "!!null":
		return c, nil
	case raw.Kind == yaml.DocumentNode && len(raw.Content) == 1:
		return parse(*raw.Content[0])
	case raw.Kind != yaml.MappingNode:
		return rawConfig{}, fmt.Errorf("line %d: config must be a mapping", raw.Line)
	}
	seen := map[string]bool{}
	for i := 0; i+1 < len(raw.Content); i += 2 {
		key, val := raw.Content[i], raw.Content[i+1]
		if seen[key.Value] {
			return rawConfig{}, fmt.Errorf("line %d: duplicate key %q", key.Line, key.Value)
		}
		seen[key.Value] = true
		var target any
		switch key.Value {
		case keyDriver:
			target = &c.Driver
		case keyTopic:
			target = &c.Topic
		case keyMaxHops:
			target = &c.MaxHops
		default:
			if val.Kind != yaml.MappingNode && (val.Kind != yaml.ScalarNode || val.Tag != "!!null") {
				return rawConfig{}, fmt.Errorf("line %d: unknown field %q (driver blocks must be mappings)", key.Line, key.Value)
			}
			c.Drivers[key.Value] = *val
			continue
		}
		if err := val.Decode(target); err != nil {
			return rawConfig{}, fmt.Errorf("%s: %w", key.Value, err)
		}
	}
	return c, nil
}

// describe renders rc through the bus describers of reg. Every driver block
// must belong to a registered bus driver, as must the selected driver; the
// selected driver's block is described even when absent so its defaults are
// shown and validated.
func describe(reg *bus.Registry, rc rawConfig) (Config, error) {
	describers := reg.Describers()
	out := Config{Driver: rc.Driver, Topic: rc.Topic, MaxHops: rc.MaxHops}
	names := make([]string, 0, len(rc.Drivers)+1)
	for name := range rc.Drivers {
		names = append(names, name)
	}
	if rc.Driver != "" {
		if _, present := rc.Drivers[rc.Driver]; !present {
			names = append(names, rc.Driver)
		}
	}
	sort.Strings(names)
	var errs []error
	for _, name := range names {
		d := describers[name]
		if d == nil {
			errs = append(errs, fmt.Errorf("unknown bus driver %q (registered: %s)", name, registered(reg)))
			continue
		}
		typed, err := d.Describe(rc.Drivers[name])
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		if out.Drivers == nil {
			out.Drivers = map[string]any{}
		}
		out.Drivers[name] = typed
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return out, nil
}

func registered(reg *bus.Registry) string {
	names := reg.Names()
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// Validate checks the block on its own: driver and topic are required and
// max_hops must be at least 1. The target driver block is checked through
// ValidateWith.
func (c Config) Validate() error {
	var errs []error
	if c.Driver == "" {
		errs = append(errs, errors.New("driver is required"))
	}
	if c.Topic == "" {
		errs = append(errs, errors.New("topic is required"))
	}
	if c.MaxHops < 1 {
		errs = append(errs, fmt.Errorf("max_hops must be >= 1 (got %d)", c.MaxHops))
	}
	return errors.Join(errs...)
}

// ValidateWith implements config.DriverValidator: Validate plus the target
// driver block's own cross-check against the router settings, as for the
// ingress bus.
func (c Config) ValidateWith(router config.Router) error {
	err := c.Validate()
	if v, ok := c.Drivers[c.Driver].(config.DriverValidator); ok {
		if derr := v.ValidateWith(router); derr != nil {
			err = errors.Join(err, fmt.Errorf("%s: %w", c.Driver, derr))
		}
	}
	return err
}

// ValidateIngress implements config.IngressValidator: the target must not be
// the ingress bus (same driver and topic), which would forward every event
// back to the sinks in a loop.
func (c Config) ValidateIngress(ingress config.Bus) error {
	if c.Driver != "" && c.Driver == ingress.Driver && c.Topic != "" && c.Topic == ingress.Topic {
		return fmt.Errorf("target (driver %q, topic %q) is the ingress bus: forwarding into the topic the sinks consume from is a loop; use another topic", c.Driver, c.Topic)
	}
	return nil
}

// Describe decodes the block through the bus drivers of bus.DefaultRegistry;
// see config.Describer and DescribeWith.
func Describe(raw yaml.Node) (any, error) {
	return DescribeWith(bus.DefaultRegistry, raw)
}

// DescribeWith decodes the block strictly on top of DefaultConfig, rendering
// every target driver block through the Describer registered for it in reg.
// An unknown key that is not a mapping, and a driver block or selected driver
// that reg does not know, are errors.
func DescribeWith(reg *bus.Registry, raw yaml.Node) (any, error) {
	rc, err := parse(raw)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return describe(reg, rc)
}

// Describer is Describe as a config.Describer.
var Describer config.Describer = config.DescriberFunc(Describe)

func init() {
	sink.RegisterDriver(sink.ClassForward, Name, Driver())
}

// Driver returns the driver descriptor bound to bus.DefaultRegistry, for
// registration in custom sink registries.
func Driver() sink.Driver {
	return sink.Driver{Factory: Factory, Describer: Describer}
}

// DriverWith returns the driver descriptor whose Describer and Factory use
// reg instead of bus.DefaultRegistry, for tests and embedders with their own
// bus registry. The Factory still prefers a non-nil sink.Deps.BusDrivers.
func DriverWith(reg *bus.Registry) sink.Driver {
	return sink.Driver{
		Factory: func(ctx context.Context, name string, raw yaml.Node, deps sink.Deps) (sink.Sink, error) {
			if deps.BusDrivers == nil {
				deps.BusDrivers = reg
			}
			return Factory(ctx, name, raw, deps)
		},
		Describer: config.DescriberFunc(func(raw yaml.Node) (any, error) { return DescribeWith(reg, raw) }),
	}
}

// Factory builds a forward sink whose target bus is opened lazily through
// deps.BusDrivers (bus.DefaultRegistry when nil). It validates the block,
// including the target driver's own block and, when deps.Ingress is set, the
// loop rule, and never touches the network.
func Factory(_ context.Context, name string, raw yaml.Node, deps sink.Deps) (sink.Sink, error) {
	reg := deps.BusDrivers
	if reg == nil {
		reg = bus.DefaultRegistry
	}
	rc, err := parse(raw)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	cfg, err := describe(reg, rc)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(cfg.Validate(), cfg.ValidateIngress(deps.Ingress)); err != nil {
		return nil, err
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	target := config.Bus{Driver: rc.Driver, Topic: rc.Topic, Drivers: rc.Drivers}
	return forward.New(name, NewPublisher(reg, target, logger), rc.Topic, cfg.MaxHops), nil
}

// Publisher is the forward.Publisher over a bus opened through a registry on
// first use. The open is retried on every Publish until it succeeds; a
// failed open is a retryable error (the target is unreachable now), so the
// bus redelivers the source message later. Once open the bus stays open
// until Close.
type Publisher struct {
	registry *bus.Registry
	target   config.Bus
	logger   *slog.Logger

	mu     sync.Mutex
	closed bool

	bus sink.SingleFlight[bus.Bus]
}

// NewPublisher returns a Publisher that opens target through registry on the
// first Publish. logger may be nil.
func NewPublisher(registry *bus.Registry, target config.Bus, logger *slog.Logger) *Publisher {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Publisher{registry: registry, target: target, logger: logger}
}

// Publish implements forward.Publisher: it opens the target bus when needed
// and publishes msg on it. Errors from the open are retryable; publish errors
// are returned as the target driver classified them.
func (p *Publisher) Publish(ctx context.Context, msg *message.Message) error {
	b, err := p.open(ctx)
	if err != nil {
		return err
	}
	if err := b.Publish(ctx, msg); err != nil {
		return fmt.Errorf("publish %s: %w", msg.UUID, err)
	}
	return nil
}

// Opened reports whether the target bus is currently open.
func (p *Publisher) Opened() bool {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return false
	}
	_, ok := p.bus.Ready()
	return ok
}

// open returns the target bus, opening it on the first call. The open runs
// outside p.mu: concurrent Process calls share one connection through the
// SingleFlight instead of blocking one another past their own ctx while it
// is established. The target bus registers no metrics: its collectors would
// collide with the ingress bus's on the shared registry, and the forward
// sink's own metrics already cover its health.
func (p *Publisher) open(ctx context.Context) (bus.Bus, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, bus.ErrClosed
	}
	return p.bus.Do(ctx, func(ctx context.Context) (bus.Bus, error) {
		b, err := p.registry.Open(ctx, p.target, p.logger, nil)
		if err != nil {
			return nil, fmt.Errorf("open target bus: %w", err)
		}
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed {
			_ = b.Close()
			return nil, bus.ErrClosed
		}
		caps := b.Capabilities()
		if !caps.DurablePublish {
			p.logger.Warn("forward target is not durable: a copy acked here can be lost by the target", "driver", p.target.Driver, "topic", p.target.Topic)
		}
		p.logger.Info("forward target opened", "driver", p.target.Driver, "topic", p.target.Topic, "capabilities", caps.String())
		return b, nil
	})
}

// Close implements forward.Publisher: it closes the target bus when it was
// opened. Idempotent; a later Publish fails with bus.ErrClosed. Peek waits
// out an open still in flight (started just before the router stopped
// delivering) instead of racing it: the open's own closure already
// self-closes the bus if it finishes after closed is set, so at most one of
// the two ever closes it.
func (p *Publisher) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	b, ok := p.bus.Peek(context.Background())
	if !ok {
		return nil
	}
	if err := b.Close(); err != nil {
		return fmt.Errorf("close target bus: %w", err)
	}
	return nil
}
