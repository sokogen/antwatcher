package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/metrics"
)

// Deps is what a driver factory receives besides its own configuration.
type Deps struct {
	// Logger is never nil; Build scopes it with the sink name and class.
	Logger *slog.Logger
	// Metrics may be nil when the caller collects no metrics.
	Metrics *metrics.Metrics
	// BusDrivers is the bus registry a forward driver opens its publisher
	// through. May be nil for classes that do not need a bus.
	BusDrivers *bus.Registry
	// Ingress is the ingress bus configuration (driver and topic) so a
	// forward driver can refuse to publish back into it. The zero value
	// disables that check.
	Ingress config.Bus
	// Version is the antwatcher version drivers report to their destinations
	// (for example service.version on OTLP exports). Empty means "dev".
	Version string
}

// DriverFactory builds a sink named name from its opaque configuration block
// (sinks[].config, zero Kind when absent). Drivers decode raw with
// config.DecodeStrict so unknown keys fail. A factory must not perform a
// network round trip: an unreachable destination is reported by Process as a
// retryable error so the sink starts degraded instead of failing startup.
type DriverFactory func(ctx context.Context, name string, raw yaml.Node, deps Deps) (Sink, error)

// Driver is what a sink driver registers for one class: how to build the sink
// and how to describe its configuration for validation and redaction.
type Driver struct {
	Factory   DriverFactory
	Describer config.Describer
}

// Registry maps (class, driver name) to drivers. The package-level functions
// use DefaultRegistry; tests build their own.
type Registry struct {
	mu      sync.RWMutex
	drivers map[Class]map[string]Driver
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{drivers: map[Class]map[string]Driver{}}
}

// DefaultRegistry is the registry drivers register with from their init functions.
var DefaultRegistry = NewRegistry()

// RegisterDriver adds a driver for class. It panics on an invalid class, an
// empty name, a nil Factory, a nil Describer, or a pair already registered:
// all programming errors caught at init time.
func (r *Registry) RegisterDriver(class Class, name string, d Driver) {
	if !class.valid() {
		panic(fmt.Sprintf("sink: RegisterDriver(%q) with invalid class %s", name, class))
	}
	if name == "" {
		panic(fmt.Sprintf("sink: RegisterDriver for class %s with empty driver name", class))
	}
	if d.Factory == nil {
		panic(fmt.Sprintf("sink: RegisterDriver(%s/%s) with nil Factory", class, name))
	}
	if d.Describer == nil {
		panic(fmt.Sprintf("sink: RegisterDriver(%s/%s) with nil Describer", class, name))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	byName := r.drivers[class]
	if byName == nil {
		byName = map[string]Driver{}
		r.drivers[class] = byName
	}
	if _, dup := byName[name]; dup {
		panic(fmt.Sprintf("sink: driver %s/%s registered twice", class, name))
	}
	byName[name] = d
}

// Drivers lists the driver names registered for class, sorted.
func (r *Registry) Drivers(class Class) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.drivers[class]))
	for name := range r.drivers[class] {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Describers returns the Describer of every registered driver keyed by
// config.SinkKey(class, driver), for config.Describers.Sinks.
func (r *Registry) Describers() map[string]config.Describer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[string]config.Describer{}
	for class, byName := range r.drivers {
		for name, d := range byName {
			out[config.SinkKey(class.String(), name)] = d.Describer
		}
	}
	return out
}

// Build constructs one Instance per configuration entry, in order. It fails
// on an unknown class or driver (listing what is registered), an invalid
// start position, a duplicate name, and any factory error; in that case the
// sinks built so far are closed. No sink is started here: Build only
// constructs, the router subscribes and delivers.
func (r *Registry) Build(ctx context.Context, cfgs []config.SinkConfig, deps Deps) ([]Instance, error) {
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	instances := make([]Instance, 0, len(cfgs))
	seen := map[string]int{}
	for i, cfg := range cfgs {
		inst, err := r.build(ctx, cfg, deps)
		if err == nil {
			if j, dup := seen[cfg.Name]; dup {
				err = fmt.Errorf("duplicate sink name (also sinks[%d])", j)
				_ = inst.Close()
			}
		}
		if err != nil {
			return nil, errors.Join(fmt.Errorf("sinks[%d] %q: %w", i, cfg.Name, err), closeAll(instances))
		}
		seen[cfg.Name] = i
		instances = append(instances, inst)
	}
	return instances, nil
}

func (r *Registry) build(ctx context.Context, cfg config.SinkConfig, deps Deps) (Instance, error) {
	if cfg.Name == "" {
		return Instance{}, errors.New("name is required")
	}
	if err := bus.ValidateConsumerName(ConsumerName(cfg.Name)); err != nil {
		return Instance{}, err
	}
	class, err := ParseClass(cfg.Class)
	if err != nil {
		return Instance{}, err
	}
	start, err := bus.ParseStartPosition(cfg.StartFrom)
	if err != nil {
		return Instance{}, fmt.Errorf("start_from: %w", err)
	}
	if cfg.Driver == "" {
		return Instance{}, fmt.Errorf("driver is required for class %s (registered: %s)", class, r.driversForError(class))
	}
	r.mu.RLock()
	d, ok := r.drivers[class][cfg.Driver]
	r.mu.RUnlock()
	if !ok {
		return Instance{}, fmt.Errorf("unknown driver %q for class %s (registered: %s)", cfg.Driver, class, r.driversForError(class))
	}
	scoped := deps
	scoped.Logger = deps.Logger.With("sink", cfg.Name, "class", class.String(), "driver", cfg.Driver)
	s, err := d.Factory(ctx, cfg.Name, cfg.Config, scoped)
	if err != nil {
		return Instance{}, fmt.Errorf("driver %s/%s: %w", class, cfg.Driver, err)
	}
	if s == nil {
		return Instance{}, fmt.Errorf("driver %s/%s returned no sink", class, cfg.Driver)
	}
	if s.Name() != cfg.Name || s.Class() != class {
		_ = s.Close()
		return Instance{}, fmt.Errorf("driver %s/%s built sink %s/%q instead of %s/%q", class, cfg.Driver, s.Class(), s.Name(), class, cfg.Name)
	}
	return Instance{Sink: s, StartFrom: start, Driver: cfg.Driver}, nil
}

func (r *Registry) driversForError(class Class) string {
	return joinNames(r.Drivers(class))
}

// closeAll closes every instance and joins the errors; used on failed builds.
func closeAll(instances []Instance) error {
	var errs []error
	for _, inst := range instances {
		if err := inst.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close sink %q: %w", inst.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// CloseAll closes every instance in order and returns the joined errors.
func CloseAll(instances []Instance) error { return closeAll(instances) }

func joinNames(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// RegisterDriver adds a driver to DefaultRegistry.
func RegisterDriver(class Class, name string, d Driver) {
	DefaultRegistry.RegisterDriver(class, name, d)
}

// Drivers lists the drivers of class in DefaultRegistry.
func Drivers(class Class) []string { return DefaultRegistry.Drivers(class) }

// Describers returns the Describers of DefaultRegistry.
func Describers() map[string]config.Describer { return DefaultRegistry.Describers() }

// Build builds sinks through DefaultRegistry.
func Build(ctx context.Context, cfgs []config.SinkConfig, deps Deps) ([]Instance, error) {
	return DefaultRegistry.Build(ctx, cfgs, deps)
}
