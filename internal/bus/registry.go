package bus

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
)

// Factory opens a bus from its opaque driver block (the `bus.<driver>` mapping,
// zero Kind when absent) for the given topic. Drivers decode raw with
// config.DecodeStrict so unknown keys fail. logger is never nil. metrics may be
// nil when the caller collects no metrics; drivers register their own
// collectors on it otherwise.
type Factory func(ctx context.Context, raw yaml.Node, topic string, logger *slog.Logger, metrics prometheus.Registerer) (Bus, error)

// Driver is what a bus driver registers: how to open it and how to describe
// its configuration for validation and redaction (see config.Describer).
type Driver struct {
	Factory   Factory
	Describer config.Describer
}

// Registry maps driver names to drivers. The package-level functions use
// DefaultRegistry; tests build their own.
type Registry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{drivers: map[string]Driver{}}
}

// DefaultRegistry is the registry drivers register with from their init functions.
var DefaultRegistry = NewRegistry()

// Register adds a driver. It panics on an empty name, a nil Factory, a nil
// Describer, or a name already registered: all of these are programming errors
// caught at init time.
func (r *Registry) Register(name string, d Driver) {
	if name == "" {
		panic("bus: Register with empty driver name")
	}
	if d.Factory == nil {
		panic(fmt.Sprintf("bus: Register(%q) with nil Factory", name))
	}
	if d.Describer == nil {
		panic(fmt.Sprintf("bus: Register(%q) with nil Describer", name))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.drivers[name]; dup {
		panic(fmt.Sprintf("bus: driver %q registered twice", name))
	}
	r.drivers[name] = d
}

// Names lists the registered driver names, sorted.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.drivers))
	for name := range r.drivers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Describers returns the Describer of every registered driver keyed by name,
// for config.Describers.Bus.
func (r *Registry) Describers() map[string]config.Describer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]config.Describer, len(r.drivers))
	for name, d := range r.drivers {
		out[name] = d.Describer
	}
	return out
}

// Open opens the bus selected by cfg.Driver with its driver block and cfg.Topic.
// An unknown driver is an error listing the registered ones.
func (r *Registry) Open(ctx context.Context, cfg config.Bus, logger *slog.Logger, metrics prometheus.Registerer) (Bus, error) {
	if cfg.Driver == "" {
		return nil, fmt.Errorf("bus.driver is required (registered: %s)", r.namesForError())
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("bus.topic is required")
	}
	r.mu.RLock()
	d, ok := r.drivers[cfg.Driver]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown bus driver %q (registered: %s)", cfg.Driver, r.namesForError())
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	b, err := d.Factory(ctx, cfg.Drivers[cfg.Driver], cfg.Topic, logger.With("bus", cfg.Driver), metrics)
	if err != nil {
		return nil, fmt.Errorf("open bus %q: %w", cfg.Driver, err)
	}
	return b, nil
}

func (r *Registry) namesForError() string {
	names := r.Names()
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// Register adds a driver to DefaultRegistry.
func Register(name string, d Driver) { DefaultRegistry.Register(name, d) }

// Names lists the drivers in DefaultRegistry.
func Names() []string { return DefaultRegistry.Names() }

// Describers returns the Describers of DefaultRegistry.
func Describers() map[string]config.Describer { return DefaultRegistry.Describers() }

// Open opens a bus through DefaultRegistry.
func Open(ctx context.Context, cfg config.Bus, logger *slog.Logger, metrics prometheus.Registerer) (Bus, error) {
	return DefaultRegistry.Open(ctx, cfg, logger, metrics)
}
