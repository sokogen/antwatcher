package config

import (
	"fmt"
	"regexp"

	"gopkg.in/yaml.v3"
)

// secretKeyPattern selects keys whose values are masked when a driver block has
// no Describer. Described blocks rely on Secret fields instead.
var secretKeyPattern = regexp.MustCompile(`(?i)(token|secret|password|passwd|authorization|key|credential)`)

// RedactedConfig is the configuration as printed by `-check` and `/status`:
// core fields typed (Secret masks itself), driver blocks rendered through their
// Describer or, without one, with values under secret-looking keys masked.
type RedactedConfig struct {
	Server   Server         `yaml:"server" json:"server"`
	Admin    Admin          `yaml:"admin" json:"admin"`
	Bus      RedactedBus    `yaml:"bus" json:"bus"`
	Router   Router         `yaml:"router" json:"router"`
	Sinks    []RedactedSink `yaml:"sinks" json:"sinks"`
	Recovery Recovery       `yaml:"recovery" json:"recovery"`
}

// RedactedBus mirrors Bus with driver blocks rendered inline, as in the source file.
type RedactedBus struct {
	Driver              string         `yaml:"driver" json:"driver"`
	Topic               string         `yaml:"topic" json:"topic"`
	OnMissingCapability string         `yaml:"on_missing_capability" json:"on_missing_capability"`
	Drivers             map[string]any `yaml:",inline" json:"drivers,omitempty"`
}

// RedactedSink mirrors SinkConfig with the driver block rendered.
type RedactedSink struct {
	Name      string `yaml:"name" json:"name"`
	Class     string `yaml:"class" json:"class"`
	Driver    string `yaml:"driver" json:"driver"`
	StartFrom string `yaml:"start_from" json:"start_from"`
	Config    any    `yaml:"config,omitempty" json:"config,omitempty"`
}

// Redacted renders cfg with every credential masked. Driver blocks with a
// Describer are decoded into the driver's typed config; the rest are masked by key.
func Redacted(cfg Config, describers Describers) (RedactedConfig, error) {
	out := RedactedConfig{
		Server:   cfg.Server,
		Admin:    cfg.Admin,
		Router:   cfg.Router,
		Recovery: cfg.Recovery,
		Bus: RedactedBus{
			Driver:              cfg.Bus.Driver,
			Topic:               cfg.Bus.Topic,
			OnMissingCapability: cfg.Bus.OnMissingCapability,
		},
	}
	if len(cfg.Bus.Drivers) > 0 {
		out.Bus.Drivers = make(map[string]any, len(cfg.Bus.Drivers))
	}
	for name, node := range cfg.Bus.Drivers {
		v, err := describe(describers.Bus[name], node)
		if err != nil {
			return RedactedConfig{}, fmt.Errorf("bus.%s: %w", name, err)
		}
		out.Bus.Drivers[name] = v
	}
	for i, s := range cfg.Sinks {
		v, err := describe(describers.Sinks[SinkKey(s.Class, s.Driver)], s.Config)
		if err != nil {
			return RedactedConfig{}, fmt.Errorf("sinks[%d] %q: %w", i, s.Name, err)
		}
		out.Sinks = append(out.Sinks, RedactedSink{
			Name: s.Name, Class: s.Class, Driver: s.Driver, StartFrom: s.StartFrom, Config: v,
		})
	}
	return out, nil
}

func describe(d Describer, node yaml.Node) (any, error) {
	if node.Kind == 0 {
		return nil, nil
	}
	if d != nil {
		return d.Describe(node)
	}
	var v any
	if err := node.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode driver block: %w", err)
	}
	return maskByKey(v), nil
}

// maskByKey walks decoded YAML and replaces any non-empty value stored under a
// secret-looking key with Mask, including whole sub-trees.
func maskByKey(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if secretKeyPattern.MatchString(k) && !isEmpty(val) {
				out[k] = Mask
				continue
			}
			out[k] = maskByKey(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = maskByKey(val)
		}
		return out
	default:
		return v
	}
}

func isEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	default:
		return false
	}
}
