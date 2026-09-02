// Package config loads, validates, and redacts the antwatcher configuration.
//
// The core configuration is typed. Driver-specific blocks (bus drivers under
// `bus.<driver>` and every `sinks[].config`) stay opaque yaml.Node values that the
// owning driver decodes with DecodeStrict, so the core never knows driver fields.
package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Start positions accepted in sinks[].start_from.
const (
	StartFromEarliest = "earliest"
	StartFromNow      = "now"
)

// Policies accepted in bus.on_missing_capability.
const (
	OnMissingCapabilityFail    = "fail"
	OnMissingCapabilityDegrade = "degrade"
)

// Recovery auth types. AuthTypeGitHubApp is reserved by the schema and rejected
// by Validate until it is implemented.
const (
	AuthTypeToken     = "token"
	AuthTypeGitHubApp = "github_app"
)

// Config is the root of the antwatcher configuration file.
type Config struct {
	Server   Server       `yaml:"server" json:"server"`
	Admin    Admin        `yaml:"admin" json:"admin"`
	Bus      Bus          `yaml:"bus" json:"bus"`
	Router   Router       `yaml:"router" json:"router"`
	Sinks    []SinkConfig `yaml:"sinks" json:"sinks"`
	Recovery Recovery     `yaml:"recovery" json:"recovery"`
}

// Server configures the webhook listener.
type Server struct {
	// Listen is the address of the webhook listener (webhook path and /healthz only).
	Listen string `yaml:"listen" json:"listen"`
	// WebhookPath is the path GitHub posts deliveries to.
	WebhookPath string `yaml:"webhook_path" json:"webhook_path"`
	// WebhookSecret is the HMAC secret configured on the GitHub webhook.
	WebhookSecret Secret `yaml:"webhook_secret" json:"webhook_secret"`
	// PublicURL is the externally visible webhook URL (informational, shown in /status).
	PublicURL string `yaml:"public_url" json:"public_url"`
	// MaxBodyBytes caps the webhook request body.
	MaxBodyBytes int64 `yaml:"max_body_bytes" json:"max_body_bytes"`
	// PublishTimeout bounds Bus.Publish for one delivery; keep it under GitHub's 10s limit.
	PublishTimeout time.Duration `yaml:"publish_timeout" json:"publish_timeout"`
}

// Admin configures the admin listener (/metrics, /status, /healthz, /readyz).
type Admin struct {
	Listen      string        `yaml:"listen" json:"listen"`
	LagInterval time.Duration `yaml:"lag_interval" json:"lag_interval"`
}

// Bus selects the bus driver and the ingress topic. Driver blocks are keyed by
// driver name directly under `bus:` and kept opaque in Drivers.
type Bus struct {
	Driver              string `yaml:"driver" json:"driver"`
	Topic               string `yaml:"topic" json:"topic"`
	OnMissingCapability string `yaml:"on_missing_capability" json:"on_missing_capability"`
	// Drivers holds every driver block found under `bus:` keyed by driver name.
	// Only the block named by Driver is used at runtime.
	Drivers map[string]yaml.Node `yaml:"-" json:"-"`
}

// UnmarshalYAML decodes the typed bus fields and keeps every other mapping-valued
// key as an opaque driver block. Unknown scalar keys are rejected as typos.
func (b *Bus) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: bus must be a mapping", node.Line)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, val := node.Content[i], node.Content[i+1]
		var target *string
		switch key.Value {
		case "driver":
			target = &b.Driver
		case "topic":
			target = &b.Topic
		case "on_missing_capability":
			target = &b.OnMissingCapability
		default:
			if val.Kind != yaml.MappingNode {
				return fmt.Errorf("line %d: bus: unknown field %q (driver blocks must be mappings)", key.Line, key.Value)
			}
			if b.Drivers == nil {
				b.Drivers = map[string]yaml.Node{}
			}
			b.Drivers[key.Value] = *val
			continue
		}
		if err := val.Decode(target); err != nil {
			return fmt.Errorf("bus.%s: %w", key.Value, err)
		}
	}
	return nil
}

// Router configures the Watermill router shared by all sinks.
type Router struct {
	// CloseTimeout bounds the graceful shutdown of the router.
	CloseTimeout time.Duration `yaml:"close_timeout" json:"close_timeout"`
	// ProcessTimeout is the deadline for one sink.Process call. Broker drivers
	// validate that their ack wait exceeds it (see DriverValidator).
	ProcessTimeout time.Duration `yaml:"process_timeout" json:"process_timeout"`
}

// SinkConfig declares one independent consumer of the bus.
type SinkConfig struct {
	Name   string `yaml:"name" json:"name"`
	Class  string `yaml:"class" json:"class"`
	Driver string `yaml:"driver" json:"driver"`
	// StartFrom is required: "earliest" replays retained history on first start,
	// "now" starts with the next event. There is no default because the two
	// choices differ by a potentially huge replay.
	StartFrom string `yaml:"start_from" json:"start_from"`
	// Config is the driver block, decoded by the driver with DecodeStrict.
	Config yaml.Node `yaml:"config" json:"-"`
}

// Recovery configures the GitHub webhook deliveries scan.
type Recovery struct {
	Enabled    bool             `yaml:"enabled" json:"enabled"`
	Auth       RecoveryAuth     `yaml:"auth" json:"auth"`
	APIBaseURL string           `yaml:"api_base_url" json:"api_base_url"`
	Interval   time.Duration    `yaml:"interval" json:"interval"`
	Lookback   time.Duration    `yaml:"lookback" json:"lookback"`
	Overlap    time.Duration    `yaml:"overlap" json:"overlap"`
	Grace      time.Duration    `yaml:"grace" json:"grace"`
	MaxPerScan int              `yaml:"max_per_scan" json:"max_per_scan"`
	Targets    []RecoveryTarget `yaml:"targets" json:"targets"`
}

// RecoveryAuth selects how the GitHub API is authenticated.
type RecoveryAuth struct {
	Type  string `yaml:"type" json:"type"`
	Token Secret `yaml:"token" json:"token"`
}

// RecoveryTarget is one repository or organization webhook to scan.
type RecoveryTarget struct {
	Repo   string `yaml:"repo,omitempty" json:"repo,omitempty"`
	Org    string `yaml:"org,omitempty" json:"org,omitempty"`
	HookID int64  `yaml:"hook_id,omitempty" json:"hook_id,omitempty"`
}

// MaxLookback is the GitHub retention window for webhook deliveries.
const MaxLookback = 72 * time.Hour

// Default returns the configuration with every documented default applied.
// Fields without a default (webhook secret, sinks, start_from) are left empty.
func Default() Config {
	return Config{
		Server: Server{
			Listen:         ":8080",
			WebhookPath:    "/webhook",
			MaxBodyBytes:   25 << 20,
			PublishTimeout: 8 * time.Second,
		},
		Admin: Admin{
			Listen:      "127.0.0.1:9090",
			LagInterval: 15 * time.Second,
		},
		Bus: Bus{
			Driver:              "nats-jetstream",
			Topic:               "antwatcher.events",
			OnMissingCapability: OnMissingCapabilityFail,
		},
		Router: Router{
			CloseTimeout:   30 * time.Second,
			ProcessTimeout: 60 * time.Second,
		},
		Recovery: Recovery{
			Auth:       RecoveryAuth{Type: AuthTypeToken},
			Interval:   10 * time.Minute,
			Lookback:   MaxLookback,
			Overlap:    15 * time.Minute,
			Grace:      5 * time.Minute,
			MaxPerScan: 100,
		},
	}
}
