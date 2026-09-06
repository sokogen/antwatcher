package config

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func validConfig() Config {
	cfg := Default()
	cfg.Server.WebhookSecret = "s"
	cfg.Sinks = []SinkConfig{
		{Name: "tempo", Class: "trace", Driver: "otlp", StartFrom: StartFromEarliest},
		{Name: "events-1", Class: "log", Driver: "stdout", StartFrom: StartFromNow},
	}
	return cfg
}

func TestValidate_Valid(t *testing.T) {
	cfg := validConfig()
	require.NoError(t, cfg.Validate())

	cfg.Recovery.Enabled = true
	cfg.Recovery.Auth.Token = "t"
	cfg.Recovery.Targets = []RecoveryTarget{{Repo: "owner/name"}, {Org: "org", HookID: 1}}
	require.NoError(t, cfg.Validate())
}

func TestValidate_Rules(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *Config)
		want   string
	}{
		{"webhook secret required", func(c *Config) { c.Server.WebhookSecret = "" }, "server.webhook_secret is required"},
		{"server listen required", func(c *Config) { c.Server.Listen = "" }, "server.listen is required"},
		{"webhook path leading slash", func(c *Config) { c.Server.WebhookPath = "hook" }, "server.webhook_path must start with"},
		{"webhook path rejects an unterminated wildcard", func(c *Config) { c.Server.WebhookPath = "/webhook/{" }, "must not contain \"{\" or \"}\""},
		{"webhook path rejects a wildcard segment", func(c *Config) { c.Server.WebhookPath = "/webhook/{id}" }, "must not contain \"{\" or \"}\""},
		{"webhook path rejects a double slash", func(c *Config) { c.Server.WebhookPath = "//webhook" }, "must be a clean path"},
		{"webhook path rejects dot segments", func(c *Config) { c.Server.WebhookPath = "/a/../webhook" }, "must be a clean path"},
		{"webhook path rejects a trailing slash", func(c *Config) { c.Server.WebhookPath = "/webhook/" }, "must not end in"},
		{"max body positive", func(c *Config) { c.Server.MaxBodyBytes = 0 }, "server.max_body_bytes must be > 0"},
		{"publish timeout positive", func(c *Config) { c.Server.PublishTimeout = 0 }, "server.publish_timeout must be > 0"},
		{"admin listen required", func(c *Config) { c.Admin.Listen = "" }, "admin.listen is required"},
		{"listeners differ exact", func(c *Config) { c.Admin.Listen = c.Server.Listen }, "must differ from server.listen"},
		{"listeners differ wildcard host", func(c *Config) { c.Server.Listen = ":9090" }, "must differ from server.listen"},
		{"listeners differ 0.0.0.0", func(c *Config) { c.Server.Listen = "0.0.0.0:9090" }, "must differ from server.listen"},
		{"lag interval positive", func(c *Config) { c.Admin.LagInterval = 0 }, "admin.lag_interval must be > 0"},
		{"bus driver required", func(c *Config) { c.Bus.Driver = "" }, "bus.driver is required"},
		{"bus topic required", func(c *Config) { c.Bus.Topic = "" }, "bus.topic is required"},
		{"on_missing_capability enum", func(c *Config) { c.Bus.OnMissingCapability = "warn" }, `bus.on_missing_capability must be "fail" or "degrade"`},
		{"process timeout positive", func(c *Config) { c.Router.ProcessTimeout = 0 }, "router.process_timeout must be > 0"},
		{"close timeout positive", func(c *Config) { c.Router.CloseTimeout = -time.Second }, "router.close_timeout must be > 0"},
		{"sink name required", func(c *Config) { c.Sinks[0].Name = "" }, "sinks[0]: name is required"},
		{"sink name charset upper", func(c *Config) { c.Sinks[0].Name = "Tempo" }, `sinks[0] "Tempo": name must match [a-z0-9-]+`},
		{"sink name charset underscore", func(c *Config) { c.Sinks[0].Name = "my_sink" }, "name must match"},
		{"sink name unique", func(c *Config) { c.Sinks[1].Name = "tempo" }, `sinks[1] "tempo": duplicate sink name (also sinks[0])`},
		{"sink class required", func(c *Config) { c.Sinks[1].Class = "" }, `sinks[1] "events-1": class is required`},
		{"sink driver required", func(c *Config) { c.Sinks[1].Driver = "" }, `sinks[1] "events-1": driver is required`},
		{"start_from required", func(c *Config) { c.Sinks[0].StartFrom = "" }, `sinks[0] "tempo": start_from is required`},
		{"start_from enum", func(c *Config) { c.Sinks[0].StartFrom = "latest" }, `start_from must be "earliest" or "now" (got "latest")`},
		{"recovery auth type enum", func(c *Config) { c.Recovery.Auth.Type = "basic" }, `recovery.auth.type must be "token"`},
		{"recovery github_app reserved", func(c *Config) { c.Recovery.Auth.Type = AuthTypeGitHubApp }, "reserved and not implemented"},
		{"recovery enabled needs token", func(c *Config) {
			c.Recovery.Enabled = true
			c.Recovery.Targets = []RecoveryTarget{{Repo: "o/n"}}
		}, "recovery.auth.token is required"},
		{"recovery enabled needs auth type", func(c *Config) {
			c.Recovery.Enabled = true
			c.Recovery.Auth.Type = ""
			c.Recovery.Auth.Token = "t"
			c.Recovery.Targets = []RecoveryTarget{{Repo: "o/n"}}
		}, "recovery.auth.type is required"},
		{"recovery enabled needs target", func(c *Config) {
			c.Recovery.Enabled = true
			c.Recovery.Auth.Token = "t"
		}, "at least one repo or org"},
		{"recovery target repo or org", func(c *Config) { c.Recovery.Targets = []RecoveryTarget{{}} }, "recovery.targets[0]: repo or org is required"},
		{"recovery target not both", func(c *Config) { c.Recovery.Targets = []RecoveryTarget{{Repo: "o/n", Org: "o"}} }, "not both"},
		{"recovery repo format", func(c *Config) { c.Recovery.Targets = []RecoveryTarget{{Repo: "name"}} }, `repo must be "owner/name"`},
		{"recovery hook id", func(c *Config) { c.Recovery.Targets = []RecoveryTarget{{Org: "o", HookID: -1}} }, "hook_id must be >= 0"},
		{"recovery interval", func(c *Config) {
			c.Recovery.Enabled = true
			c.Recovery.Auth.Token = "t"
			c.Recovery.Targets = []RecoveryTarget{{Org: "o"}}
			c.Recovery.Interval = 0
		}, "recovery.interval must be > 0"},
		{"recovery lookback window", func(c *Config) {
			c.Recovery.Enabled = true
			c.Recovery.Auth.Token = "t"
			c.Recovery.Targets = []RecoveryTarget{{Org: "o"}}
			c.Recovery.Lookback = 100 * time.Hour
		}, "recovery.lookback must be in (0, 72h0m0s]"},
		{"recovery overlap", func(c *Config) {
			c.Recovery.Enabled = true
			c.Recovery.Auth.Token = "t"
			c.Recovery.Targets = []RecoveryTarget{{Org: "o"}}
			c.Recovery.Overlap = -1
		}, "recovery.overlap must be >= 0"},
		{"recovery grace", func(c *Config) {
			c.Recovery.Enabled = true
			c.Recovery.Auth.Token = "t"
			c.Recovery.Targets = []RecoveryTarget{{Org: "o"}}
			c.Recovery.Grace = -1
		}, "recovery.grace must be >= 0"},
		{"recovery max per scan", func(c *Config) {
			c.Recovery.Enabled = true
			c.Recovery.Auth.Token = "t"
			c.Recovery.Targets = []RecoveryTarget{{Org: "o"}}
			c.Recovery.MaxPerScan = 0
		}, "recovery.max_per_scan must be > 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestValidate_ReportsAllErrors(t *testing.T) {
	cfg := validConfig()
	cfg.Server.WebhookSecret = ""
	cfg.Sinks[0].StartFrom = ""
	cfg.Bus.OnMissingCapability = "x"
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server.webhook_secret is required")
	assert.Contains(t, err.Error(), "start_from is required")
	assert.Contains(t, err.Error(), "bus.on_missing_capability")
}

func TestValidate_DisabledRecoveryIgnoresRuntimeFields(t *testing.T) {
	cfg := validConfig()
	cfg.Recovery.Interval = 0
	cfg.Recovery.MaxPerScan = 0
	cfg.Recovery.Auth.Token = ""
	require.NoError(t, cfg.Validate())
}

func TestSameListener(t *testing.T) {
	assert.True(t, sameListener(":8080", ":8080"))
	assert.True(t, sameListener(":8080", "0.0.0.0:8080"))
	assert.True(t, sameListener("127.0.0.1:8080", ":8080"))
	assert.True(t, sameListener("[::]:8080", "127.0.0.1:8080"))
	assert.False(t, sameListener("127.0.0.1:8080", "127.0.0.1:9090"))
	assert.False(t, sameListener("127.0.0.1:8080", "10.0.0.1:8080"))
	assert.False(t, sameListener("garbage", ":8080"))
}

type brokerTestConfig struct {
	AckWait time.Duration `yaml:"ack_wait"`
}

func (c brokerTestConfig) ValidateWith(r Router) error {
	if c.AckWait <= r.ProcessTimeout+10*time.Second {
		return errors.New("ack_wait must exceed router.process_timeout + 10s")
	}
	return nil
}

func brokerDescriber(raw yaml.Node) (any, error) {
	c := brokerTestConfig{AckWait: 90 * time.Second}
	if err := DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// ingressTestConfig refuses to forward into the ingress topic.
type ingressTestConfig struct {
	Topic string `yaml:"topic"`
}

func (c ingressTestConfig) ValidateIngress(ingress Bus) error {
	if c.Topic == ingress.Topic {
		return fmt.Errorf("topic %q is the ingress topic", c.Topic)
	}
	return nil
}

func ingressDescriber(raw yaml.Node) (any, error) {
	var c ingressTestConfig
	if err := DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

func TestValidateDrivers(t *testing.T) {
	describers := Describers{
		Bus:   map[string]Describer{"broker": DescriberFunc(brokerDescriber)},
		Sinks: map[string]Describer{SinkKey("forward", "bus"): DescriberFunc(brokerDescriber)},
	}

	t.Run("driver cross-check against router", func(t *testing.T) {
		cfg, err := Parse([]byte("server:\n  webhook_secret: s\nbus:\n  driver: broker\n  broker:\n    ack_wait: 30s\nrouter:\n  process_timeout: 60s\n"))
		require.NoError(t, err)
		err = ValidateDrivers(cfg, describers)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bus.broker: ack_wait must exceed")
	})

	t.Run("absent block uses driver defaults", func(t *testing.T) {
		cfg, err := Parse([]byte("server:\n  webhook_secret: s\nbus:\n  driver: broker\n"))
		require.NoError(t, err)
		require.NoError(t, ValidateDrivers(cfg, describers))
	})

	t.Run("only the selected bus driver block is checked", func(t *testing.T) {
		cfg, err := Parse([]byte("server:\n  webhook_secret: s\nbus:\n  driver: other\n  broker:\n    ack_wait: 1s\n"))
		require.NoError(t, err)
		require.NoError(t, ValidateDrivers(cfg, describers))
	})

	t.Run("sink block decode error and validation", func(t *testing.T) {
		cfg, err := Parse([]byte("server:\n  webhook_secret: s\nsinks:\n  - name: f\n    class: forward\n    driver: bus\n    start_from: now\n    config: {ack_wait: 5s}\n  - name: g\n    class: forward\n    driver: bus\n    start_from: now\n    config: {bogus: 1}\n  - name: h\n    class: log\n    driver: stdout\n    start_from: now\n    config: {bogus: 1}\n"))
		require.NoError(t, err)
		err = ValidateDrivers(cfg, describers)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `sinks[0] "f": ack_wait must exceed`)
		assert.Contains(t, err.Error(), `sinks[1] "g": decode: `)
		assert.NotContains(t, err.Error(), `"h"`, "blocks without a describer are skipped")
	})

	t.Run("sink block checked against the ingress bus", func(t *testing.T) {
		d := Describers{Sinks: map[string]Describer{SinkKey("forward", "loop"): DescriberFunc(ingressDescriber)}}
		text := "server:\n  webhook_secret: s\nbus:\n  driver: broker\n  topic: in\nsinks:\n  - name: f\n    class: forward\n    driver: loop\n    start_from: now\n    config: {topic: %s}\n"
		cfg, err := Parse(fmt.Appendf(nil, text, "in"))
		require.NoError(t, err)
		err = ValidateDrivers(cfg, d)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `sinks[0] "f": topic "in" is the ingress topic`)

		cfg, err = Parse(fmt.Appendf(nil, text, "out"))
		require.NoError(t, err)
		require.NoError(t, ValidateDrivers(cfg, d))
	})

	t.Run("bus block is not checked against itself", func(t *testing.T) {
		d := Describers{Bus: map[string]Describer{"loop": DescriberFunc(ingressDescriber)}}
		cfg, err := Parse([]byte("server:\n  webhook_secret: s\nbus:\n  driver: loop\n  topic: in\n  loop:\n    topic: in\n"))
		require.NoError(t, err)
		require.NoError(t, ValidateDrivers(cfg, d))
	})
}

// TestValidate_WebhookPathIsMountable pins the reason the webhook_path rules
// exist: net/http's ServeMux panics on a pattern it cannot parse, and the
// receiver mounts the configured path directly. Every path Validate accepts
// must mount. The converse does not hold and is not asserted -- the mux
// happily takes `/webhook/` and `/webhook/{id}`, which Validate rejects on
// purpose because they match more than the configured path -- so the table
// spells out the verdict per path instead, keeping the rules from silently
// becoming stricter than they need to be.
func TestValidate_WebhookPathIsMountable(t *testing.T) {
	paths := map[string]bool{
		"/webhook":      true,
		"/gh/hooks":     true,
		"/a":            true,
		"/webhook.json": true,
		"/":             true,
		"hook":          false,
		"":              false,
		"/webhook/{":    false,
		"/a{b}c":        false,
		"/webhook/{id}": false,
		"//webhook":     false,
		"/a/../b":       false,
		"/webhook/":     false,
	}
	for p, wantAccepted := range paths {
		t.Run(fmt.Sprintf("%q", p), func(t *testing.T) {
			cfg := validConfig()
			cfg.Server.WebhookPath = p
			accepted := cfg.Validate() == nil
			assert.Equal(t, wantAccepted, accepted, "Validate verdict on %q", p)

			mounts := func() (ok bool) {
				defer func() { ok = recover() == nil }()
				http.NewServeMux().HandleFunc("POST "+p, func(http.ResponseWriter, *http.Request) {})
				return
			}()

			if accepted {
				assert.True(t, mounts, "Validate accepted %q but the mux rejects it", p)
			}
		})
	}
}
