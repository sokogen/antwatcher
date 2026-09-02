package config

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var sinkNamePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// Validate checks every core rule and returns all violations joined together.
// Driver blocks are checked separately by ValidateDrivers.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.Server.Listen == "" {
		add("server.listen is required")
	}
	if !strings.HasPrefix(c.Server.WebhookPath, "/") {
		add("server.webhook_path must start with \"/\" (got %q)", c.Server.WebhookPath)
	}
	if c.Server.WebhookSecret == "" {
		add("server.webhook_secret is required")
	}
	if c.Server.MaxBodyBytes <= 0 {
		add("server.max_body_bytes must be > 0")
	}
	if c.Server.PublishTimeout <= 0 {
		add("server.publish_timeout must be > 0")
	}

	if c.Admin.Listen == "" {
		add("admin.listen is required")
	} else if sameListener(c.Admin.Listen, c.Server.Listen) {
		add("admin.listen (%q) must differ from server.listen (%q): admin endpoints are never served on the webhook listener", c.Admin.Listen, c.Server.Listen)
	}
	if c.Admin.LagInterval <= 0 {
		add("admin.lag_interval must be > 0")
	}

	if c.Bus.Driver == "" {
		add("bus.driver is required")
	}
	if c.Bus.Topic == "" {
		add("bus.topic is required")
	}
	switch c.Bus.OnMissingCapability {
	case OnMissingCapabilityFail, OnMissingCapabilityDegrade:
	default:
		add("bus.on_missing_capability must be %q or %q (got %q)", OnMissingCapabilityFail, OnMissingCapabilityDegrade, c.Bus.OnMissingCapability)
	}

	if c.Router.ProcessTimeout <= 0 {
		add("router.process_timeout must be > 0")
	}
	if c.Router.CloseTimeout <= 0 {
		add("router.close_timeout must be > 0")
	}

	seen := map[string]int{}
	for i, s := range c.Sinks {
		at := fmt.Sprintf("sinks[%d]", i)
		if s.Name != "" {
			at = fmt.Sprintf("sinks[%d] %q", i, s.Name)
		}
		switch {
		case s.Name == "":
			add("%s: name is required", at)
		case !sinkNamePattern.MatchString(s.Name):
			add("%s: name must match [a-z0-9-]+", at)
		default:
			if j, dup := seen[s.Name]; dup {
				add("%s: duplicate sink name (also sinks[%d])", at, j)
			}
			seen[s.Name] = i
		}
		if s.Class == "" {
			add("%s: class is required", at)
		}
		if s.Driver == "" {
			add("%s: driver is required", at)
		}
		switch s.StartFrom {
		case StartFromEarliest, StartFromNow:
		case "":
			add("%s: start_from is required (%q replays retained history, %q starts with the next event; there is no default)", at, StartFromEarliest, StartFromNow)
		default:
			add("%s: start_from must be %q or %q (got %q)", at, StartFromEarliest, StartFromNow, s.StartFrom)
		}
	}

	errs = append(errs, c.Recovery.validate()...)
	return errors.Join(errs...)
}

func (r *Recovery) validate() []error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	switch r.Auth.Type {
	case AuthTypeToken, "":
	case AuthTypeGitHubApp:
		add("recovery.auth.type %q is reserved and not implemented yet; use %q", AuthTypeGitHubApp, AuthTypeToken)
	default:
		add("recovery.auth.type must be %q (got %q)", AuthTypeToken, r.Auth.Type)
	}
	for i, t := range r.Targets {
		switch {
		case t.Repo != "" && t.Org != "":
			add("recovery.targets[%d]: set either repo or org, not both", i)
		case t.Repo == "" && t.Org == "":
			add("recovery.targets[%d]: repo or org is required", i)
		case t.Repo != "" && len(strings.Split(t.Repo, "/")) != 2:
			add("recovery.targets[%d]: repo must be \"owner/name\" (got %q)", i, t.Repo)
		}
		if t.HookID < 0 {
			add("recovery.targets[%d]: hook_id must be >= 0", i)
		}
	}
	if !r.Enabled {
		return errs
	}
	if r.Auth.Type == "" {
		add("recovery.auth.type is required when recovery is enabled")
	}
	if r.Auth.Type == AuthTypeToken && r.Auth.Token == "" {
		add("recovery.auth.token is required when recovery is enabled")
	}
	if len(r.Targets) == 0 {
		add("recovery.targets must list at least one repo or org when recovery is enabled")
	}
	if r.Interval <= 0 {
		add("recovery.interval must be > 0")
	}
	if r.Lookback <= 0 || r.Lookback > MaxLookback {
		add("recovery.lookback must be in (0, %s] (GitHub keeps deliveries for 3 days)", MaxLookback)
	}
	if r.Overlap < 0 {
		add("recovery.overlap must be >= 0")
	}
	if r.Grace < 0 {
		add("recovery.grace must be >= 0")
	}
	if r.MaxPerScan <= 0 {
		add("recovery.max_per_scan must be > 0")
	}
	return errs
}

// ValidateDrivers decodes the selected bus driver block and every sink block
// through its Describer and runs ValidateWith on the ones that implement
// DriverValidator. Blocks without a Describer are skipped: their driver is
// unknown here and the registry reports that at startup.
func ValidateDrivers(cfg Config, describers Describers) error {
	var errs []error
	if d := describers.Bus[cfg.Bus.Driver]; d != nil {
		if err := validateBlock(d, cfg.Bus.Drivers[cfg.Bus.Driver], cfg.Router); err != nil {
			errs = append(errs, fmt.Errorf("bus.%s: %w", cfg.Bus.Driver, err))
		}
	}
	for i, s := range cfg.Sinks {
		d := describers.Sinks[SinkKey(s.Class, s.Driver)]
		if d == nil {
			continue
		}
		if err := validateBlock(d, s.Config, cfg.Router); err != nil {
			errs = append(errs, fmt.Errorf("sinks[%d] %q: %w", i, s.Name, err))
		}
	}
	return errors.Join(errs...)
}

func validateBlock(d Describer, node yaml.Node, router Router) error {
	typed, err := d.Describe(node)
	if err != nil {
		return err
	}
	if v, ok := typed.(DriverValidator); ok {
		return v.ValidateWith(router)
	}
	return nil
}

// sameListener reports whether two listen addresses would bind the same socket,
// treating an empty or unspecified host as matching any host on that port.
func sameListener(a, b string) bool {
	if a == b {
		return true
	}
	ha, pa, errA := net.SplitHostPort(a)
	hb, pb, errB := net.SplitHostPort(b)
	if errA != nil || errB != nil || pa != pb {
		return false
	}
	return ha == hb || unspecified(ha) || unspecified(hb)
}

func unspecified(host string) bool {
	if host == "" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}
