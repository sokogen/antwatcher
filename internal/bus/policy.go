package bus

import (
	"errors"
	"fmt"

	"github.com/sokogen/antwatcher/internal/config"
)

// Mode is the bus.on_missing_capability policy: what to do when the driver lacks
// a capability the configuration needs.
type Mode int

const (
	// ModeFail refuses to start.
	ModeFail Mode = iota + 1
	// ModeDegrade starts with a warning shown in /status.
	ModeDegrade
)

// ParseMode maps a bus.on_missing_capability value to a Mode.
func ParseMode(s string) (Mode, error) {
	switch s {
	case config.OnMissingCapabilityFail:
		return ModeFail, nil
	case config.OnMissingCapabilityDegrade:
		return ModeDegrade, nil
	default:
		return 0, fmt.Errorf("on_missing_capability must be %q or %q (got %q)", config.OnMissingCapabilityFail, config.OnMissingCapabilityDegrade, s)
	}
}

// String returns the configuration spelling of the mode.
func (m Mode) String() string {
	switch m {
	case ModeFail:
		return config.OnMissingCapabilityFail
	case ModeDegrade:
		return config.OnMissingCapabilityDegrade
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// ErrMissingCapability is wrapped by every policy error.
var ErrMissingCapability = errors.New("bus lacks a required capability")

// Severity of a policy Warning.
type Severity int

const (
	// SeverityWarning marks a guarantee the operator loses.
	SeverityWarning Severity = iota + 1
	// SeverityInfo marks a difference that costs nothing because the
	// architecture compensates for it.
	SeverityInfo
)

// String returns the lowercase severity name.
func (s Severity) String() string {
	switch s {
	case SeverityWarning:
		return "warning"
	case SeverityInfo:
		return "info"
	default:
		return fmt.Sprintf("Severity(%d)", int(s))
	}
}

// MarshalText renders the severity by name in JSON.
func (s Severity) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// Warning is one consequence of a missing capability that did not stop startup.
// Warnings are logged at startup and listed in /status.
type Warning struct {
	Capability string   `json:"capability"`
	Severity   Severity `json:"severity"`
	Message    string   `json:"message"`
}

// String renders the warning for logs.
func (w Warning) String() string {
	return fmt.Sprintf("%s: %s: %s", w.Severity, w.Capability, w.Message)
}

// ResolveIngress decides whether the receiver may answer GitHub 2xx on this bus.
// Without DurablePublish a 2xx does not mean the event is safe: ModeFail refuses
// to start, ModeDegrade starts with a warning.
func ResolveIngress(caps Capabilities, mode Mode) ([]Warning, error) {
	if err := checkMode(mode); err != nil {
		return nil, err
	}
	if caps.DurablePublish {
		return nil, nil
	}
	if mode == ModeFail {
		return nil, missing("DurablePublish", "the receiver would answer 2xx before the event is stored")
	}
	return []Warning{{
		Capability: "DurablePublish",
		Severity:   SeverityWarning,
		Message:    "2xx does not mean durable: events accepted from GitHub can be lost on restart, dev only",
	}}, nil
}

// ResolveConsumer decides how one sink consumer is created on this bus.
//
//   - FanOut=false is always an error: sinks must each receive every message.
//   - Earliest without HistoricalReplay: error in ModeFail, downgraded to Now
//     with a warning in ModeDegrade.
//   - DurableConsumers=false: warning that positions are lost on restart.
//   - Deduplicates=false: informational; sinks are idempotent through
//     deterministic IDs anyway.
//
// The returned position is the one to pass to Subscribe.
func ResolveConsumer(caps Capabilities, requested StartPosition, mode Mode) (StartPosition, []Warning, error) {
	if err := checkMode(mode); err != nil {
		return 0, nil, err
	}
	if !requested.valid() {
		return 0, nil, fmt.Errorf("invalid start position %s", requested)
	}
	if !caps.FanOut {
		return 0, nil, missing("FanOut", "every sink must receive every message; this cannot be degraded")
	}

	effective := requested
	var warnings []Warning
	if requested == Earliest && !caps.HistoricalReplay {
		if mode == ModeFail {
			return 0, nil, missing("HistoricalReplay", "start_from: earliest needs retained history")
		}
		effective = Now
		warnings = append(warnings, Warning{
			Capability: "HistoricalReplay",
			Severity:   SeverityWarning,
			Message:    "start_from: earliest downgraded to now: the bus keeps no history",
		})
	}
	if !caps.DurableConsumers {
		warnings = append(warnings, Warning{
			Capability: "DurableConsumers",
			Severity:   SeverityWarning,
			Message:    "consumer positions are lost on restart: the sink starts over from its start position",
		})
	}
	if !caps.Deduplicates {
		warnings = append(warnings, Warning{
			Capability: "Deduplicates",
			Severity:   SeverityInfo,
			Message:    "the bus does not collapse duplicate deliveries; sinks are idempotent through deterministic IDs",
		})
	}
	return effective, warnings, nil
}

func checkMode(mode Mode) error {
	switch mode {
	case ModeFail, ModeDegrade:
		return nil
	default:
		return fmt.Errorf("invalid policy mode %s", mode)
	}
}

func missing(capability, consequence string) error {
	return fmt.Errorf("%w: %s (%s); set bus.on_missing_capability: %s to start anyway where allowed",
		ErrMissingCapability, capability, consequence, config.OnMissingCapabilityDegrade)
}
