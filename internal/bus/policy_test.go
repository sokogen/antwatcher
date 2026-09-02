package bus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/config"
)

// full declares every capability; the tables below clear one at a time.
var full = Capabilities{
	DurablePublish: true, DurableConsumers: true, FanOut: true,
	HistoricalReplay: true, Deduplicates: true, ReportsLag: true, Retention: RetentionTime,
}

func TestParseMode(t *testing.T) {
	m, err := ParseMode(config.OnMissingCapabilityFail)
	require.NoError(t, err)
	assert.Equal(t, ModeFail, m)

	m, err = ParseMode(config.OnMissingCapabilityDegrade)
	require.NoError(t, err)
	assert.Equal(t, ModeDegrade, m)

	_, err = ParseMode("maybe")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"maybe"`)

	assert.Equal(t, "fail", ModeFail.String())
	assert.Equal(t, "degrade", ModeDegrade.String())
	assert.Equal(t, "Mode(0)", Mode(0).String())
}

func TestResolveIngress(t *testing.T) {
	without := func(mutate func(*Capabilities)) Capabilities {
		c := full
		mutate(&c)
		return c
	}
	tests := []struct {
		name     string
		caps     Capabilities
		mode     Mode
		wantErr  bool
		wantWarn []string // capability names
	}{
		{name: "durable/fail", caps: full, mode: ModeFail},
		{name: "durable/degrade", caps: full, mode: ModeDegrade},
		{name: "non-durable/fail", caps: without(func(c *Capabilities) { c.DurablePublish = false }), mode: ModeFail, wantErr: true},
		{name: "non-durable/degrade", caps: without(func(c *Capabilities) { c.DurablePublish = false }), mode: ModeDegrade, wantWarn: []string{"DurablePublish"}},
		// Consumer-side capabilities never matter for ingress.
		{name: "no consumers/fail", caps: without(func(c *Capabilities) {
			c.DurableConsumers, c.FanOut, c.HistoricalReplay, c.Deduplicates, c.ReportsLag = false, false, false, false, false
		}), mode: ModeFail},
		{name: "invalid mode", caps: full, mode: Mode(0), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warnings, err := ResolveIngress(tt.caps, tt.mode)
			if tt.wantErr {
				require.Error(t, err)
				if tt.mode != Mode(0) {
					require.ErrorIs(t, err, ErrMissingCapability)
					assert.Contains(t, err.Error(), "DurablePublish")
					assert.Contains(t, err.Error(), "on_missing_capability: degrade")
				}
				assert.Nil(t, warnings)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantWarn, capabilitiesOf(warnings))
			for _, w := range warnings {
				assert.Equal(t, SeverityWarning, w.Severity)
				assert.Contains(t, w.Message, "2xx does not mean durable")
				assert.Contains(t, w.Message, "dev only")
			}
		})
	}
}

func TestResolveConsumer(t *testing.T) {
	without := func(mutate func(*Capabilities)) Capabilities {
		c := full
		mutate(&c)
		return c
	}
	noFanOut := without(func(c *Capabilities) { c.FanOut = false })
	noReplay := without(func(c *Capabilities) { c.HistoricalReplay = false })
	noDurable := without(func(c *Capabilities) { c.DurableConsumers = false })
	noDedup := without(func(c *Capabilities) { c.Deduplicates = false })
	nothing := Capabilities{FanOut: true}

	tests := []struct {
		name          string
		caps          Capabilities
		requested     StartPosition
		mode          Mode
		wantEffective StartPosition
		wantWarn      []string
		wantSeverity  []Severity
		wantErr       string // substring
	}{
		{name: "full/earliest/fail", caps: full, requested: Earliest, mode: ModeFail, wantEffective: Earliest},
		{name: "full/now/fail", caps: full, requested: Now, mode: ModeFail, wantEffective: Now},
		{name: "full/earliest/degrade", caps: full, requested: Earliest, mode: ModeDegrade, wantEffective: Earliest},
		{name: "full/now/degrade", caps: full, requested: Now, mode: ModeDegrade, wantEffective: Now},

		{name: "no fanout/earliest/fail", caps: noFanOut, requested: Earliest, mode: ModeFail, wantErr: "FanOut"},
		{name: "no fanout/now/fail", caps: noFanOut, requested: Now, mode: ModeFail, wantErr: "FanOut"},
		{name: "no fanout/earliest/degrade", caps: noFanOut, requested: Earliest, mode: ModeDegrade, wantErr: "FanOut"},
		{name: "no fanout/now/degrade", caps: noFanOut, requested: Now, mode: ModeDegrade, wantErr: "FanOut"},

		{name: "no replay/earliest/fail", caps: noReplay, requested: Earliest, mode: ModeFail, wantErr: "HistoricalReplay"},
		{name: "no replay/now/fail", caps: noReplay, requested: Now, mode: ModeFail, wantEffective: Now},
		{name: "no replay/earliest/degrade", caps: noReplay, requested: Earliest, mode: ModeDegrade, wantEffective: Now,
			wantWarn: []string{"HistoricalReplay"}, wantSeverity: []Severity{SeverityWarning}},
		{name: "no replay/now/degrade", caps: noReplay, requested: Now, mode: ModeDegrade, wantEffective: Now},

		{name: "no durable/earliest/fail", caps: noDurable, requested: Earliest, mode: ModeFail, wantEffective: Earliest,
			wantWarn: []string{"DurableConsumers"}, wantSeverity: []Severity{SeverityWarning}},
		{name: "no durable/now/fail", caps: noDurable, requested: Now, mode: ModeFail, wantEffective: Now,
			wantWarn: []string{"DurableConsumers"}, wantSeverity: []Severity{SeverityWarning}},
		{name: "no durable/earliest/degrade", caps: noDurable, requested: Earliest, mode: ModeDegrade, wantEffective: Earliest,
			wantWarn: []string{"DurableConsumers"}, wantSeverity: []Severity{SeverityWarning}},
		{name: "no durable/now/degrade", caps: noDurable, requested: Now, mode: ModeDegrade, wantEffective: Now,
			wantWarn: []string{"DurableConsumers"}, wantSeverity: []Severity{SeverityWarning}},

		{name: "no dedup/earliest/fail", caps: noDedup, requested: Earliest, mode: ModeFail, wantEffective: Earliest,
			wantWarn: []string{"Deduplicates"}, wantSeverity: []Severity{SeverityInfo}},
		{name: "no dedup/now/degrade", caps: noDedup, requested: Now, mode: ModeDegrade, wantEffective: Now,
			wantWarn: []string{"Deduplicates"}, wantSeverity: []Severity{SeverityInfo}},

		{name: "nothing/earliest/fail", caps: nothing, requested: Earliest, mode: ModeFail, wantErr: "HistoricalReplay"},
		{name: "nothing/earliest/degrade", caps: nothing, requested: Earliest, mode: ModeDegrade, wantEffective: Now,
			wantWarn:     []string{"HistoricalReplay", "DurableConsumers", "Deduplicates"},
			wantSeverity: []Severity{SeverityWarning, SeverityWarning, SeverityInfo}},
		{name: "nothing/now/fail", caps: nothing, requested: Now, mode: ModeFail, wantEffective: Now,
			wantWarn:     []string{"DurableConsumers", "Deduplicates"},
			wantSeverity: []Severity{SeverityWarning, SeverityInfo}},

		{name: "invalid position", caps: full, requested: StartPosition(0), mode: ModeFail, wantErr: "invalid start position"},
		{name: "invalid mode", caps: full, requested: Now, mode: Mode(9), wantErr: "invalid policy mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effective, warnings, err := ResolveConsumer(tt.caps, tt.requested, tt.mode)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Equal(t, StartPosition(0), effective)
				assert.Nil(t, warnings)
				if tt.wantErr == "FanOut" || tt.wantErr == "HistoricalReplay" {
					require.ErrorIs(t, err, ErrMissingCapability)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantEffective, effective)
			assert.Equal(t, tt.wantWarn, capabilitiesOf(warnings))
			assert.Equal(t, tt.wantSeverity, severitiesOf(warnings))
		})
	}
}

func TestResolveConsumer_FanOutErrorSaysCannotDegrade(t *testing.T) {
	_, _, err := ResolveConsumer(Capabilities{}, Now, ModeDegrade)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be degraded")
}

func TestWarningString(t *testing.T) {
	w := Warning{Capability: "DurablePublish", Severity: SeverityWarning, Message: "dev only"}
	assert.Equal(t, "warning: DurablePublish: dev only", w.String())
	assert.Equal(t, "info", SeverityInfo.String())
	assert.Equal(t, "Severity(0)", Severity(0).String())
	text, err := SeverityInfo.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "info", string(text))
}

func capabilitiesOf(ws []Warning) []string {
	if ws == nil {
		return nil
	}
	out := make([]string, 0, len(ws))
	for _, w := range ws {
		out = append(out, w.Capability)
	}
	return out
}

func severitiesOf(ws []Warning) []Severity {
	if ws == nil {
		return nil
	}
	out := make([]Severity, 0, len(ws))
	for _, w := range ws {
		out = append(out, w.Severity)
	}
	return out
}
