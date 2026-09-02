package bus

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/config"
)

func TestParseStartPosition(t *testing.T) {
	p, err := ParseStartPosition(config.StartFromEarliest)
	require.NoError(t, err)
	assert.Equal(t, Earliest, p)

	p, err = ParseStartPosition(config.StartFromNow)
	require.NoError(t, err)
	assert.Equal(t, Now, p)

	_, err = ParseStartPosition("")
	require.Error(t, err)
	_, err = ParseStartPosition("latest")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"latest"`)
}

func TestStartPosition_ZeroIsInvalid(t *testing.T) {
	var p StartPosition
	assert.False(t, p.valid())
	assert.True(t, Earliest.valid())
	assert.True(t, Now.valid())
	assert.Equal(t, "earliest", Earliest.String())
	assert.Equal(t, "now", Now.String())
	assert.Equal(t, "StartPosition(0)", p.String())

	out, err := json.Marshal(struct {
		From StartPosition `json:"from"`
	}{Now})
	require.NoError(t, err)
	assert.JSONEq(t, `{"from":"now"}`, string(out))
}

func TestValidateConsumerName(t *testing.T) {
	for _, ok := range []string{"sink-tempo", "a", "A_b-9"} {
		require.NoError(t, ValidateConsumerName(ok), ok)
	}
	for _, bad := range []string{"", " ", "a b", "a.b", "a*", "a>", "тест"} {
		err := ValidateConsumerName(bad)
		require.Error(t, err, "%q", bad)
		assert.ErrorIs(t, err, ErrInvalidConsumerName, "%q", bad)
	}
}

func TestCapabilities_String(t *testing.T) {
	c := Capabilities{FanOut: true, HistoricalReplay: true, Retention: RetentionNone}
	assert.Equal(t,
		"durable_publish=false durable_consumers=false fan_out=true historical_replay=true deduplicates=false reports_lag=false retention=none",
		c.String())
	c = Capabilities{DurablePublish: true, DurableConsumers: true, FanOut: true, HistoricalReplay: true, Deduplicates: true, ReportsLag: true, Retention: RetentionTime}
	assert.Equal(t,
		"durable_publish=true durable_consumers=true fan_out=true historical_replay=true deduplicates=true reports_lag=true retention=time",
		c.String())
}

func TestCapabilities_JSON(t *testing.T) {
	c := Capabilities{DurablePublish: true, FanOut: true, Retention: RetentionUntilAcked}
	out, err := json.Marshal(c)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"durable_publish": true, "durable_consumers": false, "fan_out": true,
		"historical_replay": false, "deduplicates": false, "reports_lag": false,
		"retention": "until_acked"
	}`, string(out))
}

func TestRetentionKind_String(t *testing.T) {
	assert.Equal(t, "none", RetentionNone.String())
	assert.Equal(t, "time", RetentionTime.String())
	assert.Equal(t, "size", RetentionSize.String())
	assert.Equal(t, "until_acked", RetentionUntilAcked.String())
	assert.Equal(t, "RetentionKind(9)", RetentionKind(9).String())
}
