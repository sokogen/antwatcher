package sink_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/sink"
)

func TestClass(t *testing.T) {
	want := map[string]sink.Class{
		"trace": sink.ClassTrace, "log": sink.ClassLog, "analytics": sink.ClassAnalytics,
		"archive": sink.ClassArchive, "forward": sink.ClassForward,
	}
	assert.Len(t, sink.Classes(), len(want))
	for _, name := range sink.Classes() {
		c, err := sink.ParseClass(name)
		require.NoError(t, err)
		assert.Equal(t, want[name], c)
		assert.Equal(t, name, c.String())
		text, err := json.Marshal(c)
		require.NoError(t, err)
		assert.JSONEq(t, `"`+name+`"`, string(text))
	}

	_, err := sink.ParseClass("metrics")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown sink class "metrics"`)
	assert.Contains(t, err.Error(), "trace, log, analytics, archive, forward")

	assert.Equal(t, "Class(0)", sink.Class(0).String())
	assert.Equal(t, "Class(9)", sink.Class(9).String())
}

func TestConsumerName(t *testing.T) {
	assert.Equal(t, "sink-tempo", sink.ConsumerName("tempo"))
}
