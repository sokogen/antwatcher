package stdout_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/log"
	"github.com/sokogen/antwatcher/internal/sink/log/drivers/stdout"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

func block(t *testing.T, text string) yaml.Node {
	t.Helper()
	var n yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(text), &n))
	if n.Kind == yaml.DocumentNode && len(n.Content) == 1 {
		return *n.Content[0]
	}
	return n
}

func fixtureEnvelope(t *testing.T, fixture string) event.Envelope {
	t.Helper()
	name, _, _ := strings.Cut(fixture, ".")
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, name)
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), received)
	require.NoError(t, err)
	return env
}

func fixtureRecord(t *testing.T, fixture string) otlp.LogRecord {
	t.Helper()
	exec, err := model.Normalize(fixtureEnvelope(t, fixture))
	require.NoError(t, err)
	return log.Project(exec)
}

// captureStream swaps *stream (os.Stdout or os.Stderr) for a pipe and returns
// a function that restores it and yields what was written.
func captureStream(t *testing.T, stream **os.File) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := *stream
	*stream = w
	var (
		once sync.Once
		out  string
	)
	return func() string {
		once.Do(func() {
			*stream = orig
			require.NoError(t, w.Close())
			data, err := io.ReadAll(r)
			require.NoError(t, err)
			require.NoError(t, r.Close())
			out = string(data)
		})
		return out
	}
}

func TestDriver_RegisteredInDefaultRegistry(t *testing.T) {
	assert.Contains(t, sink.Drivers(sink.ClassLog), stdout.Name)
	d, ok := sink.Describers()[config.SinkKey("log", "stdout")]
	require.True(t, ok)
	typed, err := d.Describe(block(t, "stream: stderr\npretty: true"))
	require.NoError(t, err)
	assert.Equal(t, stdout.Config{Stream: stdout.StreamStderr, Pretty: true}, typed)

	typed, err = d.Describe(yaml.Node{})
	require.NoError(t, err)
	assert.Equal(t, stdout.DefaultConfig(), typed, "absent block yields defaults")
}

func TestConfig_Validate(t *testing.T) {
	require.NoError(t, stdout.DefaultConfig().Validate())
	require.NoError(t, stdout.Config{Stream: stdout.StreamStderr}.Validate())
	require.NoError(t, stdout.Config{Stream: stdout.StreamStdout}.ValidateWith(config.Router{}))
	err := stdout.Config{Stream: "file"}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `stream must be "stdout" or "stderr", got "file"`)
	require.Error(t, stdout.Config{}.Validate(), "an empty stream is invalid without defaults")
}

func TestFactory_ConfigErrors(t *testing.T) {
	cases := map[string]string{
		"unknown key":  "stream: stdout\ncolour: true",
		"bad stream":   "stream: syslog",
		"wrong type":   "pretty: sometimes",
		"not a map":    "- stdout",
		"empty stream": "stream: \"\"",
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := stdout.Factory(context.Background(), "console", block(t, text), sink.Deps{})
			require.Error(t, err)
			assert.Nil(t, s)
		})
	}
}

func TestFactory_WritesToStdout(t *testing.T) {
	read := captureStream(t, &os.Stdout)
	reg := sink.NewRegistry()
	reg.RegisterDriver(sink.ClassLog, stdout.Name, stdout.Driver())
	instances, err := reg.Build(context.Background(), []config.SinkConfig{{
		Name: "console", Class: "log", Driver: "stdout", StartFrom: "now",
	}}, sink.Deps{})
	require.NoError(t, err)
	require.Len(t, instances, 1)
	s := instances[0]
	assert.Equal(t, "console", s.Name())
	assert.Equal(t, sink.ClassLog, s.Class())
	assert.Equal(t, "stdout", s.Driver)

	require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed")))
	require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, "ping")))
	require.NoError(t, s.Close())
	out := read()

	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	require.Len(t, lines, 2, "one compact line per record: %q", out)
	var first stdout.Record
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	assert.Equal(t, "workflow_run completed: CI", first.Body)
	assert.Equal(t, "ERROR", first.Severity)
	var second stdout.Record
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &second))
	assert.Equal(t, "ping", second.Body)
}

func TestFactory_WritesToStderr(t *testing.T) {
	readOut := captureStream(t, &os.Stdout)
	readErr := captureStream(t, &os.Stderr)
	s, err := stdout.Factory(context.Background(), "console", block(t, "stream: stderr"), sink.Deps{})
	require.NoError(t, err)
	require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, "workflow_job.queued")))
	require.NoError(t, s.Close())
	assert.Empty(t, readOut(), "nothing on stdout")
	assert.Contains(t, readErr(), `"body":"workflow_job queued: test"`)
}

func TestWriter_CompactOutputShape(t *testing.T) {
	var buf bytes.Buffer
	w := stdout.NewWriter(&buf, false)
	rec := fixtureRecord(t, "workflow_job.completed")
	require.NoError(t, w.Write(context.Background(), rec))

	out := buf.String()
	assert.True(t, strings.HasSuffix(out, "\n"))
	assert.Equal(t, 1, strings.Count(out, "\n"), "compact mode is one line per record")

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &doc))
	assert.Equal(t, "2026-09-02T10:03:15Z", doc["time"])
	assert.Equal(t, "2026-09-02T10:05:00Z", doc["observed_time"])
	assert.Equal(t, "ERROR", doc["severity"])
	assert.InDelta(t, float64(otlp.SeverityError), doc["severity_number"], 0)
	assert.Equal(t, "workflow_job completed: test", doc["body"])
	assert.Equal(t, hex.EncodeToString(rec.TraceID[:]), doc["trace_id"])
	assert.Equal(t, hex.EncodeToString(rec.SpanID[:]), doc["span_id"])
	assert.Len(t, doc["trace_id"], 32)
	assert.Len(t, doc["span_id"], 16)
	attrs, ok := doc["attributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "sokogen/antwatcher", attrs[log.AttrRepository])
	assert.InDelta(t, float64(42000000001), attrs[log.AttrJobID], 0)
	assert.Equal(t, []any{"ubuntu-latest"}, attrs[log.AttrLabels])
	assert.InDelta(t, 1, attrs[log.AttrStepsFailed], 0)
	assert.Equal(t, []string{"attributes", "body", "observed_time", "severity", "severity_number", "span_id", "time", "trace_id"}, sortedKeys(doc))
}

func TestWriter_ZeroIDsAndTimesOmitted(t *testing.T) {
	var buf bytes.Buffer
	w := stdout.NewWriter(&buf, false)
	require.NoError(t, w.Write(context.Background(), fixtureRecord(t, "ping")))
	var doc map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &doc))
	assert.NotContains(t, doc, "trace_id")
	assert.NotContains(t, doc, "span_id")
	assert.Equal(t, "ping", doc["body"])
	assert.Equal(t, "INFO", doc["severity"])

	buf.Reset()
	require.NoError(t, w.Write(context.Background(), otlp.LogRecord{Body: "bare"}))
	doc = nil
	require.NoError(t, json.Unmarshal(buf.Bytes(), &doc))
	assert.Contains(t, doc, "time", "time is always present")
	assert.Empty(t, doc["time"], "empty when unknown")
	assert.NotContains(t, doc, "observed_time")
	assert.NotContains(t, doc, "attributes")
}

func TestWriter_Pretty(t *testing.T) {
	var buf bytes.Buffer
	w := stdout.NewWriter(&buf, true)
	require.NoError(t, w.Write(context.Background(), fixtureRecord(t, "workflow_run.completed")))
	require.NoError(t, w.Write(context.Background(), fixtureRecord(t, "ping")))
	out := buf.String()
	assert.True(t, strings.HasPrefix(out, "{\n  \""), "indented document")
	assert.True(t, strings.HasSuffix(out, "\n}\n"), "trailing newline after the last document")

	dec := json.NewDecoder(strings.NewReader(out))
	var docs []stdout.Record
	for dec.More() {
		var r stdout.Record
		require.NoError(t, dec.Decode(&r))
		docs = append(docs, r)
	}
	require.Len(t, docs, 2, "both documents decode back from the stream")
	assert.Equal(t, "workflow_run completed: CI", docs[0].Body)
	assert.Equal(t, "ping", docs[1].Body)
}

func TestWriter_EncodeMatchesRecord(t *testing.T) {
	rec := fixtureRecord(t, "workflow_run.completed")
	got := stdout.Encode(rec)
	assert.Equal(t, stdout.Record{
		Time:           "2026-09-02T10:03:20Z",
		ObservedTime:   "2026-09-02T10:05:00Z",
		Severity:       "ERROR",
		SeverityNumber: int(otlp.SeverityError),
		Body:           "workflow_run completed: CI",
		TraceID:        hex.EncodeToString(rec.TraceID[:]),
		SpanID:         hex.EncodeToString(rec.SpanID[:]),
		Attributes:     rec.Attributes,
	}, got)

	local := rec
	local.Time = time.Date(2026, 9, 2, 12, 3, 20, 500, time.FixedZone("x", 2*3600))
	assert.Equal(t, "2026-09-02T10:03:20.0000005Z", stdout.Encode(local).Time, "times are UTC with nanoseconds")
}

func TestWriter_WriteErrorIsRetryable(t *testing.T) {
	w := stdout.NewWriter(&failingWriter{err: errors.New("broken pipe")}, false)
	err := w.Write(context.Background(), fixtureRecord(t, "ping"))
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err), "a stream that cannot be written now may accept the record later")
	assert.Contains(t, err.Error(), "broken pipe")
}

func TestWriter_WriteHonoursContext(t *testing.T) {
	var buf bytes.Buffer
	w := stdout.NewWriter(&buf, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, w.Write(ctx, fixtureRecord(t, "ping")), context.Canceled)
	assert.Empty(t, buf.String(), "nothing is written once ctx is already done")
}

func TestWriter_UnencodableRecordIsPermanent(t *testing.T) {
	var buf bytes.Buffer
	w := stdout.NewWriter(&buf, false)
	err := w.Write(context.Background(), otlp.LogRecord{Body: "x", Attributes: map[string]any{"ch": make(chan int)}})
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err), "%v", err)
	assert.Empty(t, buf.String(), "nothing partial is written")
}

func TestWriter_ConcurrentWritesDoNotInterleave(t *testing.T) {
	var buf bytes.Buffer
	w := stdout.NewWriter(&buf, false)
	rec := fixtureRecord(t, "workflow_job.completed")
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, w.Write(context.Background(), rec))
		}()
	}
	wg.Wait()
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	require.Len(t, lines, n)
	for _, line := range lines {
		var r stdout.Record
		require.NoError(t, json.Unmarshal([]byte(line), &r), "every line is a whole document")
		assert.Equal(t, rec.Body, r.Body)
	}
}

func TestWriter_CloseKeepsStreamOpen(t *testing.T) {
	var buf bytes.Buffer
	w := stdout.NewWriter(&buf, false)
	require.NoError(t, w.Close())
	require.NoError(t, w.Write(context.Background(), fixtureRecord(t, "ping")), "the process stream is still usable")
}

type failingWriter struct {
	err error
}

func (f *failingWriter) Write([]byte) (int, error) { return 0, f.err }

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
