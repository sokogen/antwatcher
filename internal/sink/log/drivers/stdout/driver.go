// Package stdout is the "stdout" driver of the log class: it prints the
// records projected by the log package as one JSON document per line to the
// process's standard output or standard error, for local use and for
// container platforms that collect the process streams.
//
// The driver registers itself as log/stdout on import. Its configuration
// block is Config: stream (stdout or stderr) and pretty (indented JSON, one
// document per record, for humans). A write is synchronous: the record is
// acked once the write call returned, and a failed write is retryable.
package stdout

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/log"
)

// Name is the driver name in configuration (sinks[].driver).
const Name = "stdout"

// Streams accepted by Config.Stream.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// Config is the sinks[].config block of the driver.
type Config struct {
	// Stream selects the process stream: "stdout" (default) or "stderr".
	Stream string `yaml:"stream"`
	// Pretty indents every document; the default is one compact line per
	// record, which is what log collectors expect.
	Pretty bool `yaml:"pretty"`
}

// DefaultConfig is the block with every default applied.
func DefaultConfig() Config {
	return Config{Stream: StreamStdout}
}

// Validate checks the block on its own.
func (c Config) Validate() error {
	switch c.Stream {
	case StreamStdout, StreamStderr:
		return nil
	default:
		return fmt.Errorf("stream must be %q or %q, got %q", StreamStdout, StreamStderr, c.Stream)
	}
}

// ValidateWith implements config.DriverValidator so `-check` reports an
// invalid block; the driver does not depend on the router settings.
func (c Config) ValidateWith(config.Router) error { return c.Validate() }

// Describe decodes the block strictly on top of DefaultConfig; see
// config.Describer.
func Describe(raw yaml.Node) (any, error) {
	c := DefaultConfig()
	if err := config.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// Describer is Describe as a config.Describer.
var Describer config.Describer = config.DescriberFunc(Describe)

func init() {
	sink.RegisterDriver(sink.ClassLog, Name, Driver())
}

// Driver returns the driver descriptor, for registration in custom registries.
func Driver() sink.Driver {
	return sink.Driver{Factory: Factory, Describer: Describer}
}

// Factory builds a log sink printing to the configured stream. The streams
// are resolved when the sink is built, so a test may swap os.Stdout before
// calling it.
func Factory(_ context.Context, name string, raw yaml.Node, _ sink.Deps) (sink.Sink, error) {
	typed, err := Describe(raw)
	if err != nil {
		return nil, err
	}
	cfg := typed.(Config) //nolint:errcheck // Describe returns Config
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	out := os.Stdout
	if cfg.Stream == StreamStderr {
		out = os.Stderr
	}
	return log.New(name, NewWriter(out, cfg.Pretty)), nil
}

// Writer is the log.Writer printing JSON documents to an io.Writer. Writes
// are serialised so concurrent records never interleave.
type Writer struct {
	mu     sync.Mutex
	out    io.Writer
	pretty bool
}

// NewWriter returns a Writer printing to out, indented when pretty is set.
func NewWriter(out io.Writer, pretty bool) *Writer {
	return &Writer{out: out, pretty: pretty}
}

// Record is the JSON document written per record. Times are RFC 3339 with
// nanoseconds in UTC; the ids are lowercase hex and omitted when zero.
type Record struct {
	Time           string         `json:"time"`
	ObservedTime   string         `json:"observed_time,omitempty"`
	Severity       string         `json:"severity"`
	SeverityNumber int            `json:"severity_number"`
	Body           string         `json:"body"`
	TraceID        string         `json:"trace_id,omitempty"`
	SpanID         string         `json:"span_id,omitempty"`
	Attributes     map[string]any `json:"attributes,omitempty"`
}

// Encode renders rec as the JSON document Write prints.
func Encode(rec otlp.LogRecord) Record {
	out := Record{
		Time:           formatTime(rec.Time),
		ObservedTime:   formatTime(rec.ObservedTime),
		Severity:       rec.SeverityText,
		SeverityNumber: int(rec.Severity),
		Body:           rec.Body,
		Attributes:     rec.Attributes,
	}
	if rec.TraceID != [16]byte{} {
		out.TraceID = hex.EncodeToString(rec.TraceID[:])
	}
	if rec.SpanID != [8]byte{} {
		out.SpanID = hex.EncodeToString(rec.SpanID[:])
	}
	return out
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// Write implements log.Writer. The document and its trailing newline go out
// in one call, and any error is retryable: a stream that cannot be written
// right now (a full pipe, a closed collector) may accept the record later.
func (w *Writer) Write(_ context.Context, rec otlp.LogRecord) error {
	var (
		data []byte
		err  error
	)
	if w.pretty {
		data, err = json.MarshalIndent(Encode(rec), "", "  ")
	} else {
		data, err = json.Marshal(Encode(rec))
	}
	if err != nil {
		// attributes come from the projection and always encode; anything
		// else is a bug in the record, not in the destination
		return sink.Permanent(fmt.Errorf("encode log record: %w", err))
	}
	data = append(data, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.out.Write(data); err != nil {
		return fmt.Errorf("write log record: %w", err)
	}
	return nil
}

// Close implements log.Writer. The process streams stay open.
func (w *Writer) Close() error { return nil }
