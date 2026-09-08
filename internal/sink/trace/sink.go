package trace

import (
	"context"
	"fmt"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
)

// Exporter is the driver contract of the trace class: deliver spans to a
// trace backend. ExportSpans returns nil once the spans are accepted, an error
// wrapped by sink.Permanent when a retry cannot help, and any other error
// when the export should be retried later. It must be safe for concurrent
// use. Close is called once, after the last export.
type Exporter interface {
	ExportSpans(ctx context.Context, spans []otlp.Span) error
	Close() error
}

// Sink is the trace class bound to one exporter. It implements sink.Sink.
type Sink struct {
	name     string
	exporter Exporter
}

// New builds the trace sink named name on top of exporter. It panics on a nil
// exporter: that is a driver bug, not a runtime condition.
func New(name string, exporter Exporter) *Sink {
	if exporter == nil {
		panic("trace: New(" + name + ") with nil exporter")
	}
	return &Sink{name: name, exporter: exporter}
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Class implements sink.Sink; it is always sink.ClassTrace.
func (s *Sink) Class() sink.Class { return sink.ClassTrace }

// Process normalizes the envelope, projects it, and exports the spans.
//
// A payload that does not decode is permanent (redelivering the same bytes
// cannot fix it). An execution that contributes no spans returns
// sink.ErrSkipped without touching the exporter. Exporter errors are returned
// as they are, so the driver's classification decides between redelivery
// and stalling.
func (s *Sink) Process(ctx context.Context, env event.Envelope) error {
	exec, err := model.Normalize(env)
	if err != nil {
		return sink.Permanent(err)
	}
	spans, err := Project(exec)
	if err != nil {
		return err
	}
	if err := s.exporter.ExportSpans(ctx, spans); err != nil {
		return fmt.Errorf("export %d span(s) of %s %q: %w", len(spans), env.Event, env.DeliveryGUID, err)
	}
	return nil
}

// Close implements sink.Sink by closing the exporter.
func (s *Sink) Close() error { return s.exporter.Close() }
