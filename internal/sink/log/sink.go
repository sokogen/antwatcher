package log

import (
	"context"
	"fmt"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
)

// Writer is the driver contract of the log class: deliver one record to a
// log destination. Write returns nil once the record is accepted, an error
// wrapped by sink.Permanent when a retry cannot help, and any other error
// when the write should be retried later. It must be safe for concurrent
// use. Close is called once, after the last write.
type Writer interface {
	Write(ctx context.Context, rec otlp.LogRecord) error
	Close() error
}

// Sink is the log class bound to one writer. It implements sink.Sink.
type Sink struct {
	name   string
	writer Writer
}

// New builds the log sink named name on top of writer. It panics on a nil
// writer: that is a driver bug, not a runtime condition.
func New(name string, writer Writer) *Sink {
	if writer == nil {
		panic("log: New(" + name + ") with nil writer")
	}
	return &Sink{name: name, writer: writer}
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Class implements sink.Sink; it is always sink.ClassLog.
func (s *Sink) Class() sink.Class { return sink.ClassLog }

// Process normalizes the envelope, projects it, and writes the record.
//
// A payload that does not decode is permanent (redelivering the same bytes
// cannot fix it). Every well-formed event produces a record, so Process never
// returns sink.ErrSkipped. Writer errors are returned as they are, so the
// driver's classification decides between redelivery and stalling.
func (s *Sink) Process(ctx context.Context, env event.Envelope) error {
	exec, err := model.Normalize(env)
	if err != nil {
		return sink.Permanent(err)
	}
	if err := s.writer.Write(ctx, Project(exec)); err != nil {
		return fmt.Errorf("write log record of %s %q: %w", env.Event, env.DeliveryGUID, err)
	}
	return nil
}

// Close implements sink.Sink by closing the writer.
func (s *Sink) Close() error { return s.writer.Close() }
