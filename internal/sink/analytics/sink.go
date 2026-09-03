package analytics

import (
	"context"
	"fmt"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/sink"
)

// Writer is the driver contract of the analytics class.
//
// EnsureSchema creates or verifies the base table and its current view for
// schema. The class calls it lazily before the first write (see Sink), never
// from a constructor. Write appends records to the base table and returns
// nil once they are durably accepted. Both return an error wrapped by
// sink.Permanent when a retry cannot help (schema mismatch, permission
// denied) and any other error when the call should be retried later. Both
// must be safe for concurrent use. Close is called once, after the last
// write.
//
// The contract is at-least-once: the same record (same record_id, same
// delivery) may be written more than once after a redelivery, and the same
// entity is written once per lifecycle event. Drivers append and never
// upsert; consumers query the "<table>_current" view (CurrentViewSQL) for
// the latest state per entity.
type Writer interface {
	EnsureSchema(ctx context.Context, schema Schema) error
	Write(ctx context.Context, records []Record) error
	Close() error
}

// Sink is the analytics class bound to one writer. It implements sink.Sink.
type Sink struct {
	name   string
	writer Writer
	ensure bool

	ensured sink.SingleFlight[struct{}]
}

// New builds the analytics sink named name on top of writer. When ensure is
// true the sink calls writer.EnsureSchema with Current before its first
// write. It panics on a nil writer: that is a driver bug, not a runtime
// condition.
func New(name string, writer Writer, ensure bool) *Sink {
	if writer == nil {
		panic("analytics: New(" + name + ") with nil writer")
	}
	return &Sink{name: name, writer: writer, ensure: ensure}
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Class implements sink.Sink; it is always sink.ClassAnalytics.
func (s *Sink) Class() sink.Class { return sink.ClassAnalytics }

// Process normalizes the envelope, projects it, and writes the records.
//
// A payload that does not decode is permanent (redelivering the same bytes
// cannot fix it). An event without a modelled entity returns sink.ErrSkipped
// without touching the writer. When schema management is enabled, the first
// call that has records to write runs EnsureSchema first; its error is
// returned (classified by the driver, so the message is redelivered or
// stalled like a failed write) and the call is repeated on later events
// until it succeeds, after which it never runs again. An unreachable
// warehouse at startup therefore degrades this sink instead of failing the
// process. Writer errors are returned as they are.
func (s *Sink) Process(ctx context.Context, env event.Envelope) error {
	exec, err := model.Normalize(env)
	if err != nil {
		return sink.Permanent(err)
	}
	records, err := Project(exec)
	if err != nil {
		return err
	}
	if err := s.ensureSchema(ctx); err != nil {
		return fmt.Errorf("ensure analytics schema before %s %q: %w", env.Event, env.DeliveryGUID, err)
	}
	if err := s.writer.Write(ctx, records); err != nil {
		return fmt.Errorf("write %d analytics record(s) of %s %q: %w", len(records), env.Event, env.DeliveryGUID, err)
	}
	return nil
}

// ensureSchema runs EnsureSchema once, serialising concurrent first calls so
// the writer sees a single attempt at a time. Waiters behind an in-flight
// attempt honor their own ctx instead of blocking on it: Process must return
// within router.process_timeout regardless of how long another goroutine's
// EnsureSchema call takes.
func (s *Sink) ensureSchema(ctx context.Context) error {
	if !s.ensure {
		return nil
	}
	_, err := s.ensured.Do(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.writer.EnsureSchema(ctx, Current)
	})
	return err
}

// Close implements sink.Sink by closing the writer.
func (s *Sink) Close() error { return s.writer.Close() }
