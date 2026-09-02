// Package archive is the destination class that stores every event as it
// arrived: the envelope metadata and the raw GitHub payload, without
// normalization. It defines the driver contract (Writer), the record format
// shared by file-based drivers (one JSON document per line, see Encode and
// Decode), and the JSONL store that appends, fsyncs, rotates, and compresses
// files on any filesystem (Store). Drivers such as archive/filesystem only
// choose where the store lives.
package archive

import (
	"context"
	"fmt"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
)

// Writer is the driver contract of the archive class: keep one envelope.
// Append returns nil once the envelope is durable at the destination (the
// message is acked only then), an error wrapped by sink.Permanent when a
// retry cannot help, and any other error when the append should be retried
// later. It must be safe for concurrent use. Close is called once, after the
// last append, and finalizes whatever the destination keeps open.
type Writer interface {
	Append(ctx context.Context, env event.Envelope) error
	Close() error
}

// Sink is the archive class bound to one writer. It implements sink.Sink.
type Sink struct {
	name   string
	writer Writer
}

// New builds the archive sink named name on top of writer. It panics on a
// nil writer: that is a driver bug, not a runtime condition.
func New(name string, writer Writer) *Sink {
	if writer == nil {
		panic("archive: New(" + name + ") with nil writer")
	}
	return &Sink{name: name, writer: writer}
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Class implements sink.Sink; it is always sink.ClassArchive.
func (s *Sink) Class() sink.Class { return sink.ClassArchive }

// Process hands the envelope to the writer as it is: the archive class never
// parses the payload, so an event the canonical model cannot read is still
// archived. Nothing is skipped, and writer errors are returned as they are,
// so the driver's classification decides between redelivery and stalling.
func (s *Sink) Process(ctx context.Context, env event.Envelope) error {
	if err := s.writer.Append(ctx, env); err != nil {
		return fmt.Errorf("archive %s %q: %w", env.Event, env.DeliveryGUID, err)
	}
	return nil
}

// Close implements sink.Sink by closing the writer.
func (s *Sink) Close() error { return s.writer.Close() }
