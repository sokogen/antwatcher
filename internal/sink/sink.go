// Package sink defines the destination side of antwatcher: the Sink contract
// every destination class implements, the driver registry that builds sinks
// from configuration, and the router that binds each sink to its own bus
// consumer.
//
// Each configured sink is an independent named consumer of the bus. A Sink
// receives every event at least once and classifies failures: a retryable
// error is Nacked and redelivered by the broker with a growing delay; a
// permanent error (see Permanent) is recorded as stalled for that sink and
// Nacked as well, so nothing is discarded and the message is re-attempted at
// the broker's pace until the cause is fixed. Sinks are idempotent through the
// deterministic IDs of the canonical model, so redelivery is always safe.
package sink

import (
	"context"
	"fmt"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/event"
)

// Class is a destination class: what a sink projects the event into. Every
// class owns its projection and a small driver contract; drivers of the same
// class share the projection.
type Class int

// Destination classes, as spelled in sinks[].class.
const (
	// ClassTrace projects completed runs and jobs into spans.
	ClassTrace Class = iota + 1
	// ClassLog projects every event into one log record.
	ClassLog
	// ClassAnalytics projects run, job, and step records for a warehouse.
	ClassAnalytics
	// ClassArchive stores the raw envelope.
	ClassArchive
	// ClassForward re-publishes the raw envelope on another bus or topic.
	ClassForward
)

// Configuration spellings of the classes.
const (
	classTrace     = "trace"
	classLog       = "log"
	classAnalytics = "analytics"
	classArchive   = "archive"
	classForward   = "forward"
)

// Classes lists every class in configuration spelling, sorted by value.
func Classes() []string {
	return []string{classTrace, classLog, classAnalytics, classArchive, classForward}
}

// ParseClass maps a sinks[].class value to a Class.
func ParseClass(s string) (Class, error) {
	switch s {
	case classTrace:
		return ClassTrace, nil
	case classLog:
		return ClassLog, nil
	case classAnalytics:
		return ClassAnalytics, nil
	case classArchive:
		return ClassArchive, nil
	case classForward:
		return ClassForward, nil
	default:
		return 0, fmt.Errorf("unknown sink class %q (known: %s)", s, joinNames(Classes()))
	}
}

// String returns the configuration spelling of the class.
func (c Class) String() string {
	switch c {
	case ClassTrace:
		return classTrace
	case ClassLog:
		return classLog
	case ClassAnalytics:
		return classAnalytics
	case ClassArchive:
		return classArchive
	case ClassForward:
		return classForward
	default:
		return fmt.Sprintf("Class(%d)", int(c))
	}
}

// MarshalText renders the class by name in JSON and YAML.
func (c Class) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// valid reports whether c is one of the defined classes.
func (c Class) valid() bool {
	return c >= ClassTrace && c <= ClassForward
}

// Sink is one configured destination. Implementations are built by a driver
// through the registry and must be safe for concurrent use: the router may
// call Process for several messages at once.
type Sink interface {
	// Name is the sink name from configuration; it also names the bus
	// consumer ("sink-<name>").
	Name() string
	// Class is the destination class the sink belongs to.
	Class() Class
	// Process handles one event. It returns nil once the event is durably
	// handed to the destination (the message is then acked), ErrSkipped when
	// the projection produces nothing for this event (acked as well), an
	// error wrapped by Permanent when retrying cannot help, and any other
	// error when the destination should be retried later. ctx carries the
	// router's process timeout; a call that outlives it must return ctx.Err().
	Process(ctx context.Context, env event.Envelope) error
	// Close releases the destination. It is called once, after the router has
	// stopped delivering.
	Close() error
}

// Instance is a built sink together with the start position requested for
// its consumer and the driver that built it.
type Instance struct {
	Sink
	// StartFrom is the position requested in configuration. The router
	// resolves it against the bus capabilities (bus.ResolveConsumer).
	StartFrom bus.StartPosition
	// Driver is the driver name from configuration, shown in /status.
	Driver string
}

// ConsumerName is the bus consumer name of a sink. Renaming a sink therefore
// creates a new consumer with a fresh position.
func ConsumerName(sinkName string) string {
	return "sink-" + sinkName
}
