// Package bus defines the transport contract between the webhook receiver and
// the sinks: one topic of Watermill messages carried by a driver chosen in the
// configuration.
//
// Drivers differ in what they can guarantee. Those differences are declared once
// as Capabilities and resolved in one place (ResolveIngress, ResolveConsumer)
// against the configured policy, so the receiver, the router, and the sinks
// never branch on the driver. Every driver must pass the conformance suite in
// package bustest for each capability it declares.
package bus

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Bus is one topic carried by a driver. It is safe for concurrent use.
type Bus interface {
	// Publish delivers msg to the topic. It returns nil only once the driver's
	// durability guarantee holds: on a bus that declares DurablePublish the
	// message is stored by the broker; on any other bus nil means only that the
	// message was handed to the driver. Publish honors ctx: a driver that waits
	// for a broker acknowledgement returns ctx.Err() when ctx ends first.
	// After Close every call returns ErrClosed.
	Publish(ctx context.Context, msg *message.Message) error

	// Subscribe creates or reopens the independent consumer named consumer and
	// returns a Watermill Subscriber bound to the bus topic. Every consumer
	// receives every message and keeps its own position; consumers never share
	// work. The name must satisfy ValidateConsumerName.
	//
	// ctx bounds the creation of the consumer (broker round trips), not the
	// lifetime of the subscription: messages flow until the returned Subscriber
	// is closed or the ctx given to its Subscribe method ends.
	//
	// opts.StartFrom applies when the consumer is created. Now means messages
	// published after the subscription is established (this call followed by
	// Subscribe on the returned Subscriber); Earliest means the retained
	// history first. On a bus with DurableConsumers an existing consumer keeps
	// the position and start policy it was created with.
	//
	// The returned Subscriber serves the bus topic only: its Subscribe method
	// wraps ErrWrongTopic for any other topic.
	Subscribe(ctx context.Context, consumer string, opts SubscribeOptions) (message.Subscriber, error)

	// Topic is the topic this bus carries, as configured.
	Topic() string

	// Capabilities reports what this driver, with its configuration, guarantees.
	Capabilities() Capabilities

	// Close releases the driver. It is idempotent.
	Close() error
}

// LagReporter is implemented by drivers that declare ReportsLag.
type LagReporter interface {
	// Lag returns the number of messages the named consumer has not acknowledged
	// yet: retained messages it has not received plus received ones awaiting ack.
	Lag(ctx context.Context, consumer string) (int64, error)
}

// SubscribeOptions configures a consumer at creation.
type SubscribeOptions struct {
	StartFrom StartPosition
}

// StartPosition selects where a new consumer starts. The zero value is invalid:
// the configuration requires an explicit choice because the two options differ
// by a potentially huge replay.
type StartPosition int

const (
	// Earliest replays the retained history before new messages. Requires
	// HistoricalReplay; see ResolveConsumer.
	Earliest StartPosition = iota + 1
	// Now delivers messages published after the consumer was created.
	Now
)

// Names of StartPosition values as written in sinks[].start_from.
const (
	startEarliest = "earliest"
	startNow      = "now"
)

// ParseStartPosition maps a sinks[].start_from value to a StartPosition.
func ParseStartPosition(s string) (StartPosition, error) {
	switch s {
	case startEarliest:
		return Earliest, nil
	case startNow:
		return Now, nil
	default:
		return 0, fmt.Errorf("start position must be %q or %q (got %q)", startEarliest, startNow, s)
	}
}

// String returns the configuration spelling of the position.
func (p StartPosition) String() string {
	switch p {
	case Earliest:
		return startEarliest
	case Now:
		return startNow
	default:
		return fmt.Sprintf("StartPosition(%d)", int(p))
	}
}

// MarshalText renders the position as its configuration spelling in JSON and YAML.
func (p StartPosition) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

// valid reports whether p is one of the defined positions.
func (p StartPosition) valid() bool {
	return p == Earliest || p == Now
}

// Errors returned by every driver.
var (
	// ErrClosed is returned by Publish and Subscribe after Close.
	ErrClosed = errors.New("bus is closed")
	// ErrInvalidConsumerName is wrapped by Subscribe for a name rejected by ValidateConsumerName.
	ErrInvalidConsumerName = errors.New("invalid consumer name")
	// ErrWrongTopic is wrapped by a Subscriber's Subscribe method for a topic
	// other than the bus topic.
	ErrWrongTopic = errors.New("subscriber is bound to another topic")
)

// consumerNamePattern is the character set every broker accepts in a durable
// consumer name (JetStream rejects ".", "*", ">", and whitespace).
var consumerNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ValidateConsumerName reports whether name may be used as a consumer name:
// non-empty, letters, digits, "_" and "-" only. Drivers call it from Subscribe.
func ValidateConsumerName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty", ErrInvalidConsumerName)
	}
	if !consumerNamePattern.MatchString(name) {
		return fmt.Errorf("%w: %q must match [A-Za-z0-9_-]+", ErrInvalidConsumerName, name)
	}
	return nil
}
