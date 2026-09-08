// Package forward is the destination class that re-publishes every event, as
// it arrived, on another bus or topic so a downstream system consumes the raw
// webhook stream without talking to GitHub. It defines the driver contract
// (Publisher) and the class rules: a forwarded copy travels under a new
// transport UUID derived from the delivery GUID, the sink name, and the
// target topic (ForwardUUID), keeps the delivery GUID and every envelope field
// in metadata, and carries the forward markers (event.MetaForwardedBy,
// event.MetaForwardHops) that bound the number of hops. Drivers such as
// forward/bus only choose where the copy goes.
package forward

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
)

// DefaultMaxHops is the hop limit when a driver passes 0 to New.
const DefaultMaxHops = 3

// Publisher is the driver contract of the forward class: deliver one message
// to the target. Publish returns nil once the message is accepted under the
// target's durability guarantee (the source message is acked only then), an
// error wrapped by sink.Permanent when a retry cannot help, and any other
// error when the publish should be retried later. It must be safe for
// concurrent use. Close is called once, after the last publish. bus.Bus
// satisfies this interface.
type Publisher interface {
	Publish(ctx context.Context, msg *message.Message) error
	Close() error
}

// Sink is the forward class bound to one publisher. It implements sink.Sink.
type Sink struct {
	name      string
	publisher Publisher
	topic     string
	maxHops   int
}

// New builds the forward sink named name publishing to topic through
// publisher. topic is part of the forwarded UUID, so two sinks forwarding the
// same delivery to different topics produce different UUIDs. maxHops bounds
// the forward chain (DefaultMaxHops when 0). It panics on a nil publisher, an
// empty topic, or a negative maxHops: those are driver bugs, not runtime
// conditions.
func New(name string, publisher Publisher, topic string, maxHops int) *Sink {
	if publisher == nil {
		panic("forward: New(" + name + ") with nil publisher")
	}
	if topic == "" {
		panic("forward: New(" + name + ") with empty topic")
	}
	if maxHops < 0 {
		panic(fmt.Sprintf("forward: New(%s) with negative max hops %d", name, maxHops))
	}
	if maxHops == 0 {
		maxHops = DefaultMaxHops
	}
	return &Sink{name: name, publisher: publisher, topic: topic, maxHops: maxHops}
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Class implements sink.Sink; it is always sink.ClassForward.
func (s *Sink) Class() sink.Class { return sink.ClassForward }

// Topic is the target topic the sink forwards to.
func (s *Sink) Topic() string { return s.topic }

// MaxHops is the hop limit of the sink.
func (s *Sink) MaxHops() int { return s.maxHops }

// Process publishes a copy of the envelope: the payload byte for byte, the
// envelope metadata rebuilt by event.ToMessage (so the delivery GUID stays
// explicit), event.MetaForwardedBy set to this sink, event.MetaForwardHops
// incremented, and the UUID replaced by ForwardUUID so a broker dedup window
// keyed on the original GUID cannot swallow the copy. An envelope that has
// already hopped MaxHops times is a loop: it is refused with a permanent
// error (the message stalls for this sink and is never forwarded). Nothing
// is skipped, and publisher errors are returned as they are, so the driver's
// classification decides between redelivery and stalling.
func (s *Sink) Process(ctx context.Context, env event.Envelope) error {
	if env.ForwardHops >= s.maxHops {
		return sink.Permanentf("forward %s %q to %q: loop detected: already forwarded %d times by %q (max_hops %d)",
			env.Event, env.DeliveryGUID, s.topic, env.ForwardHops, env.ForwardedBy, s.maxHops)
	}
	msg := s.Message(env)
	if err := s.publisher.Publish(ctx, msg); err != nil {
		return fmt.Errorf("forward %s %q to %q: %w", env.Event, env.DeliveryGUID, s.topic, err)
	}
	return nil
}

// Message builds the copy Process publishes for env, without publishing it.
func (s *Sink) Message(env event.Envelope) *message.Message {
	out := env
	out.ForwardedBy = s.name
	out.ForwardHops = env.ForwardHops + 1
	msg := event.ToMessage(out)
	msg.UUID = ForwardUUID(env.DeliveryGUID, s.name, s.topic)
	return msg
}

// Close implements sink.Sink by closing the publisher.
func (s *Sink) Close() error { return s.publisher.Close() }

// ForwardUUID is the transport UUID of a forwarded copy: the lowercase hex
// SHA-256 of "antwatcher:forward:<delivery GUID>:<sink name>:<topic>". It is
// deterministic, so a redelivered source message produces the same copy
// (deduplicated by a broker that keys on the UUID), and it never equals the
// delivery GUID, so the copy is not collapsed with the original when both
// travel through the same broker.
func ForwardUUID(deliveryGUID, sinkName, topic string) string {
	sum := sha256.Sum256([]byte("antwatcher:forward:" + deliveryGUID + ":" + sinkName + ":" + topic))
	return hex.EncodeToString(sum[:])
}
