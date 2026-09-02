// Package gochannel is the in-process bus driver for development and tests,
// built on Watermill's gochannel Pub/Sub.
//
// It keeps every published message in memory for the lifetime of the process
// and fans it out to every consumer, so it offers FanOut and HistoricalReplay.
// Nothing survives a restart, memory grows with every message, and Publish
// returns before any consumer has seen the message: DurablePublish,
// DurableConsumers, Deduplicates, and ReportsLag are all false, and
// ResolveIngress refuses this driver in "fail" mode.
package gochannel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
)

// Name is the driver name in bus.driver.
const Name = "gochannel"

// seqKey is the metadata key carrying the publish sequence between Publish and
// the subscriber wrapper, which strips it before handing the message on. It is
// how "start_from: now" is emulated on a Pub/Sub that replays everything.
const seqKey = "_gochannel_seq"

// Config is the driver block. The driver has no options; the block, when
// present, must be empty.
type Config struct{}

func init() {
	bus.Register(Name, bus.Driver{Factory: Open, Describer: config.DescriberFunc(Describe)})
}

// Describe decodes the driver block strictly; see config.Describer.
func Describe(raw yaml.Node) (any, error) {
	var c Config
	if err := config.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// Open is the bus.Factory for this driver.
func Open(_ context.Context, raw yaml.Node, topic string, logger *slog.Logger, _ prometheus.Registerer) (bus.Bus, error) {
	if _, err := Describe(raw); err != nil {
		return nil, err
	}
	if topic == "" {
		return nil, errors.New("gochannel: topic is required")
	}
	return New(topic, logger), nil
}

// Bus is the in-process bus. Use New.
type Bus struct {
	topic  string
	logger *slog.Logger
	ps     *gochannel.GoChannel

	mu     sync.Mutex
	seq    uint64 // sequence of the last published message
	closed bool
}

// New creates a bus for topic. logger may be nil.
func New(topic string, logger *slog.Logger) *Bus {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Bus{
		topic:  topic,
		logger: logger,
		ps:     gochannel.NewGoChannel(gochannel.Config{Persistent: true}, watermill.NopLogger{}),
	}
}

// Topic implements bus.Bus.
func (b *Bus) Topic() string { return b.topic }

// Capabilities implements bus.Bus.
func (b *Bus) Capabilities() bus.Capabilities {
	return bus.Capabilities{
		DurablePublish:   false,
		DurableConsumers: false,
		FanOut:           true,
		HistoricalReplay: true,
		Deduplicates:     false,
		ReportsLag:       false,
		Retention:        bus.RetentionNone,
	}
}

// Publish implements bus.Bus. It returns once the message is stored in memory
// and handed to the current subscribers; nothing is durable. The caller's
// message is not modified. ctx is only checked before publishing because the
// underlying Pub/Sub never blocks.
func (b *Bus) Publish(ctx context.Context, msg *message.Message) error {
	if msg == nil {
		return errors.New("gochannel: nil message")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return bus.ErrClosed
	}
	b.seq++
	tagged := msg.Copy()
	tagged.Metadata.Set(seqKey, strconv.FormatUint(b.seq, 10))
	if err := b.ps.Publish(b.topic, tagged); err != nil {
		return fmt.Errorf("gochannel: publish: %w", err)
	}
	return nil
}

// Subscribe implements bus.Bus. Consumers keep no state between calls: the
// same name subscribed twice starts twice from its start position.
func (b *Bus) Subscribe(ctx context.Context, consumer string, opts bus.SubscribeOptions) (message.Subscriber, error) {
	if err := bus.ValidateConsumerName(consumer); err != nil {
		return nil, fmt.Errorf("gochannel: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, bus.ErrClosed
	}
	var cutoff uint64
	switch opts.StartFrom {
	case bus.Earliest:
		cutoff = 0
	case bus.Now:
		cutoff = b.seq
	default:
		return nil, fmt.Errorf("gochannel: invalid start position %s", opts.StartFrom)
	}
	b.logger.Debug("consumer created", "consumer", consumer, "start_from", opts.StartFrom, "cutoff", cutoff)
	lifetime, cancel := context.WithCancel(context.Background())
	return &subscriber{bus: b, consumer: consumer, cutoff: cutoff, lifetime: lifetime, cancel: cancel}, nil
}

// Close implements bus.Bus. It closes every subscriber and waits for them.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()
	if err := b.ps.Close(); err != nil {
		return fmt.Errorf("gochannel: close: %w", err)
	}
	return nil
}

// subscriber is one named consumer bound to the bus topic. It forwards
// messages from the Pub/Sub, dropping (acking) those published before the
// consumer was created when the start position is Now.
type subscriber struct {
	bus      *Bus
	consumer string
	cutoff   uint64

	lifetime context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// Subscribe implements message.Subscriber for the bus topic only. The channel
// closes when ctx ends or the subscriber is closed.
func (s *subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if topic != s.bus.topic {
		return nil, fmt.Errorf("gochannel: %w: bound to %q, got %q", bus.ErrWrongTopic, s.bus.topic, topic)
	}
	if s.lifetime.Err() != nil {
		return nil, fmt.Errorf("gochannel: consumer %q is closed", s.consumer)
	}
	subCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	in, err := s.bus.ps.Subscribe(subCtx, topic)
	if err != nil {
		stop()
		cancel()
		return nil, fmt.Errorf("gochannel: subscribe %q: %w", s.consumer, err)
	}

	out := make(chan *message.Message)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(out)
		defer stop()
		defer cancel()
		for msg := range in {
			if !s.deliverable(msg) {
				msg.Ack()
				continue
			}
			select {
			case out <- msg:
			case <-subCtx.Done():
				return
			}
		}
	}()
	return out, nil
}

// deliverable strips the sequence tag and reports whether the message was
// published after the consumer's cutoff. A message without a tag (not
// published through this driver) is delivered as is.
func (s *subscriber) deliverable(msg *message.Message) bool {
	text, ok := msg.Metadata[seqKey]
	if !ok {
		return true
	}
	delete(msg.Metadata, seqKey)
	seq, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return true
	}
	return seq > s.cutoff
}

// Close implements message.Subscriber: it ends every subscription and waits
// for their channels to close. Idempotent.
func (s *subscriber) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}
