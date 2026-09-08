package natsjs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/sokogen/antwatcher/internal/bus"
)

const (
	// fetchWait is how long one pull request waits for a message before the
	// loop issues the next one.
	fetchWait = 5 * time.Second
	// fetchRetry is the pause after a failed pull request (broker unreachable).
	fetchRetry = time.Second
	// ackTimeout bounds the round trip confirming an ack.
	ackTimeout = 5 * time.Second
	// closeTimeout is how long Close waits for in-flight messages to be
	// acked or nacked before giving up on them (the broker redelivers).
	closeTimeout = 5 * time.Second
)

// subscriber is one durable consumer bound to the bus topic. Each Subscribe
// call runs a fetch loop pulling one message at a time (so the broker's ack
// wait starts when the message is actually handed over, not when a batch was
// prefetched); every delivered message is awaited by its own goroutine, so
// several messages can be in flight unacknowledged, as the router and the
// conformance suite require.
type subscriber struct {
	bus      *Bus
	name     string
	consumer jetstream.Consumer
	logger   *slog.Logger

	lifetime context.Context
	cancel   context.CancelFunc
	loops    sync.WaitGroup // fetch loops
	inflight sync.WaitGroup // messages awaiting ack or nack
	once     sync.Once
}

func newSubscriber(b *Bus, name string, c jetstream.Consumer) *subscriber {
	ctx, cancel := context.WithCancel(context.Background())
	return &subscriber{
		bus:      b,
		name:     name,
		consumer: c,
		logger:   b.logger.With("consumer", name),
		lifetime: ctx,
		cancel:   cancel,
	}
}

// Subscribe implements message.Subscriber for the bus topic only. The channel
// closes when ctx ends or the subscriber is closed.
func (s *subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if topic != s.bus.topic {
		return nil, fmt.Errorf("natsjs: %w: bound to %q, got %q", bus.ErrWrongTopic, s.bus.topic, topic)
	}
	if s.lifetime.Err() != nil {
		return nil, fmt.Errorf("natsjs: consumer %q is closed", s.name)
	}
	loopCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	out := make(chan *message.Message)
	s.loops.Add(1)
	go func() {
		defer s.loops.Done()
		defer close(out)
		defer stop()
		defer cancel()
		s.loop(loopCtx, out)
	}()
	return out, nil
}

// loop pulls messages one at a time until ctx ends.
func (s *subscriber) loop(ctx context.Context, out chan<- *message.Message) {
	for ctx.Err() == nil {
		fetchCtx, cancel := context.WithTimeout(ctx, fetchWait)
		batch, err := s.consumer.Fetch(1, jetstream.FetchContext(fetchCtx))
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("fetch failed; retrying", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(fetchRetry):
			}
			continue
		}
		for jmsg := range batch.Messages() {
			s.deliver(ctx, jmsg, out)
		}
		cancel()
		if err := batch.Error(); err != nil && ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
			s.logger.Debug("fetch ended with error", "error", err)
		}
	}
}

// deliver hands one JetStream message to the channel and starts the goroutine
// that forwards its ack or nack to the broker. A message that cannot be
// handed over because the subscription is ending is nacked at once so the
// broker redelivers it without waiting for the ack wait.
func (s *subscriber) deliver(ctx context.Context, jmsg jetstream.Msg, out chan<- *message.Message) {
	wm := toMessage(jmsg)
	msgCtx, cancel := context.WithCancel(ctx)
	wm.SetContext(msgCtx)
	select {
	case out <- wm:
	case <-ctx.Done():
		cancel()
		if err := jmsg.Nak(); err != nil {
			s.logger.Debug("nak on shutdown failed; the broker redelivers after ack wait", "uuid", wm.UUID, "error", err)
		}
		return
	}
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		defer cancel()
		s.await(jmsg, wm)
	}()
}

// await forwards the Watermill ack or nack to JetStream. It does not stop
// when the subscription ends: a handler that is still processing the last
// message must be able to ack it. After the broker's ack wait the message is
// the broker's again and the wait is abandoned.
func (s *subscriber) await(jmsg jetstream.Msg, wm *message.Message) {
	timer := time.NewTimer(s.bus.cfg.AckWait)
	defer timer.Stop()
	select {
	case <-wm.Acked():
		ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
		defer cancel()
		if err := jmsg.DoubleAck(ctx); err != nil {
			s.logger.Warn("ack not confirmed; the broker may redeliver", "uuid", wm.UUID, "error", err)
		}
	case <-wm.Nacked():
		delay := s.nakDelay(jmsg)
		if err := jmsg.NakWithDelay(delay); err != nil {
			s.logger.Warn("nak failed; the broker redelivers after ack wait", "uuid", wm.UUID, "error", err)
			return
		}
		s.logger.Debug("message nacked", "uuid", wm.UUID, "redeliver_in", delay)
	case <-timer.C:
		s.logger.Warn("no ack or nack within ack_wait; the broker redelivers", "uuid", wm.UUID, "ack_wait", s.bus.cfg.AckWait)
	}
}

func (s *subscriber) nakDelay(jmsg jetstream.Msg) time.Duration {
	meta, err := jmsg.Metadata()
	if err != nil {
		return s.bus.cfg.NakDelayMin
	}
	return NakDelay(s.bus.cfg.NakDelayMin, s.bus.cfg.NakDelayMax, meta.NumDelivered)
}

// Close implements message.Subscriber: it ends every subscription, waits for
// their channels to close, then waits up to closeTimeout for in-flight
// messages to be acked or nacked. Idempotent.
func (s *subscriber) Close() error {
	s.once.Do(func() {
		s.cancel()
		s.loops.Wait()
		done := make(chan struct{})
		go func() {
			s.inflight.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(closeTimeout):
			s.logger.Warn("closing with messages still awaiting ack; the broker redelivers them", "timeout", closeTimeout)
		}
		s.bus.forget(s)
	})
	return nil
}

// toMessage maps a JetStream message to a Watermill message: UUID from
// Nats-Msg-Id, metadata from every other non-NATS header, payload verbatim.
// It never fails: a message without an id gets an empty UUID and reaches the
// consumer, which decides (event.FromMessage rejects it as permanent).
func toMessage(jmsg jetstream.Msg) *message.Message {
	hdr := jmsg.Headers()
	wm := message.NewMessage(hdr.Get(jetstream.MsgIDHeader), jmsg.Data())
	for k, v := range hdr {
		if strings.HasPrefix(k, reservedHeaderPrefix) || len(v) == 0 {
			continue
		}
		wm.Metadata.Set(k, v[0])
	}
	return wm
}
