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
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
)

// reservedHeaderPrefix marks NATS/JetStream headers; metadata keys must not
// use it because they are carried as headers verbatim.
const reservedHeaderPrefix = "Nats-"

func init() {
	bus.Register(Name, bus.Driver{Factory: Open, Describer: config.DescriberFunc(Describe)})
}

// Option customizes New.
type Option func(*options)

type options struct {
	inProcess     nats.InProcessConnProvider
	reconnectWait time.Duration
}

// WithInProcess connects to an already running in-process server (for
// example a TestServer) instead of starting one. cfg.Embedded is then
// irrelevant; the bus never stops the server.
func WithInProcess(p nats.InProcessConnProvider) Option {
	return func(o *options) { o.inProcess = p }
}

// WithReconnectWait sets the pause between reconnect attempts (default 1s).
func WithReconnectWait(d time.Duration) Option {
	return func(o *options) { o.reconnectWait = d }
}

// Open is the bus.Factory for this driver. The metrics registerer is unused:
// connection state is exposed through Connected for the metrics package.
func Open(ctx context.Context, raw yaml.Node, topic string, logger *slog.Logger, _ prometheus.Registerer) (bus.Bus, error) {
	v, err := Describe(raw)
	if err != nil {
		return nil, err
	}
	cfg, _ := v.(Config)
	return New(ctx, cfg, topic, logger)
}

// Bus is the JetStream bus. Use New.
type Bus struct {
	cfg    Config
	topic  string
	logger *slog.Logger
	opts   options

	srv *server.Server // embedded server owned by this bus, nil otherwise
	nc  *nats.Conn
	js  jetstream.JetStream

	mu     sync.Mutex
	subs   map[*subscriber]struct{}
	closed bool
}

// New validates cfg, starts the embedded server when configured (unless
// WithInProcess is given), connects, and ensures the stream. ctx bounds the
// broker round trips. logger may be nil.
func New(ctx context.Context, cfg Config, topic string, logger *slog.Logger, opts ...Option) (*Bus, error) {
	if topic == "" {
		return nil, errors.New("natsjs: topic is required")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	b := &Bus{cfg: cfg, topic: topic, logger: logger, opts: options{reconnectWait: time.Second}, subs: map[*subscriber]struct{}{}}
	for _, o := range opts {
		o(&b.opts)
	}
	if b.opts.inProcess == nil {
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("natsjs: invalid config: %w", err)
		}
	} else if err := cfg.validateBroker(); err != nil {
		return nil, fmt.Errorf("natsjs: invalid config: %w", err)
	}

	if err := b.connect(); err != nil {
		return nil, err
	}
	if _, err := EnsureStream(ctx, b.js, cfg, topic); err != nil {
		_ = b.Close()
		return nil, err
	}
	logger.Info("bus ready", "stream", cfg.Stream, "topic", topic, "embedded", b.srv != nil,
		"retention", cfg.Retention, "dedup_window", cfg.DedupWindow, "ack_wait", cfg.AckWait)
	return b, nil
}

// validateBroker is Validate without the embedded/url rule, for a bus that
// is given its server through WithInProcess.
func (c Config) validateBroker() error {
	c.Embedded = true
	c.StoreDir = "-"
	return c.Validate()
}

func (b *Bus) connect() error {
	natsOpts := []nats.Option{
		nats.Name("antwatcher"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(b.opts.reconnectWait),
		// No outbound buffering while disconnected: a Publish during an outage
		// fails at once instead of being flushed later behind a 503.
		nats.ReconnectBufSize(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				b.logger.Warn("nats disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			b.logger.Info("nats reconnected", "url", nc.ConnectedUrlRedacted())
		}),
	}
	url := b.cfg.URL.Reveal()
	switch {
	case b.opts.inProcess != nil:
		natsOpts = append(natsOpts, nats.InProcessServer(b.opts.inProcess))
		url = ""
	case b.cfg.Embedded:
		srv, err := StartEmbedded(b.cfg, b.logger)
		if err != nil {
			return err
		}
		b.srv = srv
		natsOpts = append(natsOpts, nats.InProcessServer(srv))
		url = ""
	default:
		credOpt, err := credentialsOption(b.cfg.Credentials.Reveal())
		if err != nil {
			return err
		}
		if credOpt != nil {
			natsOpts = append(natsOpts, credOpt)
		}
	}

	nc, err := nats.Connect(url, natsOpts...)
	if err != nil {
		b.stopServer()
		return fmt.Errorf("natsjs: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		b.stopServer()
		return fmt.Errorf("natsjs: jetstream: %w", err)
	}
	b.nc, b.js = nc, js
	return nil
}

// credentialsOption turns the credentials setting into a nats.Option: the
// decorated content of a .creds file, or a path to one. Empty means none.
func credentialsOption(creds string) (nats.Option, error) {
	creds = strings.TrimSpace(creds)
	if creds == "" {
		return nil, nil
	}
	if strings.HasPrefix(creds, "-----BEGIN") {
		jwt, err := nkeys.ParseDecoratedJWT([]byte(creds))
		if err != nil {
			return nil, fmt.Errorf("natsjs: credentials: %w", err)
		}
		seed, err := parseSeed([]byte(creds))
		if err != nil {
			return nil, fmt.Errorf("natsjs: credentials: %w", err)
		}
		return nats.UserJWTAndSeed(jwt, seed), nil
	}
	return nats.UserCredentials(creds), nil
}

// parseSeed extracts the NKey seed from decorated credentials content.
func parseSeed(contents []byte) (string, error) {
	kp, err := nkeys.ParseDecoratedNKey(contents)
	if err != nil {
		return "", err
	}
	defer kp.Wipe()
	seed, err := kp.Seed()
	if err != nil {
		return "", err
	}
	return string(seed), nil
}

func (b *Bus) stopServer() {
	if b.srv == nil {
		return
	}
	StopEmbedded(b.srv)
	b.srv = nil
}

// Topic implements bus.Bus.
func (b *Bus) Topic() string { return b.topic }

// Capabilities implements bus.Bus.
func (b *Bus) Capabilities() bus.Capabilities {
	return bus.Capabilities{
		DurablePublish:   true,
		DurableConsumers: true,
		FanOut:           true,
		HistoricalReplay: true,
		Deduplicates:     b.cfg.DedupWindow > 0,
		ReportsLag:       true,
		Retention:        bus.RetentionTime,
	}
}

// Connected reports whether the client currently has a live connection to
// the broker.
func (b *Bus) Connected() bool {
	return b.nc != nil && b.nc.IsConnected()
}

// Publish implements bus.Bus: the message is published on the topic subject
// with its metadata as headers and Nats-Msg-Id set to its UUID (the dedup
// key), and the call returns only after the JetStream PubAck or when ctx
// ends. The caller's message is not modified.
func (b *Bus) Publish(ctx context.Context, msg *message.Message) error {
	if msg == nil {
		return errors.New("natsjs: nil message")
	}
	if msg.UUID == "" {
		return errors.New("natsjs: message UUID is required (it is the dedup key)")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.isClosed() {
		return bus.ErrClosed
	}
	m := &nats.Msg{Subject: b.topic, Data: msg.Payload, Header: make(nats.Header, len(msg.Metadata)+1)}
	for k, v := range msg.Metadata {
		if k == "" || strings.HasPrefix(k, reservedHeaderPrefix) {
			return fmt.Errorf("natsjs: metadata key %q is reserved for NATS headers", k)
		}
		m.Header.Set(k, v)
	}
	m.Header.Set(jetstream.MsgIDHeader, msg.UUID)
	ack, err := b.js.PublishMsg(ctx, m)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("natsjs: publish %s: %w", msg.UUID, ctxErr)
		}
		return fmt.Errorf("natsjs: publish %s: %w", msg.UUID, err)
	}
	if ack.Duplicate {
		b.logger.Debug("duplicate publish collapsed by the broker", "uuid", msg.UUID)
	}
	return nil
}

// Subscribe implements bus.Bus. The consumer is a durable pull consumer on
// the stream with explicit acks and unlimited deliveries; an existing
// consumer keeps the deliver policy it was created with (the start position
// applies on first creation only) and only its ack wait is updated.
func (b *Bus) Subscribe(ctx context.Context, consumer string, opts bus.SubscribeOptions) (message.Subscriber, error) {
	if err := bus.ValidateConsumerName(consumer); err != nil {
		return nil, fmt.Errorf("natsjs: %w", err)
	}
	var policy jetstream.DeliverPolicy
	switch opts.StartFrom {
	case bus.Earliest:
		policy = jetstream.DeliverAllPolicy
	case bus.Now:
		policy = jetstream.DeliverNewPolicy
	default:
		return nil, fmt.Errorf("natsjs: invalid start position %s", opts.StartFrom)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.isClosed() {
		return nil, bus.ErrClosed
	}

	stream, err := b.js.Stream(ctx, b.cfg.Stream)
	if err != nil {
		return nil, fmt.Errorf("natsjs: stream %q: %w", b.cfg.Stream, err)
	}
	want := jetstream.ConsumerConfig{
		Name:          consumer,
		Durable:       consumer,
		Description:   "antwatcher sink consumer",
		DeliverPolicy: policy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       b.cfg.AckWait,
		MaxDeliver:    -1,
	}
	var c jetstream.Consumer
	existing, err := stream.Consumer(ctx, consumer)
	switch {
	case err == nil:
		info := existing.CachedInfo()
		if info.Config.DeliverPolicy != policy {
			b.logger.Info("consumer exists: keeping its original start position",
				"consumer", consumer, "requested", opts.StartFrom, "stored", info.Config.DeliverPolicy.String())
		} else {
			b.logger.Debug("consumer exists: resuming", "consumer", consumer, "start_from", opts.StartFrom)
		}
		want.DeliverPolicy = info.Config.DeliverPolicy
		want.OptStartSeq = info.Config.OptStartSeq
		want.OptStartTime = info.Config.OptStartTime
		c = existing
		if info.Config.AckWait != want.AckWait || info.Config.MaxDeliver != want.MaxDeliver {
			if c, err = stream.UpdateConsumer(ctx, want); err != nil {
				return nil, fmt.Errorf("natsjs: update consumer %q: %w", consumer, err)
			}
		}
	case errors.Is(err, jetstream.ErrConsumerNotFound):
		b.logger.Info("consumer created", "consumer", consumer, "start_from", opts.StartFrom)
		if c, err = stream.CreateConsumer(ctx, want); err != nil {
			return nil, fmt.Errorf("natsjs: create consumer %q: %w", consumer, err)
		}
	default:
		return nil, fmt.Errorf("natsjs: consumer %q: %w", consumer, err)
	}

	s := newSubscriber(b, consumer, c)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		s.cancel()
		return nil, bus.ErrClosed
	}
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s, nil
}

// Lag implements bus.LagReporter: messages not yet delivered to the consumer
// plus delivered ones awaiting ack.
func (b *Bus) Lag(ctx context.Context, consumer string) (int64, error) {
	if b.isClosed() {
		return 0, bus.ErrClosed
	}
	c, err := b.js.Consumer(ctx, b.cfg.Stream, consumer)
	if err != nil {
		return 0, fmt.Errorf("natsjs: consumer %q: %w", consumer, err)
	}
	info := c.CachedInfo()
	return int64(info.NumPending) + int64(info.NumAckPending), nil //nolint:gosec // NumPending never approaches int64 range
}

// Close implements bus.Bus: it closes every subscriber, drains the connection,
// and stops the embedded server when this bus started it. Idempotent.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subs := make([]*subscriber, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.subs = nil
	b.mu.Unlock()

	var errs []error
	for _, s := range subs {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if b.nc != nil {
		b.nc.Close()
	}
	b.stopServer()
	return errors.Join(errs...)
}

func (b *Bus) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *Bus) forget(s *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs != nil {
		delete(b.subs, s)
	}
}
