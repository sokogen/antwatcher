package natsjs

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// StreamConfig is the JetStream stream configuration derived from cfg for
// the given topic: one subject, file storage, limits retention (messages are
// kept for Retention regardless of consumers, so an orphaned consumer of a
// renamed sink pins no data), and the dedup window.
func StreamConfig(cfg Config, topic string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        cfg.Stream,
		Description: "antwatcher GitHub webhook deliveries",
		Subjects:    []string{topic},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		Discard:     jetstream.DiscardOld,
		MaxAge:      cfg.Retention,
		Duplicates:  cfg.DedupWindow,
	}
}

// EnsureStream creates the stream or updates an existing one to the
// configuration derived from cfg and topic. It is idempotent: running it
// again with the same configuration changes nothing.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg Config, topic string) (jetstream.Stream, error) {
	s, err := js.CreateOrUpdateStream(ctx, StreamConfig(cfg, topic))
	if err != nil {
		return nil, fmt.Errorf("natsjs: ensure stream %q: %w", cfg.Stream, err)
	}
	return s, nil
}
