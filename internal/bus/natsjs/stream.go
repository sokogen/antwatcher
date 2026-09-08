package natsjs

import (
	"context"
	"errors"
	"fmt"
	"slices"

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
//
// A stream carrying other subjects is an error rather than an update: the
// subject list is the stream's identity, and rewriting it takes the subject
// away from whoever published it. Two buses with the default stream name on
// one broker — an ingress bus and a forward target, say — would otherwise
// silently break each other, the second one leaving the first publishing
// into a subject no stream binds any more.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg Config, topic string) (jetstream.Stream, error) {
	switch existing, err := js.Stream(ctx, cfg.Stream); {
	case err == nil:
		if subjects := existing.CachedInfo().Config.Subjects; !slices.Equal(subjects, []string{topic}) {
			return nil, fmt.Errorf("natsjs: stream %q already carries subjects %v, not %q: a stream belongs to one topic, give this bus a stream of its own", cfg.Stream, subjects, topic)
		}
	case errors.Is(err, jetstream.ErrStreamNotFound):
	default:
		return nil, fmt.Errorf("natsjs: look up stream %q: %w", cfg.Stream, err)
	}
	s, err := js.CreateOrUpdateStream(ctx, StreamConfig(cfg, topic))
	if err != nil {
		return nil, fmt.Errorf("natsjs: ensure stream %q: %w", cfg.Stream, err)
	}
	return s, nil
}
