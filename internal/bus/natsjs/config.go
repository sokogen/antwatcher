// Package natsjs is the NATS JetStream bus driver: a durable, replayable
// stream carried either by an embedded in-process nats-server or by an
// external NATS deployment.
//
// Publish returns only after the JetStream PubAck, so a webhook 2xx means the
// event is on disk (DurablePublish). Every sink is a durable pull consumer with
// its own position (DurableConsumers, FanOut, HistoricalReplay); the stream
// keeps messages for the configured retention regardless of consumers (limits
// retention: a renamed sink leaves an orphaned consumer that pins no data).
// Duplicate UUIDs are collapsed by the broker inside dedup_window
// (Deduplicates); consumer info reports the backlog (ReportsLag).
//
// The subscriber is implemented in this package over a nats.go pull consumer
// rather than the watermill-nats JetStream subscriber: that package is beta,
// binds the stream name to the topic, ignores ConfigureConsumer for grouped
// consumers, drains the shared connection on Close, and can hold only one
// unacknowledged message per subscription. The driver's public surface is
// unaffected by that choice.
package natsjs

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
)

// Name is the driver name in bus.driver.
const Name = "nats-jetstream"

// AckWaitMargin is how much ack_wait must exceed router.process_timeout so a
// sink that hits its process deadline Nacks before the broker redelivers the
// same message to it concurrently.
const AckWaitMargin = 10 * time.Second

// minWindow is the smallest max_age / duplicates window nats-server accepts.
const minWindow = 100 * time.Millisecond

// Config is the `bus.nats-jetstream` block.
type Config struct {
	// Embedded runs an in-process nats-server (no TCP listener); the client
	// connects over an in-memory pipe. URL and Credentials are ignored.
	Embedded bool `yaml:"embedded" json:"embedded"`
	// StoreDir is the JetStream storage directory of the embedded server.
	StoreDir string `yaml:"store_dir" json:"store_dir"`
	// URL of the external NATS server(s) when Embedded is false, for example
	// nats://user:pass@host:4222 or a comma-separated list.
	URL string `yaml:"url" json:"url"`
	// Credentials is the content of a NATS credentials file (user JWT and
	// NKey seed, the decorated ".creds" format) or a path to such a file.
	// Empty means no credentials beyond what URL carries.
	Credentials config.Secret `yaml:"credentials" json:"credentials"`
	// Stream is the JetStream stream name carrying the topic.
	Stream string `yaml:"stream" json:"stream"`
	// Retention is the stream max age: the backlog of a sink that is down
	// survives only within this window.
	Retention time.Duration `yaml:"retention" json:"retention"`
	// DedupWindow is the broker window in which a re-published UUID is
	// collapsed. Zero leaves the server default (two minutes) but the driver
	// then does not declare Deduplicates.
	DedupWindow time.Duration `yaml:"dedup_window" json:"dedup_window"`
	// AckWait is how long the broker waits for an ack before redelivering.
	// Must exceed router.process_timeout by AckWaitMargin.
	AckWait time.Duration `yaml:"ack_wait" json:"ack_wait"`
	// NakDelayMin and NakDelayMax bound the exponential redelivery delay after
	// a Nack: min on the first delivery, doubling per delivery, capped at max.
	NakDelayMin time.Duration `yaml:"nak_delay_min" json:"nak_delay_min"`
	NakDelayMax time.Duration `yaml:"nak_delay_max" json:"nak_delay_max"`
}

// DefaultConfig returns the documented defaults.
func DefaultConfig() Config {
	return Config{
		Embedded:    true,
		StoreDir:    "./data/nats",
		Stream:      "ANTWATCHER",
		Retention:   168 * time.Hour,
		DedupWindow: 2 * time.Hour,
		AckWait:     90 * time.Second,
		NakDelayMin: 5 * time.Second,
		NakDelayMax: 5 * time.Minute,
	}
}

// Describe decodes the driver block strictly on top of DefaultConfig; see
// config.Describer. The result implements config.DriverValidator.
func Describe(raw yaml.Node) (any, error) {
	c := DefaultConfig()
	if err := config.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// streamNamePattern is what nats-server accepts as a stream name.
var streamNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Validate checks the block on its own.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.Embedded {
		if c.StoreDir == "" {
			add("store_dir is required when embedded is true")
		}
	} else if c.URL == "" {
		add("url is required when embedded is false")
	}
	if c.Stream == "" {
		add("stream is required")
	} else if !streamNamePattern.MatchString(c.Stream) {
		add("stream %q must match [A-Za-z0-9_-]+", c.Stream)
	}
	if c.Retention < minWindow {
		add("retention must be >= %s (got %s)", minWindow, c.Retention)
	}
	switch {
	case c.DedupWindow < 0:
		add("dedup_window must be >= 0")
	case c.DedupWindow > 0 && c.DedupWindow < minWindow:
		add("dedup_window must be 0 or >= %s (got %s)", minWindow, c.DedupWindow)
	case c.DedupWindow > c.Retention:
		add("dedup_window (%s) must not exceed retention (%s)", c.DedupWindow, c.Retention)
	}
	if c.AckWait <= 0 {
		add("ack_wait must be > 0")
	}
	if c.NakDelayMin <= 0 {
		add("nak_delay_min must be > 0")
	}
	if c.NakDelayMax < c.NakDelayMin {
		add("nak_delay_max (%s) must be >= nak_delay_min (%s)", c.NakDelayMax, c.NakDelayMin)
	}
	return errors.Join(errs...)
}

// ValidateWith implements config.DriverValidator: Validate plus the invariant
// ack_wait > router.process_timeout + AckWaitMargin.
func (c Config) ValidateWith(router config.Router) error {
	err := c.Validate()
	if floor := router.ProcessTimeout + AckWaitMargin; c.AckWait <= floor {
		err = errors.Join(err, fmt.Errorf("ack_wait (%s) must exceed router.process_timeout + %s (%s), otherwise the broker redelivers a message while a slow sink is still processing it",
			c.AckWait, AckWaitMargin, floor))
	}
	return err
}

// NakDelay is the redelivery delay after a Nack of a message delivered
// `delivered` times (1 on first delivery): minDelay doubled per extra delivery,
// capped at maxDelay. Callers pass validated bounds (0 < minDelay <= maxDelay).
func NakDelay(minDelay, maxDelay time.Duration, delivered uint64) time.Duration {
	if delivered <= 1 {
		return minDelay
	}
	shift := delivered - 1
	if shift >= 62 || minDelay<<shift > maxDelay || minDelay<<shift < minDelay {
		return maxDelay
	}
	return minDelay << shift
}
