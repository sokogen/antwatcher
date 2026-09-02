package bus

import "fmt"

// Capabilities is what a driver, with its configuration, guarantees. A driver
// declares them once; the conformance suite in package bustest proves each
// declared capability. There is deliberately no Ordered capability: nothing in
// antwatcher relies on ordering, because GitHub itself delivers out of order.
type Capabilities struct {
	// DurablePublish: Publish returns nil only once the broker has stored the
	// message. Without it a webhook 2xx does not mean the event is safe.
	DurablePublish bool `json:"durable_publish"`
	// DurableConsumers: a consumer's position survives a restart of antwatcher
	// and of the broker, so a sink resumes after its last ack.
	DurableConsumers bool `json:"durable_consumers"`
	// FanOut: every consumer receives every message; consumers never share
	// work. Required by the architecture, see ResolveConsumer.
	FanOut bool `json:"fan_out"`
	// HistoricalReplay: a consumer created with Earliest receives the retained
	// history before new messages.
	HistoricalReplay bool `json:"historical_replay"`
	// Deduplicates: the broker collapses a re-published message with the same
	// UUID inside its dedup window.
	Deduplicates bool `json:"deduplicates"`
	// ReportsLag: the driver implements LagReporter.
	ReportsLag bool `json:"reports_lag"`
	// Retention is how the broker bounds the retained history.
	Retention RetentionKind `json:"retention"`
}

// RetentionKind is how a broker bounds the history it keeps for consumers.
type RetentionKind int

const (
	// RetentionNone means nothing is retained beyond the process lifetime.
	RetentionNone RetentionKind = iota
	// RetentionTime means messages are kept for a configured age.
	RetentionTime
	// RetentionSize means messages are kept up to a configured size or count.
	RetentionSize
	// RetentionUntilAcked means each message is kept until every consumer acked it.
	RetentionUntilAcked
)

// String returns the lowercase name used in logs and /status.
func (r RetentionKind) String() string {
	switch r {
	case RetentionNone:
		return "none"
	case RetentionTime:
		return "time"
	case RetentionSize:
		return "size"
	case RetentionUntilAcked:
		return "until_acked"
	default:
		return fmt.Sprintf("RetentionKind(%d)", int(r))
	}
}

// MarshalText renders the kind by name in JSON and YAML.
func (r RetentionKind) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

// String renders the capabilities as key=value pairs for logs and /status.
func (c Capabilities) String() string {
	return fmt.Sprintf(
		"durable_publish=%t durable_consumers=%t fan_out=%t historical_replay=%t deduplicates=%t reports_lag=%t retention=%s",
		c.DurablePublish, c.DurableConsumers, c.FanOut, c.HistoricalReplay, c.Deduplicates, c.ReportsLag, c.Retention,
	)
}
