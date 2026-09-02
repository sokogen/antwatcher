package archive

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
)

// Record is the JSON document written per archived event: the envelope
// fields under their metadata names and the GitHub payload nested as a JSON
// value, so tools such as jq can address payload fields directly. Optional
// envelope fields are omitted when empty; received_at is RFC 3339 in UTC.
type Record struct {
	SchemaVersion int             `json:"schema_version"`
	DeliveryGUID  string          `json:"delivery_guid"`
	Event         string          `json:"event"`
	Action        string          `json:"action,omitempty"`
	HookID        string          `json:"hook_id,omitempty"`
	ReceivedAt    time.Time       `json:"received_at"`
	RepositoryID  int64           `json:"repository_id,omitempty"`
	Repository    string          `json:"repository,omitempty"`
	ForwardedBy   string          `json:"forwarded_by,omitempty"`
	ForwardHops   int             `json:"forward_hops,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

// Encode renders env as one line: a compact Record followed by a newline.
//
// The payload keeps every value GitHub sent, but insignificant whitespace is
// removed so a pretty-printed delivery still fits on one line; HTML
// characters are not escaped. An envelope with an empty or invalid payload
// is a permanent error: the bytes came from the bus and will not change on
// redelivery.
func Encode(env event.Envelope) ([]byte, error) {
	if len(bytes.TrimSpace(env.Payload)) == 0 {
		return nil, sink.Permanentf("encode archive record %q: empty payload", env.DeliveryGUID)
	}
	rec := Record{
		SchemaVersion: env.SchemaVersion,
		DeliveryGUID:  env.DeliveryGUID,
		Event:         env.Event,
		Action:        env.Action,
		HookID:        env.HookID,
		ReceivedAt:    env.ReceivedAt.UTC(),
		RepositoryID:  env.RepositoryID,
		Repository:    env.Repository,
		ForwardedBy:   env.ForwardedBy,
		ForwardHops:   env.ForwardHops,
		Payload:       env.Payload,
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(rec); err != nil {
		return nil, sink.Permanent(fmt.Errorf("encode archive record %q: %w", env.DeliveryGUID, err))
	}
	return buf.Bytes(), nil
}

// Decode parses one line written by Encode back into an envelope. The
// payload is the compact JSON of the line, received_at is UTC. A line that
// is not a Record, or that lacks the delivery GUID, event, or payload, is an
// error.
func Decode(line []byte) (event.Envelope, error) {
	var rec Record
	if err := json.Unmarshal(line, &rec); err != nil {
		return event.Envelope{}, fmt.Errorf("decode archive record: %w", err)
	}
	switch {
	case rec.DeliveryGUID == "":
		return event.Envelope{}, errors.New("decode archive record: missing delivery_guid")
	case rec.Event == "":
		return event.Envelope{}, fmt.Errorf("decode archive record %q: missing event", rec.DeliveryGUID)
	case len(rec.Payload) == 0 || bytes.Equal(rec.Payload, []byte("null")):
		return event.Envelope{}, fmt.Errorf("decode archive record %q: missing payload", rec.DeliveryGUID)
	}
	return event.Envelope{
		SchemaVersion: rec.SchemaVersion,
		DeliveryGUID:  rec.DeliveryGUID,
		Event:         rec.Event,
		Action:        rec.Action,
		HookID:        rec.HookID,
		ReceivedAt:    rec.ReceivedAt.UTC(),
		RepositoryID:  rec.RepositoryID,
		Repository:    rec.Repository,
		ForwardedBy:   rec.ForwardedBy,
		ForwardHops:   rec.ForwardHops,
		Payload:       rec.Payload,
	}, nil
}

// Reader reads envelopes back from a JSONL stream written by Encode.
type Reader struct {
	r    *bufio.Reader
	line int
}

// NewReader wraps r. Lines may be of any length; a final line without a
// trailing newline is read as well.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// Next returns the next envelope, io.EOF at the end of the stream, and a
// decoding error naming the line number otherwise. Blank lines are skipped.
func (r *Reader) Next() (event.Envelope, error) {
	for {
		line, err := r.r.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return event.Envelope{}, err
		}
		if len(line) == 0 && err != nil {
			return event.Envelope{}, io.EOF
		}
		r.line++
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			if err != nil {
				return event.Envelope{}, io.EOF
			}
			continue
		}
		env, derr := Decode(trimmed)
		if derr != nil {
			return event.Envelope{}, fmt.Errorf("line %d: %w", r.line, derr)
		}
		return env, nil
	}
}

// ReadAll reads every envelope from r until the end of the stream.
func ReadAll(r io.Reader) ([]event.Envelope, error) {
	rd := NewReader(r)
	var envs []event.Envelope
	for {
		env, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return envs, nil
		}
		if err != nil {
			return envs, err
		}
		envs = append(envs, env)
	}
}

// ReadFile reads every envelope from an archive file, plain JSONL or gzip
// compressed by the ".gz" suffix.
func ReadFile(path string) ([]event.Envelope, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read only
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		defer gz.Close() //nolint:errcheck // read only
		r = gz
	}
	envs, err := ReadAll(r)
	if err != nil {
		return envs, fmt.Errorf("%s: %w", path, err)
	}
	return envs, nil
}
