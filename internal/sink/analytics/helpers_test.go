package analytics_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

const (
	repoID = int64(987654321)
	runID  = int64(15000000001)
	jobID  = int64(42000000001)
)

func ts(h, m, s int) time.Time { return time.Date(2026, 9, 2, h, m, s, 0, time.UTC) }

func str(s string) *string { return &s }

// fixtureEnvelope builds the envelope the receiver would produce for a fixture.
func fixtureEnvelope(t *testing.T, fixture string) event.Envelope {
	t.Helper()
	name, _, _ := strings.Cut(fixture, ".")
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, name)
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), received)
	require.NoError(t, err)
	return env
}

// fixtureExecution normalizes a fixture.
func fixtureExecution(t *testing.T, fixture string) model.Execution {
	t.Helper()
	exec, err := model.Normalize(fixtureEnvelope(t, fixture))
	require.NoError(t, err)
	return exec
}

// rawEnvelope builds an envelope for an arbitrary payload without going through
// FromWebhook, so malformed bodies can be injected.
func rawEnvelope(eventName, payload string) event.Envelope {
	return event.Envelope{
		SchemaVersion: event.SchemaVersion,
		DeliveryGUID:  "guid-raw",
		Event:         eventName,
		ReceivedAt:    received,
		Payload:       []byte(payload),
	}
}

func tp(h, m, s int) *time.Time {
	t := ts(h, m, s)
	return &t
}

func i64(v int64) *int64 { return &v }
