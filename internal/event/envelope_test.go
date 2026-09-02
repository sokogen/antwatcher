package event

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testNow = time.Date(2026, 9, 2, 10, 0, 1, 123456789, time.FixedZone("CEST", 2*3600))

func webhookHeaders(guid, event, hookID string) http.Header {
	h := http.Header{}
	if guid != "" {
		h.Set(HeaderDelivery, guid)
	}
	if event != "" {
		h.Set(HeaderEvent, event)
	}
	if hookID != "" {
		h.Set(HeaderHookID, hookID)
	}
	return h
}

// eventOf derives the GitHub event name from a fixture name such as
// "workflow_run.completed" or "ping".
func eventOf(fixture string) string {
	event, _, _ := strings.Cut(fixture, ".")
	return event
}

func TestFixtures_ListsEveryRecordedPayload(t *testing.T) {
	assert.Equal(t, []string{
		"ping",
		"workflow_job.completed",
		"workflow_job.in_progress",
		"workflow_job.queued",
		"workflow_run.completed",
		"workflow_run.in_progress",
		"workflow_run.requested",
	}, Fixtures())
}

func TestLoadFixture_SuffixOptional(t *testing.T) {
	assert.Equal(t, LoadFixture(t, "ping"), LoadFixture(t, "ping.json"))
	assert.NotEmpty(t, LoadFixture(t, "ping"))
}

type recordingT struct {
	helper bool
	fatal  string
}

func (r *recordingT) Helper()                           { r.helper = true }
func (r *recordingT) Fatalf(format string, args ...any) { r.fatal = fmt.Sprintf(format, args...) }

func TestLoadFixture_UnknownFails(t *testing.T) {
	var rt recordingT
	LoadFixture(&rt, "does-not-exist")
	assert.True(t, rt.helper)
	assert.NotEmpty(t, rt.fatal)
}

func TestFromWebhook_Fixtures(t *testing.T) {
	tests := []struct {
		fixture      string
		action       string
		repositoryID int64
		repository   string
	}{
		{"workflow_run.requested", "requested", 987654321, "sokogen/antwatcher"},
		{"workflow_run.in_progress", "in_progress", 987654321, "sokogen/antwatcher"},
		{"workflow_run.completed", "completed", 987654321, "sokogen/antwatcher"},
		{"workflow_job.queued", "queued", 987654321, "sokogen/antwatcher"},
		{"workflow_job.in_progress", "in_progress", 987654321, "sokogen/antwatcher"},
		{"workflow_job.completed", "completed", 987654321, "sokogen/antwatcher"},
		{"ping", "", 0, ""},
	}
	require.Len(t, tests, len(Fixtures()), "every fixture must be covered")

	for _, tc := range tests {
		t.Run(tc.fixture, func(t *testing.T) {
			body := LoadFixture(t, tc.fixture)
			guid := "guid-" + tc.fixture
			env, err := FromWebhook(webhookHeaders(guid, eventOf(tc.fixture), "570000001"), body, testNow)
			require.NoError(t, err)

			assert.Equal(t, SchemaVersion, env.SchemaVersion)
			assert.Equal(t, guid, env.DeliveryGUID)
			assert.Equal(t, eventOf(tc.fixture), env.Event)
			assert.Equal(t, tc.action, env.Action)
			assert.Equal(t, "570000001", env.HookID)
			assert.Equal(t, testNow.UTC(), env.ReceivedAt)
			assert.Equal(t, time.UTC, env.ReceivedAt.Location())
			assert.Equal(t, tc.repositoryID, env.RepositoryID)
			assert.Equal(t, tc.repository, env.Repository)
			assert.Equal(t, body, []byte(env.Payload), "payload must be the raw body")
		})
	}
}

func TestFromWebhook_PayloadIsCopied(t *testing.T) {
	body := []byte(`{"action":"x"}`)
	want := bytes.Clone(body)
	env, err := FromWebhook(webhookHeaders("g", "ping", ""), body, testNow)
	require.NoError(t, err)
	body[0] = '['
	assert.Equal(t, want, []byte(env.Payload), "mutating the request body must not change the envelope")
}

func TestFromWebhook_HeaderWhitespaceTrimmed(t *testing.T) {
	env, err := FromWebhook(webhookHeaders(" g1 ", " ping ", " 7 "), []byte(`{}`), testNow)
	require.NoError(t, err)
	assert.Equal(t, "g1", env.DeliveryGUID)
	assert.Equal(t, "ping", env.Event)
	assert.Equal(t, "7", env.HookID)
}

func TestFromWebhook_OptionalFieldsAbsent(t *testing.T) {
	env, err := FromWebhook(webhookHeaders("g", "ping", ""), []byte(`{"zen":"ok"}`), testNow)
	require.NoError(t, err)
	assert.Empty(t, env.Action)
	assert.Empty(t, env.HookID)
	assert.Zero(t, env.RepositoryID)
	assert.Empty(t, env.Repository)
}

func TestFromWebhook_Errors(t *testing.T) {
	valid := LoadFixture(t, "ping")
	tests := []struct {
		name string
		hdr  http.Header
		body []byte
		want error
	}{
		{"missing guid", webhookHeaders("", "ping", ""), valid, ErrMissingDelivery},
		{"blank guid", webhookHeaders("   ", "ping", ""), valid, ErrMissingDelivery},
		{"missing event", webhookHeaders("g", "", ""), valid, ErrMissingEvent},
		{"empty body", webhookHeaders("g", "ping", ""), nil, ErrInvalidPayload},
		{"whitespace body", webhookHeaders("g", "ping", ""), []byte("  \n"), ErrInvalidPayload},
		{"invalid json", webhookHeaders("g", "ping", ""), []byte(`{"action": `), ErrInvalidPayload},
		{"json array", webhookHeaders("g", "ping", ""), []byte(`[1,2]`), ErrInvalidPayload},
		{"json null", webhookHeaders("g", "ping", ""), []byte(`null`), ErrInvalidPayload},
		{"wrong repository type", webhookHeaders("g", "ping", ""), []byte(`{"repository":"nope"}`), ErrInvalidPayload},
		{"wrong action type", webhookHeaders("g", "ping", ""), []byte(`{"action":1}`), ErrInvalidPayload},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, err := FromWebhook(tc.hdr, tc.body, testNow)
			require.ErrorIs(t, err, tc.want)
			assert.Equal(t, Envelope{}, env)
		})
	}
}
