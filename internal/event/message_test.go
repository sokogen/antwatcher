package event

import (
	"maps"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToMessage_Metadata(t *testing.T) {
	body := LoadFixture(t, "workflow_job.completed")
	env, err := FromWebhook(webhookHeaders("d0a3f1c2-0001-4000-8000-000000000001", "workflow_job", "570000001"), body, testNow)
	require.NoError(t, err)

	msg := ToMessage(env)

	assert.Equal(t, env.DeliveryGUID, msg.UUID, "UUID is the delivery GUID")
	assert.Equal(t, body, []byte(msg.Payload))
	assert.Equal(t, message.Metadata{
		MetaDeliveryGUID:  "d0a3f1c2-0001-4000-8000-000000000001",
		MetaSchemaVersion: "1",
		MetaEvent:         "workflow_job",
		MetaAction:        "completed",
		MetaHookID:        "570000001",
		MetaReceivedAt:    "2026-09-02T08:00:01.123456789Z",
		MetaRepositoryID:  "987654321",
		MetaRepository:    "sokogen/antwatcher",
	}, msg.Metadata)
}

func TestToMessage_OmitsEmptyOptionalMetadata(t *testing.T) {
	env, err := FromWebhook(webhookHeaders("g", "ping", ""), LoadFixture(t, "ping"), testNow)
	require.NoError(t, err)

	md := ToMessage(env).Metadata
	for _, key := range []string{MetaAction, MetaHookID, MetaRepositoryID, MetaRepository} {
		_, present := md[key]
		assert.False(t, present, "%s must be omitted when empty", key)
	}
	for _, key := range []string{MetaDeliveryGUID, MetaSchemaVersion, MetaEvent, MetaReceivedAt} {
		assert.NotEmpty(t, md[key], "%s is required", key)
	}
}

func TestToMessage_ForwardMarkers(t *testing.T) {
	env, err := FromWebhook(webhookHeaders("g", "ping", ""), LoadFixture(t, "ping"), testNow)
	require.NoError(t, err)
	md := ToMessage(env).Metadata
	for _, key := range []string{MetaForwardedBy, MetaForwardHops} {
		_, present := md[key]
		assert.False(t, present, "%s must be omitted on a delivery received from GitHub", key)
	}

	env.ForwardedBy, env.ForwardHops = "downstream", 2
	msg := ToMessage(env)
	assert.Equal(t, "downstream", msg.Metadata[MetaForwardedBy])
	assert.Equal(t, "2", msg.Metadata[MetaForwardHops])
	assert.Equal(t, env.DeliveryGUID, msg.UUID, "ToMessage keeps the GUID as UUID; the forward sink replaces it")

	out, err := FromMessage(msg)
	require.NoError(t, err)
	assert.Equal(t, env, out, "forward markers survive the round trip")

	msg.Metadata.Set(MetaForwardHops, "0")
	delete(msg.Metadata, MetaForwardedBy)
	out, err = FromMessage(msg)
	require.NoError(t, err)
	assert.Empty(t, out.ForwardedBy)
	assert.Zero(t, out.ForwardHops)
}

func TestToMessage_ReceivedAtIsUTC(t *testing.T) {
	env := Envelope{DeliveryGUID: "g", Event: "ping", SchemaVersion: SchemaVersion, ReceivedAt: testNow}
	assert.Equal(t, "2026-09-02T08:00:01.123456789Z", ToMessage(env).Metadata[MetaReceivedAt])
}

func TestRoundTrip_EveryFixture(t *testing.T) {
	for _, fixture := range Fixtures() {
		t.Run(fixture, func(t *testing.T) {
			body := LoadFixture(t, fixture)
			in, err := FromWebhook(webhookHeaders("guid-"+fixture, eventOf(fixture), "570000001"), body, testNow)
			require.NoError(t, err)

			out, err := FromMessage(ToMessage(in))
			require.NoError(t, err)

			assert.Equal(t, in, out)
			assert.Equal(t, body, []byte(out.Payload), "payload bytes unchanged")
		})
	}
}

func TestFromMessage_GUIDFromMetadataWinsOverUUID(t *testing.T) {
	in, err := FromWebhook(webhookHeaders("original-guid", "ping", ""), LoadFixture(t, "ping"), testNow)
	require.NoError(t, err)

	// Simulate a forwarder re-publishing under a fresh transport UUID.
	msg := ToMessage(in)
	republished := message.NewMessage("transport-uuid", msg.Payload)
	republished.Metadata = maps.Clone(msg.Metadata)

	out, err := FromMessage(republished)
	require.NoError(t, err)
	assert.Equal(t, "original-guid", out.DeliveryGUID)
}

func TestFromMessage_LegacyUUIDFallback(t *testing.T) {
	msg := message.NewMessage("legacy-guid", []byte(`{}`))
	msg.Metadata.Set(MetaSchemaVersion, "1")
	msg.Metadata.Set(MetaEvent, "ping")
	msg.Metadata.Set(MetaReceivedAt, "2026-09-02T08:00:01Z")

	out, err := FromMessage(msg)
	require.NoError(t, err)
	assert.Equal(t, "legacy-guid", out.DeliveryGUID)
	assert.Equal(t, time.Date(2026, 9, 2, 8, 0, 1, 0, time.UTC), out.ReceivedAt)
	assert.Equal(t, time.UTC, out.ReceivedAt.Location())
}

func TestFromMessage_NilMetadataUsesUUID(t *testing.T) {
	msg := &message.Message{UUID: "u", Payload: []byte(`{}`)}
	_, err := FromMessage(msg)
	require.ErrorIs(t, err, ErrMissingMetadata)
	assert.ErrorContains(t, err, MetaSchemaVersion)
}

func TestFromMessage_Errors(t *testing.T) {
	good := func() *message.Message {
		msg := message.NewMessage("guid", []byte(`{}`))
		msg.Metadata.Set(MetaDeliveryGUID, "guid")
		msg.Metadata.Set(MetaSchemaVersion, "1")
		msg.Metadata.Set(MetaEvent, "ping")
		msg.Metadata.Set(MetaReceivedAt, "2026-09-02T08:00:01Z")
		return msg
	}

	tests := []struct {
		name   string
		mutate func(*message.Message)
		want   error
		key    string
	}{
		{"empty guid everywhere", func(m *message.Message) { m.UUID = ""; delete(m.Metadata, MetaDeliveryGUID) }, ErrMissingMetadata, MetaDeliveryGUID},
		{"empty guid metadata", func(m *message.Message) { m.Metadata.Set(MetaDeliveryGUID, "") }, ErrMissingMetadata, MetaDeliveryGUID},
		{"missing schema version", func(m *message.Message) { delete(m.Metadata, MetaSchemaVersion) }, ErrMissingMetadata, MetaSchemaVersion},
		{"non-integer schema version", func(m *message.Message) { m.Metadata.Set(MetaSchemaVersion, "one") }, ErrInvalidMetadata, MetaSchemaVersion},
		{"unsupported schema version", func(m *message.Message) { m.Metadata.Set(MetaSchemaVersion, "2") }, ErrInvalidMetadata, MetaSchemaVersion},
		{"missing event", func(m *message.Message) { delete(m.Metadata, MetaEvent) }, ErrMissingMetadata, MetaEvent},
		{"empty event", func(m *message.Message) { m.Metadata.Set(MetaEvent, "") }, ErrMissingMetadata, MetaEvent},
		{"missing received_at", func(m *message.Message) { delete(m.Metadata, MetaReceivedAt) }, ErrMissingMetadata, MetaReceivedAt},
		{"invalid received_at", func(m *message.Message) { m.Metadata.Set(MetaReceivedAt, "yesterday") }, ErrInvalidMetadata, MetaReceivedAt},
		{"invalid repository_id", func(m *message.Message) { m.Metadata.Set(MetaRepositoryID, "abc") }, ErrInvalidMetadata, MetaRepositoryID},
		{"negative repository_id", func(m *message.Message) { m.Metadata.Set(MetaRepositoryID, "-1") }, ErrInvalidMetadata, MetaRepositoryID},
		{"non-integer forward hops", func(m *message.Message) { m.Metadata.Set(MetaForwardHops, "two") }, ErrInvalidMetadata, MetaForwardHops},
		{"negative forward hops", func(m *message.Message) { m.Metadata.Set(MetaForwardHops, "-1") }, ErrInvalidMetadata, MetaForwardHops},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := good()
			tc.mutate(msg)
			env, err := FromMessage(msg)
			require.ErrorIs(t, err, tc.want)
			require.ErrorContains(t, err, tc.key)
			assert.Equal(t, Envelope{}, env)
		})
	}

	t.Run("nil message", func(t *testing.T) {
		_, err := FromMessage(nil)
		require.ErrorIs(t, err, ErrMissingMetadata)
	})

	t.Run("empty repository_id is treated as absent", func(t *testing.T) {
		msg := good()
		msg.Metadata.Set(MetaRepositoryID, "")
		env, err := FromMessage(msg)
		require.NoError(t, err)
		assert.Zero(t, env.RepositoryID)
	})
}
