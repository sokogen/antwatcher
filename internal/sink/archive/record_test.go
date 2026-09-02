package archive_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/archive"
)

func TestEncodeDecode_RoundTripEveryFixture(t *testing.T) {
	for _, fixture := range event.Fixtures() {
		t.Run(fixture, func(t *testing.T) {
			env := fixtureEnvelope(t, fixture)
			line, err := archive.Encode(env)
			require.NoError(t, err)
			assert.True(t, bytes.HasSuffix(line, []byte("\n")))
			assert.Equal(t, 1, bytes.Count(line, []byte("\n")), "one line per record even for a pretty-printed payload")

			got, err := archive.Decode(line)
			require.NoError(t, err)
			assert.Equal(t, compacted(t, env), got)
			assert.JSONEq(t, string(env.Payload), string(got.Payload), "payload values preserved")
		})
	}
}

func TestEncodeDecode_CompactPayloadIsByteIdentical(t *testing.T) {
	env := compacted(t, fixtureEnvelope(t, "workflow_job.completed"))
	line, err := archive.Encode(env)
	require.NoError(t, err)
	got, err := archive.Decode(line)
	require.NoError(t, err)
	assert.Equal(t, env, got)
	assert.Equal(t, env.Payload, got.Payload)
}

func TestEncode_Shape(t *testing.T) {
	env := fixtureEnvelope(t, "workflow_run.completed")
	line, err := archive.Encode(env)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(line, &doc))
	assert.InDelta(t, 1, doc["schema_version"], 0)
	assert.Equal(t, "guid-workflow_run.completed", doc["delivery_guid"])
	assert.Equal(t, "workflow_run", doc["event"])
	assert.Equal(t, "completed", doc["action"])
	assert.Equal(t, "12345", doc["hook_id"])
	assert.Equal(t, "2026-09-02T10:05:00Z", doc["received_at"])
	assert.Equal(t, "sokogen/antwatcher", doc["repository"])
	assert.NotZero(t, doc["repository_id"])
	payload, ok := doc["payload"].(map[string]any)
	require.True(t, ok, "payload nested as a JSON value")
	assert.Contains(t, payload, "workflow_run")
	assert.True(t, bytes.HasPrefix(line, []byte(`{"schema_version":1,"delivery_guid":`)), "field order is stable: %s", line[:60])
}

func TestEncode_OptionalFieldsOmittedAndUTC(t *testing.T) {
	env := event.Envelope{
		SchemaVersion: 1, DeliveryGUID: "g", Event: "ping",
		ReceivedAt: time.Date(2026, 9, 2, 12, 0, 0, 0, time.FixedZone("x", 2*3600)),
		Payload:    []byte(`{"zen":"a<b>&c"}`),
	}
	line, err := archive.Encode(env)
	require.NoError(t, err)
	want := `{"schema_version":1,"delivery_guid":"g","event":"ping","received_at":"2026-09-02T10:00:00Z","payload":{"zen":"a<b>&c"}}` + "\n"
	assert.True(t, bytes.Equal([]byte(want), line), "exact bytes: no optional fields, UTC, no HTML escaping\n got %s\nwant %s", line, want)
	got, err := archive.Decode(line)
	require.NoError(t, err)
	assert.Equal(t, time.UTC, got.ReceivedAt.Location())
	assert.Equal(t, env.ReceivedAt.UTC(), got.ReceivedAt)
}

func TestEncode_BadPayloadIsPermanent(t *testing.T) {
	for name, payload := range map[string][]byte{
		"nil":     nil,
		"blank":   []byte("  \n"),
		"invalid": []byte(`{"a":`),
	} {
		t.Run(name, func(t *testing.T) {
			env := fixtureEnvelope(t, "ping")
			env.Payload = payload
			_, err := archive.Encode(env)
			require.Error(t, err)
			assert.True(t, sink.IsPermanent(err), "%v", err)
			assert.Contains(t, err.Error(), "guid-ping")
		})
	}
}

func TestDecode_Errors(t *testing.T) {
	cases := map[string]string{
		"not json":       `{`,
		"not an object":  `[1]`,
		"missing guid":   `{"event":"ping","payload":{}}`,
		"missing event":  `{"delivery_guid":"g","payload":{}}`,
		"missing":        `{"delivery_guid":"g","event":"ping"}`,
		"null payload":   `{"delivery_guid":"g","event":"ping","payload":null}`,
		"wrong guid":     `{"delivery_guid":1,"event":"ping","payload":{}}`,
		"bad timestamp":  `{"delivery_guid":"g","event":"ping","received_at":"yesterday","payload":{}}`,
		"bad repository": `{"delivery_guid":"g","event":"ping","repository_id":"x","payload":{}}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := archive.Decode([]byte(line))
			require.Error(t, err)
		})
	}
}

func TestReader_ReadsLinesSkipsBlanksAndLastLineWithoutNewline(t *testing.T) {
	a := compacted(t, fixtureEnvelope(t, "ping"))
	b := compacted(t, fixtureEnvelope(t, "workflow_job.queued"))
	la, err := archive.Encode(a)
	require.NoError(t, err)
	lb, err := archive.Encode(b)
	require.NoError(t, err)
	stream := string(la) + "\n\n" + strings.TrimSuffix(string(lb), "\n")
	envs, err := archive.ReadAll(strings.NewReader(stream))
	require.NoError(t, err)
	assert.Equal(t, []event.Envelope{a, b}, envs)

	envs, err = archive.ReadAll(strings.NewReader(""))
	require.NoError(t, err)
	assert.Empty(t, envs)
	envs, err = archive.ReadAll(strings.NewReader("\n\n"))
	require.NoError(t, err)
	assert.Empty(t, envs)

	r := archive.NewReader(strings.NewReader(stream))
	_, err = r.Next()
	require.NoError(t, err)
	_, err = r.Next()
	require.NoError(t, err)
	_, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
	_, err = r.Next()
	require.ErrorIs(t, err, io.EOF, "EOF is sticky")
}

func TestReader_BadLineNamesItsNumber(t *testing.T) {
	la, err := archive.Encode(fixtureEnvelope(t, "ping"))
	require.NoError(t, err)
	envs, err := archive.ReadAll(strings.NewReader(string(la) + "garbage\n" + string(la)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "line 2")
	assert.Len(t, envs, 1, "records before the bad line are returned")
}

func TestReader_LongLines(t *testing.T) {
	env := fixtureEnvelope(t, "ping")
	env.Payload = []byte(`{"pad":"` + strings.Repeat("x", 1<<20) + `"}`)
	line, err := archive.Encode(env)
	require.NoError(t, err)
	envs, err := archive.ReadAll(bytes.NewReader(line))
	require.NoError(t, err)
	require.Len(t, envs, 1)
	assert.Len(t, envs[0].Payload, len(env.Payload))
}

func TestReadFile_PlainAndGzip(t *testing.T) {
	dir := t.TempDir()
	env := compacted(t, fixtureEnvelope(t, "workflow_run.requested"))
	line, err := archive.Encode(env)
	require.NoError(t, err)

	plain := filepath.Join(dir, "a.jsonl")
	require.NoError(t, os.WriteFile(plain, line, 0o600))
	envs, err := archive.ReadFile(plain)
	require.NoError(t, err)
	assert.Equal(t, []event.Envelope{env}, envs)

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err = gw.Write(line)
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	gz := filepath.Join(dir, "a.jsonl.gz")
	require.NoError(t, os.WriteFile(gz, buf.Bytes(), 0o600))
	envs, err = archive.ReadFile(gz)
	require.NoError(t, err)
	assert.Equal(t, []event.Envelope{env}, envs)

	_, err = archive.ReadFile(filepath.Join(dir, "missing.jsonl"))
	require.Error(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.jsonl.gz"), []byte("not gzip"), 0o600))
	_, err = archive.ReadFile(filepath.Join(dir, "bad.jsonl.gz"))
	require.Error(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.jsonl"), []byte("{\n"), 0o600))
	_, err = archive.ReadFile(filepath.Join(dir, "bad.jsonl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad.jsonl")
}
