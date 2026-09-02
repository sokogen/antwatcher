package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const examplePath = "../../antwatcher.example.yml"

func setExampleEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GITHUB_WEBHOOK_SECRET", "example-webhook-secret")
	t.Setenv("OTLP_AUTH", "Bearer example-otlp-token")
	t.Setenv("GITHUB_TOKEN", "ghp_example")
	t.Setenv("NATS_CREDENTIALS", "nats-creds")
	t.Setenv("DOWNSTREAM_NATS_CREDENTIALS", "downstream-creds")
}

func TestVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, &stdout, &stderr)
	assert.Equal(t, 0, code)
	assert.Contains(t, stdout.String(), "antwatcher dev")
	assert.Empty(t, stderr.String())
}

func TestUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	assert.Equal(t, 2, run(nil, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "usage: antwatcher")

	stdout.Reset()
	stderr.Reset()
	assert.Equal(t, 2, run([]string{"bogus"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), `unknown command "bogus"`)

	stdout.Reset()
	stderr.Reset()
	assert.Equal(t, 0, run([]string{"help"}, &stdout, &stderr))
	assert.Contains(t, stdout.String(), "usage: antwatcher")

	stdout.Reset()
	stderr.Reset()
	assert.Equal(t, 0, run([]string{"serve", "-h"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "-config")

	assert.Equal(t, 2, run([]string{"serve", "-bogus"}, &stdout, &stderr))
	assert.Equal(t, 2, run([]string{"serve", "extra"}, &stdout, &stderr))
	assert.Equal(t, 2, run([]string{"serve", "-log-level", "loud", "-config", examplePath}, &stdout, &stderr))
	assert.Equal(t, 2, run([]string{"serve", "-log-format", "xml", "-config", examplePath}, &stdout, &stderr))
}

func TestServeCheck_ExampleConfig(t *testing.T) {
	setExampleEnv(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"serve", "-config", examplePath, "-check"}, &stdout, &stderr)
	require.Equal(t, 0, code, "stderr: %s", stderr.String())

	out := stdout.String()
	assert.Contains(t, out, "configuration is valid")
	assert.Contains(t, out, "webhook_secret: '***'")
	assert.Contains(t, out, "driver: nats-jetstream")
	assert.Contains(t, out, "name: tempo")
	assert.Contains(t, out, "name: downstream")
	assert.Contains(t, out, "Authorization: '***'")
	assert.Contains(t, out, "credentials: '***'")
	for _, leak := range []string{"example-webhook-secret", "example-otlp-token", "ghp_example", "nats-creds", "downstream-creds"} {
		assert.NotContains(t, out, leak)
	}
	assert.Empty(t, stderr.String())
}

func TestServeCheck_RequiresWebhookSecretEnv(t *testing.T) {
	require.NoError(t, os.Unsetenv("GITHUB_WEBHOOK_SECRET"))
	var stdout, stderr bytes.Buffer
	code := run([]string{"serve", "-config", examplePath, "-check"}, &stdout, &stderr)
	assert.Equal(t, 1, code)
	assert.Contains(t, stderr.String(), "undefined environment variables: GITHUB_WEBHOOK_SECRET")
	assert.Empty(t, stdout.String())
}

func TestServeCheck_InvalidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yml")
	require.NoError(t, os.WriteFile(path, []byte("server:\n  webhook_secret: s\nsinks:\n  - name: BAD\n    class: log\n    driver: stdout\n"), 0o600))

	var stdout, stderr bytes.Buffer
	code := run([]string{"serve", "-config", path, "-check"}, &stdout, &stderr)
	assert.Equal(t, 1, code)
	assert.Contains(t, stderr.String(), "configuration is invalid")
	assert.Contains(t, stderr.String(), "name must match")
	assert.Contains(t, stderr.String(), "start_from is required")
}

func TestServeCheck_MissingFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"serve", "-config", filepath.Join(t.TempDir(), "none.yml"), "-check"}, &stdout, &stderr)
	assert.Equal(t, 1, code)
	assert.Contains(t, stderr.String(), "read config")
}

func TestServe_InvalidConfigLogsError(t *testing.T) {
	require.NoError(t, os.Unsetenv("GITHUB_WEBHOOK_SECRET"))
	var stdout, stderr bytes.Buffer
	code := run([]string{"serve", "-config", examplePath, "-log-format", "text"}, &stdout, &stderr)
	assert.Equal(t, 1, code)
	assert.Contains(t, stderr.String(), "level=ERROR")
	assert.Contains(t, stderr.String(), "configuration is invalid")
}
