package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "antwatcher.yml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load(writeTemp(t, "server:\n  webhook_secret: s3cret\n"))
	require.NoError(t, err)

	want := Default()
	want.Server.WebhookSecret = "s3cret"
	assert.Equal(t, want, cfg)

	assert.Equal(t, ":8080", cfg.Server.Listen)
	assert.Equal(t, "/webhook", cfg.Server.WebhookPath)
	assert.Equal(t, int64(25<<20), cfg.Server.MaxBodyBytes)
	assert.Equal(t, 8*time.Second, cfg.Server.PublishTimeout)
	assert.Equal(t, "127.0.0.1:9090", cfg.Admin.Listen)
	assert.Equal(t, 15*time.Second, cfg.Admin.LagInterval)
	assert.Equal(t, "nats-jetstream", cfg.Bus.Driver)
	assert.Equal(t, "antwatcher.events", cfg.Bus.Topic)
	assert.Equal(t, OnMissingCapabilityFail, cfg.Bus.OnMissingCapability)
	assert.Equal(t, 30*time.Second, cfg.Router.CloseTimeout)
	assert.Equal(t, 60*time.Second, cfg.Router.ProcessTimeout)
	assert.False(t, cfg.Recovery.Enabled)
	assert.Equal(t, AuthTypeToken, cfg.Recovery.Auth.Type)
	assert.Equal(t, 10*time.Minute, cfg.Recovery.Interval)
	assert.Equal(t, 72*time.Hour, cfg.Recovery.Lookback)
	assert.Equal(t, 15*time.Minute, cfg.Recovery.Overlap)
	assert.Equal(t, 5*time.Minute, cfg.Recovery.Grace)
	assert.Equal(t, 100, cfg.Recovery.MaxPerScan)
	assert.Empty(t, cfg.Sinks)
}

func TestLoad_EmptyFileIsDefaults(t *testing.T) {
	cfg, err := Load(writeTemp(t, ""))
	require.NoError(t, err)
	assert.Equal(t, Default(), cfg)
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestLoad_InvalidYAML(t *testing.T) {
	_, err := Load(writeTemp(t, "server: [unclosed\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse yaml")
}

func TestLoad_UnknownCoreFieldRejected(t *testing.T) {
	_, err := Load(writeTemp(t, "server:\n  webhook_secret: s\n  webhok_path: /x\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "webhok_path")
	assert.Contains(t, err.Error(), "not found")
}

func TestLoad_EnvExpansion(t *testing.T) {
	t.Setenv("AW_SECRET", "s3cret")
	t.Setenv("AW_PORT", "9999")
	t.Setenv("AW_MAX", "1024")
	t.Setenv("AW_TRICKY", "a: #b")
	t.Setenv("AW_EMPTY", "")
	require.NoError(t, os.Unsetenv("AW_UNSET_FOR_TEST"))

	cfg, err := Load(writeTemp(t, `
server:
  listen: ":${AW_PORT}"
  webhook_secret: ${AW_SECRET}
  public_url: "${AW_TRICKY}"
  max_body_bytes: ${AW_MAX}
  webhook_path: ${AW_UNSET_FOR_TEST:-/hooks}
admin:
  listen: ${AW_TRICKY}
bus:
  topic: prefix-${AW_SECRET}-${AW_PORT}
  nats-jetstream:
    url: ${AW_TRICKY}
    credentials: ${AW_EMPTY:-fallback}
router:
  close_timeout: ${AW_UNSET_FOR_TEST:-45s}
sinks:
  - name: a
    class: log
    driver: stdout
    start_from: now
    config:
      stream: ${AW_TRICKY}
      count: ${AW_MAX}
`))
	require.NoError(t, err)

	assert.Equal(t, ":9999", cfg.Server.Listen)
	assert.Equal(t, "s3cret", cfg.Server.WebhookSecret.Reveal())
	assert.Equal(t, "a: #b", cfg.Server.PublicURL, "quoted value with ': #' stays one scalar")
	assert.Equal(t, "a: #b", cfg.Admin.Listen, "plain value with ': #' must not become a mapping or comment")
	assert.Equal(t, int64(1024), cfg.Server.MaxBodyBytes, "expanded plain scalar re-resolves as a number")
	assert.Equal(t, "/hooks", cfg.Server.WebhookPath, "default applies when the variable is unset")
	assert.Equal(t, "prefix-s3cret-9999", cfg.Bus.Topic, "multiple references in one value")
	assert.Equal(t, 45*time.Second, cfg.Router.CloseTimeout, "default value decodes as a duration")

	// expansion reaches into opaque driver blocks
	var nats struct {
		URL         string `yaml:"url"`
		Credentials Secret `yaml:"credentials"`
	}
	require.NoError(t, DecodeStrict(cfg.Bus.Drivers["nats-jetstream"], &nats))
	assert.Equal(t, "a: #b", nats.URL)
	assert.Empty(t, nats.Credentials.Reveal(), "a set-but-empty variable wins over the default")
	var natsAny map[string]any
	natsNode := cfg.Bus.Drivers["nats-jetstream"]
	require.NoError(t, natsNode.Decode(&natsAny))
	assert.IsType(t, "", natsAny["credentials"], "an empty expansion is an empty string, not null")
	assert.Empty(t, natsAny["credentials"])

	var sinkCfg struct {
		Stream string `yaml:"stream"`
		Count  int    `yaml:"count"`
	}
	require.NoError(t, DecodeStrict(cfg.Sinks[0].Config, &sinkCfg))
	assert.Equal(t, "a: #b", sinkCfg.Stream)
	assert.Equal(t, 1024, sinkCfg.Count)
}

func TestLoad_UndefinedVariableListsNames(t *testing.T) {
	require.NoError(t, os.Unsetenv("AW_MISSING_ONE"))
	require.NoError(t, os.Unsetenv("AW_MISSING_TWO"))
	t.Setenv("AW_PRESENT", "x")
	_, err := Load(writeTemp(t, "server:\n  webhook_secret: ${AW_MISSING_TWO}\n  public_url: ${AW_PRESENT}/${AW_MISSING_ONE}\n"))
	require.Error(t, err)
	assert.Equal(t, "undefined environment variables: AW_MISSING_ONE, AW_MISSING_TWO", errorTail(err))
}

// errorTail strips the "path: " prefix Load adds.
func errorTail(err error) string {
	s := err.Error()
	if i := lastIndexOfPathSep(s); i >= 0 {
		return s[i:]
	}
	return s
}

func lastIndexOfPathSep(s string) int {
	const marker = "antwatcher.yml: "
	for i := 0; i+len(marker) <= len(s); i++ {
		if s[i:i+len(marker)] == marker {
			return i + len(marker)
		}
	}
	return -1
}

func TestLoad_MappingKeysAreNotExpanded(t *testing.T) {
	require.NoError(t, os.Unsetenv("AW_KEY_VAR"))
	cfg, err := Load(writeTemp(t, "server:\n  webhook_secret: s\nbus:\n  ${AW_KEY_VAR}:\n    a: 1\n"))
	require.NoError(t, err)
	_, ok := cfg.Bus.Drivers["${AW_KEY_VAR}"]
	assert.True(t, ok)
}

func TestLoad_OpaqueBlocksPreserved(t *testing.T) {
	cfg, err := Load(writeTemp(t, `
server:
  webhook_secret: s
bus:
  driver: nats-jetstream
  nats-jetstream:
    embedded: true
    retention: 168h
    nested:
      list: [1, 2]
  kafka:
    brokers: [a:9092]
sinks:
  - name: bq
    class: analytics
    driver: bigquery
    start_from: earliest
    config: {project: p, dataset: d, ensure_table: true}
  - name: bare
    class: log
    driver: stdout
    start_from: now
`))
	require.NoError(t, err)

	require.Len(t, cfg.Bus.Drivers, 2)
	assert.Equal(t, yaml.MappingNode, cfg.Bus.Drivers["nats-jetstream"].Kind)
	assert.Equal(t, yaml.MappingNode, cfg.Bus.Drivers["kafka"].Kind)

	var nats struct {
		Embedded  bool          `yaml:"embedded"`
		Retention time.Duration `yaml:"retention"`
		Nested    struct {
			List []int `yaml:"list"`
		} `yaml:"nested"`
	}
	require.NoError(t, DecodeStrict(cfg.Bus.Drivers["nats-jetstream"], &nats))
	assert.True(t, nats.Embedded)
	assert.Equal(t, 168*time.Hour, nats.Retention)
	assert.Equal(t, []int{1, 2}, nats.Nested.List)

	require.Len(t, cfg.Sinks, 2)
	var bq struct {
		Project     string `yaml:"project"`
		Dataset     string `yaml:"dataset"`
		EnsureTable bool   `yaml:"ensure_table"`
	}
	require.NoError(t, DecodeStrict(cfg.Sinks[0].Config, &bq))
	assert.Equal(t, "p", bq.Project)
	assert.Equal(t, "d", bq.Dataset)
	assert.True(t, bq.EnsureTable)

	assert.Equal(t, yaml.Kind(0), cfg.Sinks[1].Config.Kind, "absent config stays a zero node")
	var untouched struct {
		Stream string `yaml:"stream"`
	}
	untouched.Stream = "default"
	require.NoError(t, DecodeStrict(cfg.Sinks[1].Config, &untouched))
	assert.Equal(t, "default", untouched.Stream, "zero node leaves driver defaults in place")
}

func TestLoad_BusRejectsUnknownScalarKey(t *testing.T) {
	_, err := Load(writeTemp(t, "server:\n  webhook_secret: s\nbus:\n  topci: x\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown field "topci"`)
}

func TestLoad_BusMustBeMapping(t *testing.T) {
	_, err := Load(writeTemp(t, "server:\n  webhook_secret: s\nbus: [a]\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bus must be a mapping")

	cfg, err := Load(writeTemp(t, "server:\n  webhook_secret: s\nbus:\n"))
	require.NoError(t, err)
	assert.Equal(t, Default().Bus, cfg.Bus, "null bus keeps defaults")
}

func TestDecodeStrict_UnknownField(t *testing.T) {
	var node yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte("a: 1\nb: 2\n"), &node))
	var out struct {
		A int `yaml:"a"`
	}
	err := DecodeStrict(*node.Content[0], &out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field b not found")
}
