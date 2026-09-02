package otlp

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
)

func yamlNode(t *testing.T, src string) yaml.Node {
	t.Helper()
	var doc yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(src), &doc))
	if doc.Kind == yaml.DocumentNode && len(doc.Content) == 1 {
		return *doc.Content[0]
	}
	return doc
}

func validConfig() Config {
	c := DefaultConfig()
	c.Endpoint = "collector:4317"
	c.Insecure = true
	return c
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	assert.Empty(t, c.Endpoint)
	assert.Equal(t, ProtocolGRPC, c.Protocol)
	assert.False(t, c.Insecure)
	assert.Equal(t, 10*time.Second, c.Timeout)
	assert.Equal(t, CompressionNone, c.Compression)
	assert.Equal(t, 2, c.Retry.Attempts)
	assert.Equal(t, 500*time.Millisecond, c.Retry.Backoff)
	assert.Error(t, c.Validate(), "endpoint is required")
}

func TestDescribe(t *testing.T) {
	t.Run("defaults applied and fields decoded", func(t *testing.T) {
		v, err := Describe(yamlNode(t, `
endpoint: https://otel.example.com:4318/otlp
protocol: http
headers:
  Authorization: Bearer tok
  X-Scope-OrgID: tenant
compression: gzip
tls:
  ca_file: /etc/ca.pem
  server_name: otel.internal
retry:
  attempts: 5
`))
		require.NoError(t, err)
		c, ok := v.(Config)
		require.True(t, ok)
		assert.Equal(t, "https://otel.example.com:4318/otlp", c.Endpoint)
		assert.Equal(t, ProtocolHTTP, c.Protocol)
		assert.Equal(t, "Bearer tok", c.Headers["Authorization"].Reveal())
		assert.Equal(t, "tenant", c.Headers["X-Scope-OrgID"].Reveal())
		assert.Equal(t, CompressionGzip, c.Compression)
		assert.Equal(t, "/etc/ca.pem", c.TLS.CAFile)
		assert.Equal(t, "otel.internal", c.TLS.ServerName)
		assert.Equal(t, 5, c.Retry.Attempts)
		assert.Equal(t, 500*time.Millisecond, c.Retry.Backoff, "default kept for the field not set")
		assert.Equal(t, 10*time.Second, c.Timeout)
		require.NoError(t, c.Validate())
	})

	t.Run("unknown key rejected", func(t *testing.T) {
		_, err := Describe(yamlNode(t, "endpoint: a:1\ninsecur: true\n"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "insecur")
	})

	t.Run("absent block is defaults", func(t *testing.T) {
		v, err := Describe(yaml.Node{})
		require.NoError(t, err)
		assert.Equal(t, DefaultConfig(), v)
	})

	t.Run("Describer variable wraps Describe", func(t *testing.T) {
		v, err := Describer.Describe(yamlNode(t, "endpoint: a:1\n"))
		require.NoError(t, err)
		assert.Equal(t, "a:1", v.(Config).Endpoint)
	})
}

func TestConfig_Validate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"missing endpoint", func(c *Config) { c.Endpoint = "" }, "endpoint is required"},
		{"endpoint without port", func(c *Config) { c.Endpoint = "collector" }, "must be host:port or a URL"},
		{"path without scheme", func(c *Config) { c.Endpoint = "collector:4318/v1" }, "a path requires a scheme"},
		{"bad scheme", func(c *Config) { c.Endpoint = "grpc://collector:4317" }, "scheme must be http or https"},
		{"https with insecure", func(c *Config) { c.Endpoint = "https://collector:4317" }, "uses https but insecure is true"},
		{"url without host", func(c *Config) { c.Endpoint = "http:///v1" }, "has no host"},
		{"url with userinfo", func(c *Config) { c.Endpoint = "http://u:p@collector:4318" }, "must not carry credentials"},
		{"url with query", func(c *Config) { c.Endpoint = "http://collector:4318/?x=1" }, "must not carry credentials, a query"},
		{"bad protocol", func(c *Config) { c.Protocol = "thrift" }, `protocol "thrift" must be grpc or http`},
		{"bad compression", func(c *Config) { c.Compression = "zstd" }, `compression "zstd" must be none or gzip`},
		{"zero timeout", func(c *Config) { c.Timeout = 0 }, "timeout must be > 0"},
		{"negative attempts", func(c *Config) { c.Retry.Attempts = -1 }, "retry.attempts must be >= 0"},
		{"negative backoff", func(c *Config) { c.Retry.Backoff = -time.Second }, "retry.backoff must be >= 0"},
		{"cert without key", func(c *Config) { c.TLS.CertFile = "c.pem" }, "tls.cert_file and tls.key_file must be set together"},
		{"key without cert", func(c *Config) { c.TLS.KeyFile = "k.pem" }, "tls.cert_file and tls.key_file must be set together"},
		{"empty header name", func(c *Config) { c.Headers = map[string]config.Secret{" ": "v"} }, "headers: empty header name"},
		{"tls on insecure endpoint", func(c *Config) { c.TLS.CAFile = "ca.pem" }, "tls settings are set but the endpoint is plaintext"},
		{"tls on http endpoint", func(c *Config) { c.Endpoint = "http://c:4318"; c.Insecure = false; c.TLS.ServerName = "x" }, "endpoint is plaintext"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(&c)
			err := c.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}

	t.Run("all errors reported together", func(t *testing.T) {
		c := Config{Protocol: "x", Compression: "y"}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "endpoint is required")
		assert.Contains(t, err.Error(), `protocol "x"`)
		assert.Contains(t, err.Error(), `compression "y"`)
		assert.Contains(t, err.Error(), "timeout must be > 0")
	})

	t.Run("valid variants", func(t *testing.T) {
		for _, ep := range []string{"collector:4317", "http://collector:4318", "https://collector", "http://[::1]:4318/base/", "127.0.0.1:4317"} {
			c := validConfig()
			c.Insecure = false
			c.Endpoint = ep
			require.NoError(t, c.Validate(), ep)
		}
	})

	t.Run("ValidateWith is Validate", func(t *testing.T) {
		c := validConfig()
		c.Endpoint = ""
		var v config.DriverValidator = c
		require.Error(t, v.ValidateWith(config.Router{}))
		require.NoError(t, config.DriverValidator(validConfig()).ValidateWith(config.Router{}))
	})
}

func TestConfig_target(t *testing.T) {
	cases := []struct {
		endpoint string
		insecure bool
		want     target
	}{
		{"collector:4317", true, target{hostPort: "collector:4317", tls: false}},
		{"collector:4317", false, target{hostPort: "collector:4317", tls: true}},
		{"http://collector:4318", false, target{hostPort: "collector:4318", tls: false}},
		{"http://collector:4318", true, target{hostPort: "collector:4318", tls: false}},
		{"https://collector:4318/otlp/", false, target{hostPort: "collector:4318", tls: true, basePath: "/otlp"}},
		{"https://collector", false, target{hostPort: "collector:443", tls: true}},
		{"http://collector", false, target{hostPort: "collector:80", tls: false}},
		{"http://[::1]:4318/b", false, target{hostPort: "[::1]:4318", tls: false, basePath: "/b"}},
	}
	for _, tc := range cases {
		c := Config{Endpoint: tc.endpoint, Insecure: tc.insecure}
		got, err := c.target()
		require.NoError(t, err, tc.endpoint)
		assert.Equal(t, tc.want, got, "%s insecure=%v", tc.endpoint, tc.insecure)
	}
}

func TestConfig_Redaction(t *testing.T) {
	v, err := Describe(yamlNode(t, "endpoint: a:1\nheaders:\n  Authorization: Bearer supersecret\n  Empty: \"\"\n"))
	require.NoError(t, err)

	y, err := yaml.Marshal(v)
	require.NoError(t, err)
	assert.NotContains(t, string(y), "supersecret")
	assert.Contains(t, string(y), "Authorization: '***'")
	assert.Contains(t, string(y), `Empty: ""`, "empty secrets stay visibly empty")

	j, err := json.Marshal(v)
	require.NoError(t, err)
	assert.NotContains(t, string(j), "supersecret")
	assert.Contains(t, string(j), `"Authorization":"***"`)

	// the same through the core redaction path a sink block takes
	cfg := config.Default()
	cfg.Sinks = []config.SinkConfig{{
		Name: "tempo", Class: "trace", Driver: "otlp", StartFrom: "now",
		Config: yamlNode(t, "endpoint: a:1\nheaders:\n  Authorization: Bearer supersecret\n"),
	}}
	red, err := config.Redacted(cfg, config.Describers{Sinks: map[string]config.Describer{config.SinkKey("trace", "otlp"): Describer}})
	require.NoError(t, err)
	out, err := yaml.Marshal(red)
	require.NoError(t, err)
	assert.NotContains(t, string(out), "supersecret")
	assert.Contains(t, string(out), "endpoint: a:1")
	assert.Contains(t, string(out), "'***'")
}
