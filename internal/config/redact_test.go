package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type natsTestConfig struct {
	URL         string `yaml:"url" json:"url"`
	Credentials Secret `yaml:"credentials" json:"credentials"`
	Stream      string `yaml:"stream" json:"stream"`
}

func natsDescriber(raw yaml.Node) (any, error) {
	c := natsTestConfig{Stream: "DEFAULT"}
	if err := DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

const redactSource = `
server:
  webhook_secret: top-secret
bus:
  driver: nats-jetstream
  nats-jetstream:
    url: nats://h:4222
    credentials: creds-value
  kafka:
    brokers: [b:9092]
    sasl_password: kafka-pass
    api_key: ""
sinks:
  - name: loki
    class: log
    driver: otlp
    start_from: now
    config:
      endpoint: h:4318
      headers:
        Authorization: "Bearer tok"
        X-Scope-OrgID: tenant
      auth: {token: nested-tok, user: bob}
      credentials: {file: /x, key: y}
      items:
        - {password: p1, name: n1}
  - name: typed
    class: log
    driver: typed
    start_from: now
    config:
      url: u
      credentials: typed-secret
  - name: bare
    class: log
    driver: stdout
    start_from: now
recovery:
  auth:
    token: gh-token
`

func TestRedacted(t *testing.T) {
	cfg, err := Parse([]byte(redactSource))
	require.NoError(t, err)

	describers := Describers{
		Bus:   map[string]Describer{"nats-jetstream": DescriberFunc(natsDescriber)},
		Sinks: map[string]Describer{SinkKey("log", "typed"): DescriberFunc(natsDescriber)},
	}
	red, err := Redacted(cfg, describers)
	require.NoError(t, err)

	out, err := yaml.Marshal(red)
	require.NoError(t, err)
	text := string(out)

	for _, leak := range []string{"top-secret", "creds-value", "kafka-pass", "Bearer tok", "nested-tok", "/x", "p1", "typed-secret", "gh-token"} {
		assert.NotContains(t, text, leak, "yaml leaks %q", leak)
	}
	for _, visible := range []string{"nats://h:4222", "b:9092", "h:4318", "tenant", "bob", "n1", "url: u", "stream: DEFAULT", "stream: DEFAULT"} {
		assert.Contains(t, text, visible)
	}
	assert.Contains(t, text, "webhook_secret: '***'")

	// typed describer: Secret masks, defaults visible, decoded as the driver type
	typed, ok := red.Bus.Drivers["nats-jetstream"].(natsTestConfig)
	require.True(t, ok)
	assert.Equal(t, "creds-value", typed.Credentials.Reveal(), "redacted view holds the typed config; masking happens on render")
	assert.Equal(t, "DEFAULT", typed.Stream)

	// fallback: masked by key, empty values stay empty, nested subtrees masked whole
	kafka := red.Bus.Drivers["kafka"].(map[string]any)
	assert.Equal(t, Mask, kafka["sasl_password"])
	assert.IsType(t, "", kafka["api_key"], "empty stays an empty string, not null")
	assert.Empty(t, kafka["api_key"])
	assert.Equal(t, []any{"b:9092"}, kafka["brokers"])

	loki := red.Sinks[0].Config.(map[string]any)
	headers := loki["headers"].(map[string]any)
	assert.Equal(t, Mask, headers["Authorization"])
	assert.Equal(t, "tenant", headers["X-Scope-OrgID"])
	auth := loki["auth"].(map[string]any)
	assert.Equal(t, Mask, auth["token"])
	assert.Equal(t, "bob", auth["user"])
	assert.Equal(t, Mask, loki["credentials"], "subtree under a secret key is masked whole")
	items := loki["items"].([]any)
	assert.Equal(t, map[string]any{"password": Mask, "name": "n1"}, items[0])

	assert.Nil(t, red.Sinks[2].Config, "absent block renders as nothing")

	// JSON rendering (used by /status) masks too
	js, err := json.Marshal(red)
	require.NoError(t, err)
	for _, leak := range []string{"top-secret", "creds-value", "kafka-pass", "Bearer tok", "typed-secret", "gh-token"} {
		assert.NotContains(t, string(js), leak, "json leaks %q", leak)
	}
	assert.Contains(t, string(js), `"webhook_secret":"***"`)
}

func TestRedacted_DescriberError(t *testing.T) {
	cfg, err := Parse([]byte("server:\n  webhook_secret: s\nbus:\n  nats-jetstream:\n    unknown_key: 1\n"))
	require.NoError(t, err)
	_, err = Redacted(cfg, Describers{Bus: map[string]Describer{"nats-jetstream": DescriberFunc(natsDescriber)}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bus.nats-jetstream")
	assert.Contains(t, err.Error(), "unknown_key")
}

func TestMaskByKey_Patterns(t *testing.T) {
	in := map[string]any{
		"Token": "a", "client_secret": "b", "PASSWORD": "c", "passwd": "d", "authorization": "e",
		"api_key": "f", "api-key": "g", "apikey": "h", "credentials": "i", "GITHUB_TOKEN": "j",
		"private_key": "k", "endpoint": "keep", "tokens_per_second": 5,
	}
	out := maskByKey(in).(map[string]any)
	for k := range in {
		switch k {
		case "endpoint":
			assert.Equal(t, "keep", out[k])
		case "tokens_per_second":
			assert.Equal(t, Mask, out[k], "numbers under secret-looking keys are masked too")
		default:
			assert.Equal(t, Mask, out[k], k)
		}
	}
}

func TestMaskByKey_CredentialSpellings(t *testing.T) {
	// The fallback masks driver blocks of drivers this build does not
	// register: nothing knows their shape, so only the key name is a signal.
	// A block like this reaches Redacted because Bus.UnmarshalYAML accepts
	// any mapping under bus:, and only the selected driver is validated.
	source := `
server:
  webhook_secret: s
bus:
  driver: gochannel
  gochannel: {}
  redis:
    addr: redis:6379
    pass: leaked-pass
    pwd: leaked-pwd
    bearer: leaked-bearer
    passphrase: leaked-phrase
    user: bob
    db: 0
`
	cfg, err := Parse([]byte(source))
	require.NoError(t, err)
	red, err := Redacted(cfg, Describers{})
	require.NoError(t, err)

	block := red.Bus.Drivers["redis"].(map[string]any)
	for _, key := range []string{"pass", "pwd", "bearer", "passphrase"} {
		assert.Equal(t, Mask, block[key], "%q holds a credential and must be masked", key)
	}
	assert.Equal(t, "redis:6379", block["addr"], "an address is not a credential")
	assert.Equal(t, "bob", block["user"])

	out, err := yaml.Marshal(red)
	require.NoError(t, err)
	for _, leak := range []string{"leaked-pass", "leaked-pwd", "leaked-bearer", "leaked-phrase"} {
		assert.NotContains(t, string(out), leak)
	}
}
