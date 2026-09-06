package config

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestURL_MasksUserinfoOnEveryRenderPath(t *testing.T) {
	u := URL("nats://admin:hunter2@nats.example.com:4222")

	assert.Equal(t, "nats://***@nats.example.com:4222", u.String())
	assert.Equal(t, "nats://admin:hunter2@nats.example.com:4222", u.Reveal())
	assert.NotContains(t, fmt.Sprintf("%v %s %q %#v", u, u, u, u), "hunter2")

	y, err := yaml.Marshal(u)
	require.NoError(t, err)
	assert.Equal(t, "nats://***@nats.example.com:4222\n", string(y))

	j, err := json.Marshal(u)
	require.NoError(t, err)
	assert.JSONEq(t, `"nats://***@nats.example.com:4222"`, string(j))
}

func TestURL_String(t *testing.T) {
	tests := []struct {
		name string
		in   URL
		want string
	}{
		{"empty stays empty", "", ""},
		{"no userinfo is left alone", "nats://nats.example.com:4222", "nats://nats.example.com:4222"},
		{"user without password", "nats://admin@h:4222", "nats://***@h:4222"},
		{"user and password", "nats://admin:p@h:4222", "nats://***@h:4222"},
		{"every element of a cluster list", "nats://a:b@h1:4222, nats://c:d@h2:4222", "nats://***@h1:4222,nats://***@h2:4222"},
		{"mixed list masks only what carries credentials", "nats://h1:4222,nats://a:b@h2:4222", "nats://h1:4222,nats://***@h2:4222"},
		{"unparseable is masked whole", "nats://a:b@h:42 22\x7f", Mask},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.in.String())
		})
	}
}

func TestURL_UnmarshalYAML(t *testing.T) {
	var v struct {
		URL URL `yaml:"url"`
	}
	require.NoError(t, yaml.Unmarshal([]byte("url: nats://a:b@h:4222\n"), &v))
	assert.Equal(t, "nats://a:b@h:4222", v.URL.Reveal())

	require.ErrorContains(t, yaml.Unmarshal([]byte("url: [a, b]\n"), &v), "url must be a scalar")
}
