package config

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSecret_Masking(t *testing.T) {
	s := Secret("hunter2")

	assert.Equal(t, "hunter2", s.Reveal())
	assert.Equal(t, Mask, s.String())
	assert.Equal(t, "***/***", fmt.Sprintf("%s/%v", s, s))
	assert.Equal(t, `"***"`, fmt.Sprintf("%q", s))
	assert.NotContains(t, fmt.Sprintf("%#v", s), "hunter2")
	assert.NotContains(t, fmt.Sprintf("%+v", struct{ S Secret }{s}), "hunter2")

	js, err := json.Marshal(struct {
		Token Secret `json:"token"`
	}{s})
	require.NoError(t, err)
	assert.JSONEq(t, `{"token":"***"}`, string(js))

	ym, err := yaml.Marshal(struct {
		Token Secret `yaml:"token"`
	}{s})
	require.NoError(t, err)
	assert.Equal(t, "token: '***'\n", string(ym))
}

func TestSecret_EmptyStaysEmpty(t *testing.T) {
	var s Secret
	assert.Empty(t, s.String())
	assert.Empty(t, s.Reveal())

	js, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, `""`, string(js))

	ym, err := yaml.Marshal(struct {
		Token Secret `yaml:"token"`
	}{})
	require.NoError(t, err)
	assert.Equal(t, "token: \"\"\n", string(ym))
}

func TestSecret_UnmarshalYAML(t *testing.T) {
	var out struct {
		Plain  Secret `yaml:"plain"`
		Quoted Secret `yaml:"quoted"`
		Number Secret `yaml:"number"`
		Empty  Secret `yaml:"empty"`
	}
	require.NoError(t, yaml.Unmarshal([]byte("plain: abc\nquoted: \"a: #b\"\nnumber: 12345\nempty:\n"), &out))
	assert.Equal(t, "abc", out.Plain.Reveal())
	assert.Equal(t, "a: #b", out.Quoted.Reveal())
	assert.Equal(t, "12345", out.Number.Reveal())
	assert.Empty(t, out.Empty.Reveal())

	var bad struct {
		S Secret `yaml:"s"`
	}
	err := yaml.Unmarshal([]byte("s:\n  nested: value\n"), &bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret must be a scalar")
}
