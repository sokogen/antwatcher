package config_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
)

func TestParseByteSize(t *testing.T) {
	cases := map[string]config.ByteSize{
		"0":          0,
		"12":         12,
		"12B":        12,
		"12 b":       12,
		"1KiB":       config.KiB,
		"1kib":       config.KiB,
		"1K":         config.KiB,
		"1KB":        1000,
		"128MiB":     128 * config.MiB,
		"128MB":      128 * 1000 * 1000,
		"128M":       128 * config.MiB,
		"2GiB":       2 * config.GiB,
		"2GB":        2 * 1000 * 1000 * 1000,
		"1TiB":       config.TiB,
		"1TB":        1000 * 1000 * 1000 * 1000,
		"  64 MiB  ": 64 * config.MiB,
	}
	for text, want := range cases {
		t.Run(text, func(t *testing.T) {
			got, err := config.ParseByteSize(text)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestParseByteSize_Errors(t *testing.T) {
	for _, text := range []string{"", "  ", "MiB", "-1", "1.5GiB", "1PiB", "1 MiBs", "12x", "99999999999999999999", "9223372036854775807KiB"} {
		t.Run(text, func(t *testing.T) {
			_, err := config.ParseByteSize(text)
			require.Error(t, err)
		})
	}
}

func TestByteSize_String(t *testing.T) {
	assert.Equal(t, "0B", config.ByteSize(0).String())
	assert.Equal(t, "12B", config.ByteSize(12).String())
	assert.Equal(t, "1KiB", config.KiB.String())
	assert.Equal(t, "128MiB", (128 * config.MiB).String())
	assert.Equal(t, "3GiB", (3 * config.GiB).String())
	assert.Equal(t, "2TiB", (2 * config.TiB).String())
	assert.Equal(t, "1536KiB", (config.MiB + 512*config.KiB).String())
	assert.Equal(t, "1000000B", config.ByteSize(1000000).String())
	assert.Equal(t, "-5B", config.ByteSize(-5).String())
}

func TestByteSize_YAMLRoundTrip(t *testing.T) {
	type block struct {
		Size config.ByteSize `yaml:"size"`
	}
	var b block
	require.NoError(t, yaml.Unmarshal([]byte("size: 128MiB"), &b))
	assert.Equal(t, 128*config.MiB, b.Size)
	require.NoError(t, yaml.Unmarshal([]byte("size: 4096"), &b), "a plain integer scalar is bytes")
	assert.Equal(t, 4*config.KiB, b.Size)

	out, err := yaml.Marshal(block{Size: 128 * config.MiB})
	require.NoError(t, err)
	assert.Equal(t, "size: 128MiB\n", string(out))

	require.Error(t, yaml.Unmarshal([]byte("size: big"), &b))
	require.Error(t, yaml.Unmarshal([]byte("size: -1"), &b))

	var n yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte("size: 1GiB"), &n))
	var strict block
	require.NoError(t, config.DecodeStrict(*n.Content[0], &strict), "strict driver decoding accepts the type")
	assert.Equal(t, config.GiB, strict.Size)
}

func TestByteSize_JSON(t *testing.T) {
	out, err := json.Marshal(map[string]config.ByteSize{"size": 2 * config.GiB})
	require.NoError(t, err)
	assert.JSONEq(t, `{"size":"2GiB"}`, string(out))

	var in map[string]config.ByteSize
	require.NoError(t, json.Unmarshal([]byte(`{"size":"512KiB"}`), &in))
	assert.Equal(t, 512*config.KiB, in["size"])
}
