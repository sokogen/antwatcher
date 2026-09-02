package filesystem_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/archive"
	"github.com/sokogen/antwatcher/internal/sink/archive/drivers/filesystem"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

func block(t *testing.T, text string) yaml.Node {
	t.Helper()
	var n yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(text), &n))
	if n.Kind == yaml.DocumentNode && len(n.Content) == 1 {
		return *n.Content[0]
	}
	return n
}

func fixtureEnvelope(t *testing.T, fixture string) event.Envelope {
	t.Helper()
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, "workflow_run")
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), received)
	require.NoError(t, err)
	return env
}

func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	require.NoError(t, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	}))
	sort.Strings(files)
	return files
}

func TestDriver_RegisteredInDefaultRegistry(t *testing.T) {
	assert.Contains(t, sink.Drivers(sink.ClassArchive), filesystem.Name)
	d, ok := sink.Describers()[config.SinkKey("archive", "filesystem")]
	require.True(t, ok)
	typed, err := d.Describe(block(t, "dir: ./data/archive\ncompress: gzip\nrotate_every: 1h\nrotate_size: 128MiB"))
	require.NoError(t, err)
	assert.Equal(t, filesystem.Config{Dir: "./data/archive", Compress: "gzip", RotateEvery: time.Hour, RotateSize: 128 * config.MiB}, typed)

	typed, err = d.Describe(yaml.Node{})
	require.NoError(t, err)
	assert.Equal(t, filesystem.DefaultConfig(), typed, "absent block yields defaults")
	assert.Equal(t, filesystem.Config{Compress: "none", RotateSize: 128 * config.MiB, RotateEvery: time.Hour}, filesystem.DefaultConfig())
}

func TestDescribe_RendersWithUnits(t *testing.T) {
	typed, err := filesystem.Describe(block(t, "dir: /var/lib/antwatcher\nrotate_size: 1GiB\nrotate_every: 30m"))
	require.NoError(t, err)
	out, err := yaml.Marshal(typed)
	require.NoError(t, err)
	assert.Equal(t, "dir: /var/lib/antwatcher\ncompress: none\nrotate_size: 1GiB\nrotate_every: 30m0s\n", string(out))
	js, err := json.Marshal(typed)
	require.NoError(t, err)
	assert.Contains(t, string(js), `"RotateSize":"1GiB"`)
}

func TestConfig_Validate(t *testing.T) {
	ok := filesystem.DefaultConfig()
	ok.Dir = "x"
	require.NoError(t, ok.Validate())
	require.NoError(t, ok.ValidateWith(config.Router{}))
	require.NoError(t, filesystem.Config{Dir: "x"}.Validate(), "empty compress means none")
	require.NoError(t, filesystem.Config{Dir: "x", Compress: "gzip"}.Validate())

	err := filesystem.Config{Compress: "zstd", RotateSize: -1, RotateEvery: -time.Second}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dir is required")
	assert.Contains(t, err.Error(), `compress must be "none" or "gzip", got "zstd"`)
	assert.Contains(t, err.Error(), "rotate_size must not be negative")
	assert.Contains(t, err.Error(), "rotate_every must not be negative")
}

func TestFactory_ConfigErrors(t *testing.T) {
	cases := map[string]string{
		"unknown key":   "dir: x\nbucket: y",
		"missing dir":   "compress: gzip",
		"bad compress":  "dir: x\ncompress: lz4",
		"bad size":      "dir: x\nrotate_size: big",
		"bad size unit": "dir: x\nrotate_size: 1PiB",
		"bad duration":  "dir: x\nrotate_every: soon",
		"negative":      "dir: x\nrotate_every: -1h",
		"not a map":     "- x",
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := filesystem.Factory(context.Background(), "raw", block(t, text), sink.Deps{})
			require.Error(t, err)
			assert.Nil(t, s)
		})
	}
	t.Run("unusable dir", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(file, nil, 0o600))
		_, err := filesystem.Factory(context.Background(), "raw", block(t, "dir: "+file), sink.Deps{})
		require.Error(t, err)
	})
}

func TestFactory_WritesRotatesAndCompresses(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	reg := sink.NewRegistry()
	reg.RegisterDriver(sink.ClassArchive, filesystem.Name, filesystem.Driver())
	instances, err := reg.Build(context.Background(), []config.SinkConfig{{
		Name: "raw", Class: "archive", Driver: "filesystem", StartFrom: "earliest",
		Config: block(t, "dir: "+dir+"\ncompress: gzip\nrotate_size: 1KiB\nrotate_every: 0s"),
	}}, sink.Deps{Logger: slog.New(slog.DiscardHandler)})
	require.NoError(t, err)
	require.Len(t, instances, 1)
	s := instances[0]
	assert.Equal(t, "raw", s.Name())
	assert.Equal(t, sink.ClassArchive, s.Class())
	assert.Equal(t, "filesystem", s.Driver)
	assert.DirExists(t, dir, "the factory creates the directory")

	envs := []event.Envelope{
		fixtureEnvelope(t, "workflow_run.requested"),
		fixtureEnvelope(t, "workflow_run.in_progress"),
		fixtureEnvelope(t, "workflow_run.completed"),
	}
	for _, env := range envs {
		require.NoError(t, s.Process(context.Background(), env))
	}
	require.NoError(t, s.Close())

	files := listFiles(t, dir)
	require.Len(t, files, 3, "every fixture exceeds 1KiB, so each gets its own file: %v", files)
	var got []string
	for _, rel := range files {
		assert.Regexp(t, `^2026/09/02/raw-20260902T100500Z-\d\.jsonl\.gz$`, rel)
		read, err := archive.ReadFile(filepath.Join(dir, rel))
		require.NoError(t, err)
		require.Len(t, read, 1)
		got = append(got, read[0].DeliveryGUID)
	}
	assert.Equal(t, []string{"guid-workflow_run.requested", "guid-workflow_run.in_progress", "guid-workflow_run.completed"}, got)
}

func TestFactory_DefaultsKeepOneOpenFile(t *testing.T) {
	dir := t.TempDir()
	s, err := filesystem.Factory(context.Background(), "raw", block(t, "dir: "+dir), sink.Deps{})
	require.NoError(t, err)
	for _, fixture := range []string{"workflow_run.requested", "workflow_run.completed"} {
		require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, fixture)))
	}
	assert.Equal(t, []string{"raw-current.jsonl.part"}, listFiles(t, dir))
	require.NoError(t, s.Close())
	assert.Equal(t, []string{"2026/09/02/raw-20260902T100500Z-1.jsonl"}, listFiles(t, dir), "plain by default")
}
