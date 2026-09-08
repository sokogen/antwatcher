package archive_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink/archive"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

func fixtureEnvelope(t *testing.T, fixture string) event.Envelope {
	t.Helper()
	return fixtureEnvelopeAt(t, fixture, received)
}

func fixtureEnvelopeAt(t *testing.T, fixture string, at time.Time) event.Envelope {
	t.Helper()
	name, _, _ := strings.Cut(fixture, ".")
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, name)
	hdr.Set(event.HeaderHookID, "12345")
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), at)
	require.NoError(t, err)
	return env
}

// compacted is env with its payload compacted, which is what an archive line
// decodes back to.
func compacted(t *testing.T, env event.Envelope) event.Envelope {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, json.Compact(&buf, env.Payload))
	env.Payload = buf.Bytes()
	env.ReceivedAt = env.ReceivedAt.UTC()
	return env
}

// listFiles returns every file under dir relative to it, sorted.
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

// readTree reads every archive file under dir keyed by relative path.
func readTree(t *testing.T, dir string) map[string][]event.Envelope {
	t.Helper()
	out := map[string][]event.Envelope{}
	for _, rel := range listFiles(t, dir) {
		if !strings.HasSuffix(rel, archive.Extension) && !strings.HasSuffix(rel, archive.GzipExtension) && !strings.HasSuffix(rel, ".part") {
			continue
		}
		envs, err := archive.ReadFile(filepath.Join(dir, rel))
		require.NoError(t, err, rel)
		out[rel] = envs
	}
	return out
}

func guids(envs []event.Envelope) []string {
	out := make([]string, 0, len(envs))
	for _, e := range envs {
		out = append(out, e.DeliveryGUID)
	}
	return out
}
