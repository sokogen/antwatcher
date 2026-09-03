package archive

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
)

func envelope(t *testing.T) event.Envelope {
	t.Helper()
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid")
	hdr.Set(event.HeaderEvent, "ping")
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, "ping"), time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC))
	require.NoError(t, err)
	return env
}

// TestStore_WriteFailureIsRetryableAndCut swaps the open file for one that
// cannot be written, the closest a test can get to a failing disk, and
// checks that the failure is retryable, nothing partial stays, and the next
// append reopens and succeeds.
func TestStore_WriteFailureIsRetryableAndCut(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(StoreOptions{Dir: dir, Prefix: "raw"})
	require.NoError(t, err)
	env := envelope(t)
	require.NoError(t, s.Append(context.Background(), env))

	s.mu.Lock()
	require.NoError(t, s.f.Close())
	ro, err := os.Open(s.part) // read-only handle: Write fails
	require.NoError(t, err)
	s.f = ro
	sizeBefore := s.size
	s.mu.Unlock()

	err = s.Append(context.Background(), env)
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	assert.Contains(t, err.Error(), "write archive record")
	s.mu.Lock()
	assert.Nil(t, s.f, "the file is closed after a failure")
	s.mu.Unlock()
	info, err := os.Stat(s.part)
	require.NoError(t, err)
	assert.Equal(t, sizeBefore, info.Size(), "no partial bytes")

	require.NoError(t, s.Append(context.Background(), env), "the next append reopens")
	require.NoError(t, s.Close())
	envs, err := ReadFile(filepath.Join(dir, "2026", "09", "02", "raw-20260902T100500Z-1.jsonl"))
	require.NoError(t, err)
	assert.Len(t, envs, 2)
}

// TestStore_CloseFinalizeFailureIsStickyOnRetry closes the underlying file
// out from under the store so Close's own finalize fails, then checks that a
// second Close call reports the same failure instead of silently returning
// nil (rotate has already dropped the file from the store's state by then,
// so there is nothing left to actually retry).
func TestStore_CloseFinalizeFailureIsStickyOnRetry(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(StoreOptions{Dir: dir, Prefix: "raw"})
	require.NoError(t, err)
	require.NoError(t, s.Append(context.Background(), envelope(t)))

	s.mu.Lock()
	require.NoError(t, s.f.Close()) // force the file Close inside rotate to fail
	s.mu.Unlock()

	err1 := s.Close()
	require.Error(t, err1)
	assert.Contains(t, err1.Error(), "finalize archive")

	err2 := s.Close()
	require.Error(t, err2, "a retried Close must not silently report success")
	assert.Equal(t, err1.Error(), err2.Error(), "the same failure is returned again")
}

func TestStore_ReopenedFileRotatesAtNextAppend(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	s, err := OpenStore(StoreOptions{Dir: dir, Prefix: "raw", RotateEvery: time.Hour, Now: func() time.Time { return now }})
	require.NoError(t, err)
	env := envelope(t)
	require.NoError(t, s.Append(context.Background(), env))

	// lose the handle as a failed write would, then append again: the file's
	// age is unknown, so it is finalized before the next record
	s.mu.Lock()
	s.abandon(0)
	s.mu.Unlock()
	require.NoError(t, s.Append(context.Background(), env))
	assert.FileExists(t, filepath.Join(dir, "2026", "09", "02", "raw-20260902T100500Z-1.jsonl"))
	s.mu.Lock()
	assert.Equal(t, now, s.openedAt, "the new file starts a fresh age")
	s.mu.Unlock()
	require.NoError(t, s.Close())
}

// TestStore_RotateCutsPartialTrailingLine simulates bytes left on disk by a
// write/fsync failure whose own abandon() truncate also failed (e.g. a
// second disk fault) - a torn line written directly to the file, bypassing
// Append/abandon entirely. rotate must cut that trailing partial line before
// finalizing, not rely solely on OpenStore's startup recover, or the torn
// line would be archived (and possibly gzip-compressed) permanently.
func TestStore_RotateCutsPartialTrailingLine(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(StoreOptions{Dir: dir, Prefix: "raw"})
	require.NoError(t, err)
	require.NoError(t, s.Append(context.Background(), envelope(t)))

	s.mu.Lock()
	_, err = s.f.WriteString(`{"broken`) // no trailing newline: a torn line
	require.NoError(t, err)
	require.NoError(t, s.f.Sync())
	s.mu.Unlock()

	require.NoError(t, s.Close())

	envs, err := ReadFile(filepath.Join(dir, "2026", "09", "02", "raw-20260902T100500Z-1.jsonl"))
	require.NoError(t, err)
	assert.Len(t, envs, 1, "the torn trailing line is cut, not archived")
}

func TestCutPartialLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	cases := map[string]struct {
		in, want string
	}{
		"empty":            {"", ""},
		"complete":         {"a\nb\n", "a\nb\n"},
		"torn":             {"a\nb\nc", "a\nb\n"},
		"torn only":        {"abc", ""},
		"long torn record": {"a\n" + string(make([]byte, 200*1024)), "a\n"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(c.in), 0o600))
			size, err := cutPartialLine(path, int64(len(c.in)))
			require.NoError(t, err)
			assert.Equal(t, int64(len(c.want)), size)
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, c.want, string(got))
		})
	}
	_, err := cutPartialLine(filepath.Join(dir, "missing"), 3)
	require.Error(t, err)
}

func TestParseCompression(t *testing.T) {
	for _, s := range []string{"", "none"} {
		c, err := ParseCompression(s)
		require.NoError(t, err)
		assert.Equal(t, CompressNone, c)
	}
	c, err := ParseCompression("gzip")
	require.NoError(t, err)
	assert.Equal(t, CompressGzip, c)
	_, err = ParseCompression("zstd")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `compress must be "none" or "gzip", got "zstd"`)
}
