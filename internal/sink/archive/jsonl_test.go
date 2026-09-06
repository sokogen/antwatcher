package archive_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/archive"
)

// clock is a settable fake clock for StoreOptions.Now.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock(at time.Time) *clock { return &clock{now: at} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func openStore(t *testing.T, dir string, mutate func(*archive.StoreOptions)) *archive.Store {
	t.Helper()
	opts := archive.StoreOptions{Dir: dir, Prefix: "raw", Now: newClock(received).Now, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if mutate != nil {
		mutate(&opts)
	}
	s, err := archive.OpenStore(opts)
	require.NoError(t, err)
	return s
}

func lineLen(t *testing.T, env event.Envelope) int64 {
	t.Helper()
	line, err := archive.Encode(env)
	require.NoError(t, err)
	return int64(len(line))
}

func TestOpenStore_Validation(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]archive.StoreOptions{
		"no dir":          {Prefix: "raw"},
		"no prefix":       {Dir: dir},
		"prefix path":     {Dir: dir, Prefix: "a/b"},
		"prefix dot":      {Dir: dir, Prefix: "."},
		"prefix dotdot":   {Dir: dir, Prefix: ".."},
		"bad compression": {Dir: dir, Prefix: "raw", Compress: "zstd"},
		"negative size":   {Dir: dir, Prefix: "raw", RotateSize: -1},
		"negative every":  {Dir: dir, Prefix: "raw", RotateEvery: -time.Second},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := archive.OpenStore(opts)
			require.Error(t, err)
		})
	}
	t.Run("dir is a file", func(t *testing.T) {
		file := filepath.Join(dir, "file")
		require.NoError(t, os.WriteFile(file, nil, 0o600))
		_, err := archive.OpenStore(archive.StoreOptions{Dir: file, Prefix: "raw"})
		require.Error(t, err)
	})
	t.Run("creates missing directory with defaults", func(t *testing.T) {
		nested := filepath.Join(dir, "a", "b")
		s, err := archive.OpenStore(archive.StoreOptions{Dir: nested, Prefix: "raw"})
		require.NoError(t, err)
		assert.DirExists(t, nested)
		assert.Equal(t, nested, s.Dir())
		assert.Equal(t, filepath.Join(nested, "raw-current.jsonl.part"), s.CurrentPath())
		assert.Empty(t, listFiles(t, nested), "no file is opened before the first append")
		require.NoError(t, s.Close())
		assert.Empty(t, listFiles(t, nested), "closing without appends leaves nothing behind")
		require.NoError(t, s.Close(), "closing twice is a no-op")
	})
}

func TestStore_AppendWritesReadableJSONL(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, nil)
	var want []event.Envelope
	for _, fixture := range event.Fixtures() {
		env := fixtureEnvelope(t, fixture)
		require.NoError(t, s.Append(context.Background(), env), fixture)
		want = append(want, compacted(t, env))
	}
	assert.Equal(t, []string{"raw-current.jsonl.part"}, listFiles(t, dir), "the open file is the only file")

	got, err := archive.ReadFile(s.CurrentPath())
	require.NoError(t, err, "the open file is plain JSONL readable at any time")
	assert.Equal(t, want, got)
	raw, err := os.ReadFile(s.CurrentPath())
	require.NoError(t, err)
	assert.Equal(t, len(want), bytes.Count(raw, []byte("\n")), "one line per event")

	require.NoError(t, s.Close())
	assert.Equal(t, []string{"2026/09/02/raw-20260902T100500Z-1.jsonl"}, listFiles(t, dir), "close finalizes into the dated tree")
	tree := readTree(t, dir)
	assert.Equal(t, want, tree["2026/09/02/raw-20260902T100500Z-1.jsonl"])

	err = s.Append(context.Background(), fixtureEnvelope(t, "ping"))
	require.Error(t, err, "no appends after close")
	assert.False(t, sink.IsPermanent(err))
}

func TestStore_AppendHonoursContext(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.Append(ctx, fixtureEnvelope(t, "ping")), context.Canceled)
	assert.Empty(t, listFiles(t, dir))
	require.NoError(t, s.Close())
}

func TestStore_AppendBadPayloadIsPermanentAndWritesNothing(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, nil)
	env := fixtureEnvelope(t, "ping")
	env.Payload = []byte("{")
	err := s.Append(context.Background(), env)
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err))
	assert.Empty(t, listFiles(t, dir))
	require.NoError(t, s.Close())
}

func TestStore_RotateBySize(t *testing.T) {
	dir := t.TempDir()
	ping := fixtureEnvelope(t, "ping")
	n := lineLen(t, ping)
	s := openStore(t, dir, func(o *archive.StoreOptions) { o.RotateSize = 2*n + 1 })
	for i := 0; i < 5; i++ {
		env := ping
		env.DeliveryGUID = "guid-" + string(rune('a'+i))
		require.NoError(t, s.Append(context.Background(), env))
	}
	assert.Equal(t, []string{
		"2026/09/02/raw-20260902T100500Z-1.jsonl",
		"2026/09/02/raw-20260902T100500Z-2.jsonl",
		"raw-current.jsonl.part",
	}, listFiles(t, dir), "two full files finalized, the third open")
	tree := readTree(t, dir)
	assert.Equal(t, []string{"guid-a", "guid-b"}, guids(tree["2026/09/02/raw-20260902T100500Z-1.jsonl"]))
	assert.Equal(t, []string{"guid-c", "guid-d"}, guids(tree["2026/09/02/raw-20260902T100500Z-2.jsonl"]))
	assert.Equal(t, []string{"guid-e"}, guids(tree["raw-current.jsonl.part"]))
	for _, rel := range listFiles(t, dir)[:2] {
		info, err := os.Stat(filepath.Join(dir, rel))
		require.NoError(t, err)
		assert.LessOrEqual(t, info.Size(), 2*n+1, rel)
	}
	require.NoError(t, s.Close())
	assert.Len(t, listFiles(t, dir), 3)
}

func TestStore_OversizedRecordStillWritten(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, func(o *archive.StoreOptions) { o.RotateSize = 10 })
	require.NoError(t, s.Append(context.Background(), fixtureEnvelope(t, "ping")), "an empty file never rotates")
	require.NoError(t, s.Append(context.Background(), fixtureEnvelope(t, "workflow_run.completed")))
	require.NoError(t, s.Close())
	files := listFiles(t, dir)
	require.Len(t, files, 2, "each record in its own file: %v", files)
}

func TestStore_RotateByTime(t *testing.T) {
	dir := t.TempDir()
	clk := newClock(received)
	s := openStore(t, dir, func(o *archive.StoreOptions) { o.Now = clk.Now; o.RotateEvery = time.Hour })
	at := func(d time.Duration) event.Envelope { return fixtureEnvelopeAt(t, "ping", received.Add(d)) }

	require.NoError(t, s.Append(context.Background(), at(0)))
	clk.Advance(30 * time.Minute)
	require.NoError(t, s.Append(context.Background(), at(30*time.Minute)))
	clk.Advance(29 * time.Minute)
	require.NoError(t, s.Append(context.Background(), at(59*time.Minute)))
	assert.Equal(t, []string{"raw-current.jsonl.part"}, listFiles(t, dir), "younger than rotate_every")

	clk.Advance(time.Minute)
	require.NoError(t, s.Append(context.Background(), at(time.Hour)), "the append at the threshold rotates first")
	assert.Equal(t, []string{"2026/09/02/raw-20260902T100500Z-1.jsonl", "raw-current.jsonl.part"}, listFiles(t, dir))
	tree := readTree(t, dir)
	assert.Len(t, tree["2026/09/02/raw-20260902T100500Z-1.jsonl"], 3)
	assert.Len(t, tree["raw-current.jsonl.part"], 1)

	clk.Advance(25 * time.Hour)
	require.NoError(t, s.Append(context.Background(), at(26*time.Hour)))
	require.NoError(t, s.Close())
	assert.Equal(t, []string{
		"2026/09/02/raw-20260902T100500Z-1.jsonl",
		"2026/09/02/raw-20260902T110500Z-2.jsonl",
		"2026/09/03/raw-20260903T120500Z-3.jsonl",
	}, listFiles(t, dir), "files are placed by the received_at of their first record")
}

func TestStore_GzipAfterRotation(t *testing.T) {
	dir := t.TempDir()
	ping := fixtureEnvelope(t, "ping")
	s := openStore(t, dir, func(o *archive.StoreOptions) { o.Compress = archive.CompressGzip; o.RotateSize = lineLen(t, ping) + 1 })
	require.NoError(t, s.Append(context.Background(), ping))
	assert.Equal(t, []string{"raw-current.jsonl.part"}, listFiles(t, dir), "the open file is never compressed")
	require.NoError(t, s.Append(context.Background(), fixtureEnvelope(t, "workflow_job.queued")))
	assert.Equal(t, []string{"2026/09/02/raw-20260902T100500Z-1.jsonl.gz", "raw-current.jsonl.part"}, listFiles(t, dir), "plain file removed after compression")
	require.NoError(t, s.Close())
	files := listFiles(t, dir)
	assert.Equal(t, []string{"2026/09/02/raw-20260902T100500Z-1.jsonl.gz", "2026/09/02/raw-20260902T100500Z-2.jsonl.gz"}, files)

	gzPath := filepath.Join(dir, files[0])
	f, err := os.Open(gzPath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	gr, err := gzip.NewReader(f)
	require.NoError(t, err)
	assert.Equal(t, "raw-20260902T100500Z-1.jsonl", gr.Name, "gzip header names the plain file")
	raw, err := io.ReadAll(gr)
	require.NoError(t, err, "a valid, complete gzip stream")
	require.NoError(t, gr.Close())
	line, err := archive.Encode(ping)
	require.NoError(t, err)
	assert.Equal(t, string(line), string(raw))
	tree := readTree(t, dir)
	assert.Equal(t, []string{"guid-ping"}, guids(tree[files[0]]))
	assert.Equal(t, []string{"guid-workflow_job.queued"}, guids(tree[files[1]]))
}

func TestStore_CrashLeavesReadablePartThatStartupFinalizes(t *testing.T) {
	dir := t.TempDir()
	first := openStore(t, dir, nil)
	require.NoError(t, first.Append(context.Background(), fixtureEnvelope(t, "workflow_run.requested")))
	require.NoError(t, first.Append(context.Background(), fixtureEnvelope(t, "workflow_run.in_progress")))
	// the process dies here: the store is never closed and a record was cut
	// mid-write (never acked, so the bus redelivers it)
	f, err := os.OpenFile(first.CurrentPath(), os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString(`{"schema_version":1,"delivery_guid":"guid-torn","event":"workflow_run","payload":{"half`)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	envs, err := archive.ReadFile(first.CurrentPath())
	require.Error(t, err, "the torn line is reported")
	assert.Len(t, envs, 2, "the whole records before it still read")

	second := openStore(t, dir, nil)
	assert.Equal(t, []string{"2026/09/02/raw-20260902T100500Z-1.jsonl"}, listFiles(t, dir), "startup finalized the leftover file")
	tree := readTree(t, dir)
	assert.Equal(t, []string{"guid-workflow_run.requested", "guid-workflow_run.in_progress"}, guids(tree["2026/09/02/raw-20260902T100500Z-1.jsonl"]), "torn record cut, whole ones kept")

	require.NoError(t, second.Append(context.Background(), fixtureEnvelope(t, "workflow_run.completed")))
	require.NoError(t, second.Close())
	assert.Equal(t, []string{
		"2026/09/02/raw-20260902T100500Z-1.jsonl",
		"2026/09/02/raw-20260902T100500Z-2.jsonl",
	}, listFiles(t, dir), "the sequence skips the name taken by the previous run")

	third := openStore(t, dir, nil)
	require.NoError(t, third.Append(context.Background(), fixtureEnvelope(t, "ping")))
	require.NoError(t, third.Close())
	assert.Contains(t, listFiles(t, dir), "2026/09/02/raw-20260902T100500Z-3.jsonl")
}

func TestStore_StartupFinalizesWithGzipAndSweepsLeftovers(t *testing.T) {
	dir := t.TempDir()
	first := openStore(t, dir, nil)
	require.NoError(t, first.Append(context.Background(), fixtureEnvelope(t, "ping")))
	// crash without close; an interrupted compression and a plain file of a
	// run that had compression off are left in the tree, plus another
	// sink's file that must not be touched
	day := filepath.Join(dir, "2026", "09", "01")
	require.NoError(t, os.MkdirAll(day, 0o750))
	line, err := archive.Encode(fixtureEnvelope(t, "workflow_job.completed"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(day, "raw-20260901T000000Z-1.jsonl"), line, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(day, "raw-20260901T000000Z-2.jsonl.gz.tmp"), []byte("partial"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(day, "other-20260901T000000Z-1.jsonl"), line, 0o600))

	second := openStore(t, dir, func(o *archive.StoreOptions) { o.Compress = archive.CompressGzip })
	assert.Equal(t, []string{
		"2026/09/01/other-20260901T000000Z-1.jsonl",
		"2026/09/01/raw-20260901T000000Z-1.jsonl.gz",
		"2026/09/02/raw-20260902T100500Z-1.jsonl.gz",
	}, listFiles(t, dir))
	tree := readTree(t, dir)
	assert.Equal(t, []string{"guid-workflow_job.completed"}, guids(tree["2026/09/01/raw-20260901T000000Z-1.jsonl.gz"]))
	assert.Equal(t, []string{"guid-ping"}, guids(tree["2026/09/02/raw-20260902T100500Z-1.jsonl.gz"]))
	require.NoError(t, second.Close())
}

func TestStore_StartupRemovesEmptyAndTornOnlyPart(t *testing.T) {
	dir := t.TempDir()
	part := filepath.Join(dir, "raw-current.jsonl.part")
	require.NoError(t, os.WriteFile(part, nil, 0o600))
	s := openStore(t, dir, nil)
	assert.Empty(t, listFiles(t, dir), "an empty leftover is removed")
	require.NoError(t, s.Close())

	require.NoError(t, os.WriteFile(part, []byte(`{"schema_version":1,"deliv`), 0o600))
	s = openStore(t, dir, nil)
	assert.Empty(t, listFiles(t, dir), "a leftover holding only a torn record is removed")
	require.NoError(t, s.Close())
}

func TestStore_StartupNamesUnreadableFirstLineByModTime(t *testing.T) {
	dir := t.TempDir()
	part := filepath.Join(dir, "raw-current.jsonl.part")
	require.NoError(t, os.WriteFile(part, []byte("not a record\n"), 0o600))
	mtime := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)
	require.NoError(t, os.Chtimes(part, mtime, mtime))
	s := openStore(t, dir, nil)
	assert.Equal(t, []string{"2025/12/31/raw-20251231T235959Z-1.jsonl"}, listFiles(t, dir), "the bytes are kept, named by mtime")
	require.NoError(t, s.Close())
}

func TestStore_ExistingCompressedTwinOnlyRemovesPlain(t *testing.T) {
	dir := t.TempDir()
	day := filepath.Join(dir, "2026", "09", "01")
	require.NoError(t, os.MkdirAll(day, 0o750))
	line, err := archive.Encode(fixtureEnvelope(t, "ping"))
	require.NoError(t, err)
	plain := filepath.Join(day, "raw-20260901T000000Z-1.jsonl")
	require.NoError(t, os.WriteFile(plain, line, 0o600))
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err = gw.Write(line)
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	require.NoError(t, os.WriteFile(plain+".gz", buf.Bytes(), 0o600))

	s := openStore(t, dir, func(o *archive.StoreOptions) { o.Compress = archive.CompressGzip })
	assert.Equal(t, []string{"2026/09/01/raw-20260901T000000Z-1.jsonl.gz"}, listFiles(t, dir))
	got, err := os.ReadFile(plain + ".gz")
	require.NoError(t, err)
	assert.Equal(t, buf.Bytes(), got, "the complete compressed file is not rewritten")
	require.NoError(t, s.Close())
}

func TestStore_OpenErrorIsRetryableAndRecovers(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, nil)
	// the current path is taken by a directory: opening fails
	require.NoError(t, os.Mkdir(s.CurrentPath(), 0o750))
	err := s.Append(context.Background(), fixtureEnvelope(t, "ping"))
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err), "a filesystem problem is retryable: %v", err)
	require.NoError(t, os.Remove(s.CurrentPath()))
	require.NoError(t, s.Append(context.Background(), fixtureEnvelope(t, "ping")), "the next append opens the file")
	require.NoError(t, s.Close())
	assert.Equal(t, []string{"2026/09/02/raw-20260902T100500Z-1.jsonl"}, listFiles(t, dir))
}

func TestStore_RotationErrorKeepsFileAndRetries(t *testing.T) {
	dir := t.TempDir()
	ping := fixtureEnvelope(t, "ping")
	s := openStore(t, dir, func(o *archive.StoreOptions) { o.RotateSize = lineLen(t, ping) + 1 })
	require.NoError(t, s.Append(context.Background(), ping))
	// the dated directory cannot be created: "2026" is a file
	require.NoError(t, os.WriteFile(filepath.Join(dir, "2026"), nil, 0o600))
	err := s.Append(context.Background(), ping)
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	assert.Contains(t, err.Error(), "rotate archive")
	envs, err := archive.ReadFile(s.CurrentPath())
	require.NoError(t, err)
	assert.Len(t, envs, 1, "the current file is intact and the failed record not written")

	require.NoError(t, os.Remove(filepath.Join(dir, "2026")))
	require.NoError(t, s.Append(context.Background(), ping), "rotation is retried")
	require.NoError(t, s.Close())
	assert.Equal(t, []string{
		"2026/09/02/raw-20260902T100500Z-1.jsonl",
		"2026/09/02/raw-20260902T100500Z-2.jsonl",
	}, listFiles(t, dir))
}

func TestStore_ConcurrentAppendsProduceWholeLines(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, nil)
	const n = 32
	envs := make([]event.Envelope, n)
	for i := range envs {
		envs[i] = fixtureEnvelope(t, event.Fixtures()[i%len(event.Fixtures())])
	}
	var wg sync.WaitGroup
	for _, env := range envs {
		wg.Add(1)
		go func(env event.Envelope) {
			defer wg.Done()
			assert.NoError(t, s.Append(context.Background(), env))
		}(env)
	}
	wg.Wait()
	envs, err := archive.ReadFile(s.CurrentPath())
	require.NoError(t, err)
	assert.Len(t, envs, n)
	require.NoError(t, s.Close())
}

func TestStore_LogsRotation(t *testing.T) {
	dir := t.TempDir()
	var buf strings.Builder
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(lockedWriter{&mu, &buf}, nil))
	s := openStore(t, dir, func(o *archive.StoreOptions) { o.Logger = logger })
	require.NoError(t, s.Append(context.Background(), fixtureEnvelope(t, "ping")))
	require.NoError(t, s.Close())
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, buf.String(), "file finalized")
	assert.Contains(t, buf.String(), "raw-20260902T100500Z-1.jsonl")
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *strings.Builder
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func TestStore_SweepLeavesAPrefixSharingSinkAlone(t *testing.T) {
	// Sink names may contain "-" and digits, so "raw" is a string prefix of
	// every file "raw-2" writes. Sweeping on the bare prefix would gzip and
	// delete the other sink's archive when both share a directory.
	dir := t.TempDir()
	day := filepath.Join(dir, "2026", "09", "01")
	require.NoError(t, os.MkdirAll(day, 0o750))
	line, err := archive.Encode(fixtureEnvelope(t, "ping"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(day, "raw-20260901T000000Z-1.jsonl"), line, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(day, "raw-2-20260901T000000Z-1.jsonl"), line, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(day, "raw-2-20260901T000000Z-2.jsonl.gz.tmp"), []byte("partial"), 0o600))

	s := openStore(t, dir, func(o *archive.StoreOptions) { o.Compress = archive.CompressGzip })
	assert.Equal(t, []string{
		"2026/09/01/raw-2-20260901T000000Z-1.jsonl",
		"2026/09/01/raw-2-20260901T000000Z-2.jsonl.gz.tmp",
		"2026/09/01/raw-20260901T000000Z-1.jsonl.gz",
	}, listFiles(t, dir), "only this store's own file is compressed; raw-2 keeps its plain file and its tmp")
	require.NoError(t, s.Close())
}
