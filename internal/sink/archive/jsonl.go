package archive

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sokogen/antwatcher/internal/event"
)

// Compression applied to finalized files.
type Compression string

// Compressions accepted by StoreOptions.Compress.
const (
	CompressNone Compression = "none"
	CompressGzip Compression = "gzip"
)

// ParseCompression maps a configuration value to a Compression; the empty
// string means none.
func ParseCompression(s string) (Compression, error) {
	switch Compression(s) {
	case "", CompressNone:
		return CompressNone, nil
	case CompressGzip:
		return CompressGzip, nil
	default:
		return "", fmt.Errorf("compress must be %q or %q, got %q", CompressNone, CompressGzip, s)
	}
}

// File name parts of a Store.
const (
	// Extension of a finalized plain file.
	Extension = ".jsonl"
	// GzipExtension of a finalized compressed file.
	GzipExtension = ".jsonl.gz"
	// partSuffix follows the prefix on the open file.
	partSuffix = "-current.jsonl.part"
	// tmpSuffix marks a compressed file still being written.
	tmpSuffix = ".tmp"
	// stampLayout renders the first timestamp of a file in its name.
	stampLayout = "20060102T150405Z"

	dirPerm  = 0o750
	filePerm = 0o600
)

// StoreOptions configure a Store.
type StoreOptions struct {
	// Dir is the root directory; it is created when missing.
	Dir string
	// Prefix names the files (usually the sink name). It must be a single
	// path element.
	Prefix string
	// Compress selects what happens to a finalized file (default none).
	Compress Compression
	// RotateSize finalizes the open file before an append would grow it
	// past this many bytes; 0 disables size rotation.
	RotateSize int64
	// RotateEvery finalizes the open file before an append once it has been
	// open this long; 0 disables time rotation.
	RotateEvery time.Duration
	// Now is the clock (default time.Now); tests inject a fake.
	Now func() time.Time
	// Logger receives rotation and recovery messages (default discard).
	Logger *slog.Logger
}

// Store is the JSONL store shared by file-based archive drivers. It
// implements Writer.
//
// The open file is always plain JSONL at "<dir>/<prefix>-current.jsonl.part".
// Append writes one line, then fsyncs before returning, so an acked event is
// on disk. Rotation (by size, by age, and at Close) closes the file and
// renames it to "<dir>/YYYY/MM/DD/<prefix>-<first_ts>-<seq>.jsonl", where
// first_ts is the received_at of the file's first record in UTC and seq
// disambiguates files that share it; with gzip compression the finalized
// file is then compressed to ".jsonl.gz" and the plain file is removed only
// after the compressed one is fsynced. Rotation is evaluated on Append, so
// an idle store keeps its current file open until the next event or Close;
// that file is readable at any time.
//
// OpenStore repairs what a crash left behind: a trailing partial line in the
// open file is cut (it was never acked), the file is finalized, leftover
// temporary compressed files are removed, and plain finalized files are
// compressed when compression is on. A crash therefore never leaves a
// truncated gzip stream or a lost record.
type Store struct {
	opts StoreOptions
	part string

	mu       sync.Mutex
	f        *os.File
	size     int64
	openedAt time.Time // wall clock of the first append into f; zero means "rotate soon"
	firstTS  time.Time // received_at of the first record in f
	seq      int
	closed   bool
	closeErr error
}

// OpenStore validates opts, creates the directory, recovers files left by a
// previous run, and returns a store with no file open. It performs no
// network activity; a directory that cannot be created or repaired is an
// error.
func OpenStore(opts StoreOptions) (*Store, error) {
	if opts.Dir == "" {
		return nil, errors.New("archive store: dir is required")
	}
	if opts.Prefix == "" || filepath.Base(opts.Prefix) != opts.Prefix || opts.Prefix == "." || opts.Prefix == ".." {
		return nil, fmt.Errorf("archive store: prefix %q must be a single path element", opts.Prefix)
	}
	if opts.Compress == "" {
		opts.Compress = CompressNone
	}
	if opts.Compress != CompressNone && opts.Compress != CompressGzip {
		return nil, fmt.Errorf("archive store: unknown compression %q", opts.Compress)
	}
	if opts.RotateSize < 0 || opts.RotateEvery < 0 {
		return nil, errors.New("archive store: rotation thresholds must not be negative")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if err := os.MkdirAll(opts.Dir, dirPerm); err != nil {
		return nil, fmt.Errorf("archive store: %w", err)
	}
	s := &Store{opts: opts, part: filepath.Join(opts.Dir, opts.Prefix+partSuffix)}
	if err := s.recover(); err != nil {
		return nil, fmt.Errorf("archive store: recover %s: %w", opts.Dir, err)
	}
	return s, nil
}

// Dir is the root directory of the store.
func (s *Store) Dir() string { return s.opts.Dir }

// CurrentPath is the path of the open file, whether or not it exists now.
func (s *Store) CurrentPath() string { return s.part }

// Append implements Writer: one line, written and fsynced before returning.
// Any failure to write or fsync is retryable: the partial line is cut, the
// file is reopened on the next call, and the event is redelivered by the
// bus. An undecodable envelope is permanent (see Encode).
func (s *Store) Append(ctx context.Context, env event.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	line, err := Encode(env)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("archive store is closed")
	}
	now := s.opts.Now()
	if s.f == nil {
		if err := s.open(now); err != nil {
			return err
		}
	}
	if s.shouldRotate(int64(len(line)), now) {
		if err := s.rotate(); err != nil {
			return fmt.Errorf("rotate archive: %w", err)
		}
		if err := s.open(now); err != nil {
			return err
		}
	}

	n, err := s.f.Write(line)
	if err != nil {
		s.abandon(n)
		return fmt.Errorf("write archive record: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		s.abandon(n)
		return fmt.Errorf("fsync archive record: %w", err)
	}
	if s.size == 0 {
		s.firstTS = env.ReceivedAt.UTC()
		s.openedAt = now
	}
	s.size += int64(n)
	return nil
}

// Close implements Writer: the open file is finalized. Closing twice is a
// no-op that returns the same result as the first call: a finalize failure
// is sticky (rotate has already dropped the in-memory file state by the time
// it fails, so there is nothing left here to retry; the leftover file is
// picked up by the next OpenStore's recover instead).
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	if err := s.rotate(); err != nil {
		s.closeErr = fmt.Errorf("finalize archive: %w", err)
	}
	return s.closeErr
}

// shouldRotate reports whether the open file must be finalized before
// appending n more bytes at now. An empty file never rotates.
func (s *Store) shouldRotate(n int64, now time.Time) bool {
	if s.size == 0 {
		return false
	}
	if s.opts.RotateSize > 0 && s.size+n > s.opts.RotateSize {
		return true
	}
	if s.opts.RotateEvery > 0 && (s.openedAt.IsZero() || now.Sub(s.openedAt) >= s.opts.RotateEvery) {
		return true
	}
	return false
}

// open opens the current file for appending. A file that already has
// content (left by a failed write or rotation) keeps its first timestamp and
// is rotated at the next opportunity, since its age is unknown.
func (s *Store) open(now time.Time) error {
	f, err := os.OpenFile(s.part, os.O_WRONLY|os.O_CREATE|os.O_APPEND, filePerm)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("open archive: %w", err)
	}
	s.f, s.size, s.openedAt, s.firstTS = f, info.Size(), now, time.Time{}
	if s.size > 0 {
		s.openedAt = time.Time{}
		s.firstTS = s.firstTimestamp(info)
	}
	return nil
}

// abandon closes the current file after a failed write of n bytes, cutting
// the bytes that may have landed so the next line starts clean.
func (s *Store) abandon(n int) {
	if n > 0 {
		if err := s.f.Truncate(s.size); err != nil {
			s.opts.Logger.Warn("archive: cannot cut partial record", "path", s.part, "error", err)
		}
	}
	if err := s.f.Close(); err != nil {
		s.opts.Logger.Warn("archive: close after failed write", "path", s.part, "error", err)
	}
	s.f, s.size, s.openedAt, s.firstTS = nil, 0, time.Time{}, time.Time{}
}

// rotate closes the open file, cuts any trailing partial line left by a
// write/fsync failure abandon could not itself repair (e.g. a failed
// Truncate), and finalizes it; an empty file is removed. On failure the file
// stays in place and the next Append retries.
func (s *Store) rotate() error {
	if s.f == nil {
		return nil
	}
	f, firstTS := s.f, s.firstTS
	s.f, s.size, s.openedAt, s.firstTS = nil, 0, time.Time{}, time.Time{}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", s.part, err)
	}
	info, err := os.Stat(s.part)
	if err != nil {
		return fmt.Errorf("stat %s: %w", s.part, err)
	}
	size, err := cutPartialLine(s.part, info.Size())
	if err != nil {
		return fmt.Errorf("repair %s: %w", s.part, err)
	}
	if size < info.Size() {
		s.opts.Logger.Warn("archive: cut partial record before rotation", "path", s.part, "bytes", info.Size()-size)
	}
	if size == 0 {
		if err := os.Remove(s.part); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}
	_, err = s.finalize(firstTS)
	return err
}

// finalize renames the current file into the dated tree and compresses it
// when configured. A compression failure is logged and leaves the plain
// file, which OpenStore compresses on the next start.
func (s *Store) finalize(firstTS time.Time) (string, error) {
	if firstTS.IsZero() {
		info, err := os.Stat(s.part)
		if err != nil {
			return "", err
		}
		firstTS = s.firstTimestamp(info)
	}
	day := firstTS.UTC()
	dir := filepath.Join(s.opts.Dir, day.Format("2006"), day.Format("01"), day.Format("02"))
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return "", err
	}
	var target string
	for {
		s.seq++
		target = filepath.Join(dir, fmt.Sprintf("%s-%s-%d%s", s.opts.Prefix, day.Format(stampLayout), s.seq, Extension))
		if !exists(target) && !exists(target+".gz") {
			break
		}
	}
	if err := os.Rename(s.part, target); err != nil {
		return "", err
	}
	s.syncDir(dir)
	s.syncDir(s.opts.Dir)
	s.opts.Logger.Info("archive: file finalized", "path", target)
	if s.opts.Compress == CompressGzip {
		if err := s.compress(target); err != nil {
			s.opts.Logger.Warn("archive: compression failed, plain file kept", "path", target, "error", err)
			return target, nil
		}
		return target + ".gz", nil
	}
	return target, nil
}

// compress writes plain to "<plain>.gz" through a temporary file, fsyncs
// it, renames it into place, and removes the plain file only after the
// directory entry is durable. A compressed file that already exists is
// complete (it was renamed after its fsync), so plain is just removed.
func (s *Store) compress(plain string) error {
	gzPath := plain + ".gz"
	dir := filepath.Dir(plain)
	if !exists(gzPath) {
		tmp := gzPath + tmpSuffix
		if err := writeGzip(plain, tmp); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		if err := os.Rename(tmp, gzPath); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("fsync %s: %w", dir, err)
		}
	}
	if err := os.Remove(plain); err != nil {
		return err
	}
	s.syncDir(dir)
	return nil
}

// writeGzip compresses src into dst and fsyncs dst.
func writeGzip(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close() //nolint:errcheck // read only
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	gw := gzip.NewWriter(out)
	gw.Name = filepath.Base(src)
	if _, err := io.Copy(gw, in); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	return out.Sync()
}

// recover finalizes the file left open by a previous run and sweeps the
// tree for interrupted compressions.
func (s *Store) recover() error {
	info, err := os.Stat(s.part)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return err
	default:
		size, err := cutPartialLine(s.part, info.Size())
		if err != nil {
			return fmt.Errorf("repair %s: %w", s.part, err)
		}
		if size < info.Size() {
			s.opts.Logger.Warn("archive: cut partial record left by a crash", "path", s.part, "bytes", info.Size()-size)
		}
		if size == 0 {
			if err := os.Remove(s.part); err != nil {
				return err
			}
		} else {
			target, err := s.finalize(time.Time{})
			if err != nil {
				return fmt.Errorf("finalize %s: %w", s.part, err)
			}
			s.opts.Logger.Info("archive: finalized file left open by a previous run", "path", target, "bytes", size)
		}
	}
	return s.sweep()
}

// sweep removes temporary compressed files and, with compression on,
// compresses plain finalized files of this store's prefix. Paths are
// collected first and handled after the walk.
func (s *Store) sweep() error {
	prefix := s.opts.Prefix + "-"
	var interrupted, plain []string
	err := filepath.WalkDir(s.opts.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() || !strings.HasPrefix(name, prefix) {
			return nil
		}
		switch {
		case strings.HasSuffix(name, GzipExtension+tmpSuffix):
			interrupted = append(interrupted, path)
		case strings.HasSuffix(name, Extension):
			plain = append(plain, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, path := range interrupted {
		s.opts.Logger.Warn("archive: removing interrupted compression", "path", path)
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	if s.opts.Compress != CompressGzip {
		return nil
	}
	for _, path := range plain {
		if err := s.compress(path); err != nil {
			s.opts.Logger.Warn("archive: compression failed, plain file kept", "path", path, "error", err)
		}
	}
	return nil
}

// firstTimestamp reads the received_at of the first record in the current
// file, falling back to the file's modification time when the line is not
// readable.
func (s *Store) firstTimestamp(info fs.FileInfo) time.Time {
	f, err := os.Open(s.part)
	if err != nil {
		return info.ModTime().UTC()
	}
	defer f.Close() //nolint:errcheck // read only
	env, err := NewReader(f).Next()
	if err != nil {
		s.opts.Logger.Warn("archive: first record unreadable, naming file by modification time", "path", s.part, "error", err)
		return info.ModTime().UTC()
	}
	return env.ReceivedAt
}

// cutPartialLine truncates path to its last newline when it does not end
// with one and returns the resulting size.
func cutPartialLine(path string, size int64) (int64, error) {
	if size == 0 {
		return 0, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, filePerm)
	if err != nil {
		return 0, err
	}
	defer f.Close() //nolint:errcheck // truncate and sync are checked
	end, err := lastNewline(f, size)
	if err != nil {
		return 0, err
	}
	if end == size {
		return size, nil
	}
	if err := f.Truncate(end); err != nil {
		return 0, err
	}
	return end, f.Sync()
}

// lastNewline returns the offset just after the last newline in f, or 0.
func lastNewline(f *os.File, size int64) (int64, error) {
	const chunk = 64 * 1024
	buf := make([]byte, chunk)
	for pos := size; pos > 0; {
		n := int64(len(buf))
		if pos < n {
			n = pos
		}
		pos -= n
		if _, err := f.ReadAt(buf[:n], pos); err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		for i := n - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				return pos + i + 1, nil
			}
		}
	}
	return 0, nil
}

// syncDir fsyncs a directory so a rename or removal is durable; failures
// only cost the name, never the data, so they are logged.
func (s *Store) syncDir(dir string) {
	if err := syncDir(dir); err != nil {
		s.opts.Logger.Debug("archive: directory fsync failed", "path", dir, "error", err)
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close() //nolint:errcheck // read only
	return d.Sync()
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
