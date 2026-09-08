// Package filesystem is the "filesystem" driver of the archive class: it
// keeps the raw events in a directory of JSON lines files through
// archive.Store, one line per event, fsynced before the event is acked,
// rotated by size and age into a YYYY/MM/DD tree, and optionally gzip
// compressed once finalized.
//
// The driver registers itself as archive/filesystem on import. Its
// configuration block is Config: dir, compress (none or gzip), rotate_size,
// and rotate_every. Building the sink creates the directory and repairs
// files left by a previous run; it performs no network activity. Every
// write error is retryable, so a full or unmounted disk degrades the sink
// until the operator fixes it, and no event is discarded. The directory is
// owned by one sink: file names carry the sink name, so a renamed sink
// starts a new series and leaves the old current file untouched.
package filesystem

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/archive"
)

// Name is the driver name in configuration (sinks[].driver).
const Name = "filesystem"

// Config is the sinks[].config block of the driver.
type Config struct {
	// Dir is the root directory of the archive; created when missing.
	Dir string `yaml:"dir"`
	// Compress is "none" (default) or "gzip", applied to finalized files.
	Compress string `yaml:"compress"`
	// RotateSize finalizes the current file before it would grow past this
	// size (default 128MiB); 0 disables size rotation.
	RotateSize config.ByteSize `yaml:"rotate_size"`
	// RotateEvery finalizes the current file once it is this old (default
	// 1h); 0 disables time rotation.
	RotateEvery time.Duration `yaml:"rotate_every"`
}

// DefaultConfig is the block with every default applied.
func DefaultConfig() Config {
	return Config{
		Compress:    string(archive.CompressNone),
		RotateSize:  128 * config.MiB,
		RotateEvery: time.Hour,
	}
}

// Validate checks the block on its own.
func (c Config) Validate() error {
	var errs []error
	if c.Dir == "" {
		errs = append(errs, errors.New("dir is required"))
	}
	if _, err := archive.ParseCompression(c.Compress); err != nil {
		errs = append(errs, err)
	}
	if c.RotateSize < 0 {
		errs = append(errs, fmt.Errorf("rotate_size must not be negative, got %s", c.RotateSize))
	}
	if c.RotateEvery < 0 {
		errs = append(errs, fmt.Errorf("rotate_every must not be negative, got %s", c.RotateEvery))
	}
	return errors.Join(errs...)
}

// ValidateWith implements config.DriverValidator so `-check` reports an
// invalid block; the driver does not depend on the router settings.
func (c Config) ValidateWith(config.Router) error { return c.Validate() }

// Describe decodes the block strictly on top of DefaultConfig; see
// config.Describer.
func Describe(raw yaml.Node) (any, error) {
	c := DefaultConfig()
	if err := config.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// Describer is Describe as a config.Describer.
var Describer config.Describer = config.DescriberFunc(Describe)

func init() {
	sink.RegisterDriver(sink.ClassArchive, Name, Driver())
}

// Driver returns the driver descriptor, for registration in custom registries.
func Driver() sink.Driver {
	return sink.Driver{Factory: Factory, Describer: Describer}
}

// Factory builds an archive sink over a store in the configured directory.
func Factory(_ context.Context, name string, raw yaml.Node, deps sink.Deps) (sink.Sink, error) {
	typed, err := Describe(raw)
	if err != nil {
		return nil, err
	}
	cfg := typed.(Config) //nolint:errcheck // Describe returns Config
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	compress, _ := archive.ParseCompression(cfg.Compress) //nolint:errcheck // validated above
	store, err := archive.OpenStore(archive.StoreOptions{
		Dir:         cfg.Dir,
		Prefix:      name,
		Compress:    compress,
		RotateSize:  int64(cfg.RotateSize),
		RotateEvery: cfg.RotateEvery,
		Logger:      deps.Logger,
	})
	if err != nil {
		return nil, err
	}
	return archive.New(name, store), nil
}
