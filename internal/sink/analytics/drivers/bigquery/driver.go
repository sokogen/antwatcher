// Package bigquery is the "bigquery" driver of the analytics class: it
// appends the records projected by the analytics package to a BigQuery table
// through the Storage Write API and manages the table and its deduplicating
// "<table>_current" view.
//
// The driver registers itself as analytics/bigquery on import. Its
// configuration block is Config: project, dataset, table, an optional
// credentials_file (a service account key; Application Default Credentials
// otherwise), and the
// ensure_table / ensure_view switches. Building the sink validates the block
// and constructs the BigQuery clients, which reads the credentials but
// performs no network round trip. A project, dataset, or credentials that
// turn out to be wrong surface from the first Process as classified errors,
// so the sink starts degraded instead of failing startup.
//
// Schema management (EnsureSchema, run lazily by the class before the first
// write) creates the base table from the portable analytics.Schema, DAY
// partitioned on event_time and clustered by repository and kind, with every
// non-required column nullable, and the current view when they are missing.
// An existing table is verified: every schema column must exist with the
// same type, or the sink stalls with a permanent error until an operator
// migrates the table.
//
// Writes go through a managedwriter default stream: rows are encoded as
// dynamicpb messages of a proto2 descriptor derived from the table schema,
// appended in one request per Write, and acked only after the service
// confirmed the append. The default stream commits at once, keeps no
// offsets, and has no exactly-once semantics: the base table is at-least-once
// by contract, consumers read the current view. The managed stream's own
// transient retries are enabled; anything that still fails is classified
// (schema, permission, and argument errors permanent, transport and quota
// errors retryable) and handed back to the bus.
package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
)

// Name is the driver name in configuration (sinks[].driver).
const Name = "bigquery"

// Config is the sinks[].config block of the driver.
type Config struct {
	// Project is the Google Cloud project that owns the dataset and is
	// billed for the writes.
	Project string `yaml:"project"`
	// Dataset is the dataset id inside Project; it must already exist.
	Dataset string `yaml:"dataset"`
	// Table is the base table id. The current view is named Table plus
	// analytics.ViewSuffix.
	Table string `yaml:"table"`
	// CredentialsFile is a service account JSON key file; any other
	// credential type in it is rejected. Empty selects Application Default
	// Credentials.
	CredentialsFile string `yaml:"credentials_file"`
	// EnsureTable creates the base table when it is missing and verifies
	// its columns when it exists (default true).
	EnsureTable bool `yaml:"ensure_table"`
	// EnsureView creates the current view when it is missing (default true).
	EnsureView bool `yaml:"ensure_view"`
}

// DefaultConfig is the block with every default applied.
func DefaultConfig() Config {
	return Config{EnsureTable: true, EnsureView: true}
}

var (
	// project ids: lowercase letters, digits, and hyphens, optionally
	// prefixed by a domain ("example.com:project").
	projectRE = regexp.MustCompile(`^([a-z0-9.-]+:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	// dataset ids: letters, digits, and underscores.
	datasetRE = regexp.MustCompile(`^[A-Za-z0-9_]{1,1000}$`)
	// table ids: unicode letters, marks, numbers, connectors, dashes, and
	// spaces.
	tableRE = regexp.MustCompile(`^[\pL\pM\pN\p{Pc}\p{Pd}\p{Zs}]{1,1000}$`)
)

// Validate checks the block on its own.
func (c Config) Validate() error {
	switch {
	case c.Project == "":
		return fmt.Errorf("project is required")
	case !projectRE.MatchString(c.Project):
		return fmt.Errorf("project %q is not a valid project id", c.Project)
	case c.Dataset == "":
		return fmt.Errorf("dataset is required")
	case !datasetRE.MatchString(c.Dataset):
		return fmt.Errorf("dataset %q may contain only letters, digits, and underscores", c.Dataset)
	case c.Table == "":
		return fmt.Errorf("table is required")
	case !tableRE.MatchString(c.Table):
		return fmt.Errorf("table %q may contain only letters, marks, numbers, underscores, dashes, and spaces", c.Table)
	case strings.HasSuffix(c.Table, analytics.ViewSuffix):
		return fmt.Errorf("table %q must not end with %q, that suffix names the current view", c.Table, analytics.ViewSuffix)
	}
	return nil
}

// ValidateWith implements config.DriverValidator so `-check` reports an
// invalid block; the driver does not depend on the router settings.
func (c Config) ValidateWith(config.Router) error { return c.Validate() }

// TableID is the fully qualified "project.dataset.table" of the base table,
// as used in SQL.
func (c Config) TableID() string {
	return c.Project + "." + c.Dataset + "." + c.Table
}

// ViewID is the fully qualified id of the current view.
func (c Config) ViewID() string {
	return c.Project + "." + c.Dataset + "." + analytics.ViewName(c.Table)
}

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
	sink.RegisterDriver(sink.ClassAnalytics, Name, Driver())
}

// Driver returns the driver descriptor, for registration in custom registries.
func Driver() sink.Driver {
	return sink.Driver{Factory: Factory, Describer: Describer}
}

// Factory builds an analytics sink over BigQuery from raw, the sinks[].config
// block. It validates the block (unknown keys and invalid values fail) and
// constructs the clients (credentials are read, nothing is dialled).
func Factory(_ context.Context, name string, raw yaml.Node, deps sink.Deps) (sink.Sink, error) {
	typed, err := Describe(raw)
	if err != nil {
		return nil, err
	}
	cfg := typed.(Config) //nolint:errcheck // Describe returns Config
	w, err := New(cfg, Options{Logger: deps.Logger})
	if err != nil {
		return nil, err
	}
	return analytics.New(name, w, cfg.EnsureTable || cfg.EnsureView), nil
}

// Options tunes New.
type Options struct {
	// Logger receives schema management events; nil discards them.
	Logger *slog.Logger
	// ClientOptions are appended to the options derived from Config when
	// the BigQuery clients are built (endpoints and credentials in tests).
	ClientOptions []option.ClientOption
}

// New validates cfg and returns a Writer over real BigQuery clients. The
// credentials are resolved here (a missing credentials file or absent
// Application Default Credentials is a construction error, like any other
// invalid static configuration) but no request is sent: the table is
// touched by EnsureSchema and the write stream is opened by the first Write.
func New(cfg Config, opts Options) (*Writer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	clientOpts := make([]option.ClientOption, 0, 1+len(opts.ClientOptions))
	if cfg.CredentialsFile != "" {
		clientOpts = append(clientOpts, option.WithAuthCredentialsFile(option.ServiceAccount, cfg.CredentialsFile))
	}
	clientOpts = append(clientOpts, opts.ClientOptions...)

	ctx, cancel := context.WithCancel(context.Background())
	meta, err := bq.NewClient(ctx, cfg.Project, clientOpts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("bigquery client: %w", err)
	}
	mw, err := managedwriter.NewClient(ctx, cfg.Project, clientOpts...)
	if err != nil {
		cancel()
		_ = meta.Close()
		return nil, fmt.Errorf("bigquery storage write client: %w", err)
	}
	w := NewWriter(cfg, datasetTables{ds: meta.Dataset(cfg.Dataset)}, managedOpener(ctx, mw, cfg), opts.Logger)
	w.closers = append(w.closers, mw.Close, meta.Close)
	w.cancel = cancel
	return w, nil
}
