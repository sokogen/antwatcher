package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
)

// Tables is the slice of the BigQuery metadata API EnsureSchema needs: a
// handle per table id inside the configured dataset. The real
// implementation wraps *bigquery.Dataset; tests inject a fake.
type Tables interface {
	Table(id string) Table
}

// Table is one table handle: fetch its metadata (a googleapi 404 when it
// does not exist) and create it.
type Table interface {
	Metadata(ctx context.Context) (*bq.TableMetadata, error)
	Create(ctx context.Context, md *bq.TableMetadata) error
}

// Appender is the slice of a managed stream Write needs: append one batch of
// encoded rows and wait for the service's answer. The real implementation
// wraps *managedwriter.ManagedStream; tests inject a fake.
type Appender interface {
	// Append sends rows and blocks until the service accepted or rejected
	// them (or ctx ends). A nil error means the rows are committed.
	Append(ctx context.Context, rows [][]byte) error
	Close() error
}

// StreamOpener opens the appender on the first write, given the row
// descriptor the rows will be encoded with.
type StreamOpener func(ctx context.Context, desc *descriptorpb.DescriptorProto) (Appender, error)

// Writer is the analytics.Writer over BigQuery. Build it with New for real
// clients or NewWriter with injected ones.
type Writer struct {
	cfg    Config
	tables Tables
	open   StreamOpener
	logger *slog.Logger

	mu     sync.Mutex
	schema analytics.Schema
	desc   *RowDescriptor
	closed bool

	stream sink.SingleFlight[Appender]

	closers []func() error
	cancel  context.CancelFunc
}

// NewWriter builds a Writer over tables and open. It encodes rows with
// analytics.Current until EnsureSchema is called with another schema.
func NewWriter(cfg Config, tables Tables, open StreamOpener, logger *slog.Logger) *Writer {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Writer{
		cfg:    cfg,
		tables: tables,
		open:   open,
		logger: logger.With("project", cfg.Project, "dataset", cfg.Dataset, "table", cfg.Table),
		schema: analytics.Current,
	}
}

// EnsureSchema implements analytics.Writer. With ensure_table it fetches the
// base table: when it exists, its columns are verified against schema
// (mismatch is permanent); when it is missing, it is created from
// TableMetadata (a concurrent creation is fine). With ensure_view it does
// the same for the current view, without verification. Rows written later
// are encoded with schema. Errors are classified.
func (w *Writer) EnsureSchema(ctx context.Context, schema analytics.Schema) error {
	if err := schema.Validate(); err != nil {
		return sink.Permanent(err)
	}
	w.mu.Lock()
	w.schema = schema
	w.desc = nil
	w.mu.Unlock()

	if w.cfg.EnsureTable {
		md, err := TableMetadata(schema)
		if err != nil {
			return sink.Permanent(err)
		}
		if err := w.ensureTable(ctx, w.cfg.Table, md, func(have *bq.TableMetadata) error {
			return VerifySchema(schema, have.Schema)
		}); err != nil {
			return fmt.Errorf("table %s: %w", w.cfg.TableID(), err)
		}
	}
	if w.cfg.EnsureView {
		if err := w.ensureTable(ctx, analytics.ViewName(w.cfg.Table), ViewMetadata(w.cfg.TableID()), nil); err != nil {
			return fmt.Errorf("view %s: %w", w.cfg.ViewID(), err)
		}
	}
	return nil
}

// ensureTable creates table id from md when it is missing and runs verify
// on it when it exists.
func (w *Writer) ensureTable(ctx context.Context, id string, md *bq.TableMetadata, verify func(*bq.TableMetadata) error) error {
	t := w.tables.Table(id)
	have, err := t.Metadata(ctx)
	switch {
	case err == nil:
		if verify != nil {
			if err := verify(have); err != nil {
				return sink.Permanent(err)
			}
		}
		w.logger.Debug("bigquery: exists", "id", id)
		return nil
	case !isNotFound(err):
		return fmt.Errorf("metadata: %w", classify(err))
	}
	if err := t.Create(ctx, md); err != nil {
		if isConflict(err) {
			w.logger.Debug("bigquery: created concurrently", "id", id)
			return nil
		}
		return fmt.Errorf("create: %w", classify(err))
	}
	w.logger.Info("bigquery: created", "id", id, "view", md.ViewQuery != "")
	return nil
}

// Write implements analytics.Writer: it encodes records with the row
// descriptor of the current schema, opens the managed stream on the first
// call, appends the rows in one request, and waits for the result. An
// encoding failure is permanent (the rows would be rejected every time);
// an append failure is classified. Nothing is acked before the service
// confirmed the append.
func (w *Writer) Write(ctx context.Context, records []analytics.Record) error {
	if len(records) == 0 {
		return nil
	}
	stream, desc, err := w.streamFor(ctx)
	if err != nil {
		return err
	}
	rows := make([][]byte, 0, len(records))
	for i := range records {
		row, err := Encode(desc, &records[i])
		if err != nil {
			return sink.Permanent(fmt.Errorf("encode: %w", err))
		}
		rows = append(rows, row)
	}
	if err := stream.Append(ctx, rows); err != nil {
		return fmt.Errorf("append %d row(s) to %s: %w", len(rows), w.cfg.TableID(), classify(err))
	}
	return nil
}

// streamFor returns the open stream and descriptor, opening the stream on
// the first call. An opener failure is classified and retried on the next
// write. The open itself runs outside w.mu: a concurrent caller waits for it
// through stream's SingleFlight instead of blocking past its own ctx.
func (w *Writer) streamFor(ctx context.Context) (Appender, *RowDescriptor, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, nil, sink.Permanent(errors.New("bigquery writer is closed"))
	}
	if w.desc == nil {
		desc, err := NewRowDescriptor(w.schema)
		if err != nil {
			w.mu.Unlock()
			return nil, nil, sink.Permanent(fmt.Errorf("row descriptor: %w", err))
		}
		w.desc = desc
	}
	desc := w.desc
	w.mu.Unlock()

	stream, err := w.stream.Do(ctx, func(ctx context.Context) (Appender, error) {
		s, err := w.open(ctx, desc.Proto)
		if err != nil {
			return nil, err
		}
		w.mu.Lock()
		closed := w.closed
		w.mu.Unlock()
		if closed {
			_ = s.Close()
			return nil, sink.Permanent(errors.New("bigquery writer is closed"))
		}
		w.logger.Info("bigquery: write stream opened")
		return s, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open write stream to %s: %w", w.cfg.TableID(), classify(err))
	}
	return stream, desc, nil
}

// Close implements analytics.Writer: it closes the stream and the clients.
// Later writes fail permanently.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	// Peek waits out a stream open still in flight (started by a Write just
	// before the router stopped delivering) instead of racing it: the open's
	// own closure already self-closes the stream if it finishes after closed
	// is set, so at most one of the two ever closes it.
	var errs []error
	if stream, ok := w.stream.Peek(context.Background()); ok {
		if err := stream.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close write stream: %w", err))
		}
	}
	for _, c := range w.closers {
		if err := c(); err != nil {
			errs = append(errs, err)
		}
	}
	if w.cancel != nil {
		w.cancel()
	}
	return errors.Join(errs...)
}

// datasetTables adapts *bigquery.Dataset to Tables.
type datasetTables struct {
	ds *bq.Dataset
}

// Table implements Tables.
func (d datasetTables) Table(id string) Table { return bqTable{t: d.ds.Table(id)} }

// bqTable adapts *bigquery.Table to Table.
type bqTable struct {
	t *bq.Table
}

// Metadata implements Table.
func (t bqTable) Metadata(ctx context.Context) (*bq.TableMetadata, error) { return t.t.Metadata(ctx) }

// Create implements Table.
func (t bqTable) Create(ctx context.Context, md *bq.TableMetadata) error { return t.t.Create(ctx, md) }

// managedOpener opens a managedwriter default stream on the configured
// table: rows commit immediately, no offsets are tracked (exactly-once is
// out of scope), and the stream's own transient retries are on.
//
// NewManagedStream performs a GetWriteStream call with the context it is
// given and then keeps that context as the stream's lifetime, so neither the
// per-write context (its deadline would kill the stream later) nor the bare
// base context (the call would retry an unreachable service forever) fits.
// The stream therefore gets a child of base that is cancelled only if the
// per-write context ends while the stream is still being set up; once the
// stream is open, its lifetime is bound to base alone.
func managedOpener(base context.Context, client *managedwriter.Client, cfg Config) StreamOpener {
	return func(ctx context.Context, desc *descriptorpb.DescriptorProto) (Appender, error) {
		streamCtx, cancel := context.WithCancel(base)
		setupDone := make(chan struct{})
		var abandoned atomic.Bool
		go func() {
			select {
			case <-ctx.Done():
				abandoned.Store(true)
				cancel()
			case <-setupDone:
			}
		}()
		ms, err := client.NewManagedStream(streamCtx,
			managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(cfg.Project, cfg.Dataset, cfg.Table)),
			managedwriter.WithType(managedwriter.DefaultStream),
			managedwriter.WithSchemaDescriptor(desc),
			managedwriter.EnableWriteRetries(true),
		)
		close(setupDone)
		if err != nil {
			cancel()
			if ctxErr := ctx.Err(); ctxErr != nil && abandoned.Load() {
				return nil, fmt.Errorf("%w (%v)", ctxErr, err)
			}
			return nil, err
		}
		if abandoned.Load() {
			// the per-write context ended just as setup finished: the
			// stream's context is cancelled, so it must not be used
			_ = ms.Close()
			return nil, ctx.Err()
		}
		return managedAppender{ms: ms, cancel: cancel}, nil
	}
}

// managedAppender adapts *managedwriter.ManagedStream to Appender.
type managedAppender struct {
	ms     *managedwriter.ManagedStream
	cancel context.CancelFunc
}

// Append sends rows and waits for the full response, so an error embedded
// in the response and the row errors it carries are reported.
func (a managedAppender) Append(ctx context.Context, rows [][]byte) error {
	res, err := a.ms.AppendRows(ctx, rows)
	if err != nil {
		return err
	}
	resp, err := res.FullResponse(ctx)
	if err != nil {
		return withRowErrors(err, resp)
	}
	return nil
}

// Close implements Appender. The managed stream reports io.EOF for a
// normal close; that is success.
func (a managedAppender) Close() error {
	defer a.cancel()
	if err := a.ms.Close(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// withRowErrors appends the row-level errors of resp to err's message,
// keeping err in the chain for classification.
func withRowErrors(err error, resp *storagepb.AppendRowsResponse) error {
	rowErrs := resp.GetRowErrors()
	if len(rowErrs) == 0 {
		return err
	}
	return fmt.Errorf("%w (row errors: %s)", err, describeRowErrors(rowErrs))
}

func describeRowErrors(rowErrs []*storagepb.RowError) string {
	const limit = 3
	out := ""
	for i, re := range rowErrs {
		if i == limit {
			out += fmt.Sprintf("; and %d more", len(rowErrs)-limit)
			break
		}
		if i > 0 {
			out += "; "
		}
		out += fmt.Sprintf("row %d %s: %s", re.GetIndex(), re.GetCode(), re.GetMessage())
	}
	return out
}
