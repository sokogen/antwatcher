package bigquery_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
	bqdriver "github.com/sokogen/antwatcher/internal/sink/analytics/drivers/bigquery"
)

func TestEnsureSchema_CreatesTableAndViewWhenMissing(t *testing.T) {
	tables := newFakeTables()
	w := bqdriver.NewWriter(validConfig(), tables, nil, slog.Default())
	require.NoError(t, w.EnsureSchema(context.Background(), analytics.Current))

	assert.Equal(t, []string{"metadata actions", "create actions", "metadata actions_current", "create actions_current"}, tables.callList())

	table := tables.created["actions"]
	require.NotNil(t, table)
	want, err := bqdriver.TableMetadata(analytics.Current)
	require.NoError(t, err)
	assert.Equal(t, want, table)
	assert.Equal(t, bq.DayPartitioningType, table.TimePartitioning.Type)
	assert.Equal(t, "event_time", table.TimePartitioning.Field)
	assert.Equal(t, []string{"repository", "kind"}, table.Clustering.Fields)

	view := tables.created["actions_current"]
	require.NotNil(t, view)
	assert.Equal(t, analytics.CurrentViewSQL("my-proj.github.actions"), view.ViewQuery)
	assert.Nil(t, view.Schema)

	// a second call finds both and creates nothing
	require.NoError(t, w.EnsureSchema(context.Background(), analytics.Current))
	assert.Equal(t, []string{"metadata actions", "create actions", "metadata actions_current", "create actions_current", "metadata actions", "metadata actions_current"}, tables.callList())
}

func TestEnsureSchema_ExistingTableIsVerified(t *testing.T) {
	want, err := bqdriver.TableMetadata(analytics.Current)
	require.NoError(t, err)

	t.Run("matching", func(t *testing.T) {
		tables := newFakeTables()
		tables.existing["actions"] = want
		tables.existing["actions_current"] = bqdriver.ViewMetadata("my-proj.github.actions")
		w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
		require.NoError(t, w.EnsureSchema(context.Background(), analytics.Current))
		assert.Empty(t, tables.created)
	})
	t.Run("mismatch is permanent", func(t *testing.T) {
		tables := newFakeTables()
		tables.existing["actions"] = &bq.TableMetadata{Schema: bq.Schema{{Name: "record_id", Type: bq.IntegerFieldType}}}
		w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
		err := w.EnsureSchema(context.Background(), analytics.Current)
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
		assert.Contains(t, err.Error(), "table my-proj.github.actions")
		assert.Contains(t, err.Error(), `column "record_id" is INTEGER, want STRING`)
		assert.Equal(t, []string{"metadata actions"}, tables.callList(), "the view is not touched after a table failure")
	})
}

func TestEnsureSchema_Switches(t *testing.T) {
	t.Run("table only", func(t *testing.T) {
		cfg := validConfig()
		cfg.EnsureView = false
		tables := newFakeTables()
		w := bqdriver.NewWriter(cfg, tables, nil, nil)
		require.NoError(t, w.EnsureSchema(context.Background(), analytics.Current))
		assert.Equal(t, []string{"metadata actions", "create actions"}, tables.callList())
	})
	t.Run("view only", func(t *testing.T) {
		cfg := validConfig()
		cfg.EnsureTable = false
		tables := newFakeTables()
		w := bqdriver.NewWriter(cfg, tables, nil, nil)
		require.NoError(t, w.EnsureSchema(context.Background(), analytics.Current))
		assert.Equal(t, []string{"metadata actions_current", "create actions_current"}, tables.callList())
	})
	t.Run("both disabled", func(t *testing.T) {
		cfg := validConfig()
		cfg.EnsureTable = false
		cfg.EnsureView = false
		tables := newFakeTables()
		w := bqdriver.NewWriter(cfg, tables, nil, nil)
		require.NoError(t, w.EnsureSchema(context.Background(), analytics.Current))
		assert.Empty(t, tables.callList())
	})
}

func TestEnsureSchema_Errors(t *testing.T) {
	t.Run("invalid schema is permanent", func(t *testing.T) {
		tables := newFakeTables()
		w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
		err := w.EnsureSchema(context.Background(), analytics.Schema{Columns: []analytics.Column{{Name: "x", Type: "float"}}})
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
		assert.Empty(t, tables.callList())
	})
	t.Run("concurrent creation is fine", func(t *testing.T) {
		tables := newFakeTables()
		tables.createErr["actions"] = apiError(http.StatusConflict)
		tables.createErr["actions_current"] = apiError(http.StatusConflict)
		w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
		require.NoError(t, w.EnsureSchema(context.Background(), analytics.Current))
	})
	t.Run("create forbidden is permanent", func(t *testing.T) {
		tables := newFakeTables()
		tables.createErr["actions"] = apiError(http.StatusForbidden)
		w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
		err := w.EnsureSchema(context.Background(), analytics.Current)
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
		assert.Contains(t, err.Error(), "create:")
	})
	t.Run("view create unavailable is retryable", func(t *testing.T) {
		tables := newFakeTables()
		tables.createErr["actions_current"] = apiError(http.StatusServiceUnavailable)
		w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
		err := w.EnsureSchema(context.Background(), analytics.Current)
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err))
		assert.Contains(t, err.Error(), "view my-proj.github.actions_current")
		assert.Contains(t, tables.created, "actions", "the table was created before the view failed")
	})
	t.Run("metadata transport error is retryable", func(t *testing.T) {
		tables := newFakeTables()
		tables.metadataErr["actions"] = errors.New("dial tcp: connection refused")
		w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
		err := w.EnsureSchema(context.Background(), analytics.Current)
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err))
		assert.Equal(t, []string{"metadata actions"}, tables.callList())
	})
}

func TestWrite_OpensStreamOnceAndAppendsEncodedRows(t *testing.T) {
	app := &fakeAppender{}
	opener := &fakeOpener{app: app}
	tables := newFakeTables()
	w := bqdriver.NewWriter(validConfig(), tables, opener.open, nil)

	assert.Equal(t, 0, opener.openCount(), "construction opens nothing")
	require.NoError(t, w.EnsureSchema(context.Background(), analytics.Current))
	assert.Equal(t, 0, opener.openCount(), "schema management opens nothing")

	run := fixtureRecords(t, "workflow_run.completed")
	job := fixtureRecords(t, "workflow_job.completed")
	require.NoError(t, w.Write(context.Background(), run))
	require.NoError(t, w.Write(context.Background(), job))
	assert.Equal(t, 1, opener.openCount(), "the stream is opened on the first write and reused")

	d, err := bqdriver.NewRowDescriptor(analytics.Current)
	require.NoError(t, err)
	assert.Equal(t, d.Proto.String(), opener.descs[0].String(), "the stream is opened with the row descriptor")

	batches := app.batchList()
	require.Len(t, batches, 2)
	require.Len(t, batches[0], 1)
	require.Len(t, batches[1], len(job))
	assert.Equal(t, run[0].Fields(), decodeRow(t, d, analytics.Current, batches[0][0]))
	for i := range job {
		assert.Equal(t, job[i].Fields(), decodeRow(t, d, analytics.Current, batches[1][i]), "job row %d", i)
	}

	require.NoError(t, w.Write(context.Background(), nil), "no records, no request")
	assert.Len(t, app.batchList(), 2)

	require.NoError(t, w.Close())
	assert.Equal(t, 1, app.closed)
	require.NoError(t, w.Close(), "idempotent")
	assert.Equal(t, 1, app.closed)
}

func TestWrite_WithoutEnsureUsesCurrentSchema(t *testing.T) {
	cfg := validConfig()
	cfg.EnsureTable = false
	cfg.EnsureView = false
	app := &fakeAppender{}
	opener := &fakeOpener{app: app}
	w := bqdriver.NewWriter(cfg, newFakeTables(), opener.open, nil)
	require.NoError(t, w.Write(context.Background(), fixtureRecords(t, "workflow_job.queued")))
	d, err := bqdriver.NewRowDescriptor(analytics.Current)
	require.NoError(t, err)
	assert.Equal(t, d.Proto.String(), opener.descs[0].String())
}

func TestWrite_OpenerFailureIsRetriedNextWrite(t *testing.T) {
	app := &fakeAppender{}
	opener := &fakeOpener{app: app, err: status.Error(codes.Unavailable, "no connection")}
	w := bqdriver.NewWriter(validConfig(), newFakeTables(), opener.open, nil)
	recs := fixtureRecords(t, "workflow_run.completed")

	err := w.Write(context.Background(), recs)
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	assert.Contains(t, err.Error(), "open write stream to my-proj.github.actions")
	assert.Empty(t, app.batchList())

	opener.mu.Lock()
	opener.err = status.Error(codes.PermissionDenied, "no bigquery.tables.updateData")
	opener.mu.Unlock()
	err = w.Write(context.Background(), recs)
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err))

	opener.mu.Lock()
	opener.err = nil
	opener.mu.Unlock()
	require.NoError(t, w.Write(context.Background(), recs))
	assert.Equal(t, 3, opener.openCount())
	assert.Len(t, app.batchList(), 1)
}

func TestWrite_AppendFailureKeepsStreamAndPropagates(t *testing.T) {
	app := &fakeAppender{fn: func(call int) error {
		if call == 0 {
			return status.Error(codes.Unavailable, "stream reset")
		}
		return nil
	}}
	opener := &fakeOpener{app: app}
	w := bqdriver.NewWriter(validConfig(), newFakeTables(), opener.open, nil)
	recs := fixtureRecords(t, "workflow_run.completed")

	err := w.Write(context.Background(), recs)
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	assert.Contains(t, err.Error(), "append 1 row(s) to my-proj.github.actions")

	require.NoError(t, w.Write(context.Background(), recs), "the redelivery succeeds on the same stream")
	assert.Equal(t, 1, opener.openCount())
	assert.Len(t, app.batchList(), 2)
}

func TestWrite_EncodingFailureIsPermanent(t *testing.T) {
	app := &fakeAppender{}
	opener := &fakeOpener{app: app}
	cfg := validConfig()
	cfg.EnsureTable = false
	cfg.EnsureView = false
	w := bqdriver.NewWriter(cfg, newFakeTables(), opener.open, nil)
	// a schema that lacks most columns cannot hold a projected record
	reduced := analytics.Schema{Columns: []analytics.Column{{Name: analytics.ColRecordID, Type: analytics.TypeString, Required: true}}}
	require.NoError(t, w.EnsureSchema(context.Background(), reduced))
	err := w.Write(context.Background(), fixtureRecords(t, "workflow_run.completed"))
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err))
	assert.Contains(t, err.Error(), "encode:")
	assert.Empty(t, app.batchList(), "nothing is sent when a row does not encode")
}

func TestWrite_ConcurrentFirstWritesShareOneStream(t *testing.T) {
	app := &fakeAppender{}
	opener := &fakeOpener{app: app}
	w := bqdriver.NewWriter(validConfig(), newFakeTables(), opener.open, nil)
	recs := fixtureRecords(t, "workflow_job.completed")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			assert.NoError(t, w.Write(context.Background(), recs))
		})
	}
	wg.Wait()
	assert.Equal(t, 1, opener.openCount())
	assert.Len(t, app.batchList(), 8)
}

func TestWrite_WaiterRespectsOwnCtxWhileAnotherOpens(t *testing.T) {
	app := &fakeAppender{}
	opener := &fakeOpener{app: app, block: make(chan struct{})}
	w := bqdriver.NewWriter(validConfig(), newFakeTables(), opener.open, nil)
	recs := fixtureRecords(t, "workflow_job.completed")

	firstStarted := make(chan struct{})
	go func() {
		close(firstStarted)
		_ = w.Write(context.Background(), recs)
	}()
	<-firstStarted
	time.Sleep(5 * time.Millisecond) // let the first Write reach the fake opener's block

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := w.Write(ctx, recs)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, time.Second, "a concurrent stream open in flight must not block a caller past its own ctx")
	close(opener.block)
}

func TestClose_WaitsOutStreamOpenInFlightAndClosesItExactlyOnce(t *testing.T) {
	app := &fakeAppender{}
	opener := &fakeOpener{app: app, block: make(chan struct{}), entered: make(chan struct{})}
	entered := opener.entered
	w := bqdriver.NewWriter(validConfig(), newFakeTables(), opener.open, nil)
	recs := fixtureRecords(t, "workflow_job.completed")

	writeDone := make(chan error, 1)
	go func() { writeDone <- w.Write(context.Background(), recs) }()
	<-entered // the open is now in flight and parked

	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close() }()

	// The point of the test: Close must not return while the open it will have
	// to clean up after is still running. Without the wait in Close, this fires.
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while a stream open was in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(opener.block)
	require.NoError(t, <-closeDone)
	<-writeDone // either it opened before Close observed it, or the open self-closed once it saw closed
	assert.Equal(t, 1, app.closed, "the stream opened concurrently with Close is closed exactly once, never leaked")
}

func TestClose_ReportsStreamAndClientErrors(t *testing.T) {
	open := func(context.Context, *descriptorpb.DescriptorProto) (bqdriver.Appender, error) {
		return closeFailingAppender{}, nil
	}
	w := bqdriver.NewWriter(validConfig(), newFakeTables(), open, nil)
	require.NoError(t, w.Write(context.Background(), fixtureRecords(t, "workflow_run.completed")))
	err := w.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "close write stream: stream busy")
}

// closeFailingAppender accepts rows and fails to close.
type closeFailingAppender struct{}

func (closeFailingAppender) Append(context.Context, [][]byte) error { return nil }
func (closeFailingAppender) Close() error                           { return errors.New("stream busy") }
