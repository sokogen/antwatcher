package bigquery_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
	bqdriver "github.com/sokogen/antwatcher/internal/sink/analytics/drivers/bigquery"
)

// writeStub is an in-process BigQuery Storage Write service: it answers
// GetWriteStream for the default stream and serves AppendRows, recording
// every request and replying with whatever respond returns.
type writeStub struct {
	storagepb.UnimplementedBigQueryWriteServer
	mu      sync.Mutex
	reqs    []*storagepb.AppendRowsRequest
	respond func(call int, req *storagepb.AppendRowsRequest) *storagepb.AppendRowsResponse
}

func (s *writeStub) GetWriteStream(_ context.Context, req *storagepb.GetWriteStreamRequest) (*storagepb.WriteStream, error) {
	return &storagepb.WriteStream{Name: req.GetName(), Type: storagepb.WriteStream_COMMITTED, Location: "us"}, nil
}

func (s *writeStub) AppendRows(srv storagepb.BigQueryWrite_AppendRowsServer) error {
	for {
		req, err := srv.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		call := len(s.reqs)
		s.reqs = append(s.reqs, req)
		s.mu.Unlock()
		resp := s.respond(call, req)
		resp.WriteStream = req.GetWriteStream()
		if err := srv.Send(resp); err != nil {
			return err
		}
	}
}

func (s *writeStub) requests() []*storagepb.AppendRowsRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*storagepb.AppendRowsRequest(nil), s.reqs...)
}

func okResponse() *storagepb.AppendRowsResponse {
	return &storagepb.AppendRowsResponse{Response: &storagepb.AppendRowsResponse_AppendResult_{
		AppendResult: &storagepb.AppendRowsResponse_AppendResult{},
	}}
}

func errResponse(code codes.Code, msg string, rowErrs ...*storagepb.RowError) *storagepb.AppendRowsResponse {
	return &storagepb.AppendRowsResponse{
		Response:  &storagepb.AppendRowsResponse_Error{Error: &status.Status{Code: int32(code), Message: msg}},
		RowErrors: rowErrs,
	}
}

func startWriteStub(t *testing.T, stub *writeStub) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(srv, stub)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func newStubWriter(t *testing.T, addr string) *bqdriver.Writer {
	t.Helper()
	cfg := validConfig()
	cfg.EnsureTable = false
	cfg.EnsureView = false
	w, err := bqdriver.New(cfg, bqdriver.Options{ClientOptions: []option.ClientOption{
		option.WithEndpoint(addr),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestManagedStream_RowsReachTheService(t *testing.T) {
	stub := &writeStub{respond: func(int, *storagepb.AppendRowsRequest) *storagepb.AppendRowsResponse { return okResponse() }}
	w := newStubWriter(t, startWriteStub(t, stub))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job := fixtureRecords(t, "workflow_job.completed")
	require.NoError(t, w.Write(ctx, job))
	run := fixtureRecords(t, "workflow_run.completed")
	require.NoError(t, w.Write(ctx, run))

	reqs := stub.requests()
	require.Len(t, reqs, 2)
	first := reqs[0]
	assert.Equal(t, "projects/my-proj/datasets/github/tables/actions/streams/_default", first.GetWriteStream(),
		"the default stream of the configured table")
	d, err := bqdriver.NewRowDescriptor(analytics.Current)
	require.NoError(t, err)
	require.NotNil(t, first.GetProtoRows().GetWriterSchema(), "the first request carries the schema")
	assert.True(t, proto.Equal(d.Proto, first.GetProtoRows().GetWriterSchema().GetProtoDescriptor()),
		"the schema is the row descriptor")
	rows := first.GetProtoRows().GetRows().GetSerializedRows()
	require.Len(t, rows, len(job))
	for i := range job {
		assert.Equal(t, job[i].Fields(), decodeRow(t, d, analytics.Current, rows[i]), "job row %d as received by the service", i)
	}
	assert.Len(t, reqs[1].GetProtoRows().GetRows().GetSerializedRows(), 1)

	require.NoError(t, w.Close())
	err = w.Write(ctx, run)
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err))
}

func TestManagedStream_EmbeddedErrorsAreClassified(t *testing.T) {
	stub := &writeStub{respond: func(call int, _ *storagepb.AppendRowsRequest) *storagepb.AppendRowsResponse {
		switch call {
		case 0:
			return errResponse(codes.InvalidArgument, "schema mismatch",
				&storagepb.RowError{Index: 0, Code: storagepb.RowError_FIELDS_ERROR, Message: "field status: bad"})
		case 1:
			return errResponse(codes.PermissionDenied, "no bigquery.tables.updateData")
		default:
			return okResponse()
		}
	}}
	w := newStubWriter(t, startWriteStub(t, stub))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	recs := fixtureRecords(t, "workflow_run.completed")

	err := w.Write(ctx, recs)
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err), err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
	assert.Contains(t, err.Error(), "row 0 FIELDS_ERROR: field status: bad")

	err = w.Write(ctx, recs)
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err), err)
	assert.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))

	require.NoError(t, w.Write(ctx, recs), "the stream stays usable after rejected appends")
	assert.Len(t, stub.requests(), 3)
}

func TestManagedStream_SetupBoundedByWriteContext(t *testing.T) {
	// nothing listens here: GetWriteStream retries until the write context
	// ends, and the error is retryable so the bus redelivers
	w := newStubWriter(t, unroutable(t))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := w.Write(ctx, fixtureRecords(t, "workflow_run.completed"))
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err), err)
	assert.Less(t, time.Since(start), 15*time.Second, "setup gives up with the write context")
	assert.Contains(t, err.Error(), "open write stream to my-proj.github.actions")
}
