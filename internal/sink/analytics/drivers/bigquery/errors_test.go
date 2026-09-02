package bigquery_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
	bqdriver "github.com/sokogen/antwatcher/internal/sink/analytics/drivers/bigquery"
)

// writeWith returns the classified error of one Write whose append fails
// with appendErr.
func writeWith(t *testing.T, appendErr error) error {
	t.Helper()
	app := &fakeAppender{fn: func(int) error { return appendErr }}
	opener := &fakeOpener{app: app}
	w := bqdriver.NewWriter(validConfig(), newFakeTables(), opener.open, nil)
	t.Cleanup(func() { _ = w.Close() })
	return w.Write(context.Background(), fixtureRecords(t, "workflow_run.completed"))
}

func TestClassify_GRPCCodes(t *testing.T) {
	permanent := []codes.Code{
		codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.NotFound,
		codes.FailedPrecondition, codes.OutOfRange, codes.AlreadyExists, codes.Unimplemented,
	}
	retryable := []codes.Code{
		codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Internal,
		codes.Aborted, codes.Canceled, codes.Unknown, codes.DataLoss,
	}
	for _, code := range permanent {
		t.Run("permanent "+code.String(), func(t *testing.T) {
			cause := status.Error(code, "storage: "+code.String())
			err := writeWith(t, fmt.Errorf("wrapped: %w", cause))
			require.Error(t, err)
			assert.True(t, sink.IsPermanent(err), err)
			assert.Equal(t, code, status.Code(err), "the status survives wrapping")
			assert.Contains(t, err.Error(), "my-proj.github.actions")
		})
	}
	for _, code := range retryable {
		t.Run("retryable "+code.String(), func(t *testing.T) {
			err := writeWith(t, status.Error(code, "storage: "+code.String()))
			require.Error(t, err)
			assert.False(t, sink.IsPermanent(err), err)
		})
	}
}

func TestClassify_ContextAndPlainErrors(t *testing.T) {
	err := writeWith(t, context.DeadlineExceeded)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, sink.IsPermanent(err))

	err = writeWith(t, context.Canceled)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, sink.IsPermanent(err))

	plain := errors.New("connection reset")
	err = writeWith(t, plain)
	require.ErrorIs(t, err, plain)
	assert.False(t, sink.IsPermanent(err), "unknown errors are retryable")

	already := sink.Permanent(errors.New("already"))
	err = writeWith(t, already)
	require.ErrorIs(t, err, already)
	assert.True(t, sink.IsPermanent(err))
}

func TestClassify_RESTStatuses(t *testing.T) {
	permanent := []int{
		http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusPreconditionFailed,
	}
	retryable := []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusBadGateway}
	for _, code := range permanent {
		t.Run(fmt.Sprintf("permanent %d", code), func(t *testing.T) {
			tables := newFakeTables()
			tables.metadataErr["actions"] = apiError(code)
			w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
			err := w.EnsureSchema(context.Background(), analytics.Current)
			require.Error(t, err)
			assert.True(t, sink.IsPermanent(err), err)
		})
	}
	for _, code := range retryable {
		t.Run(fmt.Sprintf("retryable %d", code), func(t *testing.T) {
			tables := newFakeTables()
			tables.metadataErr["actions"] = apiError(code)
			w := bqdriver.NewWriter(validConfig(), tables, nil, nil)
			err := w.EnsureSchema(context.Background(), analytics.Current)
			require.Error(t, err)
			assert.False(t, sink.IsPermanent(err), err)
		})
	}
}
