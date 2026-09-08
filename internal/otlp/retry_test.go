package otlp

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/sokogen/antwatcher/internal/sink"
)

func withRetryInfo(t *testing.T, code codes.Code, d time.Duration) error {
	t.Helper()
	st, err := status.New(code, "slow down").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(d)})
	require.NoError(t, err)
	return st.Err()
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want verdict
	}{
		{"unavailable", status.Error(codes.Unavailable, "down"), verdict{retryable: true}},
		{"deadline", status.Error(codes.DeadlineExceeded, "slow"), verdict{retryable: true}},
		{"aborted", status.Error(codes.Aborted, ""), verdict{retryable: true}},
		{"out of range", status.Error(codes.OutOfRange, ""), verdict{retryable: true}},
		{"data loss", status.Error(codes.DataLoss, ""), verdict{retryable: true}},
		{"cancelled", status.Error(codes.Canceled, ""), verdict{retryable: true}},
		{"unavailable with retry info", withRetryInfo(t, codes.Unavailable, 3*time.Second), verdict{retryable: true, wait: 3 * time.Second}},
		{"resource exhausted without retry info", status.Error(codes.ResourceExhausted, "quota"), verdict{}},
		{"resource exhausted with retry info", withRetryInfo(t, codes.ResourceExhausted, 2*time.Second), verdict{retryable: true, wait: 2 * time.Second}},
		{"invalid argument", status.Error(codes.InvalidArgument, "bad"), verdict{}},
		{"unauthenticated", status.Error(codes.Unauthenticated, ""), verdict{}},
		{"permission denied", status.Error(codes.PermissionDenied, ""), verdict{}},
		{"unimplemented", status.Error(codes.Unimplemented, ""), verdict{}},
		{"internal", status.Error(codes.Internal, ""), verdict{}},
		{"unknown", status.Error(codes.Unknown, ""), verdict{}},
		{"wrapped grpc status", errors.Join(errors.New("ctx"), status.Error(codes.InvalidArgument, "")), verdict{}},
		{"http 429", &HTTPError{StatusCode: 429, RetryAfter: 7 * time.Second}, verdict{retryable: true, wait: 7 * time.Second}},
		{"http 502", &HTTPError{StatusCode: 502}, verdict{retryable: true}},
		{"http 503", &HTTPError{StatusCode: 503}, verdict{retryable: true}},
		{"http 504", &HTTPError{StatusCode: 504}, verdict{retryable: true}},
		{"http 400", &HTTPError{StatusCode: 400}, verdict{}},
		{"http 401", &HTTPError{StatusCode: 401}, verdict{}},
		{"http 404", &HTTPError{StatusCode: 404}, verdict{}},
		{"http 500", &HTTPError{StatusCode: 500}, verdict{}},
		{"plain transport error", errors.New("connection reset"), verdict{retryable: true}},
		{"context deadline", context.DeadlineExceeded, verdict{retryable: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, classify(tc.err))
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, time.Duration(0), parseRetryAfter("", now))
	assert.Equal(t, 30*time.Second, parseRetryAfter("30", now))
	assert.Equal(t, time.Duration(0), parseRetryAfter("-5", now))
	assert.Equal(t, 90*time.Second, parseRetryAfter(now.Add(90*time.Second).Format(http.TimeFormat), now))
	assert.Equal(t, time.Duration(0), parseRetryAfter(now.Add(-time.Minute).Format(http.TimeFormat), now), "past dates mean now")
	assert.Equal(t, time.Duration(0), parseRetryAfter("soon", now))
}

func TestBackoffDelay(t *testing.T) {
	assert.Equal(t, time.Duration(0), backoffDelay(0, 1))
	assert.Equal(t, time.Duration(0), backoffDelay(time.Second, 0))
	assert.Equal(t, 500*time.Millisecond, backoffDelay(500*time.Millisecond, 1))
	assert.Equal(t, time.Second, backoffDelay(500*time.Millisecond, 2))
	assert.Equal(t, 2*time.Second, backoffDelay(500*time.Millisecond, 3))
	assert.Equal(t, time.Minute, backoffDelay(30*time.Second, 10), "capped at one minute")
}

func TestFinalize(t *testing.T) {
	ctx := context.Background()
	perm := finalize(ctx, SignalTraces, "a:1", 1, status.Error(codes.InvalidArgument, "bad span"))
	assert.True(t, sink.IsPermanent(perm))
	assert.Contains(t, perm.Error(), "otlp export traces to a:1 after 1 attempt(s)")
	assert.Contains(t, perm.Error(), "bad span")
	assert.Equal(t, codes.InvalidArgument, status.Code(perm), "status survives wrapping")

	retry := finalize(ctx, SignalLogs, "a:1", 3, status.Error(codes.Unavailable, "down"))
	assert.False(t, sink.IsPermanent(retry))
	assert.Contains(t, retry.Error(), "after 3 attempt(s)")

	var httpErr *HTTPError
	h := finalize(ctx, SignalLogs, "a:1", 1, &HTTPError{StatusCode: 401, Message: "nope"})
	assert.True(t, sink.IsPermanent(h))
	require.ErrorAs(t, h, &httpErr)
	assert.Equal(t, 401, httpErr.StatusCode)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	assert.Equal(t, context.Canceled, finalize(canceled, SignalTraces, "a:1", 1, status.Error(codes.Canceled, "")))
}

func TestHTTPError_Error(t *testing.T) {
	assert.Equal(t, "http 503 Service Unavailable", (&HTTPError{StatusCode: 503}).Error())
	assert.Equal(t, "http 400 Bad Request: bad span", (&HTTPError{StatusCode: 400, Message: "bad span"}).Error())
}
