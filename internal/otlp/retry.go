package otlp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sokogen/antwatcher/internal/sink"
)

// HTTPError is a non-2xx OTLP/HTTP response.
type HTTPError struct {
	StatusCode int
	// Message is the decoded google.rpc.Status message or the (truncated)
	// response body.
	Message string
	// RetryAfter is the parsed Retry-After header, 0 when absent.
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("http %d %s", e.StatusCode, http.StatusText(e.StatusCode))
	}
	return fmt.Sprintf("http %d %s: %s", e.StatusCode, http.StatusText(e.StatusCode), e.Message)
}

// verdict is the classification of one failed attempt.
type verdict struct {
	retryable bool
	// wait is the delay the server asked for (gRPC RetryInfo, HTTP Retry-After);
	// 0 when it gave none.
	wait time.Duration
}

// classify applies the OTLP specification to a failed attempt.
//
// gRPC: CANCELLED, DEADLINE_EXCEEDED, ABORTED, OUT_OF_RANGE, UNAVAILABLE, and
// DATA_LOSS are retryable; RESOURCE_EXHAUSTED only when the status carries
// RetryInfo; every other code is permanent. HTTP: 429, 502, 503, and 504 are
// retryable (Retry-After honored); every other status is permanent. Errors
// that are neither a gRPC status nor an HTTP response (connection refused,
// timeouts, resets) are retryable.
func classify(err error) verdict {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return verdict{retryable: true, wait: httpErr.RetryAfter}
		default:
			return verdict{}
		}
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.OK {
		switch st.Code() {
		case codes.Canceled, codes.DeadlineExceeded, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss:
			return verdict{retryable: true, wait: retryInfo(st)}
		case codes.ResourceExhausted:
			if wait, ok := retryInfoDelay(st); ok {
				return verdict{retryable: true, wait: wait}
			}
			return verdict{}
		default:
			return verdict{}
		}
	}
	return verdict{retryable: true}
}

// retryInfo returns the RetryInfo delay of st or 0.
func retryInfo(st *status.Status) time.Duration {
	wait, _ := retryInfoDelay(st)
	return wait
}

// retryInfoDelay returns the RetryInfo delay of st and whether one is present.
func retryInfoDelay(st *status.Status) (time.Duration, bool) {
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			return max(ri.GetRetryDelay().AsDuration(), 0), true
		}
	}
	return 0, false
}

// parseRetryAfter reads a Retry-After header (delay seconds or HTTP date)
// relative to now; 0 when absent or unparsable.
func parseRetryAfter(h string, now time.Time) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil {
		return max(time.Duration(secs)*time.Second, 0)
	}
	if at, err := http.ParseTime(h); err == nil {
		return max(at.Sub(now), 0)
	}
	return 0
}

// finalize turns the last error of an export into what the sink contract
// expects: ctx errors pass through, permanent failures are wrapped by
// sink.Permanent, retryable ones are returned as they are.
func finalize(ctx context.Context, signal, endpoint string, attempts int, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	wrapped := fmt.Errorf("otlp export %s to %s after %d attempt(s): %w", signal, endpoint, attempts, err)
	if classify(err).retryable {
		return wrapped
	}
	return sink.Permanent(wrapped)
}

// backoffDelay is the delay before retry number n (1-based) with base
// doubling per retry.
func backoffDelay(base time.Duration, n int) time.Duration {
	if base <= 0 || n <= 0 {
		return 0
	}
	d := base
	for i := 1; i < n; i++ {
		d *= 2
		if d > time.Minute {
			return time.Minute
		}
	}
	return d
}
