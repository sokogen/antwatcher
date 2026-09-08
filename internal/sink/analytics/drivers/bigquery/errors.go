package bigquery

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sokogen/antwatcher/internal/sink"
)

// classify applies the sink error rule to a BigQuery error: failures a retry
// cannot change come back wrapped by sink.Permanent, everything else stays
// retryable.
//
// Storage Write API (gRPC) codes: InvalidArgument (schema mismatch, bad
// row), PermissionDenied, Unauthenticated, NotFound (table or dataset),
// FailedPrecondition, OutOfRange, AlreadyExists, and Unimplemented are
// permanent; Unavailable, DeadlineExceeded, ResourceExhausted (quota),
// Internal, Aborted, Canceled, and Unknown are retryable.
//
// REST (googleapi) statuses from the metadata calls: 400, 401, 403, 404,
// 409, and 412 are permanent; 429 and 5xx are retryable.
//
// A context error from the caller is left as it is (the router treats it
// as retryable), and an error that is neither is retryable: a wrongly
// permanent error stalls the message until an operator acts, a wrongly
// retryable one only costs redeliveries.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		switch gerr.Code {
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusConflict, http.StatusPreconditionFailed:
			return sink.Permanent(err)
		}
		return err
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.NotFound,
			codes.FailedPrecondition, codes.OutOfRange, codes.AlreadyExists, codes.Unimplemented:
			return sink.Permanent(err)
		}
	}
	return err
}

// isNotFound reports a 404 from a metadata call.
func isNotFound(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == http.StatusNotFound
}

// isConflict reports a 409 from a metadata call: the resource was created
// concurrently.
func isConflict(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == http.StatusConflict
}
