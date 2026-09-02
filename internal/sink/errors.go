package sink

import (
	"errors"
	"fmt"
)

// ErrSkipped is returned by Process when the projection produces nothing for
// the event, for example a trace sink receiving a queued job. The router
// counts the event as skipped and acks it.
var ErrSkipped = errors.New("sink: event skipped")

// permanentError marks an error retrying cannot fix.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not retryable. The rule for drivers: wrap failures
// that a retry cannot change (authentication and authorization, schema
// mismatch, invalid argument, malformed input) in Permanent; leave everything
// else (network, timeouts, 5xx, rate limits, unknown) retryable. When in doubt
// the error is retryable: a wrongly permanent error stalls the message until
// an operator acts, a wrongly retryable one only costs redeliveries.
//
// Permanent(nil) is nil and an already permanent error is returned unchanged.
// The result unwraps to err, so errors.Is and errors.As keep working.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	if IsPermanent(err) {
		return err
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, was marked by
// Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// Permanentf is Permanent(fmt.Errorf(format, args...)).
func Permanentf(format string, args ...any) error {
	return Permanent(fmt.Errorf(format, args...))
}
