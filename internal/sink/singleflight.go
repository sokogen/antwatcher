package sink

import (
	"context"
	"sync"
)

// SingleFlight coordinates concurrent attempts to establish a resource that
// only needs to be opened once (a schema, a connection, a write stream): the
// Sink contract requires Process to be safe for concurrent use and to honor
// its own ctx, but the resource itself may forbid concurrent opens (a second
// connection would be wasted or would corrupt shared state). Do runs open at
// most once at a time; other callers wait for that attempt instead of
// starting their own, and stop waiting the moment their own ctx ends instead
// of blocking on it. A failed attempt is not cached: the next call retries.
type SingleFlight[T any] struct {
	mu    sync.Mutex
	ready bool
	value T
	call  *sfCall
}

type sfCall struct {
	done chan struct{}
}

// Do returns the shared value, calling open to produce it when it is not
// ready yet. When an attempt is already in flight, Do waits for it to finish
// and then re-checks the result instead of starting another one; the wait
// returns ctx.Err() the moment ctx ends, leaving the in-flight attempt (owned
// by whichever caller started it) to finish on its own.
func (f *SingleFlight[T]) Do(ctx context.Context, open func(context.Context) (T, error)) (T, error) {
	for {
		f.mu.Lock()
		if f.ready {
			v := f.value
			f.mu.Unlock()
			return v, nil
		}
		if f.call != nil {
			call := f.call
			f.mu.Unlock()
			select {
			case <-call.done:
				continue
			case <-ctx.Done():
				var zero T
				return zero, ctx.Err()
			}
		}
		call := &sfCall{done: make(chan struct{})}
		f.call = call
		f.mu.Unlock()

		v, err := f.attempt(ctx, call, open)

		if err != nil {
			var zero T
			return zero, err
		}
		return v, nil
	}
}

// attempt runs open and always clears f.call and closes call.done afterward,
// even if open panics, so a panicking attempt does not permanently wedge
// every future Do call behind a done channel that never closes. A panic is
// treated like a failed attempt (nothing cached) and re-raised once cleanup
// is done.
func (f *SingleFlight[T]) attempt(ctx context.Context, call *sfCall, open func(context.Context) (T, error)) (v T, err error) {
	panicked := true
	defer func() {
		r := recover()
		f.mu.Lock()
		f.call = nil
		if err == nil && !panicked {
			f.ready, f.value = true, v
		}
		f.mu.Unlock()
		close(call.done)
		if r != nil {
			panic(r)
		}
	}()
	v, err = open(ctx)
	panicked = false
	return v, err
}

// Ready reports the cached value and whether Do has already produced one,
// without waiting for an attempt in flight.
func (f *SingleFlight[T]) Ready() (T, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ready {
		var zero T
		return zero, false
	}
	return f.value, true
}

// Peek waits for any attempt already in flight to finish (bounded by ctx)
// and reports the cached value and whether one ever succeeded. Unlike Do, it
// never starts an attempt itself, so it is safe to call once the caller is
// done starting new ones, e.g. to pick up a resource a concurrent Do is
// still opening while closing down.
func (f *SingleFlight[T]) Peek(ctx context.Context) (T, bool) {
	for {
		f.mu.Lock()
		if f.ready {
			v := f.value
			f.mu.Unlock()
			return v, true
		}
		call := f.call
		f.mu.Unlock()
		if call == nil {
			var zero T
			return zero, false
		}
		select {
		case <-call.done:
			continue
		case <-ctx.Done():
			var zero T
			return zero, false
		}
	}
}
