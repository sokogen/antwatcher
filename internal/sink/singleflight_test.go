package sink_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/sink"
)

func TestSingleFlight_OpensOnceUnderConcurrency(t *testing.T) {
	var f sink.SingleFlight[int]
	var calls, inFlight, maxInFlight atomic.Int32

	const n = 16
	var wg sync.WaitGroup
	results := make([]int, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = f.Do(context.Background(), func(context.Context) (int, error) {
				calls.Add(1)
				cur := inFlight.Add(1)
				for {
					m := maxInFlight.Load()
					if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
						break
					}
				}
				time.Sleep(2 * time.Millisecond)
				inFlight.Add(-1)
				return 42, nil
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d", i)
		assert.Equal(t, 42, results[i])
	}
	assert.EqualValues(t, 1, calls.Load(), "one open shared by every concurrent caller")
	assert.EqualValues(t, 1, maxInFlight.Load(), "never overlapping")
}

func TestSingleFlight_FailedAttemptIsNotCached(t *testing.T) {
	var f sink.SingleFlight[int]
	boom := errors.New("unreachable")
	calls := 0

	_, err := f.Do(context.Background(), func(context.Context) (int, error) {
		calls++
		return 0, boom
	})
	require.ErrorIs(t, err, boom)

	v, err := f.Do(context.Background(), func(context.Context) (int, error) {
		calls++
		return 7, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 7, v)
	assert.Equal(t, 2, calls, "retried after failure")

	v, err = f.Do(context.Background(), func(context.Context) (int, error) {
		calls++
		return 0, errors.New("must not run: already ready")
	})
	require.NoError(t, err)
	assert.Equal(t, 7, v)
	assert.Equal(t, 2, calls, "ready value is cached")
}

func TestSingleFlight_WaiterStopsAtItsOwnCtxDeadline(t *testing.T) {
	var f sink.SingleFlight[int]
	openStarted := make(chan struct{})
	release := make(chan struct{})

	go func() {
		_, _ = f.Do(context.Background(), func(context.Context) (int, error) {
			close(openStarted)
			<-release
			return 1, nil
		})
	}()
	<-openStarted

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := f.Do(ctx, func(context.Context) (int, error) {
		return 0, errors.New("must not run: an attempt is already in flight")
	})
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, time.Second, "the waiter returns at its own deadline, not the in-flight attempt's")
	close(release)
}

func TestSingleFlight_PanicInOpenDoesNotWedgeFutureCalls(t *testing.T) {
	var f sink.SingleFlight[int]

	require.PanicsWithValue(t, "boom", func() {
		_, _ = f.Do(context.Background(), func(context.Context) (int, error) {
			panic("boom")
		})
	})

	v, err := f.Do(context.Background(), func(context.Context) (int, error) {
		return 7, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 7, v, "a later call retries and succeeds instead of blocking forever")
}
