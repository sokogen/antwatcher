package sink_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/sink"
)

func TestPermanent(t *testing.T) {
	base := errors.New("schema mismatch")

	t.Run("nil stays nil", func(t *testing.T) {
		assert.NoError(t, sink.Permanent(nil))
	})

	t.Run("marks and unwraps", func(t *testing.T) {
		err := sink.Permanent(base)
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
		require.ErrorIs(t, err, base)
		assert.Equal(t, "permanent: schema mismatch", err.Error())
	})

	t.Run("idempotent", func(t *testing.T) {
		once := sink.Permanent(base)
		assert.Same(t, once, sink.Permanent(once))
	})

	t.Run("survives further wrapping", func(t *testing.T) {
		err := fmt.Errorf("export: %w", sink.Permanent(base))
		assert.True(t, sink.IsPermanent(err))
		require.ErrorIs(t, err, base)
	})

	t.Run("wrapped permanent is still permanent when re-marked", func(t *testing.T) {
		inner := fmt.Errorf("driver: %w", sink.Permanent(base))
		assert.Same(t, inner, sink.Permanent(inner), "an error that already carries the mark is returned unchanged")
	})

	t.Run("plain errors are retryable", func(t *testing.T) {
		assert.False(t, sink.IsPermanent(base))
		assert.False(t, sink.IsPermanent(context.DeadlineExceeded))
		assert.False(t, sink.IsPermanent(nil))
	})

	t.Run("skipped is not permanent", func(t *testing.T) {
		assert.False(t, sink.IsPermanent(sink.ErrSkipped))
		assert.False(t, sink.IsPermanent(fmt.Errorf("trace: %w", sink.ErrSkipped)))
	})

	t.Run("Permanentf", func(t *testing.T) {
		err := sink.Permanentf("bad argument %d: %w", 7, base)
		assert.True(t, sink.IsPermanent(err))
		require.ErrorIs(t, err, base)
		assert.Contains(t, err.Error(), "bad argument 7")
	})
}
