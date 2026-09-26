package lockmetrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// TestRWMutex_CloseInvalidatesCachedHandles pins that a post-Close
// observation lands on a live series, not a handle Close already deleted.
func TestRWMutex_CloseInvalidatesCachedHandles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build func(name string) *lockmetrics.RWMutex
		name  string
	}{
		{
			name: "use_after_close_same_instance",
			build: func(name string) *lockmetrics.RWMutex {
				m := lockmetrics.New(name)
				m.Lock("first")
				m.Unlock()
				m.Close()

				return m
			},
		},
		{
			name: "recreate_same_name_after_close",
			build: func(name string) *lockmetrics.RWMutex {
				old := lockmetrics.New(name)
				old.Lock("first")
				old.Unlock()
				old.Close()

				return lockmetrics.New(name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name := "close.invalidate." + t.Name()
			m := tt.build(name)
			t.Cleanup(m.Close)

			m.Lock("second")
			m.Unlock()
			m.RLock("second")
			m.RUnlock()

			mfs, err := prometheus.DefaultGatherer.Gather()
			require.NoError(t, err)

			held := seriesFor(mfs, "gopherstack_lock_hold_seconds", name)
			require.Len(t, held, 1, "post-Close write observation must land on a live series")
			assert.EqualValues(t, 1, held[0].GetHistogram().GetSampleCount())
			assert.Equal(t, "second", labelValue(held[0], "operation"))

			waited := seriesFor(mfs, "gopherstack_lock_wait_seconds", name)
			assert.Len(t, waited, 2, "post-Close read and write wait observations must land on live series")
		})
	}
}
