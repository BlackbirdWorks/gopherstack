package cwmetric_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

func TestAsync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		size     int
		blocked  bool
		sent     int
		wantSeen int
	}{
		{name: "delivers", size: 8, sent: 5, wantSeen: 5},
		{name: "drops when full", size: 1, blocked: true, sent: 50, wantSeen: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				mu   sync.Mutex
				seen int
			)

			gate := make(chan struct{})
			if !tt.blocked {
				close(gate)
			}

			inner := cwmetric.EmitterFunc(func(cwmetric.Point) error {
				<-gate

				mu.Lock()
				seen++
				mu.Unlock()

				return nil
			})

			a := cwmetric.NewAsync(t.Context(), inner, tt.size, 0)
			for range tt.sent {
				require.NoError(t, a.EmitMetric(cwmetric.Point{Name: "m"}))
			}

			if tt.blocked {
				assert.Positive(t, a.Dropped())
				close(gate)

				return
			}

			assert.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()

				return seen == tt.wantSeen
			}, 5*time.Second, 5*time.Millisecond)
		})
	}
}

func TestAsyncAggregatesWindow(t *testing.T) {
	t.Parallel()

	var (
		mu  sync.Mutex
		got []cwmetric.Point
	)

	a := cwmetric.NewAsync(t.Context(), cwmetric.EmitterFunc(func(p cwmetric.Point) error {
		mu.Lock()
		defer mu.Unlock()

		got = append(got, p)

		return nil
	}), 64, 20*time.Millisecond)

	var s cwmetric.Sink

	s.Set(a)

	for _, v := range []float64{3, 1, 5} {
		s.Put("r", "ns", "n", "Count", v, cwmetric.Dimension{Name: "d", Value: "v"})
	}

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(got) == 1
	}, 5*time.Second, 5*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	assert.InDelta(t, 3.0, got[0].Samples, 0)
	assert.InDelta(t, 9.0, got[0].Sum, 0)
	assert.InDelta(t, 1.0, got[0].Min, 0)
	assert.InDelta(t, 5.0, got[0].Max, 0)
	assert.Equal(t, "v", got[0].Dimensions[0].Value)
}

func TestSink(t *testing.T) {
	t.Parallel()

	var s cwmetric.Sink

	s.Put("r", "ns", "n", "Count", 1)
	assert.False(t, s.Enabled())

	var got cwmetric.Point

	s.Set(cwmetric.EmitterFunc(func(p cwmetric.Point) error {
		got = p

		return nil
	}))
	s.Put("r", "ns", "n", "Count", 2, cwmetric.Dimension{Name: "d", Value: "v"})
	assert.True(t, s.Enabled())
	assert.InDelta(t, 2.0, got.Value, 0)
	assert.Equal(t, "d", got.Dimensions[0].Name)
}
