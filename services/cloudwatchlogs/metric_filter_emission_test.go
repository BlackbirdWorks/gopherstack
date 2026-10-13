package cloudwatchlogs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestMetricFilter_DimensionLimit(t *testing.T) {
	t.Parallel()

	dims := func(n int) map[string]string {
		m := map[string]string{}
		for _, k := range []string{"a", "b", "c", "d"}[:n] {
			m[k] = "$." + k
		}

		return m
	}

	tests := []struct {
		name    string
		dims    int
		wantErr bool
	}{
		{"three_ok", 3, false},
		{"four_rejected", 4, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			ctx := context.Background()
			_, err := b.CreateLogGroup(ctx, "g", "", "")
			require.NoError(t, err)

			err = b.PutMetricFilter(ctx, "g", "f", `{ $.a = 1 }`, []cloudwatchlogs.MetricTransformation{{
				MetricNamespace: "N", MetricName: "M", MetricValue: "1", Dimensions: dims(tt.dims),
			}})
			if tt.wantErr {
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestMetricFilter_DefaultValueEmission(t *testing.T) {
	t.Parallel()

	def := 0.0
	seven := 7.0

	tests := []struct {
		name       string
		def        *float64
		dims       map[string]string
		messages   []string
		wantValues []float64
	}{
		{name: "no_match_emits_default", def: &def, messages: []string{"INFO"}, wantValues: []float64{0}},
		{name: "no_events_emits_default", def: &seven, wantValues: []float64{7}},
		{name: "match_suppresses_default", def: &def, messages: []string{"ERROR"}, wantValues: []float64{}},
		{name: "unset_emits_nothing", messages: []string{"INFO"}, wantValues: []float64{}},
		{
			name: "dimensions_skip_default", def: &def, dims: map[string]string{"x": "$.x"},
			messages: []string{"INFO"}, wantValues: []float64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var mu sync.Mutex
			defaults := []float64{}
			sweep := false

			b := cloudwatchlogs.NewInMemoryBackend()
			b.SetMetricEmitter(cwmetric.EmitterFunc(func(p cwmetric.Point) error {
				mu.Lock()
				defer mu.Unlock()
				if sweep {
					defaults = append(defaults, p.Value)
				}

				return nil
			}))

			ctx := context.Background()
			_, err := b.CreateLogGroup(ctx, "g", "", "")
			require.NoError(t, err)
			_, err = b.CreateLogStream(ctx, "g", "s")
			require.NoError(t, err)

			pattern := "ERROR"
			if tt.dims != nil {
				pattern = `{ $.x = 1 }`
			}

			require.NoError(t, b.PutMetricFilter(ctx, "g", "f", pattern, []cloudwatchlogs.MetricTransformation{{
				MetricNamespace: "N", MetricName: "M", MetricValue: "1", DefaultValue: tt.def, Dimensions: tt.dims,
			}}))

			now := time.Now().UnixMilli()
			for _, m := range tt.messages {
				_, err = b.PutLogEvents(ctx, "g", "s", "", []cloudwatchlogs.InputLogEvent{{Message: m, Timestamp: now}})
				require.NoError(t, err)
			}

			mu.Lock()
			sweep = true
			mu.Unlock()

			b.EmitMetricFilterDefaults()

			mu.Lock()
			assert.Equal(t, tt.wantValues, defaults)
			mu.Unlock()

			b.EmitMetricFilterDefaults()

			mu.Lock()
			defer mu.Unlock()
			if len(tt.messages) > 0 && tt.name == "match_suppresses_default" {
				assert.Equal(t, []float64{0}, defaults, "next period without matches emits the default")
			}
		})
	}
}
