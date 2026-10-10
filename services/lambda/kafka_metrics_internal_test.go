package lambda

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

type kafkaPointRecorder struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (r *kafkaPointRecorder) EmitMetric(p cwmetric.Point) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.points = append(r.points, p)

	return nil
}

func (r *kafkaPointRecorder) sums() map[string]float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := map[string]float64{}
	for _, p := range r.points {
		out[p.Name] += p.Value
	}

	return out
}

func TestKafkaWorkerMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    map[string]float64
		name    string
		metrics []string
	}{
		{
			name:    "event_count_group",
			metrics: []string{"EventCount"},
			want:    map[string]float64{"InvokedEventCount": 3, "CommittedEventCount": 3},
		},
		{
			name:    "both_groups",
			metrics: []string{"EventCount", "ErrorCount"},
			want:    map[string]float64{"InvokedEventCount": 3, "CommittedEventCount": 3},
		},
		{name: "none", metrics: nil, want: map[string]float64{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)
			rec := &kafkaPointRecorder{}
			b.SetMetricEmitter(rec)

			p := NewEventSourcePoller(b, nil)
			p.kafkaInvoker = func(context.Context, string, []byte) error { return nil }

			spec := kafkaWorkerSpec{UUID: "u-1", FunctionARN: "arn:aws:lambda:us-east-1:000000000000:function:f"}
			if tt.metrics != nil {
				spec.Metrics = &ESMMetricsConfig{Metrics: tt.metrics}
			}

			cons := newFakeKafkaConsumer()
			p.deliverKafkaBatch(t.Context(), cons, spec, kafkaOffsets(3))

			assert.Equal(t, tt.want, rec.sums())
			require.Len(t, cons.committedOffsets(), 3)
		})
	}
}
