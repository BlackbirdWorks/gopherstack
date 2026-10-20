package app

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeakOwnedSeries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "suite resource", value: "s3.bucket.leak-b-1", want: true},
		{name: "foreign resource", value: "s3.bucket.other", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{Name: "leak_scope_test_active", Help: "test"},
				[]string{"lock"},
			)
			g.WithLabelValues(tt.value).Set(1)

			reg := prometheus.NewPedanticRegistry()
			require.NoError(t, reg.Register(g))

			mfs, err := reg.Gather()
			require.NoError(t, err)
			require.Len(t, mfs, 1)
			assert.Equal(t, tt.want, leakOwnedSeries(mfs[0].GetMetric()[0]))
		})
	}
}
