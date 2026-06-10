package mwaa_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// TestPublishMetrics_TrimDoesNotRetainOversizedArray verifies that capping the
// per-environment metrics slice copies the survivors into a right-sized backing
// array, so the trimmed-off prefix is released for GC rather than pinned.
func TestPublishMetrics_TrimDoesNotRetainOversizedArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		publish int
	}{
		{name: "just over cap", publish: 1200},
		{name: "far over cap", publish: 5000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(testRegion, testAccountID, "leak-env", newCreateReq())
			require.NoError(t, err)

			data := make([]mwaa.ExportedMetricDatum, tc.publish)
			for i := range data {
				data[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("M%d", i)}
			}
			require.NoError(t, b.PublishMetrics("leak-env",
				&mwaa.ExportedPublishMetricsRequest{MetricData: data}))

			// len is capped...
			assert.Equal(t, 1000, mwaa.MetricsCount(b, "leak-env"))
			// ...and the backing array is right-sized, not the oversized original.
			assert.Equal(t, 1000, mwaa.MetricsCapacity(b, "leak-env"),
				"backing array must not retain trimmed-off capacity")
		})
	}
}
