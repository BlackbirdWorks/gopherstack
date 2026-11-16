package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func TestAzureTableMetrics_PutMetricRow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		total int64
	}{
		{name: "first write creates table", total: 3},
		{name: "second write replaces row", total: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azuretable.NewInMemoryBackend()
			m := azureTableMetrics{backend: b}
			table := "$MetricsHourPrimaryTransactionsFile"

			require.NoError(
				t,
				m.PutMetricRow(table, "20260101T0100", "user;All", map[string]any{"TotalRequests": int64(1)}),
			)
			require.NoError(t, m.PutMetricRow(table, "20260101T0100", "user;All",
				map[string]any{"TotalRequests": tt.total, "Availability": 100.0}))

			e, err := b.GetEntity(table, "20260101T0100", "user;All")
			require.NoError(t, err)
			assert.EqualValues(t, tt.total, e.Properties["TotalRequests"].Value)
			assert.InDelta(t, 100.0, e.Properties["Availability"].Value, 0.001)
		})
	}
}
