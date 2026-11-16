package azurefile_test

import (
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

type metricRows struct {
	rows map[string]map[string]any
	mu   sync.Mutex
}

var _ azurefile.MetricsSink = (*metricRows)(nil)

func (m *metricRows) PutMetricRow(table, pk, rk string, props map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.rows == nil {
		m.rows = map[string]map[string]any{}
	}

	m.rows[table+"|"+pk+"|"+rk] = props

	return nil
}

func (m *metricRows) find(table, rk string) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, v := range m.rows {
		if len(k) > len(table)+1 && k[:len(table)] == table && k[len(k)-len(rk):] == rk {
			return v
		}
	}

	return nil
}

func TestSDK_StorageAnalyticsMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, m *metricRows)
		name  string
	}{
		{name: "all row counts requests", check: func(t *testing.T, m *metricRows) {
			t.Helper()

			row := m.find("$MetricsHourPrimaryTransactionsFile", ";All")
			require.NotNil(t, row)
			assert.EqualValues(t, 3, row["TotalRequests"])
			assert.EqualValues(t, 2, row["Success"])
			assert.EqualValues(t, 1, row["ClientOtherError"])
			assert.InDelta(t, 100.0*2/3, row["PercentSuccess"], 0.001)
			assert.InDelta(t, 100.0, row["Availability"], 0.001)
			assert.InDelta(t, 100.0/3, row["PercentClientOtherError"], 0.001)
			assert.EqualValues(t, 0, row["AnonymousSuccess"])
		}},
		{name: "api row per operation", check: func(t *testing.T, m *metricRows) {
			t.Helper()

			row := m.find("$MetricsHourPrimaryTransactionsFile", ";CreateShare")
			require.NotNil(t, row)
			assert.EqualValues(t, 2, row["TotalRequests"])
			assert.EqualValues(t, 1, row["ClientOtherError"])
			assert.EqualValues(t, 2, row["TotalBillableRequests"])
		}},
		{name: "minute table disabled by default", check: func(t *testing.T, m *metricRows) {
			t.Helper()

			assert.Nil(t, m.find("$MetricsMinutePrimaryTransactionsFile", ";All"))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := &metricRows{}
			e := newEnv(t, func(h *azurefile.Handler) { h.Metrics = m })
			sc := e.svc.NewShareClient("share1")

			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)
			_, err = sc.Create(ctx(t), nil)
			require.Error(t, err)
			_, err = e.svc.GetProperties(ctx(t), &service.GetPropertiesOptions{})
			require.NoError(t, err)

			tt.check(t, m)
		})
	}
}
