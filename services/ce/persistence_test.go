package ce_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ce.InMemoryBackend) string
		verify func(t *testing.T, b *ce.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "cost_category_round_trip",
			setup: func(b *ce.InMemoryBackend) string {
				cat, err := b.CreateCostCategoryDefinition(
					"TestCat",
					"CostCategoryExpression.v1",
					"INHERITED_VALUE",
					[]ce.CostCategoryRule{{Value: "Engineering"}},
					nil,
				)
				if err != nil {
					return ""
				}

				return cat.ARN
			},
			verify: func(t *testing.T, b *ce.InMemoryBackend, id string) {
				t.Helper()

				cat, err := b.DescribeCostCategoryDefinition(id)
				require.NoError(t, err)
				assert.Equal(t, "TestCat", cat.Name)
				assert.Equal(t, "INHERITED_VALUE", cat.DefaultValue)
			},
		},
		{
			name: "anomaly_monitor_round_trip",
			setup: func(b *ce.InMemoryBackend) string {
				mon, err := b.CreateAnomalyMonitor("MyMonitor", "DIMENSIONAL", "SERVICE", nil)
				if err != nil {
					return ""
				}

				return mon.MonitorARN
			},
			verify: func(t *testing.T, b *ce.InMemoryBackend, id string) {
				t.Helper()

				monitors, _ := b.GetAnomalyMonitors([]string{id}, 0, "")
				require.Len(t, monitors, 1)
				assert.Equal(t, "MyMonitor", monitors[0].MonitorName)
			},
		},
		{
			name: "anomaly_subscription_round_trip",
			setup: func(b *ce.InMemoryBackend) string {
				mon, err := b.CreateAnomalyMonitor("SubMon", "DIMENSIONAL", "SERVICE", nil)
				if err != nil {
					return ""
				}

				sub, err := b.CreateAnomalySubscription(
					"MySub", "DAILY",
					[]string{mon.MonitorARN},
					[]ce.Subscriber{{Address: "test@example.com", Type: "EMAIL", Status: "CONFIRMED"}},
					10.0,
					nil,
				)
				if err != nil {
					return ""
				}

				return sub.SubscriptionARN
			},
			verify: func(t *testing.T, b *ce.InMemoryBackend, id string) {
				t.Helper()

				subs, _ := b.GetAnomalySubscriptions([]string{id}, "", 0, "")
				require.Len(t, subs, 1)
				assert.Equal(t, "MySub", subs[0].SubscriptionName)
				assert.Equal(t, "DAILY", subs[0].Frequency)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *ce.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *ce.InMemoryBackend, _ string) {
				t.Helper()

				cats, _ := b.ListCostCategoryDefinitions(0, "")
				assert.Empty(t, cats)
				monitors, _ := b.GetAnomalyMonitors(nil, 0, "")
				assert.Empty(t, monitors)
				subs, _ := b.GetAnomalySubscriptions(nil, "", 0, "")
				assert.Empty(t, subs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ce.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := ce.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateAnomalyMonitor("Mon1", "DIMENSIONAL", "SERVICE", nil)
	require.NoError(t, err)

	_, err = b.CreateCostCategoryDefinition("Cat1", "CostCategoryExpression.v1", "", nil, nil)
	require.NoError(t, err)

	b.Reset()

	cats, _ := b.ListCostCategoryDefinitions(0, "")
	assert.Empty(t, cats)
	monitors, _ := b.GetAnomalyMonitors(nil, 0, "")
	assert.Empty(t, monitors)
}

func TestCeHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := ce.NewInMemoryBackend("000000000000", "us-east-1")
	h := ce.NewHandler(backend)

	_, err := backend.CreateAnomalyMonitor("snap-mon", "DIMENSIONAL", "SERVICE", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ce.NewInMemoryBackend("000000000000", "us-east-1")
	freshH := ce.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	monitors, _ := fresh.GetAnomalyMonitors(nil, 0, "")
	assert.Len(t, monitors, 1)
	assert.Equal(t, "snap-mon", monitors[0].MonitorName)
}
