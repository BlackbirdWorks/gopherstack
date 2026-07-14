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

				monitors, _, err := b.GetAnomalyMonitors([]string{id}, 0, "")
				require.NoError(t, err)
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

				subs, _, err := b.GetAnomalySubscriptions([]string{id}, "", 0, "")
				require.NoError(t, err)
				require.Len(t, subs, 1)
				assert.Equal(t, "MySub", subs[0].SubscriptionName)
				assert.Equal(t, "DAILY", subs[0].Frequency)
			},
		},
		{
			name: "anomaly_round_trip",
			setup: func(b *ce.InMemoryBackend) string {
				b.AddAnomaly(ce.Anomaly{
					AnomalyID:        "anomaly-1",
					AnomalyStartDate: "2024-01-01",
					AnomalyEndDate:   "2024-01-02",
					TotalImpact:      42.5,
				})

				return "anomaly-1"
			},
			verify: func(t *testing.T, b *ce.InMemoryBackend, id string) {
				t.Helper()

				anomalies, _ := b.GetAnomalies("", "", "", "", 0, "")
				require.Len(t, anomalies, 1)
				assert.Equal(t, id, anomalies[0].AnomalyID)
				assert.InDelta(t, 42.5, anomalies[0].TotalImpact, 0.0001)
			},
		},
		{
			name: "cost_allocation_tag_round_trip",
			setup: func(b *ce.InMemoryBackend) string {
				b.UpdateCostAllocationTagsStatus([]ce.CostAllocationTagStatusEntry{
					{TagKey: "env", Status: "Active"},
				})

				return "env"
			},
			verify: func(t *testing.T, b *ce.InMemoryBackend, id string) {
				t.Helper()

				tags := b.ListCostAllocationTags("", "", nil)
				require.Len(t, tags, 1)
				assert.Equal(t, id, tags[0].TagKey)
				assert.Equal(t, "Active", tags[0].Status)
			},
		},
		{
			name: "commitment_analysis_round_trip",
			setup: func(b *ce.InMemoryBackend) string {
				a := b.CreateCommitmentAnalysis()

				return a.AnalysisID
			},
			verify: func(t *testing.T, b *ce.InMemoryBackend, id string) {
				t.Helper()

				a, err := b.GetCommitmentAnalysis(id)
				require.NoError(t, err)
				assert.Equal(t, "PROCESSING", a.AnalysisStatus)
			},
		},
		{
			name: "backfill_job_round_trip",
			setup: func(b *ce.InMemoryBackend) string {
				job := b.CreateBackfillJob("2024-01-01T00:00:00Z")

				return job.BackfillFrom
			},
			verify: func(t *testing.T, b *ce.InMemoryBackend, id string) {
				t.Helper()

				jobs := b.ListBackfillHistory()
				require.Len(t, jobs, 1)
				assert.Equal(t, id, jobs[0].BackfillFrom)
				assert.Equal(t, "PROCESSING", jobs[0].BackfillStatus)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *ce.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *ce.InMemoryBackend, _ string) {
				t.Helper()

				cats, _ := b.ListCostCategoryDefinitions(0, "")
				assert.Empty(t, cats)
				monitors, _, err := b.GetAnomalyMonitors(nil, 0, "")
				require.NoError(t, err)
				assert.Empty(t, monitors)
				subs, _, err := b.GetAnomalySubscriptions(nil, "", 0, "")
				require.NoError(t, err)
				assert.Empty(t, subs)
				anomalies, _ := b.GetAnomalies("", "", "", "", 0, "")
				assert.Empty(t, anomalies)
				assert.Empty(t, b.ListCostAllocationTags("", "", nil))
				assert.Empty(t, b.ListCommitmentAnalyses())
				assert.Empty(t, b.ListBackfillHistory())
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

// TestInMemoryBackend_FullStateSnapshotRestore populates every store.Table
// (costCategories, anomalyMonitors, anomalySubscriptions, anomalies,
// costAllocationTags, commitmentAnalyses) plus the raw-left backfillJobs
// slice in one backend, then verifies a Snapshot/Restore round trip
// reproduces every one of them in a fresh backend.
func TestInMemoryBackend_FullStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	original := ce.NewInMemoryBackend("000000000000", "us-east-1")

	cat, err := original.CreateCostCategoryDefinition(
		"FullCat", "CostCategoryExpression.v1", "INHERITED_VALUE",
		[]ce.CostCategoryRule{{Value: "Engineering"}}, nil,
	)
	require.NoError(t, err)

	mon, err := original.CreateAnomalyMonitor("FullMonitor", "DIMENSIONAL", "SERVICE", nil)
	require.NoError(t, err)

	sub, err := original.CreateAnomalySubscription(
		"FullSub", "DAILY",
		[]string{mon.MonitorARN},
		[]ce.Subscriber{{Address: "full@example.com", Type: "EMAIL", Status: "CONFIRMED"}},
		10.0, nil,
	)
	require.NoError(t, err)

	original.AddAnomaly(ce.Anomaly{AnomalyID: "full-anomaly", MonitorARN: mon.MonitorARN})
	original.UpdateCostAllocationTagsStatus([]ce.CostAllocationTagStatusEntry{
		{TagKey: "team", Status: "Active"},
	})

	analysis := original.CreateCommitmentAnalysis()
	job := original.CreateBackfillJob("2024-02-01T00:00:00Z")

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ce.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	gotCat, err := fresh.DescribeCostCategoryDefinition(cat.ARN)
	require.NoError(t, err)
	assert.Equal(t, "FullCat", gotCat.Name)

	monitors, _, err := fresh.GetAnomalyMonitors([]string{mon.MonitorARN}, 0, "")
	require.NoError(t, err)
	require.Len(t, monitors, 1)
	assert.Equal(t, "FullMonitor", monitors[0].MonitorName)

	subs, _, err := fresh.GetAnomalySubscriptions([]string{sub.SubscriptionARN}, "", 0, "")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, "FullSub", subs[0].SubscriptionName)

	anomalies, _ := fresh.GetAnomalies("", "", "", "", 0, "")
	require.Len(t, anomalies, 1)
	assert.Equal(t, "full-anomaly", anomalies[0].AnomalyID)

	tags := fresh.ListCostAllocationTags("", "", nil)
	require.Len(t, tags, 1)
	assert.Equal(t, "team", tags[0].TagKey)

	gotAnalysis, err := fresh.GetCommitmentAnalysis(analysis.AnalysisID)
	require.NoError(t, err)
	assert.Equal(t, analysis.AnalysisID, gotAnalysis.AnalysisID)

	jobs := fresh.ListBackfillHistory()
	require.Len(t, jobs, 1)
	assert.Equal(t, job.BackfillFrom, jobs[0].BackfillFrom)
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
	monitors, _, err := b.GetAnomalyMonitors(nil, 0, "")
	require.NoError(t, err)
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

	monitors, _, err := fresh.GetAnomalyMonitors(nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, monitors, 1)
	assert.Equal(t, "snap-mon", monitors[0].MonitorName)
}
