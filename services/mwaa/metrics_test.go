package mwaa_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestBackend_PublishMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *mwaa.ExportedPublishMetricsRequest
		name    string
		envName string
		seed    bool
		wantErr bool
	}{
		{
			name:    "success",
			envName: "metrics-env",
			seed:    true,
			req: &mwaa.ExportedPublishMetricsRequest{
				MetricData: []mwaa.ExportedMetricDatum{
					{MetricName: "TaskInstance"},
				},
			},
		},
		{
			name:    "empty_metrics",
			envName: "metrics-env-empty",
			seed:    true,
			req:     &mwaa.ExportedPublishMetricsRequest{MetricData: []mwaa.ExportedMetricDatum{}},
		},
		{
			name:    "env_not_found",
			envName: "nonexistent",
			seed:    false,
			req:     &mwaa.ExportedPublishMetricsRequest{MetricData: []mwaa.ExportedMetricDatum{}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.seed {
				_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
				require.NoError(t, err)
			}

			err := b.PublishMetrics(context.Background(), tt.envName, tt.req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestPublishMetrics_EnvNotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	err := b.PublishMetrics(context.Background(), "nonexistent", &mwaa.ExportedPublishMetricsRequest{})

	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

func TestGetMetrics_IsolatedBetweenEnvironments(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	for _, name := range []string{"metrics-env-a", "metrics-env-b"} {
		_, err := b.CreateEnvironment(context.Background(), name, newCreateReq())
		require.NoError(t, err)
	}

	err := b.PublishMetrics(context.Background(), "metrics-env-a", &mwaa.ExportedPublishMetricsRequest{
		MetricData: []mwaa.ExportedMetricDatum{
			{MetricName: "OnlyForA"},
		},
	})
	require.NoError(t, err)

	dataB, err := b.GetMetrics(context.Background(), "metrics-env-b")
	require.NoError(t, err)
	assert.Empty(t, dataB, "metrics for env-b must not contain env-a's metrics")
}

func TestGetMetrics_EmptyBeforePublish(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "no-metrics-env", newCreateReq())
	require.NoError(t, err)

	data, err := b.GetMetrics(context.Background(), "no-metrics-env")
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestMetrics_Cap_AtExactLimit(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "metrics-cap-env", newCreateReq())
	require.NoError(t, err)

	// Publish exactly 1000 metrics.
	data := make([]mwaa.ExportedMetricDatum, 1000)
	for i := range data {
		data[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("Metric%d", i)}
	}
	err = b.PublishMetrics(
		context.Background(),
		"metrics-cap-env",
		&mwaa.ExportedPublishMetricsRequest{MetricData: data},
	)
	require.NoError(t, err)

	assert.Equal(t, 1000, mwaa.MetricsCount(b, "metrics-cap-env"))
}

func TestMetrics_Cap_ExceedsLimit_TrimsOldest(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "metrics-overflow-env", newCreateReq())
	require.NoError(t, err)

	// Publish 1100 metrics in two batches.
	first := make([]mwaa.ExportedMetricDatum, 600)
	for i := range first {
		first[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("Old%d", i)}
	}
	err = b.PublishMetrics(
		context.Background(),
		"metrics-overflow-env",
		&mwaa.ExportedPublishMetricsRequest{MetricData: first},
	)
	require.NoError(t, err)

	second := make([]mwaa.ExportedMetricDatum, 500)
	for i := range second {
		second[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("New%d", i)}
	}
	err = b.PublishMetrics(
		context.Background(),
		"metrics-overflow-env",
		&mwaa.ExportedPublishMetricsRequest{MetricData: second},
	)
	require.NoError(t, err)

	// Total 1100 → capped at 1000.
	assert.Equal(t, 1000, mwaa.MetricsCount(b, "metrics-overflow-env"))
}

func TestMetrics_Cap_PublishSingleBatch_Over1000(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "metrics-big-batch", newCreateReq())
	require.NoError(t, err)

	data := make([]mwaa.ExportedMetricDatum, 1200)
	for i := range data {
		data[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("Datum%d", i)}
	}
	err = b.PublishMetrics(
		context.Background(),
		"metrics-big-batch",
		&mwaa.ExportedPublishMetricsRequest{MetricData: data},
	)
	require.NoError(t, err)

	// Capped at 1000.
	assert.Equal(t, 1000, mwaa.MetricsCount(b, "metrics-big-batch"))
}

// ─────────────────────────────────────────────────────────────
// 11. PublishMetrics datum field coverage
// ─────────────────────────────────────────────────────────────

func TestPublishMetrics_DatumFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		datums []mwaa.ExportedMetricDatum
	}{
		{
			name: "metric_with_value_and_unit",
			datums: func() []mwaa.ExportedMetricDatum {
				v := 42.5

				return []mwaa.ExportedMetricDatum{{MetricName: "WorkerCount", Value: &v, Unit: "Count"}}
			}(),
		},
		{
			name: "metric_with_statistic_set",
			datums: func() []mwaa.ExportedMetricDatum {
				maxV, minV, sum := 10.0, 1.0, 55.0
				sampleCount := int32(10)

				return []mwaa.ExportedMetricDatum{{
					MetricName: "TaskDuration",
					StatisticValues: &mwaa.StatisticSet{
						Maximum: &maxV, Minimum: &minV, Sum: &sum, SampleCount: &sampleCount,
					},
				}}
			}(),
		},
		{
			name: "metric_with_dimensions",
			datums: []mwaa.ExportedMetricDatum{{
				MetricName: "SchedulerHeartbeat",
				Dimensions: []mwaa.Dimension{
					{Name: "Environment", Value: "prod"},
					{Name: "Region", Value: "us-east-1"},
				},
			}},
		},
		{
			name: "metric_with_timestamp",
			datums: func() []mwaa.ExportedMetricDatum {
				ts := float64(1700000000)

				return []mwaa.ExportedMetricDatum{{MetricName: "DagRunCount", Timestamp: &ts}}
			}(),
		},
		{
			name:   "empty_metric_data_ok",
			datums: []mwaa.ExportedMetricDatum{},
		},
		{
			name: "multiple_metrics",
			datums: func() []mwaa.ExportedMetricDatum {
				v1, v2 := 1.0, 2.0

				return []mwaa.ExportedMetricDatum{
					{MetricName: "M1", Value: &v1},
					{MetricName: "M2", Value: &v2, Unit: "Percent"},
				}
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "datum-env", newCreateReq())
			require.NoError(t, err)

			err = b.PublishMetrics(
				context.Background(),
				"datum-env",
				&mwaa.ExportedPublishMetricsRequest{MetricData: tt.datums},
			)
			require.NoError(t, err)
		})
	}
}

func TestPublishMetrics_NotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	err := b.PublishMetrics(context.Background(), "nonexistent-env", &mwaa.ExportedPublishMetricsRequest{})
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

func TestGetMetrics_ReturnsCopy(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "get-metrics-copy-env", newCreateReq())
	require.NoError(t, err)

	v := 5.0
	err = b.PublishMetrics(context.Background(), "get-metrics-copy-env", &mwaa.ExportedPublishMetricsRequest{
		MetricData: []mwaa.ExportedMetricDatum{{MetricName: "TaskCount", Value: &v}},
	})
	require.NoError(t, err)

	data, err := b.GetMetrics(context.Background(), "get-metrics-copy-env")
	require.NoError(t, err)
	assert.Len(t, data, 1)
	assert.Equal(t, "TaskCount", data[0].MetricName)
}

// ─────────────────────────────────────────────────────────────
// 12. InvokeRestApi path / method / body / query variations
// ─────────────────────────────────────────────────────────────

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
			_, err := b.CreateEnvironment(context.Background(), "leak-env", newCreateReq())
			require.NoError(t, err)

			data := make([]mwaa.ExportedMetricDatum, tc.publish)
			for i := range data {
				data[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("M%d", i)}
			}
			require.NoError(t, b.PublishMetrics(context.Background(), "leak-env",
				&mwaa.ExportedPublishMetricsRequest{MetricData: data}))

			// len is capped...
			assert.Equal(t, 1000, mwaa.MetricsCount(b, "leak-env"))
			// ...and the backing array is right-sized, not the oversized original.
			assert.Equal(t, 1000, mwaa.MetricsCapacity(b, "leak-env"),
				"backing array must not retain trimmed-off capacity")
		})
	}
}

func TestReset_ClearsMetrics(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "reset-metrics-env", newCreateReq())
	require.NoError(t, err)

	v := 1.0
	err = b.PublishMetrics(context.Background(), "reset-metrics-env", &mwaa.ExportedPublishMetricsRequest{
		MetricData: []mwaa.ExportedMetricDatum{{MetricName: "M", Value: &v}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.MetricsCount(b, "reset-metrics-env"))

	b.Reset()
	assert.Equal(t, 0, mwaa.EnvironmentCount(b))
	assert.Equal(t, 0, mwaa.MetricsCount(b, "reset-metrics-env"))
}
