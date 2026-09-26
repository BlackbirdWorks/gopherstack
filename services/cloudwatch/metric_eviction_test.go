package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestSweepExpiredMetrics_KeyEviction covers gopherstack-4thzo: a metric
// series with no new datapoints must be evicted entirely (not just
// point-trimmed) once its last datapoint falls outside
// cwListMetricsVisibilityWindow, matching aws-sdk-go-v2 service/cloudwatch
// api_op_ListMetrics.go's doc comment: "ListMetrics doesn't return
// information about metrics if those metrics haven't reported data in the
// past two weeks." The filter applies on every ListMetrics call, independent
// of whether SweepExpiredMetrics has run yet.
func TestSweepExpiredMetrics_KeyEviction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		age       time.Duration
		wantAlive bool
	}{
		{
			name:      "key within window stays",
			age:       cloudwatch.CwListMetricsVisibilityWindow - time.Hour,
			wantAlive: true,
		},
		{
			name:      "key past window evicted and delisted",
			age:       cloudwatch.CwListMetricsVisibilityWindow + time.Hour,
			wantAlive: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			ts := time.Now().UTC().Add(-tc.age)

			// ts may predate PutMetricData's write-time acceptance window (two
			// weeks), so seed it directly via StoreDatumForTest: a datum that
			// was valid when written ages out of ListMetrics visibility the
			// same way regardless of how it was stored.
			b.StoreDatumForTest("NS/Evict", cloudwatch.MetricDatum{
				MetricName: "M", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts,
			})

			// ListMetrics must already reflect visibility before any sweep runs.
			before, err := b.ListMetrics("NS/Evict", "M", nil, "", "", 0)
			require.NoError(t, err)
			assert.Equal(t, tc.wantAlive, len(before.Data) == 1, "visibility before sweep")

			startCount := b.TotalMetricsForTest()

			b.SweepExpiredMetrics()

			after, err := b.ListMetrics("NS/Evict", "M", nil, "", "", 0)
			require.NoError(t, err)
			assert.Equal(t, tc.wantAlive, len(after.Data) == 1, "visibility after sweep")

			if tc.wantAlive {
				assert.Equal(t, startCount, b.TotalMetricsForTest(), "live key must not be evicted")
			} else {
				assert.Equal(t, startCount-1, b.TotalMetricsForTest(), "stale key must be evicted from the map")
			}
		})
	}
}

// TestSweepExpiredMetrics_NewDatapointResetsTimer verifies that a key which
// keeps receiving new datapoints never goes stale, even though it also holds
// points older than the visibility window.
func TestSweepExpiredMetrics_NewDatapointResetsTimer(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	old := time.Now().UTC().Add(-(cloudwatch.CwListMetricsVisibilityWindow + time.Hour))
	b.StoreDatumForTest("NS/Reset", cloudwatch.MetricDatum{
		MetricName: "M", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: old,
	})

	fresh := time.Now().UTC()
	require.NoError(t, b.PutMetricData("NS/Reset", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: fresh},
	}))

	b.SweepExpiredMetrics()

	p, err := b.ListMetrics("NS/Reset", "M", nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1, "a key with a recent datapoint must survive even with old points still on it")
}

// TestEvaluateAlarms_EvictedMetricReportsInsufficientData verifies that once a
// metric series is evicted by SweepExpiredMetrics, an alarm referencing it
// evaluates to INSUFFICIENT_DATA rather than erroring or getting stuck: real
// CloudWatch behaviour for a metric with no datapoints available.
func TestEvaluateAlarms_EvictedMetricReportsInsufficientData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	stale := now.Add(-(cloudwatch.CwListMetricsVisibilityWindow + time.Hour))
	b.StoreDatumForTest("NS/AlarmEvict", cloudwatch.MetricDatum{
		MetricName: "Requests", Value: 500, Count: 1, Sum: 500, Min: 500, Max: 500, Timestamp: stale,
	})

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "evicted-metric-alarm",
		Namespace:          "NS/AlarmEvict",
		MetricName:         "Requests",
		Statistic:          "Average",
		ComparisonOperator: "GreaterThanThreshold",
		EvaluationPeriods:  1,
		DatapointsToAlarm:  1,
		Period:             60,
		Threshold:          100,
		StateValue:         "ALARM",
		ActionsEnabled:     false,
	}))

	b.SweepExpiredMetrics()

	b.EvaluateAlarms(t.Context(), now)

	got := describeMetricAlarm(t, b, "evicted-metric-alarm")
	assert.Equal(t, "INSUFFICIENT_DATA", got.StateValue,
		"an alarm on an evicted metric series must read INSUFFICIENT_DATA")
}

// TestListMetrics_VisibilityWindow_RealClient is the typed-client assertion
// for gopherstack-4thzo: a real aws-sdk-go-v2 ListMetrics call must omit a
// metric whose last datapoint is older than the documented two-week
// visibility window, alongside one still within it.
func TestListMetrics_VisibilityWindow_RealClient(t *testing.T) {
	t.Parallel()

	client, backend := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	_, err := client.PutMetricData(ctx, &cwsdk.PutMetricDataInput{
		Namespace: aws.String("NS/RealVisibility"),
		MetricData: []cwtypes.MetricDatum{
			{MetricName: aws.String("Fresh"), Value: aws.Float64(1), Timestamp: aws.Time(time.Now().UTC())},
		},
	})
	require.NoError(t, err)

	// Seeded directly: predates PutMetricData's own write-time acceptance
	// window (two weeks), modeling a series that stopped reporting long ago.
	stale := time.Now().UTC().Add(-(cloudwatch.CwListMetricsVisibilityWindow + time.Hour))
	backend.StoreDatumForTest("NS/RealVisibility", cloudwatch.MetricDatum{
		MetricName: "Stale", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: stale,
	})

	out, err := client.ListMetrics(ctx, &cwsdk.ListMetricsInput{
		Namespace: aws.String("NS/RealVisibility"),
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.Metrics))
	for _, m := range out.Metrics {
		names = append(names, aws.ToString(m.MetricName))
	}

	assert.Contains(t, names, "Fresh", "recently reported metric must remain visible")
	assert.NotContains(t, names, "Stale", "metric past the two-week window must not be returned by ListMetrics")
}
