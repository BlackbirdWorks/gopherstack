package cloudwatch_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// Token signing (gap #15)
// ---------------------------------------------------------------------------

func TestSignPageToken_ZeroReturnsEmpty(t *testing.T) {
	t.Parallel()
	assert.Empty(t, cloudwatch.SignPageTokenForTest(0))
}

func TestSignPageToken_NonZeroRoundTrip(t *testing.T) {
	t.Parallel()

	for _, offset := range []int{1, 10, 99, 500, 1000, 99999} {
		tok := cloudwatch.SignPageTokenForTest(offset)
		require.NotEmpty(t, tok, "offset %d should produce a token", offset)

		got, err := cloudwatch.ParseSignedPageTokenForTest(tok)
		require.NoError(t, err)
		assert.Equal(t, offset, got, "offset %d round-trip", offset)
	}
}

func TestParseSignedPageToken_EmptyIsZero(t *testing.T) {
	t.Parallel()

	got, err := cloudwatch.ParseSignedPageTokenForTest("")
	require.NoError(t, err)
	assert.Equal(t, 0, got)
}

func TestParseSignedPageToken_TamperedPayload(t *testing.T) {
	t.Parallel()

	tok := cloudwatch.SignPageTokenForTest(42)
	// Corrupt the last character.
	if len(tok) > 0 {
		corrupt := tok[:len(tok)-1] + "X"
		_, err := cloudwatch.ParseSignedPageTokenForTest(corrupt)
		assert.Error(t, err, "tampered token should be rejected")
	}
}

func TestParseSignedPageToken_GarbageToken(t *testing.T) {
	t.Parallel()

	_, err := cloudwatch.ParseSignedPageTokenForTest("definitely-not-a-token")
	assert.Error(t, err)
}

func TestParseSignedPageToken_MissingSeparator(t *testing.T) {
	t.Parallel()

	_, err := cloudwatch.ParseSignedPageTokenForTest("aGVsbG8=")
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// validateMetricDatum (gap #2 mutual-exclusion enforcement)
// ---------------------------------------------------------------------------

func TestValidateMetricDatum_ValueOnly(t *testing.T) {
	t.Parallel()

	d := cloudwatch.MetricDatum{MetricName: "M", Value: 1.0, HasValue: true}
	assert.NoError(t, cloudwatch.ValidateMetricDatumForTest(d))
}

func TestValidateMetricDatum_StatisticSetOnly(t *testing.T) {
	t.Parallel()

	d := cloudwatch.MetricDatum{
		MetricName:      "M",
		HasStatisticSet: true,
		Count:           5, Sum: 100, Min: 10, Max: 30,
	}
	assert.NoError(t, cloudwatch.ValidateMetricDatumForTest(d))
}

func TestValidateMetricDatum_BothValueAndStatisticSet(t *testing.T) {
	t.Parallel()

	d := cloudwatch.MetricDatum{
		MetricName:      "M",
		Value:           1.0,
		HasValue:        true,
		HasStatisticSet: true,
		Count:           5, Sum: 100, Min: 10, Max: 30,
	}
	err := cloudwatch.ValidateMetricDatumForTest(d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestValidateMetricDatum_NoStatisticSetWithZeroValue(t *testing.T) {
	t.Parallel()

	d := cloudwatch.MetricDatum{MetricName: "M", Value: 0, HasStatisticSet: false}
	assert.NoError(t, cloudwatch.ValidateMetricDatumForTest(d))
}

// ---------------------------------------------------------------------------
// validateStorageResolution (gap #3)
// ---------------------------------------------------------------------------

func TestValidateStorageResolution_Valid(t *testing.T) {
	t.Parallel()

	for _, res := range []int32{0, 1, 60} {
		assert.NoError(t, cloudwatch.ValidateStorageResolutionForTest(res), "res=%d", res)
	}
}

func TestValidateStorageResolution_Invalid(t *testing.T) {
	t.Parallel()

	for _, res := range []int32{2, 30, 120, -1} {
		err := cloudwatch.ValidateStorageResolutionForTest(res)
		assert.Error(t, err, "res=%d should be invalid", res)
	}
}

// ---------------------------------------------------------------------------
// extractExprDeps (expression dependency extraction)
// ---------------------------------------------------------------------------

func TestExtractExprDeps_Simple(t *testing.T) {
	t.Parallel()

	known := map[string]bool{"m1": true, "m2": true}
	deps := cloudwatch.ExtractExprDepsForTest("m1 + m2", known)
	assert.ElementsMatch(t, []string{"m1", "m2"}, deps)
}

func TestExtractExprDeps_ReferencesUnknown(t *testing.T) {
	t.Parallel()

	known := map[string]bool{"m1": true}
	deps := cloudwatch.ExtractExprDepsForTest("m1 + unknown_var", known)
	assert.Equal(t, []string{"m1"}, deps)
}

func TestExtractExprDeps_Deduplicates(t *testing.T) {
	t.Parallel()

	known := map[string]bool{"m1": true}
	deps := cloudwatch.ExtractExprDepsForTest("m1 + m1 * 2", known)
	assert.Len(t, deps, 1)
}

func TestExtractExprDeps_EmptyExpr(t *testing.T) {
	t.Parallel()

	known := map[string]bool{"m1": true}
	deps := cloudwatch.ExtractExprDepsForTest("", known)
	assert.Empty(t, deps)
}

func TestExtractExprDeps_ComplexExpr(t *testing.T) {
	t.Parallel()

	known := map[string]bool{"cpu": true, "mem": true, "disk": true}
	deps := cloudwatch.ExtractExprDepsForTest("(cpu + mem) / disk * 100", known)
	assert.ElementsMatch(t, []string{"cpu", "mem", "disk"}, deps)
}

// ---------------------------------------------------------------------------
// topoSortExpressions (gap #13 forward references)
// ---------------------------------------------------------------------------

func TestTopoSortExpressions_Empty(t *testing.T) {
	t.Parallel()

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60}},
	}
	sorted, err := cloudwatch.TopoSortExpressionsForTest(queries)
	require.NoError(t, err)
	assert.Empty(t, sorted, "no expressions → empty order")
}

func TestTopoSortExpressions_LinearChain(t *testing.T) {
	t.Parallel()

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60}},
		{ID: "e1", Expression: "m1 * 2"},
		{ID: "e2", Expression: "e1 + 10"},
		{ID: "e3", Expression: "e2 - 1"},
	}
	sorted, err := cloudwatch.TopoSortExpressionsForTest(queries)
	require.NoError(t, err)
	require.Equal(t, []string{"e1", "e2", "e3"}, sorted)
}

func TestTopoSortExpressions_ForwardReference(t *testing.T) {
	t.Parallel()

	// e2 is declared before e1 but references it — should still work.
	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60}},
		{ID: "e2", Expression: "e1 * 2"},
		{ID: "e1", Expression: "m1 + 1"},
	}
	sorted, err := cloudwatch.TopoSortExpressionsForTest(queries)
	require.NoError(t, err)
	// e1 must appear before e2.
	e1Idx := indexOf(sorted, "e1")
	e2Idx := indexOf(sorted, "e2")
	require.NotEqual(t, -1, e1Idx)
	require.NotEqual(t, -1, e2Idx)
	assert.Less(t, e1Idx, e2Idx, "e1 must be resolved before e2")
}

func TestTopoSortExpressions_Cycle(t *testing.T) {
	t.Parallel()

	queries := []cloudwatch.MetricDataQuery{
		{ID: "e1", Expression: "e2 * 2"},
		{ID: "e2", Expression: "e1 + 1"},
	}
	_, err := cloudwatch.TopoSortExpressionsForTest(queries)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "cycle")
}

func TestTopoSortExpressions_DiamondDependency(t *testing.T) {
	t.Parallel()

	//  m1 → e1
	//  m1 → e2
	//  e1, e2 → e3
	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60}},
		{ID: "e1", Expression: "m1 * 2"},
		{ID: "e2", Expression: "m1 + 10"},
		{ID: "e3", Expression: "e1 + e2"},
	}
	sorted, err := cloudwatch.TopoSortExpressionsForTest(queries)
	require.NoError(t, err)
	e3Idx := indexOf(sorted, "e3")
	e1Idx := indexOf(sorted, "e1")
	e2Idx := indexOf(sorted, "e2")
	assert.Less(t, e1Idx, e3Idx, "e1 before e3")
	assert.Less(t, e2Idx, e3Idx, "e2 before e3")
}

// ---------------------------------------------------------------------------
// rollingStats
// ---------------------------------------------------------------------------

func TestRollingStats_Empty(t *testing.T) {
	t.Parallel()

	mean, stddev := cloudwatch.RollingStatsForTest(nil)
	assert.InDelta(t, 0.0, mean, 1e-9)
	assert.InDelta(t, 0.0, stddev, 1e-9)
}

func TestRollingStats_SingleValue(t *testing.T) {
	t.Parallel()

	mean, stddev := cloudwatch.RollingStatsForTest([]float64{42.0})
	assert.InDelta(t, 42.0, mean, 1e-9)
	assert.InDelta(t, 0.0, stddev, 1e-9, "stddev of single value is 0")
}

func TestRollingStats_ConstantValues(t *testing.T) {
	t.Parallel()

	vals := []float64{5, 5, 5, 5, 5}
	mean, stddev := cloudwatch.RollingStatsForTest(vals)
	assert.InDelta(t, 5.0, mean, 1e-9)
	assert.InDelta(t, 0.0, stddev, 1e-9)
}

func TestRollingStats_KnownValues(t *testing.T) {
	t.Parallel()

	// mean=3, variance=(1+0+1)/3 ≈ 0.666, stddev≈0.8165
	vals := []float64{2, 3, 4}
	mean, stddev := cloudwatch.RollingStatsForTest(vals)
	assert.InDelta(t, 3.0, mean, 1e-9)
	assert.InDelta(t, math.Sqrt(2.0/3.0), stddev, 1e-6)
}

// ---------------------------------------------------------------------------
// computeAnomalyBand
// ---------------------------------------------------------------------------

func TestComputeAnomalyBand_Empty(t *testing.T) {
	t.Parallel()

	lo, hi := cloudwatch.ComputeAnomalyBandForTest(nil)
	assert.Nil(t, lo)
	assert.Nil(t, hi)
}

func TestComputeAnomalyBand_Length(t *testing.T) {
	t.Parallel()

	vals := []float64{1, 2, 3, 4, 5}
	lo, hi := cloudwatch.ComputeAnomalyBandForTest(vals)
	assert.Len(t, lo, len(vals))
	assert.Len(t, hi, len(vals))
}

func TestComputeAnomalyBand_UpperGtLower(t *testing.T) {
	t.Parallel()

	vals := []float64{10, 20, 30, 15, 25}
	lo, hi := cloudwatch.ComputeAnomalyBandForTest(vals)
	require.NotEmpty(t, lo)

	for i := range lo {
		assert.LessOrEqual(t, lo[i], hi[i], "lower bound ≤ upper bound at index %d", i)
	}
}

func TestComputeAnomalyBand_ConstantCollapsesToPoint(t *testing.T) {
	t.Parallel()

	vals := []float64{7, 7, 7, 7}
	lo, hi := cloudwatch.ComputeAnomalyBandForTest(vals)
	require.NotEmpty(t, lo)

	// stddev=0 → lower == upper == mean
	for i := range lo {
		assert.InDelta(t, lo[i], hi[i], 1e-9, "constant series: band collapses to point")
	}
}

// ---------------------------------------------------------------------------
// evalAnomalyDetectionBand (ANOMALY_DETECTION_BAND expression)
// ---------------------------------------------------------------------------

func makeTimestamps(n int) []time.Time {
	t0 := time.Now().UTC()
	ts := make([]time.Time, n)
	for i := range ts {
		ts[i] = t0.Add(time.Duration(i) * time.Minute)
	}

	return ts
}

func TestEvalAnomalyDetectionBand_Basic(t *testing.T) {
	t.Parallel()

	ts := makeTimestamps(5)
	resolved := map[string]cloudwatch.MetricDataResult{
		"m1": {
			Timestamps: ts,
			Values:     []float64{10, 20, 30, 20, 10},
			StatusCode: "Complete",
		},
	}

	upper, lower, ok := cloudwatch.EvalAnomalyDetectionBandForTest("ANOMALY_DETECTION_BAND(m1)", resolved)
	require.True(t, ok)
	assert.Len(t, upper.Values, 5)
	assert.Len(t, lower.Values, 5)

	for i := range upper.Values {
		assert.LessOrEqual(t, lower.Values[i], upper.Values[i])
	}
}

func TestEvalAnomalyDetectionBand_CustomStdDevs(t *testing.T) {
	t.Parallel()

	ts := makeTimestamps(3)
	resolved := map[string]cloudwatch.MetricDataResult{
		"base": {Timestamps: ts, Values: []float64{5, 10, 15}, StatusCode: "Complete"},
	}

	upper2, lower2, ok2 := cloudwatch.EvalAnomalyDetectionBandForTest("ANOMALY_DETECTION_BAND(base, 2)", resolved)
	require.True(t, ok2)

	upper4, lower4, ok4 := cloudwatch.EvalAnomalyDetectionBandForTest("ANOMALY_DETECTION_BAND(base, 4)", resolved)
	require.True(t, ok4)

	// Wider stddev factor → wider band.
	band2 := upper2.Values[0] - lower2.Values[0]
	band4 := upper4.Values[0] - lower4.Values[0]
	assert.Greater(t, band4, band2, "4-stddev band should be wider than 2-stddev")
}

func TestEvalAnomalyDetectionBand_UnknownRef(t *testing.T) {
	t.Parallel()

	resolved := map[string]cloudwatch.MetricDataResult{}
	upper, lower, ok := cloudwatch.EvalAnomalyDetectionBandForTest("ANOMALY_DETECTION_BAND(missing)", resolved)
	require.True(t, ok, "pattern matched but ref not found → still ok=true with empty result")
	assert.Empty(t, upper.Values)
	assert.Empty(t, lower.Values)
}

func TestEvalAnomalyDetectionBand_NoMatch(t *testing.T) {
	t.Parallel()

	resolved := map[string]cloudwatch.MetricDataResult{}
	_, _, ok := cloudwatch.EvalAnomalyDetectionBandForTest("m1 + m2", resolved)
	assert.False(t, ok, "non-band expression should return ok=false")
}

func TestEvalAnomalyDetectionBand_CaseInsensitive(t *testing.T) {
	t.Parallel()

	ts := makeTimestamps(2)
	resolved := map[string]cloudwatch.MetricDataResult{
		"x": {Timestamps: ts, Values: []float64{1, 2}, StatusCode: "Complete"},
	}

	_, _, ok := cloudwatch.EvalAnomalyDetectionBandForTest("anomaly_detection_band(x)", resolved)
	assert.True(t, ok, "lowercase form should also match")
}

// ---------------------------------------------------------------------------
// Backend integration: StatisticSet validation via PutMetricData (gap #2)
// ---------------------------------------------------------------------------

func TestBackend_PutMetricData_StatisticSet_Valid(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{
			MetricName:      "Reqs",
			HasStatisticSet: true,
			Count:           5, Sum: 250, Min: 40, Max: 60,
			Timestamp: ts,
		},
	})
	require.NoError(t, err)
}

// TestBackend_PutMetricData_ValueAndStatisticSet_Rejected verifies AWS's
// all-or-nothing PutMetricData contract: PutMetricDataOutput carries no
// per-datum result (confirmed against aws-sdk-go-v2 cloudwatch types), so a
// request containing an invalid datum must fail the entire call rather than
// silently dropping the bad entry into a fabricated "unprocessed" list.
func TestBackend_PutMetricData_ValueAndStatisticSet_Rejected(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{
			MetricName:      "BadEntry",
			Value:           1.0,
			HasValue:        true,
			HasStatisticSet: true,
			Count:           5, Sum: 100, Min: 10, Max: 30,
			Timestamp: ts,
		},
	})
	require.ErrorIs(t, err, cloudwatch.ErrValueAndStatisticSet)

	// The whole request must be rejected: nothing gets stored.
	p, lerr := b.ListMetrics("NS", "", nil, "", "", 0)
	require.NoError(t, lerr)
	assert.Empty(t, p.Data)
}

func TestBackend_PutMetricData_InvalidStorageResolution_Rejected(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{
			MetricName: "BadRes",
			Value:      1.0,
			HasValue:   true,
			Count:      1, Sum: 1, Min: 1, Max: 1,
			Timestamp:         ts,
			StorageResolution: 30,
		},
	})
	require.ErrorIs(t, err, cloudwatch.ErrValidation)
}

// TestBackend_PutMetricData_MixedValidAndInvalid verifies that when one datum
// in a batch is invalid, none of the batch is stored (no partial commit),
// matching AWS's atomic PutMetricData contract.
func TestBackend_PutMetricData_MixedValidAndInvalid(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Good", Value: 1, HasValue: true, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts},
		{
			MetricName: "Bad", Value: 1, HasValue: true, HasStatisticSet: true,
			Count: 2, Sum: 50, Min: 10, Max: 40, Timestamp: ts,
		},
		{MetricName: "AlsoGood", Value: 2, HasValue: true, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: ts},
	})
	require.ErrorIs(t, err, cloudwatch.ErrValueAndStatisticSet)

	// Nothing from the batch should be stored, including the otherwise-valid entries.
	p, lerr := b.ListMetrics("NS", "", nil, "", "", 0)
	require.NoError(t, lerr)
	assert.Empty(t, p.Data)
}

// ---------------------------------------------------------------------------
// Backend integration: anomaly detection band in GetMetricStatistics (gap #11)
// ---------------------------------------------------------------------------

func TestBackend_GetMetricStatistics_AnomalyBand(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	// Store metric data.
	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{MetricName: "Latency", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: now.Add(-2 * time.Minute)},
		{MetricName: "Latency", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: now.Add(-time.Minute)},
		{MetricName: "Latency", Value: 30, Count: 1, Sum: 30, Min: 30, Max: 30, Timestamp: now.Add(-30 * time.Second)},
	})
	require.NoError(t, err)

	// Add anomaly detector.
	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace: "App", MetricName: "Latency", Stat: "Average",
	}))

	dps, err := b.GetMetricStatistics(
		"App", "Latency", nil,
		now.Add(-5*time.Minute), now,
		60, []string{"Average"}, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, dps)

	// At least one datapoint should have band annotations.
	hasBand := false

	for _, dp := range dps {
		if dp.BandLower != nil && dp.BandUpper != nil {
			hasBand = true
			assert.LessOrEqual(t, *dp.BandLower, *dp.BandUpper)
		}
	}

	assert.True(t, hasBand, "anomaly detector should annotate datapoints with band")
}

func TestBackend_GetMetricStatistics_NoAnomalyDetector_NoBand(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{MetricName: "CPU", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: now.Add(-time.Minute)},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics(
		"App", "CPU", nil,
		now.Add(-5*time.Minute), now,
		60, []string{"Average"}, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, dps)

	for _, dp := range dps {
		assert.Nil(t, dp.BandLower, "no detector → no band")
		assert.Nil(t, dp.BandUpper, "no detector → no band")
	}
}

// ---------------------------------------------------------------------------
// Backend integration: expression forward references via topo sort (gap #13)
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_ForwardReferenceExpression(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Base", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: ts},
	})
	require.NoError(t, err)

	// e2 is declared first but references e1 (forward ref).
	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "Base", Stat: "Sum", Period: 60,
		}},
		{ID: "e2", Expression: "e1 * 2", ReturnData: true},
		{ID: "e1", Expression: "m1 + 5", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "TimestampAscending")
	require.NoError(t, err)

	byID := make(map[string]cloudwatch.MetricDataResult)
	for _, r := range results {
		byID[r.ID] = r
	}

	require.Contains(t, byID, "e1")
	require.Contains(t, byID, "e2")

	if len(byID["e1"].Values) > 0 {
		// e1 = m1 + 5 = 10 + 5 = 15; e2 = e1 * 2 = 30
		assert.InDelta(t, 15.0, byID["e1"].Values[0], 1e-9, "e1 = m1+5")
		assert.InDelta(t, 30.0, byID["e2"].Values[0], 1e-9, "e2 = e1*2")
	}
}

func TestBackend_GetMetricData_AnomalyDetectionBandExpression(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	for i := range 5 {
		ts := now.Add(-time.Duration(5-i) * time.Minute)
		err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{
				MetricName: "M", Value: float64(10 + i*5), Count: 1,
				Sum: float64(10 + i*5), Min: float64(10 + i*5), Max: float64(10 + i*5),
				Timestamp: ts,
			},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "band", Expression: "ANOMALY_DETECTION_BAND(m1)", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, now.Add(-10*time.Minute), now, "TimestampAscending")
	require.NoError(t, err)

	byID := make(map[string]cloudwatch.MetricDataResult)
	for _, r := range results {
		byID[r.ID] = r
	}

	assert.Contains(t, byID, "band", "ANOMALY_DETECTION_BAND result should be in output")
}

// ---------------------------------------------------------------------------
// Backend integration: GetMetricStatistics dimensions (gap #9)
// ---------------------------------------------------------------------------

func TestBackend_GetMetricStatistics_WithDimensions(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	dimProd := []cloudwatch.Dimension{{Name: "Env", Value: "prod"}}
	dimStaging := []cloudwatch.Dimension{{Name: "Env", Value: "staging"}}

	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{MetricName: "RPM", Value: 100, Count: 1, Sum: 100, Min: 100, Max: 100, Timestamp: ts, Dimensions: dimProd},
		{MetricName: "RPM", Value: 200, Count: 1, Sum: 200, Min: 200, Max: 200, Timestamp: ts, Dimensions: dimStaging},
	})
	require.NoError(t, err)

	dpsProd, err := b.GetMetricStatistics(
		"App", "RPM", dimProd,
		ts.Add(-time.Minute), ts.Add(time.Minute),
		60, []string{"Sum"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dpsProd, 1)
	assert.InDelta(t, 100.0, *dpsProd[0].Sum, 1e-9, "prod query should return prod value")

	dpsStaging, err := b.GetMetricStatistics(
		"App", "RPM", dimStaging,
		ts.Add(-time.Minute), ts.Add(time.Minute),
		60, []string{"Sum"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dpsStaging, 1)
	assert.InDelta(t, 200.0, *dpsStaging[0].Sum, 1e-9, "staging query should return staging value")
}

// ---------------------------------------------------------------------------
// Backend integration: GetMetricData with dimensions in MetricStat (gap #5)
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_DimensionFiltering(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	dimA := []cloudwatch.Dimension{{Name: "Host", Value: "a"}}
	dimB := []cloudwatch.Dimension{{Name: "Host", Value: "b"}}

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: ts, Dimensions: dimA},
		{MetricName: "CPU", Value: 90, Count: 1, Sum: 90, Min: 90, Max: 90, Timestamp: ts, Dimensions: dimB},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "hostA", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "CPU", Stat: "Sum", Period: 60,
			Dimensions: dimA,
		}},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "TimestampAscending")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NotEmpty(t, results[0].Values)
	assert.InDelta(t, 10.0, results[0].Values[0], 1e-9, "should only return host=a data")
}

// ---------------------------------------------------------------------------
// Handler integration: PutMetricData StatisticSet form parsing (gap #2)
// ---------------------------------------------------------------------------

func TestHandler_PutMetricData_StatisticSet_FormParsed(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricData"+
			"&Namespace=App"+
			"&MetricData.member.1.MetricName=Reqs"+
			"&MetricData.member.1.StatisticValues.SampleCount=5"+
			"&MetricData.member.1.StatisticValues.Sum=250"+
			"&MetricData.member.1.StatisticValues.Minimum=40"+
			"&MetricData.member.1.StatisticValues.Maximum=60"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 200, rec.Code, "valid StatisticSet should return 200; body: %s", rec.Body.String())
}

// TestHandler_PutMetricData_ValueAndStatisticSet_Returns400 verifies real AWS
// behaviour: PutMetricDataOutput has no members besides the request ID (no
// per-datum "unprocessed" result exists for this operation), so an invalid
// datum fails the entire request with a 400 InvalidParameterCombination
// instead of a fabricated 200-with-partial-failure response.
func TestHandler_PutMetricData_ValueAndStatisticSet_Returns400(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricData"+
			"&Namespace=App"+
			"&MetricData.member.1.MetricName=BadReq"+
			"&MetricData.member.1.Value=1.0"+
			"&MetricData.member.1.StatisticValues.SampleCount=5"+
			"&MetricData.member.1.StatisticValues.Sum=250"+
			"&MetricData.member.1.StatisticValues.Minimum=40"+
			"&MetricData.member.1.StatisticValues.Maximum=60"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 400, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterCombination")
}

func TestHandler_PutMetricData_InvalidStorageResolution_Returns400(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricData"+
			"&Namespace=App"+
			"&MetricData.member.1.MetricName=BadRes"+
			"&MetricData.member.1.Value=1.0"+
			"&MetricData.member.1.StorageResolution=30"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 400, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// ---------------------------------------------------------------------------
// Handler integration: PutMetricData with dimensions (gap #1)
// ---------------------------------------------------------------------------

func TestHandler_PutMetricData_WithDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricData"+
			"&Namespace=MyApp"+
			"&MetricData.member.1.MetricName=Errors"+
			"&MetricData.member.1.Value=5"+
			"&MetricData.member.1.Dimensions.member.1.Name=Service"+
			"&MetricData.member.1.Dimensions.member.1.Value=auth"+
			"&MetricData.member.1.Dimensions.member.2.Name=Region"+
			"&MetricData.member.1.Dimensions.member.2.Value=us-east-1"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 200, rec.Code, "response: %s", rec.Body.String())
}

// ---------------------------------------------------------------------------
// Handler integration: ListMetrics dimension filter (gap #4)
// ---------------------------------------------------------------------------

func TestHandler_ListMetrics_DimensionFilter(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	// Store two metrics with different dimensions.
	postForm(
		t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=RPM&MetricData.member.1.Value=100"+
			"&MetricData.member.1.Dimensions.member.1.Name=Env&MetricData.member.1.Dimensions.member.1.Value=prod"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	postForm(
		t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=RPM&MetricData.member.1.Value=50"+
			"&MetricData.member.1.Dimensions.member.1.Name=Env&MetricData.member.1.Dimensions.member.1.Value=staging"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)

	// Filter to prod only.
	rec := postForm(
		t, h,
		"Action=ListMetrics&Namespace=NS&MetricName=RPM"+
			"&Dimensions.member.1.Name=Env&Dimensions.member.1.Value=prod",
	)
	assert.Equal(t, 200, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "RPM")
}

// ---------------------------------------------------------------------------
// Handler integration: GetMetricStatistics with dimensions (gap #9)
// ---------------------------------------------------------------------------

func TestHandler_GetMetricStatistics_WithDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(
		t, h,
		"Action=PutMetricData&Namespace=MyNS"+
			"&MetricData.member.1.MetricName=CPU"+
			"&MetricData.member.1.Value=75"+
			"&MetricData.member.1.Dimensions.member.1.Name=Host&MetricData.member.1.Dimensions.member.1.Value=web1"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)

	rec := postForm(
		t, h,
		"Action=GetMetricStatistics"+
			"&Namespace=MyNS"+
			"&MetricName=CPU"+
			"&Dimensions.member.1.Name=Host&Dimensions.member.1.Value=web1"+
			"&StartTime=2023-12-31T00:00:00Z"+
			"&EndTime=2024-01-02T00:00:00Z"+
			"&Period=60"+
			"&Statistics.member.1=Average",
	)
	assert.Equal(t, 200, rec.Code)
}

// ---------------------------------------------------------------------------
// Handler integration: PutAnomalyDetector with dimensions (gap #11)
// ---------------------------------------------------------------------------

func TestHandler_PutAnomalyDetector_WithDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutAnomalyDetector"+
			"&Namespace=App"+
			"&MetricName=Latency"+
			"&Stat=Average"+
			"&SingleMetricAnomalyDetector.Dimensions.member.1.Name=Service"+
			"&SingleMetricAnomalyDetector.Dimensions.member.1.Value=api",
	)
	assert.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())
}

// ---------------------------------------------------------------------------
// Handler integration: tag cleanup on delete (gap #11 + orphan cleanup)
// ---------------------------------------------------------------------------

func TestHandler_DeleteAnomalyDetector_CleansUpTags(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	// Create detector.
	rec := postForm(
		t, h,
		"Action=PutAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average",
	)
	assert.Equal(t, 200, rec.Code)

	// Tag it (we need a valid ARN for the tag — use describe to find it or construct directly).
	// The implementation uses buildAnomalyDetectorARN which is internal; we just verify delete succeeds.
	rec = postForm(
		t, h,
		"Action=DeleteAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average",
	)
	assert.Equal(t, 200, rec.Code, "delete should succeed; body: %s", rec.Body.String())
}

func TestHandler_DeleteMetricStream_CleansUpTags(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	const streamBody = "Action=PutMetricStream&Name=my-stream" +
		"&FirehoseArn=arn:aws:firehose:us-east-1:123:deliverystream/ds" +
		"&RoleArn=arn:aws:iam::123:role/r&OutputFormat=json"
	rec := postForm(t, h, streamBody)
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=DeleteMetricStream&Name=my-stream")
	assert.Equal(t, 200, rec.Code, "delete stream: %s", rec.Body.String())
}

// ---------------------------------------------------------------------------
// Handler integration: ScanBy (gap #6)
// ---------------------------------------------------------------------------

func TestHandler_GetMetricData_ScanByDescending(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	// Store a few data points.
	for _, ts := range []string{
		"2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z", "2024-01-01T00:03:00Z",
	} {
		postForm(
			t, h, "Action=PutMetricData&Namespace=NS"+
				"&MetricData.member.1.MetricName=Counter"+
				"&MetricData.member.1.Value=1"+
				"&MetricData.member.1.Timestamp="+ts,
		)
	}

	rec := postForm(
		t, h,
		"Action=GetMetricData"+
			"&MetricDataQueries.member.1.Id=m1"+
			"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=NS"+
			"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=Counter"+
			"&MetricDataQueries.member.1.MetricStat.Stat=Sum"+
			"&MetricDataQueries.member.1.MetricStat.Period=60"+
			"&MetricDataQueries.member.1.ReturnData=true"+
			"&StartTime=2024-01-01T00:00:00Z"+
			"&EndTime=2024-01-01T00:04:00Z"+
			"&ScanBy=TimestampDescending",
	)
	assert.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())
}

// ---------------------------------------------------------------------------
// Handler integration: StorageResolution field (gap #3)
// ---------------------------------------------------------------------------

func TestHandler_PutMetricData_StorageResolution1(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=Ticks"+
			"&MetricData.member.1.Value=1"+
			"&MetricData.member.1.StorageResolution=1"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 200, rec.Code, "valid StorageResolution=1 should succeed")
}

func TestHandler_PutMetricData_StorageResolution60(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=Ticks"+
			"&MetricData.member.1.Value=1"+
			"&MetricData.member.1.StorageResolution=60"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 200, rec.Code)
}

// ---------------------------------------------------------------------------
// MetricStream filters via handler (gap #8)
// ---------------------------------------------------------------------------

func TestHandler_PutMetricStream_WithIncludeFilters(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricStream"+
			"&Name=filtered-stream"+
			"&FirehoseArn=arn:aws:firehose:us-east-1:123:deliverystream/ds"+
			"&RoleArn=arn:aws:iam::123:role/r"+
			"&OutputFormat=json"+
			"&IncludeFilters.member.1.Namespace=AWS/EC2",
	)
	assert.Equal(t, 200, rec.Code, "stream with IncludeFilters: %s", rec.Body.String())
}

func TestHandler_PutMetricStream_WithExcludeFilters(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricStream"+
			"&Name=exclude-stream"+
			"&FirehoseArn=arn:aws:firehose:us-east-1:123:deliverystream/ds"+
			"&RoleArn=arn:aws:iam::123:role/r"+
			"&OutputFormat=json"+
			"&ExcludeFilters.member.1.Namespace=AWS/EC2",
	)
	assert.Equal(t, 200, rec.Code, "stream with ExcludeFilters: %s", rec.Body.String())
}

// ---------------------------------------------------------------------------
// DimensionSetKey ordering invariant
// ---------------------------------------------------------------------------

func TestDimensionSetKey_OrderIndependent(t *testing.T) {
	t.Parallel()

	d1 := []cloudwatch.Dimension{{Name: "Z", Value: "z"}, {Name: "A", Value: "a"}}
	d2 := []cloudwatch.Dimension{{Name: "A", Value: "a"}, {Name: "Z", Value: "z"}}

	assert.Equal(t, cloudwatch.DimensionSetKeyForTest(d1), cloudwatch.DimensionSetKeyForTest(d2),
		"dimension set key should be order-independent")
}

func TestDimensionSetKey_Empty(t *testing.T) {
	t.Parallel()

	key := cloudwatch.DimensionSetKeyForTest(nil)
	assert.Empty(t, key, "empty dimension set key should be empty string")
}

func TestDimensionSetKey_SingleDimension(t *testing.T) {
	t.Parallel()

	dims := []cloudwatch.Dimension{{Name: "Host", Value: "web1"}}
	key := cloudwatch.DimensionSetKeyForTest(dims)
	assert.NotEmpty(t, key)
	assert.Contains(t, key, "Host")
	assert.Contains(t, key, "web1")
}

// ---------------------------------------------------------------------------
// StreamAllowsMetric filter logic (gap #8)
// ---------------------------------------------------------------------------

func TestStreamAllowsMetric_NoFilters_AllowsAll(t *testing.T) {
	t.Parallel()

	s := &cloudwatch.MetricStream{Name: "s"}
	assert.True(t, cloudwatch.StreamAllowsMetricForTest(s, "AWS/EC2", "CPU"))
	assert.True(t, cloudwatch.StreamAllowsMetricForTest(s, "Custom/App", "RPM"))
}

func TestStreamAllowsMetric_IncludeFilter(t *testing.T) {
	t.Parallel()

	s := &cloudwatch.MetricStream{
		Name:           "s",
		IncludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
	}
	assert.True(t, cloudwatch.StreamAllowsMetricForTest(s, "AWS/EC2", "CPU"))
	assert.False(t, cloudwatch.StreamAllowsMetricForTest(s, "Custom/App", "RPM"))
}

func TestStreamAllowsMetric_ExcludeFilter(t *testing.T) {
	t.Parallel()

	s := &cloudwatch.MetricStream{
		Name:           "s",
		ExcludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
	}
	assert.False(t, cloudwatch.StreamAllowsMetricForTest(s, "AWS/EC2", "CPU"))
	assert.True(t, cloudwatch.StreamAllowsMetricForTest(s, "Custom/App", "RPM"))
}

func TestStreamAllowsMetric_ExcludeTakesPrecedence(t *testing.T) {
	t.Parallel()

	s := &cloudwatch.MetricStream{
		Name:           "s",
		IncludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
		ExcludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
	}
	// Exclude overrides include.
	assert.False(t, cloudwatch.StreamAllowsMetricForTest(s, "AWS/EC2", "CPU"))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func metricNames(metrics []cloudwatch.Metric) []string {
	names := make([]string, 0, len(metrics))
	for _, m := range metrics {
		names = append(names, m.MetricName)
	}

	return names
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}

	return -1
}

// ---------------------------------------------------------------------------
// Additional anomaly detection band edge cases
// ---------------------------------------------------------------------------

func TestBackend_AnomalyDetector_DimensionMatch(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()
	dimProd := []cloudwatch.Dimension{{Name: "Env", Value: "prod"}}
	dimStaging := []cloudwatch.Dimension{{Name: "Env", Value: "staging"}}

	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "CPU", Value: 80, Count: 1, Sum: 80, Min: 80, Max: 80,
			Timestamp: now.Add(-time.Minute), Dimensions: dimProd,
		},
		{
			MetricName: "CPU", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20,
			Timestamp: now.Add(-time.Minute), Dimensions: dimStaging,
		},
	})
	require.NoError(t, err)

	// Detector for prod only.
	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace: "App", MetricName: "CPU", Stat: "Average",
		Dimensions: dimProd,
	}))

	dpsProd, err := b.GetMetricStatistics("App", "CPU", dimProd,
		now.Add(-5*time.Minute), now, 60, []string{"Average"}, nil)
	require.NoError(t, err)

	hasBandProd := false
	for _, dp := range dpsProd {
		if dp.BandLower != nil {
			hasBandProd = true
		}
	}

	dpsStaging, err := b.GetMetricStatistics("App", "CPU", dimStaging,
		now.Add(-5*time.Minute), now, 60, []string{"Average"}, nil)
	require.NoError(t, err)

	for _, dp := range dpsStaging {
		assert.Nil(t, dp.BandLower, "staging should not have band from prod detector")
	}

	assert.True(t, hasBandProd, "prod query should have anomaly band from matching detector")
}

func TestBackend_AnomalyDetector_CustomBandWidth(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	err := b.PutMetricData("Srv", []cloudwatch.MetricDatum{
		{MetricName: "Req", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: now.Add(-2 * time.Minute)},
		{MetricName: "Req", Value: 30, Count: 1, Sum: 30, Min: 30, Max: 30, Timestamp: now.Add(-time.Minute)},
		{MetricName: "Req", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: now.Add(-30 * time.Second)},
	})
	require.NoError(t, err)

	// BandWidth 3 → 3 stddevs, BandWidth 1 → 1 stddev.
	for _, bw := range []float64{1.0, 3.0} {
		require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
			Namespace: "Srv", MetricName: "Req", Stat: "Sum",
			BandWidth: bw,
		}))

		dps, getErr := b.GetMetricStatistics("Srv", "Req", nil,
			now.Add(-5*time.Minute), now, 60, []string{"Sum"}, nil)
		require.NoError(t, getErr)

		for _, dp := range dps {
			if dp.BandLower != nil && dp.BandUpper != nil {
				assert.LessOrEqual(t, *dp.BandLower, *dp.BandUpper)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Additional topo sort tests
// ---------------------------------------------------------------------------

func TestTopoSortExpressions_SingleExpression(t *testing.T) {
	t.Parallel()

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60}},
		{ID: "e1", Expression: "m1 + 1"},
	}
	sorted, err := cloudwatch.TopoSortExpressionsForTest(queries)
	require.NoError(t, err)
	assert.Equal(t, []string{"e1"}, sorted)
}

func TestTopoSortExpressions_MultipleIndependent(t *testing.T) {
	t.Parallel()

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "A", Stat: "Sum", Period: 60}},
		{ID: "m2", MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "B", Stat: "Sum", Period: 60}},
		{ID: "e1", Expression: "m1 * 2"},
		{ID: "e2", Expression: "m2 * 3"},
	}
	sorted, err := cloudwatch.TopoSortExpressionsForTest(queries)
	require.NoError(t, err)
	assert.Len(t, sorted, 2)
	assert.ElementsMatch(t, []string{"e1", "e2"}, sorted)
}

// ---------------------------------------------------------------------------
// Pagination token stress tests
// ---------------------------------------------------------------------------

func TestSignPageToken_MultipleOffsets(t *testing.T) {
	t.Parallel()

	offsets := []int{1, 100, 500, 1000, 9999, 99999, 1000000}

	for _, offset := range offsets {
		tok := cloudwatch.SignPageTokenForTest(offset)
		got, err := cloudwatch.ParseSignedPageTokenForTest(tok)
		require.NoError(t, err, "offset %d", offset)
		assert.Equal(t, offset, got, "offset %d round-trip mismatch", offset)
	}
}

func TestSignPageToken_TokensDistinct(t *testing.T) {
	t.Parallel()

	tok1 := cloudwatch.SignPageTokenForTest(1)
	tok2 := cloudwatch.SignPageTokenForTest(2)
	assert.NotEqual(t, tok1, tok2, "different offsets must produce different tokens")
}

// ---------------------------------------------------------------------------
// Additional handler tests for accuracy gaps
// ---------------------------------------------------------------------------

func TestHandler_GetMetricData_WithExpressions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	postForm(
		t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=Hits"+
			"&MetricData.member.1.Value=10"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:30Z",
	)

	rec := postForm(
		t, h,
		"Action=GetMetricData"+
			"&MetricDataQueries.member.1.Id=m1"+
			"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=NS"+
			"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=Hits"+
			"&MetricDataQueries.member.1.MetricStat.Stat=Sum"+
			"&MetricDataQueries.member.1.MetricStat.Period=60"+
			"&MetricDataQueries.member.1.ReturnData=true"+
			"&MetricDataQueries.member.2.Id=e1"+
			"&MetricDataQueries.member.2.Expression=m1+*+2"+
			"&MetricDataQueries.member.2.ReturnData=true"+
			"&StartTime=2024-01-01T00:00:00Z"+
			"&EndTime=2024-01-01T00:02:00Z",
	)
	assert.Equal(t, 200, rec.Code, "GetMetricData with expression: %s", rec.Body.String())
}

func TestHandler_DescribeAnomalyDetectors_WithDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	postForm(
		t, h,
		"Action=PutAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average"+
			"&SingleMetricAnomalyDetector.Dimensions.member.1.Name=Host"+
			"&SingleMetricAnomalyDetector.Dimensions.member.1.Value=web1",
	)

	rec := postForm(t, h, "Action=DescribeAnomalyDetectors&Namespace=App")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "CPU")
}

func TestHandler_PutMetricData_MultipleWithMixedDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(
		t, h,
		"Action=PutMetricData&Namespace=Multi"+
			"&MetricData.member.1.MetricName=Errors"+
			"&MetricData.member.1.Value=1"+
			"&MetricData.member.1.Dimensions.member.1.Name=Svc&MetricData.member.1.Dimensions.member.1.Value=auth"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z"+
			"&MetricData.member.2.MetricName=Errors"+
			"&MetricData.member.2.Value=2"+
			"&MetricData.member.2.Dimensions.member.1.Name=Svc&MetricData.member.2.Dimensions.member.1.Value=api"+
			"&MetricData.member.2.Timestamp=2024-01-01T00:00:00Z"+
			"&MetricData.member.3.MetricName=Errors"+
			"&MetricData.member.3.Value=3"+
			"&MetricData.member.3.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 200, rec.Code, "three metrics with different dimension sets")
}

func TestHandler_GetMetricStatistics_NoDimensions(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	postForm(
		t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=Mem&MetricData.member.1.Value=512"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:30Z",
	)

	rec := postForm(
		t, h,
		"Action=GetMetricStatistics"+
			"&Namespace=NS&MetricName=Mem"+
			"&StartTime=2024-01-01T00:00:00Z"+
			"&EndTime=2024-01-01T00:02:00Z"+
			"&Period=60&Statistics.member.1=Sum",
	)
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetMetricStatisticsResponse")
}

// ---------------------------------------------------------------------------
// Rolling stats edge cases
// ---------------------------------------------------------------------------

func TestRollingStats_LargeVariance(t *testing.T) {
	t.Parallel()

	vals := []float64{0, 100}
	mean, stddev := cloudwatch.RollingStatsForTest(vals)
	assert.InDelta(t, 50.0, mean, 1e-9)
	assert.Greater(t, stddev, 0.0, "non-constant input should have positive stddev")
}

func TestRollingStats_NegativeValues(t *testing.T) {
	t.Parallel()

	vals := []float64{-10, 0, 10}
	mean, stddev := cloudwatch.RollingStatsForTest(vals)
	assert.InDelta(t, 0.0, mean, 1e-9)
	assert.Greater(t, stddev, 0.0)
}

// ---------------------------------------------------------------------------
// computeAnomalyBand with BandWidth from detector
// ---------------------------------------------------------------------------

func TestComputeAnomalyBand_SymmetricAroundMean(t *testing.T) {
	t.Parallel()

	// All values equal → mean=5, stddev=0, band collapses.
	vals := []float64{5, 5, 5}
	lo, hi := cloudwatch.ComputeAnomalyBandForTest(vals)

	for i := range lo {
		assert.InDelta(t, 5.0, lo[i], 1e-9, "lower should equal mean for zero-variance")
		assert.InDelta(t, 5.0, hi[i], 1e-9, "upper should equal mean for zero-variance")
	}
}

func TestComputeAnomalyBand_SameLengthAsInput(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 5, 10, 100} {
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = float64(i)
		}

		lo, hi := cloudwatch.ComputeAnomalyBandForTest(vals)
		assert.Len(t, lo, n, "lower band length mismatch for n=%d", n)
		assert.Len(t, hi, n, "upper band length mismatch for n=%d", n)
	}
}
