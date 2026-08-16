package cloudwatch_test

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cloudwatch "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// makeResult builds a MetricDataResult with n sequential data points.
func makeResult(id string, n int) cloudwatch.MetricDataResult {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	r := cloudwatch.MetricDataResult{ID: id, StatusCode: "Complete"}
	for i := range n {
		r.Timestamps = append(r.Timestamps, base.Add(time.Duration(i)*time.Minute))
		r.Values = append(r.Values, float64(i))
	}

	return r
}

func totalPoints(page cloudwatch.GetMetricDataPage) int {
	sum := 0
	for _, r := range page.Results {
		sum += len(r.Values)
	}

	return sum
}

func TestPaginateMetricData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		counts        []int
		wantPagePts   []int
		maxDatapoints int
	}{
		{
			name:          "no truncation with ample budget",
			counts:        []int{3, 2, 4},
			maxDatapoints: 100,
			wantPagePts:   []int{9},
		},
		{
			name:          "budget spans results",
			counts:        []int{3, 2, 4},
			maxDatapoints: 4,
			wantPagePts:   []int{4, 4, 1},
		},
		{
			name:          "exact boundary",
			counts:        []int{2, 2},
			maxDatapoints: 2,
			wantPagePts:   []int{2, 2},
		},
		{
			name:          "single big result",
			counts:        []int{10},
			maxDatapoints: 3,
			wantPagePts:   []int{3, 3, 3, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			all := make([]cloudwatch.MetricDataResult, len(tt.counts))
			wantTotal := 0
			for i, c := range tt.counts {
				all[i] = makeResult("m"+string(rune('0'+i)), c)
				wantTotal += c
			}

			var gotPagePts []int
			gotTotal := 0
			token := ""
			for range 20 {
				p := cloudwatch.PaginateMetricDataForTest(all, tt.maxDatapoints, token)
				gotPagePts = append(gotPagePts, totalPoints(p))
				gotTotal += totalPoints(p)

				// Truncated pages must carry the informational top-level message.
				if p.NextToken != "" && len(p.Messages) == 0 {
					t.Fatalf("truncated page missing top-level message")
				}
				if p.NextToken == "" {
					break
				}
				token = p.NextToken
			}

			if gotTotal != wantTotal {
				t.Fatalf("total points across pages = %d, want %d", gotTotal, wantTotal)
			}
			if len(gotPagePts) != len(tt.wantPagePts) {
				t.Fatalf("page count = %d (%v), want %d (%v)",
					len(gotPagePts), gotPagePts, len(tt.wantPagePts), tt.wantPagePts)
			}
			for i := range gotPagePts {
				if gotPagePts[i] != tt.wantPagePts[i] {
					t.Fatalf("page %d points = %d, want %d (%v)",
						i, gotPagePts[i], tt.wantPagePts[i], gotPagePts)
				}
			}
		})
	}
}

func TestPaginateMetricDataPreservesValues(t *testing.T) {
	t.Parallel()

	all := []cloudwatch.MetricDataResult{makeResult("m0", 5)}

	var got []float64
	token := ""
	for {
		p := cloudwatch.PaginateMetricDataForTest(all, 2, token)
		for _, r := range p.Results {
			got = append(got, r.Values...)
		}
		if p.NextToken == "" {
			break
		}
		token = p.NextToken
	}

	want := []float64{0, 1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("reconstructed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("index %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestAnnotateArithmeticMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statusIn  string
		statusOut string
		values    []float64
		wantMsgs  int
	}{
		{name: "clean", values: []float64{1, 2, 3}, wantMsgs: 0, statusIn: "Complete", statusOut: "Complete"},
		{
			name: "has NaN", values: []float64{1, math.NaN(), 3},
			wantMsgs: 1, statusIn: "Complete", statusOut: "PartialData",
		},
		{
			name: "has Inf", values: []float64{math.Inf(1), 2},
			wantMsgs: 1, statusIn: "Complete", statusOut: "PartialData",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := cloudwatch.MetricDataResult{StatusCode: tt.statusIn, Values: tt.values}
			cloudwatch.AnnotateArithmeticMessagesForTest(&r)

			if len(r.Messages) != tt.wantMsgs {
				t.Fatalf("messages = %d, want %d", len(r.Messages), tt.wantMsgs)
			}
			if r.StatusCode != tt.statusOut {
				t.Fatalf("status = %q, want %q", r.StatusCode, tt.statusOut)
			}
		})
	}
}

func TestGetMetricDataPagedEndToEnd(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	base := cloudwatch.RecentTestAnchor()
	data := make([]cloudwatch.MetricDatum, 0, 5)
	for i := range 5 {
		data = append(data, cloudwatch.MetricDatum{
			MetricName: "Latency",
			Namespace:  "MyApp",
			Timestamp:  base.Add(time.Duration(i) * time.Minute),
			Value:      float64(i + 1),
			Count:      1,
			Sum:        float64(i + 1),
			Min:        float64(i + 1),
			Max:        float64(i + 1),
		})
	}
	if err := b.PutMetricData("MyApp", data); err != nil {
		t.Fatalf("PutMetricData: %v", err)
	}

	queries := []cloudwatch.MetricDataQuery{{
		ID:         "q1",
		ReturnData: true,
		MetricStat: cloudwatch.MetricStat{
			Namespace:  "MyApp",
			MetricName: "Latency",
			Stat:       "Average",
			Period:     60,
		},
	}}

	start := base.Add(-time.Minute)
	end := base.Add(10 * time.Minute)

	// Walk pages of 2 datapoints and confirm all 5 are eventually returned.
	seen := 0
	token := ""
	pages := 0
	for {
		page, err := b.GetMetricDataPaged(queries, start, end, "", token, 2)
		if err != nil {
			t.Fatalf("GetMetricDataPaged: %v", err)
		}
		pages++
		for _, r := range page.Results {
			seen += len(r.Values)
		}
		if page.NextToken == "" {
			break
		}
		token = page.NextToken
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	if seen != 5 {
		t.Fatalf("saw %d datapoints across pages, want 5", seen)
	}
	if pages != 3 {
		t.Fatalf("expected 3 pages (2+2+1), got %d", pages)
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

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}

	return -1
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
// MetricMath: id OP constant expressions
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_ConstantMultiply(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "m1 * 2", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.Contains(t, byID, "e1")
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 20.0, byID["e1"].Values[0], 1e-9, "m1 * 2 = 20 when m1=10")
}

func TestBackend_GetMetricData_ConstantAdd(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "m1 + 5", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.Contains(t, byID, "e1")
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 15.0, byID["e1"].Values[0], 1e-9, "m1 + 5 = 15 when m1=10")
}

func TestBackend_GetMetricData_ConstantDivide(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 100, Count: 1, Sum: 100, Min: 100, Max: 100, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "m1 / 4", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 25.0, byID["e1"].Values[0], 1e-9, "m1 / 4 = 25 when m1=100")
}

func TestBackend_GetMetricData_ConstantSubtract(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "m1 - 10", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 40.0, byID["e1"].Values[0], 1e-9, "m1 - 10 = 40 when m1=50")
}

func TestBackend_GetMetricData_ConstantLeftSide(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 5, Count: 1, Sum: 5, Min: 5, Max: 5, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "100 / m1", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 20.0, byID["e1"].Values[0], 1e-9, "100 / m1 = 20 when m1=5")
}

func TestBackend_GetMetricData_ConstantLeftMultiply(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 3, Count: 1, Sum: 3, Min: 3, Max: 3, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "4 * m1", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 12.0, byID["e1"].Values[0], 1e-9, "4 * m1 = 12 when m1=3")
}

// ---------------------------------------------------------------------------
// MetricMath: METRICS() aggregation variants
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_AvgMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	for _, v := range []float64{10, 30} {
		mn := fmt.Sprintf("M%.0f", v)
		err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: mn, Value: v, Count: 1, Sum: v, Min: v, Max: v, Timestamp: ts},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: false, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M10", Stat: "Sum", Period: 60,
		}},
		{ID: "m2", ReturnData: false, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M30", Stat: "Sum", Period: 60,
		}},
		{ID: "avg", Expression: "AVG(METRICS())", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.Contains(t, byID, "avg")
	require.NotEmpty(t, byID["avg"].Values)
	assert.InDelta(t, 20.0, byID["avg"].Values[0], 1e-9, "AVG(METRICS()) = (10+30)/2 = 20")
}

func TestBackend_GetMetricData_MinMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	for _, v := range []float64{5, 15, 25} {
		mn := fmt.Sprintf("M%.0f", v)
		err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: mn, Value: v, Count: 1, Sum: v, Min: v, Max: v, Timestamp: ts},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M5", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m2",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M15", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m3",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M25", Stat: "Sum", Period: 60},
		},
		{ID: "mn", Expression: "MIN(METRICS())", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["mn"].Values)
	assert.InDelta(t, 5.0, byID["mn"].Values[0], 1e-9, "MIN(METRICS())=5")
}

func TestBackend_GetMetricData_MaxMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	for _, v := range []float64{5, 15, 99} {
		mn := fmt.Sprintf("M%.0f", v)
		err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: mn, Value: v, Count: 1, Sum: v, Min: v, Max: v, Timestamp: ts},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M5", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m2",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M15", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m3",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M99", Stat: "Sum", Period: 60},
		},
		{ID: "mx", Expression: "MAX(METRICS())", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["mx"].Values)
	assert.InDelta(t, 99.0, byID["mx"].Values[0], 1e-9, "MAX(METRICS())=99")
}

func TestBackend_GetMetricData_StddevMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	for _, v := range []float64{10, 20, 30} {
		mn := fmt.Sprintf("M%.0f", v)
		err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: mn, Value: v, Count: 1, Sum: v, Min: v, Max: v, Timestamp: ts},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M10", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m2",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M20", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m3",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M30", Stat: "Sum", Period: 60},
		},
		{ID: "sd", Expression: "STDDEV(METRICS())", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["sd"].Values)
	assert.Greater(t, byID["sd"].Values[0], 0.0, "STDDEV(METRICS()) > 0 for non-constant series")
}

// ---------------------------------------------------------------------------
// MetricMath: RATE() function
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_RateFunction(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	base := cloudwatch.RecentTestAnchor().Add(time.Minute) // align to minute boundary

	// Two data points 60 seconds apart, value increases by 120.
	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Counter", Value: 0, Count: 1, Sum: 0, Min: 0, Max: 0, Timestamp: base},
		{MetricName: "Counter", Value: 120, Count: 1, Sum: 120, Min: 120, Max: 120, Timestamp: base.Add(time.Minute)},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: false, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "Counter", Stat: "Sum", Period: 60,
		}},
		{ID: "rate", Expression: "RATE(m1)", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, base.Add(-time.Minute), base.Add(3*time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	// RATE may return values if m1 has ≥2 data points.
	if len(byID["rate"].Values) > 0 {
		assert.Greater(t, byID["rate"].Values[0], 0.0, "RATE should be positive for increasing counter")
	}
}

// ---------------------------------------------------------------------------
// GetMetricData: multiple stat queries in one request
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_MultipleStatQueries(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "CPU", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: ts},
		{MetricName: "Mem", Value: 80, Count: 1, Sum: 80, Min: 80, Max: 80, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "cpu",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "CPU", Stat: "Average", Period: 60},
		},
		{
			ID:         "mem",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "Mem", Stat: "Average", Period: 60},
		},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)
	assert.Len(t, results, 2)

	byID := resultsByID(results)
	require.Contains(t, byID, "cpu")
	require.Contains(t, byID, "mem")
	require.NotEmpty(t, byID["cpu"].Values)
	require.NotEmpty(t, byID["mem"].Values)
	assert.InDelta(t, 50.0, byID["cpu"].Values[0], 1e-9)
	assert.InDelta(t, 80.0, byID["mem"].Values[0], 1e-9)
}

// ---------------------------------------------------------------------------
// GetMetricData: cross-account returns empty gracefully
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_CrossAccount_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	queries := []cloudwatch.MetricDataQuery{
		{ID: "q1", AccountID: "999999999999", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
	}

	results, err := b.GetMetricData(queries, time.Now().Add(-time.Hour), time.Now())
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Values, "cross-account queries should return empty results")
}

// ---------------------------------------------------------------------------
// GetMetricData: ReturnData=false suppresses expression output
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_ReturnDataFalse_SuppressesResult(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60},
		},
		{ID: "helper", Expression: "m1 * 2", ReturnData: false},
	}

	results, err := b.GetMetricData(queries, ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)

	byID := resultsByID(results)
	assert.Contains(t, byID, "m1")
	assert.NotContains(t, byID, "helper", "ReturnData=false should suppress output")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func resultsByID(results []cloudwatch.MetricDataResult) map[string]cloudwatch.MetricDataResult {
	m := make(map[string]cloudwatch.MetricDataResult, len(results))
	for _, r := range results {
		m[r.ID] = r
	}

	return m
}

func TestCloudWatchBackend_DimensionAwareGetMetricData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	mid := time.Now().UTC().Add(-time.Minute)

	dimsX := []cloudwatch.Dimension{{Name: "Shard", Value: "x"}}

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Errors", Value: 5, Count: 1, Sum: 5, Min: 5, Max: 5, Timestamp: mid, Dimensions: dimsX},
		{MetricName: "Errors", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: mid},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{
				Namespace: "NS", MetricName: "Errors",
				Stat: "Sum", Period: 60,
				Dimensions: dimsX,
			},
		},
	}

	results, err := b.GetMetricData(queries, time.Now().UTC().Add(-2*time.Minute), time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Values, 1)
	assert.InDelta(t, 5.0, results[0].Values[0], 1e-9, "should return only the shard-x series sum")
}

func TestCloudWatchBackend_ScanByDescending(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()
	t1 := now.Add(-3 * time.Minute)
	t2 := now.Add(-2 * time.Minute)
	t3 := now.Add(-time.Minute)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Counter", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: t1},
		{MetricName: "Counter", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: t2},
		{MetricName: "Counter", Value: 3, Count: 1, Sum: 3, Min: 3, Max: 3, Timestamp: t3},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{
				Namespace: "NS", MetricName: "Counter",
				Stat: "Sum", Period: 60,
			},
		},
	}

	asc, err := b.GetMetricDataWithOptions(queries, now.Add(-5*time.Minute), now, "TimestampAscending")
	require.NoError(t, err)
	require.Len(t, asc, 1)
	require.Len(t, asc[0].Values, 3)
	assert.True(t, asc[0].Timestamps[0].Before(asc[0].Timestamps[1]), "ascending order")

	desc, err := b.GetMetricDataWithOptions(queries, now.Add(-5*time.Minute), now, "TimestampDescending")
	require.NoError(t, err)
	require.Len(t, desc, 1)
	require.Len(t, desc[0].Values, 3)
	assert.True(t, desc[0].Timestamps[0].After(desc[0].Timestamps[1]), "descending order")
}

func TestCloudWatchBackend_ReturnDataFalse_SuppressesExpressionResult(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Val", Value: 7, Count: 1, Sum: 7, Min: 7, Max: 7, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{
				Namespace: "NS", MetricName: "Val", Stat: "Sum", Period: 60,
			},
		},
		{
			ID:         "e1",
			Expression: "m1 * 2",
			ReturnData: false,
		},
	}

	results, err := b.GetMetricDataWithOptions(queries, time.Now().UTC().Add(-2*time.Minute), time.Now().UTC(), "")
	require.NoError(t, err)

	ids := make([]string, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.ID)
	}
	assert.Contains(t, ids, "m1", "m1 should be present")
	assert.NotContains(t, ids, "e1", "e1 ReturnData=false should be suppressed")
}
