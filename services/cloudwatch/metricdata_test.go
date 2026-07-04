package cloudwatch_test

import (
	"math"
	"testing"
	"time"

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

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
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
	if _, err := b.PutMetricData("MyApp", data); err != nil {
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
