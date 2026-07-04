package cloudwatch_test

import (
	"math"
	"testing"

	cloudwatch "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func TestComputeExtendedStat(t *testing.T) {
	t.Parallel()

	// Primary asymmetric data set: 10, 20, 30, 40, 100 (already sorted).
	// percentile(80) = 40*0.8 + 100*0.2 = 52.
	asym := []float64{10, 20, 30, 40, 100}
	// Symmetric 1..10 used for percentile checks.
	oneToTen := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	tests := []struct {
		name   string
		stat   string
		vals   []float64
		want   float64
		wantOK bool
	}{
		{name: "p50 symmetric", vals: oneToTen, stat: "p50", want: 5.5, wantOK: true},
		{name: "p90 symmetric", vals: oneToTen, stat: "p90", want: 9.1, wantOK: true},
		{name: "p10 symmetric", vals: oneToTen, stat: "p10", want: 1.9, wantOK: true},
		{name: "p100 is max", vals: oneToTen, stat: "p100", want: 10, wantOK: true},
		{name: "p0 is min", vals: oneToTen, stat: "p0", want: 1, wantOK: true},
		{name: "p99.5 fractional", vals: oneToTen, stat: "p99.5", want: 9.955, wantOK: true},

		// Trimmed statistics, absolute bounds [15,45] keep {20,30,40}.
		{name: "TC absolute", vals: asym, stat: "TC(15:45)", want: 3, wantOK: true},
		{name: "TS absolute", vals: asym, stat: "TS(15:45)", want: 90, wantOK: true},
		{name: "TM absolute", vals: asym, stat: "TM(15:45)", want: 30, wantOK: true},

		// Winsorized mean clamps 10->15 and 100->45: (15+20+30+40+45)/5 = 30.
		{name: "WM absolute", vals: asym, stat: "WM(15:45)", want: 30, wantOK: true},

		// Percentile rank counts fraction of points inside the absolute range.
		{name: "PR range", vals: asym, stat: "PR(15:45)", want: 60, wantOK: true},
		{name: "PR upper only", vals: asym, stat: "PR(:45)", want: 80, wantOK: true},
		{name: "PR lower only", vals: asym, stat: "PR(30:)", want: 60, wantOK: true},

		// Single-value form is treated as an upper bound (80th pct = 52),
		// keeping {10,20,30,40} -> mean 25.
		{name: "TM single value percent", vals: asym, stat: "TM(80%)", want: 25, wantOK: true},

		// IQM = TM(25%:75%). On 1..10, p25=3.25 p75=7.75 -> keep {4,5,6,7} = 5.5.
		{name: "IQM symmetric", vals: oneToTen, stat: "IQM", want: 5.5, wantOK: true},

		// Percent-form trimmed mean: p10=1.9 p90=9.1 keep {2..9} = 5.5.
		{name: "TM percent bounds", vals: oneToTen, stat: "TM(10%:90%)", want: 5.5, wantOK: true},

		// Unrecognised / invalid stats.
		{name: "unknown func", vals: asym, stat: "ZZ(1:2)", want: 0, wantOK: false},
		{name: "percentile over 100", vals: oneToTen, stat: "p150", want: 0, wantOK: false},
		{name: "empty args", vals: asym, stat: "TM()", want: 0, wantOK: false},
		{name: "non-numeric bound", vals: asym, stat: "TM(a:b)", want: 0, wantOK: false},
		{name: "plain word", vals: asym, stat: "average", want: 0, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := cloudwatch.ComputeExtendedStatForTest(tt.vals, tt.stat)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (value %v)", ok, tt.wantOK, got)
			}
			if !tt.wantOK {
				return
			}
			if math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("%s = %v, want %v", tt.stat, got, tt.want)
			}
		})
	}
}

func TestComputeExtendedStatsEmpty(t *testing.T) {
	t.Parallel()

	got := cloudwatch.ComputeExtendedStatsForTest(nil, []string{"p50", "TM(1:2)"})
	if len(got) != 0 {
		t.Fatalf("expected empty map for no data, got %v", got)
	}
}

func TestComputeExtendedStatsMap(t *testing.T) {
	t.Parallel()

	vals := []float64{10, 20, 30, 40, 100}
	got := cloudwatch.ComputeExtendedStatsForTest(vals, []string{"TC(15:45)", "PR(15:45)", "bogus"})

	if got["TC(15:45)"] != 3 {
		t.Fatalf("TC = %v, want 3", got["TC(15:45)"])
	}
	if got["PR(15:45)"] != 60 {
		t.Fatalf("PR = %v, want 60", got["PR(15:45)"])
	}
	if _, ok := got["bogus"]; ok {
		t.Fatalf("bogus stat should be dropped")
	}
}

func TestExpandDatumValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want []float64
		in   cloudwatch.MetricDatum
	}{
		{
			name: "plain value",
			in:   cloudwatch.MetricDatum{Value: 42},
			want: []float64{42},
		},
		{
			name: "stat set count 1",
			in:   cloudwatch.MetricDatum{HasStatisticSet: true, Count: 1, Sum: 7},
			want: []float64{7},
		},
		{
			name: "stat set count 2",
			in:   cloudwatch.MetricDatum{HasStatisticSet: true, Count: 2, Sum: 12, Min: 3, Max: 9},
			want: []float64{3, 9},
		},
		{
			name: "stat set count 5 preserves sum/min/max",
			in: cloudwatch.MetricDatum{
				HasStatisticSet: true, Count: 5, Sum: 200, Min: 10, Max: 100,
			},
			want: []float64{10, 30, 30, 30, 100},
		},
		{
			name: "stat set count 0 is empty",
			in:   cloudwatch.MetricDatum{HasStatisticSet: true, Count: 0},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudwatch.ExpandDatumValuesForTest(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d (%v)", len(got), len(tt.want), got)
			}
			var sum float64
			for i := range got {
				if math.Abs(got[i]-tt.want[i]) > 1e-9 {
					t.Fatalf("index %d = %v, want %v", i, got[i], tt.want[i])
				}
				sum += got[i]
			}
			// Sum must be preserved for stat sets with count >= 1.
			if tt.in.HasStatisticSet && tt.in.Count >= 1 && math.Abs(sum-tt.in.Sum) > 1e-9 {
				t.Fatalf("reconstructed sum = %v, want %v", sum, tt.in.Sum)
			}
		})
	}
}
