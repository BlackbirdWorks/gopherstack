package ce_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

func TestLedgerSpend(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	start := now.AddDate(0, 0, -10).Format("2006-01-02")
	end := now.AddDate(0, 0, -5).Format("2006-01-02")

	tests := []struct {
		filters map[string][]string
		name    string
		metric  string
		wantPos bool
		less    bool
	}{
		{name: "all", metric: "UnblendedCost", wantPos: true},
		{
			name:    "service",
			metric:  "BlendedCost",
			filters: map[string][]string{"Service": {"AWS Lambda"}},
			wantPos: true,
			less:    true,
		},
		{name: "nomatch", metric: "BlendedCost", filters: map[string][]string{"Service": {"Nope"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("123456789012", "us-east-1")
			total := b.LedgerSpend(start, end, nil, tc.metric)
			got := b.LedgerSpend(start, end, tc.filters, tc.metric)

			assert.Equal(t, tc.wantPos, got > 0)

			if tc.less {
				assert.Less(t, got, total)
			}
		})
	}
}

func TestLedgerSpendExcludesChargeTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		exclude []string
		want    float64
	}{
		{name: "none", want: 17},
		{name: "tax", exclude: []string{"Tax"}, want: 10},
		{name: "usage", exclude: []string{"usage"}, want: 7},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("123456789012", "us-east-1")
			b.RecordCharge(ce.CostEntry{Date: "2001-01-01", Service: "X", UnblendedCost: 10})
			b.RecordCharge(ce.CostEntry{Date: "2001-01-01", Service: "X", UnblendedCost: 7, ChargeType: "Tax"})

			filters := map[string][]string{ce.ExcludeChargeTypeFilter: tc.exclude}
			assert.InDelta(t, tc.want, b.LedgerSpend("2001-01-01", "2001-01-02", filters, "UnblendedCost"), 1e-9)
		})
	}
}

func TestLedgerSpendFiltered(t *testing.T) {
	t.Parallel()

	dim := func(key string, opts []string, vals ...string) *ce.SpendFilter {
		return &ce.SpendFilter{Dimensions: &ce.SpendValues{Key: key, Values: vals, MatchOptions: opts}}
	}

	tests := []struct {
		filter *ce.SpendFilter
		name   string
		want   float64
	}{
		{name: "nil", want: 17},
		{
			name:   "or",
			filter: &ce.SpendFilter{Or: []ce.SpendFilter{*dim("SERVICE", nil, "X"), *dim("SERVICE", nil, "Y")}},
			want:   17,
		},
		{
			name:   "or one",
			filter: &ce.SpendFilter{Or: []ce.SpendFilter{*dim("SERVICE", nil, "Y"), *dim("SERVICE", nil, "Z")}},
			want:   7,
		},
		{name: "not", filter: &ce.SpendFilter{Not: dim("SERVICE", nil, "X")}, want: 7},
		{name: "starts with", filter: dim("SERVICE", []string{"STARTS_WITH"}, "y"), want: 7},
		{name: "contains", filter: dim("SERVICE", []string{"CONTAINS"}, "Z"), want: 0},
		{
			name:   "case sensitive",
			filter: dim("SERVICE", []string{"CASE_SENSITIVE"}, "x"),
			want:   0,
		},
		{
			name:   "tag",
			filter: &ce.SpendFilter{Tags: &ce.SpendValues{Key: "team", Values: []string{"a"}}},
			want:   10,
		},
		{
			name:   "tag absent",
			filter: &ce.SpendFilter{Tags: &ce.SpendValues{Key: "team", MatchOptions: []string{"ABSENT"}}},
			want:   7,
		},
		{
			name: "and not",
			filter: &ce.SpendFilter{And: []ce.SpendFilter{
				*dim("SERVICE", nil, "X"),
				{Not: &ce.SpendFilter{Tags: &ce.SpendValues{Key: "team", Values: []string{"a"}}}},
			}},
			want: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("123456789012", "us-east-1")
			b.RecordCharge(ce.CostEntry{
				Date: "2001-01-01", Service: "X", UnblendedCost: 10, Tags: map[string]string{"team": "a"},
			})
			b.RecordCharge(ce.CostEntry{Date: "2001-01-01", Service: "Y", UnblendedCost: 7})

			got := b.LedgerSpendFiltered("2001-01-01", "2001-01-02", tc.filter, nil, "UnblendedCost")
			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}
}
