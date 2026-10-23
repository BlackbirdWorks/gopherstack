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
