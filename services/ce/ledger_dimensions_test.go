package ce_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

func TestLedgerSpendNarrowsOnDimensions(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	start := now.AddDate(0, 0, -10).Format("2006-01-02")
	end := now.AddDate(0, 0, -5).Format("2006-01-02")

	tests := []struct {
		filters map[string][]string
		name    string
		wantPos bool
	}{
		{name: "az hit", filters: map[string][]string{"AZ": {"us-east-1a"}}, wantPos: true},
		{name: "az miss", filters: map[string][]string{"AZ": {"us-east-1z"}}},
		{name: "instance type hit", filters: map[string][]string{"InstanceType": {"t3.medium"}}, wantPos: true},
		{name: "instance type miss", filters: map[string][]string{"InstanceType": {"m5.large"}}},
		{name: "operation hit", filters: map[string][]string{"Operation": {"RunInstances"}}, wantPos: true},
		{name: "operation miss", filters: map[string][]string{"Operation": {"Nope"}}},
		{name: "purchase type hit", filters: map[string][]string{"PurchaseType": {"On Demand"}}, wantPos: true},
		{name: "purchase type miss", filters: map[string][]string{"PurchaseType": {"Spot"}}},
		{
			name:    "usage type group",
			filters: map[string][]string{"UsageTypeGroup": {"EC2: Running Hours"}},
			wantPos: true,
		},
		{
			name:    "legal entity",
			filters: map[string][]string{"LegalEntityName": {"Amazon Web Services, Inc."}},
			wantPos: true,
		},
		{name: "billing entity miss", filters: map[string][]string{"BillingEntity": {"AWS Marketplace"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("123456789012", "us-east-1")
			got := b.LedgerSpend(start, end, tc.filters, "UnblendedCost")
			assert.Equal(t, tc.wantPos, got > 0)
		})
	}
}

func TestLedgerSpendFilteredDimensions(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	start := now.AddDate(0, 0, -10).Format("2006-01-02")
	end := now.AddDate(0, 0, -5).Format("2006-01-02")
	dim := func(key string, vals ...string) *ce.SpendFilter {
		return &ce.SpendFilter{Dimensions: &ce.SpendValues{Key: key, Values: vals}}
	}

	tests := []struct {
		filter  *ce.SpendFilter
		name    string
		wantPos bool
	}{
		{name: "az", filter: dim("AZ", "us-east-1a"), wantPos: true},
		{name: "not az", filter: &ce.SpendFilter{Not: dim("AZ", "us-east-1a")}, wantPos: true},
		{
			name:   "and",
			filter: &ce.SpendFilter{And: []ce.SpendFilter{*dim("OPERATION", "RunInstances"), *dim("AZ", "us-east-1b")}},
		},
		{
			name: "or",
			filter: &ce.SpendFilter{
				Or: []ce.SpendFilter{*dim("OPERATION", "Nope"), *dim("INSTANCE_TYPE", "t3.medium")},
			},
			wantPos: true,
		},
		{name: "charge type", filter: dim("CHARGE_TYPE", "Tax")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("123456789012", "us-east-1")
			got := b.LedgerSpendFiltered(start, end, tc.filter, nil, "UnblendedCost")
			assert.Equal(t, tc.wantPos, got > 0)
		})
	}
}

func TestLedgerEntriesCarryDimensions(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("123456789012", "us-east-1")
	day := time.Now().UTC().AddDate(0, 0, -1)
	entries := b.LedgerEntries(day.Format("2006-01-02"), day.AddDate(0, 0, 1).Format("2006-01-02"))
	require.NotEmpty(t, entries)

	var found bool

	for _, e := range entries {
		if e.Service != "Amazon Elastic Compute Cloud - Compute" {
			continue
		}

		found = true

		assert.Equal(t, "us-east-1a", e.AvailabilityZone)
		assert.Equal(t, "t3.medium", e.InstanceType)
		assert.Equal(t, "RunInstances", e.Operation)
		assert.Equal(t, "Compute Instance", e.ProductFamily)
	}

	assert.True(t, found)
}

func TestGetCostAndUsageGroupsByLedgerDimensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "az", key: "AZ", want: "us-east-1a"},
		{name: "instance type", key: "INSTANCE_TYPE", want: "t3.medium"},
		{name: "operation", key: "OPERATION", want: "RunInstances"},
		{name: "purchase type", key: "PURCHASE_TYPE", want: "On Demand"},
		{name: "usage type group", key: "USAGE_TYPE_GROUP", want: "EC2: Running Hours"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("123456789012", "us-east-1")
			day := time.Now().UTC().AddDate(0, 0, -1)
			res := b.GetCostAndUsage(
				day.Format("2006-01-02"), day.AddDate(0, 0, 1).Format("2006-01-02"), "DAILY",
				[]string{"UnblendedCost"}, []ce.GroupBySpec{{Type: "DIMENSION", Key: tc.key}}, nil,
			)
			require.NotEmpty(t, res)

			var keys []string
			for _, g := range res[0].Groups {
				keys = append(keys, g.Keys...)
			}

			assert.Contains(t, keys, tc.want)
		})
	}
}
