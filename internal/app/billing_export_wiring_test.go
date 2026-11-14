package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
)

func TestCECostSourceCarriesLedgerDimensions(t *testing.T) {
	t.Parallel()

	b := cebackend.NewInMemoryBackend("123456789012", "us-east-1")
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	b.RecordCharge(cebackend.CostEntry{
		Date: day.Format("2006-01-02"), Service: "Amazon Elastic Compute Cloud - Compute", Region: "us-east-1",
		Account: "123456789012", ChargeType: "Tax", AvailabilityZone: "us-east-1b", Operation: "RunInstances",
		Tags: map[string]string{"team": "blue"}, UnblendedCost: 3,
	})

	lines := ceCostSource{ce: b}.CostLines(day, day.AddDate(0, 0, 1))

	var tax, usage int

	for _, l := range lines {
		switch l.ChargeType {
		case "Tax":
			tax++

			assert.Equal(t, "us-east-1b", l.AvailabilityZone)
			assert.Equal(t, "blue", l.Tags["team"])
			assert.InDelta(t, 3, l.Unblended, 1e-9)
		case "":
			usage++
		}
	}

	require.Equal(t, 1, tax)
	assert.Positive(t, usage)
}

func TestBudgetsCESpendLedgerDimensionsAndChargeTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filters map[string][]string
		name    string
		want    float64
	}{
		{name: "tax in az", filters: map[string][]string{"AZ": {"zz-1a"}}, want: 7},
		{name: "tax excluded", filters: map[string][]string{"AZ": {"zz-1a"}, "ExcludeChargeType": {"Tax"}}, want: 0},
		{name: "other az", filters: map[string][]string{"AZ": {"zz-1b"}}, want: 0},
		{name: "operation", filters: map[string][]string{"Operation": {"RunInstances"}, "AZ": {"zz-1a"}}, want: 7},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cebackend.NewInMemoryBackend("123456789012", "us-east-1")
			b.Reset()
			b.RecordCharge(cebackend.CostEntry{
				Date: "2030-01-02", Service: "svc", ChargeType: "Tax", AvailabilityZone: "zz-1a",
				Operation: "RunInstances", UnblendedCost: 7,
			})

			from, to := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2030, 1, 3, 0, 0, 0, 0, time.UTC)
			got, ok := budgetsCESpend{ce: b}.Spend(from, to, tc.filters, "UnblendedCost")
			assert.True(t, ok)
			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}
}
