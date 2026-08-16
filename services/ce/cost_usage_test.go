package ce_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestInMemoryBackend_GetDimensionValues verifies backend direct calls.
func TestInMemoryBackend_GetDimensionValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dimension string
		name      string
		wantEmpty bool
	}{
		{name: "service", dimension: "SERVICE", wantEmpty: false},
		{name: "region", dimension: "REGION", wantEmpty: false},
		{name: "usage_type", dimension: "USAGE_TYPE", wantEmpty: false},
		{name: "linked_account", dimension: "LINKED_ACCOUNT", wantEmpty: false},
		{name: "instance_type", dimension: "INSTANCE_TYPE", wantEmpty: false},
		{name: "operating_system", dimension: "OPERATING_SYSTEM", wantEmpty: false},
		{name: "unknown_dim", dimension: "UNKNOWN_DIM_XYZ", wantEmpty: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("000000000000", "us-east-1")
			vals := b.GetDimensionValues(tt.dimension)

			if tt.wantEmpty {
				assert.Empty(t, vals)
			} else {
				assert.NotEmpty(t, vals)
			}
		})
	}
}

// TestInMemoryBackend_GetCostAndUsage_MultipleMetrics verifies multi-metric aggregation.
func TestInMemoryBackend_GetCostAndUsage_MultipleMetrics(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")
	results := b.GetCostAndUsage(
		"2026-03-01", "2026-04-01", "MONTHLY",
		[]string{"BlendedCost", "UnblendedCost", "UsageQuantity"},
		nil,
	)

	require.NotEmpty(t, results)

	first := results[0]
	_, hasBlended := first.Total["BlendedCost"]
	_, hasUnblended := first.Total["UnblendedCost"]
	_, hasUsage := first.Total["UsageQuantity"]

	assert.True(t, hasBlended, "Total must have BlendedCost")
	assert.True(t, hasUnblended, "Total must have UnblendedCost")
	assert.True(t, hasUsage, "Total must have UsageQuantity")
	assert.Equal(t, "USD", first.Total["BlendedCost"].Unit)
	assert.Equal(t, "N/A", first.Total["UsageQuantity"].Unit)
}

// TestInMemoryBackend_Reset_ReSeedsLedger verifies the ledger is re-seeded after reset.
func TestInMemoryBackend_Reset_ReSeedsLedger(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")

	// Ledger should have data before reset
	pre := b.GetDimensionValues("SERVICE")
	assert.NotEmpty(t, pre)

	b.Reset()

	// Ledger should still have data after reset (re-seeded)
	post := b.GetDimensionValues("SERVICE")
	assert.NotEmpty(t, post)
}

// TestInMemoryBackend_GetForecastByTime_VariousBuckets verifies forecast bucket generation.
func TestInMemoryBackend_GetForecastByTime_VariousBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		start       string
		end         string
		granularity string
		wantBuckets int
	}{
		{
			name:        "monthly_3_buckets",
			start:       "2026-06-01",
			end:         "2026-09-01",
			granularity: "MONTHLY",
			wantBuckets: 3,
		},
		{
			name:        "daily_7_buckets",
			start:       "2026-06-01",
			end:         "2026-06-08",
			granularity: "DAILY",
			wantBuckets: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("000000000000", "us-east-1")
			buckets, totalMean, totalLo, totalHi := b.GetForecastByTime(
				tt.start, tt.end, tt.granularity, 80,
			)

			assert.Len(t, buckets, tt.wantBuckets)
			assert.Positive(t, totalMean)
			assert.InDelta(t, totalLo, totalMean, totalMean*2.0)
			assert.InDelta(t, totalHi, totalMean, totalMean*2.0)
		})
	}
}
