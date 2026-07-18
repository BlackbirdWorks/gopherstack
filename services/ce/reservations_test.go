package ce_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_GetReservationPurchaseRecommendations_PaymentOptions verifies
// that different payment options produce different savings estimates.
func TestInMemoryBackend_GetReservationPurchaseRecommendations_PaymentOptions(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")

	tests := []struct {
		name    string
		payment string
	}{
		{name: "no_upfront", payment: "NO_UPFRONT"},
		{name: "partial_upfront", payment: "PARTIAL_UPFRONT"},
		{name: "all_upfront", payment: "ALL_UPFRONT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			recs := b.GetReservationPurchaseRecommendations(
				"Amazon Elastic Compute Cloud - Compute",
				"THIRTY_DAYS", "ONE_YEAR", tt.payment,
			)
			require.NotEmpty(t, recs)
			require.NotEmpty(t, recs[0].RecommendationDetails)
			assert.NotEmpty(t, recs[0].RecommendationDetails[0].EstimatedMonthlySavingsAmount)
		})
	}
}
