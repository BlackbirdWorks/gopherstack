package ce

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// GetSavingsPlansUtilization returns a synthetic savings-plans utilization aggregate.
func (b *InMemoryBackend) GetSavingsPlansUtilization(
	start, end string,
) *SavingsPlansUtilizationResult {
	b.mu.RLock("GetSavingsPlansUtilization")
	defer b.mu.RUnlock()

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		total += e.BlendedCost
	}

	commitment := total * spCommitmentRatio
	used := commitment * spUsedCommitmentRatio
	unused := commitment - used

	return &SavingsPlansUtilizationResult{
		Utilization: SavingsPlansUtilizationAgg{
			TotalCommitment:       fmt.Sprintf("%.4f", commitment),
			UsedCommitment:        fmt.Sprintf("%.4f", used),
			UnusedCommitment:      fmt.Sprintf("%.4f", unused),
			UtilizationPercentage: spUtilizationPct,
		},
		Savings: SavingsPlansSavings{
			NetSavings:             fmt.Sprintf("%.4f", total*spNetSavingsRatio),
			OnDemandCostEquivalent: fmt.Sprintf("%.4f", total),
		},
		AmortizedCommitment: SavingsPlansAmortized{
			AmortizedRecurringCommitment: fmt.Sprintf("%.4f", commitment),
			AmortizedUpfrontCommitment:   zeroAmountStr,
			TotalAmortizedCommitment:     fmt.Sprintf("%.4f", commitment),
		},
	}
}

// GetSavingsPlansUtilizationDetails returns per-plan utilization details.
func (b *InMemoryBackend) GetSavingsPlansUtilizationDetails(
	start, end string,
) []SavingsPlansUtilizationDetail {
	b.mu.RLock("GetSavingsPlansUtilizationDetails")
	defer b.mu.RUnlock()

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		total += e.BlendedCost
	}

	if total == 0 {
		total = syntheticBaseMonthlyTotal
	}

	commitment := total * spCommitmentRatio
	used := commitment * spUsedCommitmentRatio

	return []SavingsPlansUtilizationDetail{
		{
			SavingsPlanARN: arn.Build(
				"savingsplans",
				"",
				b.accountID,
				"savingsplan/synthetic-sp-1",
			),
			Utilization: SavingsPlansUtilizationAgg{
				TotalCommitment:       fmt.Sprintf("%.4f", commitment),
				UsedCommitment:        fmt.Sprintf("%.4f", used),
				UnusedCommitment:      fmt.Sprintf("%.4f", commitment-used),
				UtilizationPercentage: spUtilizationPct,
			},
			Savings: SavingsPlansSavings{
				NetSavings:             fmt.Sprintf("%.4f", total*spNetSavingsRatio),
				OnDemandCostEquivalent: fmt.Sprintf("%.4f", total),
			},
			AmortizedCommitment: SavingsPlansAmortized{
				AmortizedRecurringCommitment: fmt.Sprintf("%.4f", commitment),
				AmortizedUpfrontCommitment:   zeroAmountStr,
				TotalAmortizedCommitment:     fmt.Sprintf("%.4f", commitment),
			},
			Attributes: map[string]string{
				"SavingsPlansType": defaultSavingsPlansType,
				mapKeyRegion:       b.region,
				"InstanceFamily":   "m5",
				"PaymentOption":    "No Upfront",
			},
		},
	}
}
