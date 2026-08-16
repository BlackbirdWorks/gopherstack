package ce

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

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

// CreateSavingsPlansGeneration starts a new Savings Plans purchase recommendation
// generation job and persists it, mirroring the CommitmentAnalysis
// start/persist/list/get pattern used elsewhere in this backend.
func (b *InMemoryBackend) CreateSavingsPlansGeneration() *SavingsPlansGeneration {
	b.mu.Lock("CreateSavingsPlansGeneration")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	estimated := now.Add(analysisETAMinutes * time.Minute)
	g := &SavingsPlansGeneration{
		RecommendationID:        uuid.NewString(),
		GenerationStatus:        statusProcessing,
		GenerationStartedTime:   now.Format(time.RFC3339),
		EstimatedCompletionTime: estimated.Format(time.RFC3339),
	}
	b.savingsPlansGenerations.Put(g)

	return g
}

// ListSavingsPlansGenerations returns generation jobs, optionally filtered by
// GenerationStatus, most recently started first.
func (b *InMemoryBackend) ListSavingsPlansGenerations(status string) []*SavingsPlansGeneration {
	b.mu.RLock("ListSavingsPlansGenerations")
	defer b.mu.RUnlock()

	all := b.savingsPlansGenerations.All()
	result := make([]*SavingsPlansGeneration, 0, len(all))

	for _, g := range all {
		if status != "" && g.GenerationStatus != status {
			continue
		}

		cp := *g
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].GenerationStartedTime > result[j].GenerationStartedTime
	})

	return result
}
