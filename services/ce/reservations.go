package ce

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// GetReservationUtilization returns synthetic RI utilization by time.
func (b *InMemoryBackend) GetReservationUtilization(
	start, end, granularity string,
) []ReservationUtilizationByTime {
	return b.GetReservationUtilizationFiltered(start, end, granularity, nil)
}

// GetReservationUtilizationFiltered is GetReservationUtilization narrowed to
// ledger entries whose Service is in serviceFilter (when non-empty), giving
// GetReservationUtilizationInput.Filter's SERVICE dimension a real,
// non-fabricated effect. Other documented Filter dimensions (AZ, PLATFORM,
// TENANCY, ...) have no per-entry breakdown in this emulator's ledger and are
// not applied.
func (b *InMemoryBackend) GetReservationUtilizationFiltered(
	start, end, granularity string, serviceFilter []string,
) []ReservationUtilizationByTime {
	b.mu.RLock("GetReservationUtilizationFiltered")
	defer b.mu.RUnlock()

	buckets := buildTimeBuckets(start, end, granularity)
	result := make([]ReservationUtilizationByTime, 0, len(buckets))

	for _, bucket := range buckets {
		var total float64
		for _, e := range b.costLedgerInBucket(bucket.start, bucket.end) {
			if !strings.Contains(e.Service, "Elastic Compute Cloud") {
				continue
			}

			if len(serviceFilter) > 0 && !stringSliceContainsFold(serviceFilter, e.Service) {
				continue
			}

			total += e.BlendedCost
		}

		purchased := total * riPurchasedCostRatio
		actual := purchased * riActualUsageRatio
		unused := purchased - actual

		result = append(result, ReservationUtilizationByTime{
			TimePeriod: map[string]string{timePeriodKeyStart: bucket.start, timePeriodKeyEnd: bucket.end},
			Groups:     []any{},
			Total: ReservationUtilizationAgg{
				UtilizationPercentage:     riUtilizationPct,
				PurchasedHours:            fmt.Sprintf("%.4f", purchased*costToHoursMultiplier),
				TotalActualHours:          fmt.Sprintf("%.4f", actual*costToHoursMultiplier),
				UnusedHours:               fmt.Sprintf("%.4f", unused*costToHoursMultiplier),
				OnDemandCostOfRIHoursUsed: fmt.Sprintf("%.4f", actual),
				NetRISavings:              fmt.Sprintf("%.4f", total*riNetSavingsRatio),
				TotalPotentialRISavings:   fmt.Sprintf("%.4f", total*riPotentialSavingsRatio),
				AmortizedUpfrontFee:       zeroAmountStr,
				AmortizedRecurringFee:     fmt.Sprintf("%.4f", purchased*riAmortizedFeeRatio),
				TotalAmortizedFee:         fmt.Sprintf("%.4f", purchased*riAmortizedFeeRatio),
				RICostForUnusedHours:      fmt.Sprintf("%.4f", unused*riAmortizedFeeRatio),
				RealizedSavings:           fmt.Sprintf("%.4f", total*riRealizedSavingsRatio),
				UnrealizedSavings:         fmt.Sprintf("%.4f", total*riUnrealizedSavingsRatio),
			},
		})
	}

	return result
}

// GetReservationCoverage returns synthetic RI coverage by time.
func (b *InMemoryBackend) GetReservationCoverage(
	start, end, granularity string,
) []ReservationCoverageByTime {
	return b.GetReservationCoverageFiltered(start, end, granularity, nil)
}

// GetReservationCoverageFiltered is GetReservationCoverage narrowed to ledger
// entries whose Service is in serviceFilter (when non-empty), giving
// GetReservationCoverageInput.Filter's SERVICE dimension a real,
// non-fabricated effect. Other documented Filter dimensions (AZ, PLATFORM,
// TENANCY, ...) have no per-entry breakdown in this emulator's ledger and are
// not applied.
func (b *InMemoryBackend) GetReservationCoverageFiltered(
	start, end, granularity string, serviceFilter []string,
) []ReservationCoverageByTime {
	b.mu.RLock("GetReservationCoverageFiltered")
	defer b.mu.RUnlock()

	buckets := buildTimeBuckets(start, end, granularity)
	result := make([]ReservationCoverageByTime, 0, len(buckets))

	for _, bucket := range buckets {
		var total float64
		for _, e := range b.costLedgerInBucket(bucket.start, bucket.end) {
			if len(serviceFilter) > 0 && !stringSliceContainsFold(serviceFilter, e.Service) {
				continue
			}

			total += e.BlendedCost
		}

		hours := total * costToHoursMultiplier
		riHours := hours * riCoverageRatio
		odHours := hours - riHours

		result = append(result, ReservationCoverageByTime{
			TimePeriod: map[string]string{timePeriodKeyStart: bucket.start, timePeriodKeyEnd: bucket.end},
			Groups:     []any{},
			Total: ReservationCoverageAgg{
				CoverageHours: ReservationCoverageHours{
					OnDemandHours:           fmt.Sprintf("%.4f", odHours),
					ReservedHours:           fmt.Sprintf("%.4f", riHours),
					TotalRunningHours:       fmt.Sprintf("%.4f", hours),
					CoverageHoursPercentage: riCoveragePct,
				},
				CoverageNormalizedUnits: ReservationCoverageNormalizedUnits{
					OnDemandNormalizedUnits:           fmt.Sprintf("%.4f", odHours*normalizedUnitsPerHour),
					ReservedNormalizedUnits:           fmt.Sprintf("%.4f", riHours*normalizedUnitsPerHour),
					TotalRunningNormalizedUnits:       fmt.Sprintf("%.4f", hours*normalizedUnitsPerHour),
					CoverageNormalizedUnitsPercentage: riCoveragePct,
				},
				CoverageCost: ReservationCoverageCost{
					OnDemandCost: fmt.Sprintf("%.4f", odHours*onDemandCostRate),
				},
			},
		})
	}

	return result
}

func (b *InMemoryBackend) riDetail(
	monthlyCost, riMonthlyCost, savings float64, termMonths int,
) ReservationRecommendationDetail {
	return ReservationRecommendationDetail{
		AccountID: b.accountID,
		InstanceDetails: map[string]any{
			"EC2InstanceDetails": map[string]string{
				"InstanceType": syntheticInstanceType,
				mapKeyRegion:   b.region,
				"Platform":     "Linux/UNIX",
			},
		},
		RecommendedNumberOfInstancesToPurchase:    "2",
		RecommendedNormalizedUnitsToPurchase:      "16",
		MinimumNumberOfInstancesUsedPerHour:       "1",
		MinimumNormalizedUnitsUsedPerHour:         "8",
		MaximumNumberOfInstancesUsedPerHour:       "3",
		MaximumNormalizedUnitsUsedPerHour:         "24",
		AverageNumberOfInstancesUsedPerHour:       "2",
		AverageNormalizedUnitsUsedPerHour:         "16",
		AverageUtilization:                        "80.0000",
		EstimatedBreakEvenInMonths:                strconv.Itoa(termMonths / riBreakEvenDivisor),
		CurrencyCode:                              metricUnitUSD,
		EstimatedMonthlySavingsAmount:             fmt.Sprintf("%.4f", savings),
		EstimatedMonthlySavingsPercentage:         "40.0000",
		EstimatedMonthlyOnDemandCost:              fmt.Sprintf("%.4f", monthlyCost),
		EstimatedReservationCostForLookbackPeriod: fmt.Sprintf("%.4f", riMonthlyCost*float64(termMonths)),
		UpfrontCost:                  fmt.Sprintf("%.4f", riMonthlyCost*float64(termMonths)*riUpfrontSplitRatio),
		RecurringStandardMonthlyCost: fmt.Sprintf("%.4f", riMonthlyCost*riUpfrontSplitRatio),
	}
}

// GetReservationPurchaseRecommendations returns synthetic RI purchase recommendations.
func (b *InMemoryBackend) GetReservationPurchaseRecommendations(
	service, lookback, term, payment string,
) []ReservationRecommendation {
	b.mu.RLock("GetReservationPurchaseRecommendations")
	defer b.mu.RUnlock()

	days := daysPerMonth
	switch lookback {
	case "SIXTY_DAYS":
		days = 60
	case "SEVEN_DAYS":
		days = 7
	}

	end := time.Now().UTC().Format("2006-01-02")
	start := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02")

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		if service == "" || strings.EqualFold(e.Service, service) ||
			strings.Contains(strings.ToLower(e.Service), strings.ToLower(service)) {
			total += e.BlendedCost
		}
	}

	if total == 0 {
		return nil
	}

	upfrontMultiplier := 1.0
	switch payment {
	case "ALL_UPFRONT":
		upfrontMultiplier = 0.90
	case "PARTIAL_UPFRONT":
		upfrontMultiplier = 0.95
	}

	termMonths := 12
	if term == "THREE_YEARS" {
		termMonths = 36
		upfrontMultiplier *= spUsedCommitmentRatio
	}

	monthlyCost := total / float64(days) * daysPerMonth
	riMonthlyCost := monthlyCost * upfrontMultiplier * riMonthlyCostRatio
	savings := monthlyCost - riMonthlyCost

	return []ReservationRecommendation{
		{
			AccountScope:         accountScopeLinked,
			LookbackPeriodInDays: lookback,
			TermInYears:          term,
			PaymentOption:        payment,
			ServiceSpecification: map[string]any{
				"EC2Specification": map[string]string{"OfferingClass": "STANDARD"},
			},
			RecommendationDetails: []ReservationRecommendationDetail{
				b.riDetail(monthlyCost, riMonthlyCost, savings, termMonths),
			},
			RecommendationSummary: map[string]string{
				"TotalEstimatedMonthlySavingsAmount":     fmt.Sprintf("%.4f", savings),
				"TotalEstimatedMonthlySavingsPercentage": "40.0000",
				mapKeyCurrencyCode:                       metricUnitUSD,
			},
		},
	}
}

// GetRightsizingRecommendations returns synthetic rightsizing recommendations.
func (b *InMemoryBackend) GetRightsizingRecommendations(
	_ string,
) []RightsizingRecommendation {
	b.mu.RLock("GetRightsizingRecommendations")
	defer b.mu.RUnlock()

	end := time.Now().UTC().Format("2006-01-02")
	start := time.Now().UTC().AddDate(0, 0, -14).Format("2006-01-02")

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		if strings.Contains(e.Service, "Elastic Compute Cloud") {
			total += e.BlendedCost
		}
	}

	if total == 0 {
		return nil
	}

	instanceID := fmt.Sprintf("i-synthetic%s", b.accountID[:8])
	resourceARN := arn.Build("ec2", b.region, b.accountID, fmt.Sprintf("instance/%s", instanceID))

	return []RightsizingRecommendation{
		{
			AccountID: b.accountID,
			CurrentInstance: RightsizingCurrentInstance{
				ResourceID:   resourceARN,
				InstanceType: "t3.large",
				MonthlyCost:  fmt.Sprintf("%.4f", total/14*daysPerMonth),
				CurrencyCode: metricUnitUSD,
			},
			RightsizingType: "MODIFY",
			ModifyRecommendationDetail: &RightsizingModifyDetail{
				TargetInstances: []RightsizingTargetInstance{
					{
						EstimatedMonthlyCost:       fmt.Sprintf("%.4f", total/14*daysPerMonth*rightsizingSavingsRatio),
						EstimatedMonthlySavings:    fmt.Sprintf("%.4f", total/14*daysPerMonth*rightsizingSavingsRatio),
						EstimatedSavingsPercentage: "50.0000",
						CurrencyCode:               "USD",
						DefaultTargetInstance:      true,
						ResourceDetails: map[string]any{
							"EC2ResourceDetails": map[string]string{
								"InstanceType":    "t3.medium",
								mapKeyRegion:      b.region,
								"Platform":        "Linux/UNIX",
								"Tenancy":         "Shared",
								"OperatingSystem": "Linux",
							},
						},
					},
				},
			},
		},
	}
}
