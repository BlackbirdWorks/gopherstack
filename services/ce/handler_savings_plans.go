package ce

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type getSavingsPlanPurchaseRecommendationDetailsInput struct {
	RecommendationDetailID string `json:"RecommendationDetailId"`
}

type getSavingsPlanPurchaseRecommendationDetailsOutput struct {
	RecommendationDetail   any    `json:"RecommendationDetail,omitempty"`
	RecommendationDetailID string `json:"RecommendationDetailId,omitempty"`
}

func (h *Handler) handleGetSavingsPlanPurchaseRecommendationDetails(
	_ context.Context,
	_ *getSavingsPlanPurchaseRecommendationDetailsInput,
) (*getSavingsPlanPurchaseRecommendationDetailsOutput, error) {
	return &getSavingsPlanPurchaseRecommendationDetailsOutput{}, nil
}

type getSavingsPlansCoverageInput struct {
	Filter      any               `json:"Filter"`
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	NextToken   string            `json:"NextToken"`
	GroupBy     []groupBySpec     `json:"GroupBy"`
	Metrics     []string          `json:"Metrics"`
	MaxResults  int               `json:"MaxResults"`
}

type savingsPlanCoverage struct {
	Attributes map[string]string `json:"Attributes,omitempty"`
	Coverage   map[string]string `json:"Coverage,omitempty"`
	TimePeriod map[string]string `json:"TimePeriod,omitempty"`
}

type getSavingsPlansCoverageOutput struct {
	NextToken             string                `json:"NextToken,omitempty"`
	SavingsPlansCoverages []savingsPlanCoverage `json:"SavingsPlansCoverages"`
}

func (h *Handler) handleGetSavingsPlansCoverage(
	ctx context.Context,
	in *getSavingsPlansCoverageInput,
) (*getSavingsPlansCoverageOutput, error) {
	start, end := defaultStartDate, defaultEndDate
	if in.TimePeriod != nil {
		if s := in.TimePeriod["Start"]; s != "" {
			start = s
		}
		if e := in.TimePeriod["End"]; e != "" {
			end = e
		}
	}

	spUtil := h.Backend.GetSavingsPlansUtilization(start, end)

	coverages := []savingsPlanCoverage{
		{
			Attributes: map[string]string{
				"SavingsPlansType": handlerSavingsPlansType,
				"Region":           ceRegion(ctx),
			},
			Coverage: map[string]string{
				"OnDemandCost":              spUtil.Savings.OnDemandCostEquivalent,
				"SpendCoveredBySavingsPlan": spUtil.Utilization.UsedCommitment,
				"TotalCost":                 spUtil.Savings.OnDemandCostEquivalent,
				"CoveragePercentage":        handlerCoverPct,
			},
			TimePeriod: map[string]string{timePeriodKeyStart: start, timePeriodKeyEnd: end},
		},
	}

	return &getSavingsPlansCoverageOutput{
		SavingsPlansCoverages: coverages,
	}, nil
}

type getSavingsPlansPurchaseRecommendationInput struct {
	SavingsPlansType     string `json:"SavingsPlansType"`
	TermInYears          string `json:"TermInYears"`
	PaymentOption        string `json:"PaymentOption"`
	LookbackPeriodInDays string `json:"LookbackPeriodInDays"`
	AccountScope         string `json:"AccountScope"`
	NextPageToken        string `json:"NextPageToken"`
	PageSize             int    `json:"PageSize"`
}

type savingsPlansPurchaseRecommendation struct {
	RecommendationSummary map[string]string `json:"SavingsPlansPurchaseRecommendationSummary,omitempty"`
	SavingsPlansType      string            `json:"SavingsPlansType"`
	TermInYears           string            `json:"TermInYears"`
	PaymentOption         string            `json:"PaymentOption"`
	LookbackPeriodInDays  string            `json:"LookbackPeriodInDays"`
	RecommendationDetails []map[string]any  `json:"SavingsPlansPurchaseRecommendationDetails"`
}

type getSavingsPlansPurchaseRecommendationOutput struct {
	Metadata               map[string]string                   `json:"Metadata,omitempty"`
	PurchaseRecommendation *savingsPlansPurchaseRecommendation `json:"SavingsPlansPurchaseRecommendation,omitempty"`
	NextPageToken          string                              `json:"NextPageToken,omitempty"`
}

func (h *Handler) handleGetSavingsPlansPurchaseRecommendation(
	ctx context.Context,
	in *getSavingsPlansPurchaseRecommendationInput,
) (*getSavingsPlansPurchaseRecommendationOutput, error) {
	end := "2024-01-01"
	start := "2023-10-01"

	spUtil := h.Backend.GetSavingsPlansUtilization(start, end)
	spType := in.SavingsPlansType
	if spType == "" {
		spType = handlerSavingsPlansType
	}

	return &getSavingsPlansPurchaseRecommendationOutput{
		PurchaseRecommendation: &savingsPlansPurchaseRecommendation{
			SavingsPlansType:     spType,
			TermInYears:          in.TermInYears,
			PaymentOption:        in.PaymentOption,
			LookbackPeriodInDays: in.LookbackPeriodInDays,
			RecommendationDetails: []map[string]any{
				{
					"SavingsPlansDetails": map[string]string{
						"Region":         ceRegion(ctx),
						"InstanceFamily": "m5",
						"OfferingId":     "synthetic-sp-offer-1",
					},
					"AccountId":             awsmeta.Account(ctx),
					"UpfrontCost":           handlerZeroAmount,
					"EstimatedROI":          handlerROI,
					handlerCurrencyCode:     metricUnitUSD,
					"EstimatedSPCost":       spUtil.Utilization.TotalCommitment,
					"EstimatedOnDemandCost": spUtil.Savings.OnDemandCostEquivalent,
					"EstimatedOnDemandCostWithCurrentCommitment": spUtil.Savings.OnDemandCostEquivalent,
					"EstimatedSavingsAmount":                     spUtil.Savings.NetSavings,
					"EstimatedSavingsPercentage":                 handlerROI,
					"HourlyCommitmentToPurchase":                 "1.0000",
					"EstimatedAverageUtilization":                handlerSPUtilPct,
					"EstimatedMonthlySavingsAmount":              spUtil.Savings.NetSavings,
					"CurrentMinimumHourlyOnDemandSpend":          "1.5000",
					"CurrentMaximumHourlyOnDemandSpend":          "3.0000",
					"CurrentAverageHourlyOnDemandSpend":          "2.0000",
				},
			},
			RecommendationSummary: map[string]string{
				"EstimatedROI":                               handlerROI,
				handlerCurrencyCode:                          metricUnitUSD,
				"EstimatedTotalCost":                         spUtil.Utilization.TotalCommitment,
				"CurrentOnDemandSpend":                       spUtil.Savings.OnDemandCostEquivalent,
				"EstimatedSavingsAmount":                     spUtil.Savings.NetSavings,
				"TotalRecommendationCount":                   "1",
				"DailyCommitmentToPurchase":                  "24.0000",
				"HourlyCommitmentToPurchase":                 "1.0000",
				"EstimatedSavingsPercentage":                 handlerROI,
				"EstimatedMonthlySavingsAmount":              spUtil.Savings.NetSavings,
				"EstimatedOnDemandCostWithCurrentCommitment": spUtil.Savings.OnDemandCostEquivalent,
			},
		},
		Metadata: map[string]string{
			"RecommendationTotalCount": "1",
			"GenerationTimestamp":      "2024-01-01T00:00:00Z",
			"AdditionalMetadata":       "lookback=30days",
		},
	}, nil
}

type getSavingsPlansUtilizationInput struct {
	Filter      any               `json:"Filter"`
	SortBy      any               `json:"SortBy"`
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
}

type getSavingsPlansUtilizationByTimeEntry struct {
	TimePeriod          map[string]string          `json:"TimePeriod"`
	Utilization         SavingsPlansUtilizationAgg `json:"Utilization"`
	Savings             SavingsPlansSavings        `json:"Savings"`
	AmortizedCommitment SavingsPlansAmortized      `json:"AmortizedCommitment"`
}

type getSavingsPlansUtilizationOutput struct {
	Total                          *SavingsPlansUtilizationResult          `json:"Total,omitempty"`
	SavingsPlansUtilizationsByTime []getSavingsPlansUtilizationByTimeEntry `json:"SavingsPlansUtilizationsByTime"`
}

func (h *Handler) handleGetSavingsPlansUtilization(
	_ context.Context,
	in *getSavingsPlansUtilizationInput,
) (*getSavingsPlansUtilizationOutput, error) {
	start, end := defaultStartDate, defaultEndDate
	if in.TimePeriod != nil {
		if s := in.TimePeriod["Start"]; s != "" {
			start = s
		}
		if e := in.TimePeriod["End"]; e != "" {
			end = e
		}
	}

	granularity := in.Granularity
	if granularity == "" {
		granularity = defaultGranularity
	}

	total := h.Backend.GetSavingsPlansUtilization(start, end)
	buckets := buildTimeBuckets(start, end, granularity)

	byTime := make([]getSavingsPlansUtilizationByTimeEntry, 0, len(buckets))

	for _, bucket := range buckets {
		bucketUtil := h.Backend.GetSavingsPlansUtilization(bucket.start, bucket.end)
		byTime = append(byTime, getSavingsPlansUtilizationByTimeEntry{
			TimePeriod:          map[string]string{"Start": bucket.start, "End": bucket.end},
			Utilization:         bucketUtil.Utilization,
			Savings:             bucketUtil.Savings,
			AmortizedCommitment: bucketUtil.AmortizedCommitment,
		})
	}

	return &getSavingsPlansUtilizationOutput{
		Total:                          total,
		SavingsPlansUtilizationsByTime: byTime,
	}, nil
}

type getSavingsPlansUtilizationDetailsInput struct {
	Filter     any               `json:"Filter"`
	SortBy     any               `json:"SortBy"`
	TimePeriod map[string]string `json:"TimePeriod"`
	NextToken  string            `json:"NextToken"`
	Fields     []string          `json:"Fields"`
	MaxResults int               `json:"MaxResults"`
}

type getSavingsPlansUtilizationDetailsOutput struct {
	NextToken                      string                          `json:"NextToken,omitempty"`
	Total                          *SavingsPlansUtilizationResult  `json:"Total,omitempty"`
	TimePeriod                     map[string]string               `json:"TimePeriod,omitempty"`
	SavingsPlansUtilizationDetails []SavingsPlansUtilizationDetail `json:"SavingsPlansUtilizationDetails"`
}

func (h *Handler) handleGetSavingsPlansUtilizationDetails(
	_ context.Context,
	in *getSavingsPlansUtilizationDetailsInput,
) (*getSavingsPlansUtilizationDetailsOutput, error) {
	start, end := defaultStartDate, defaultEndDate
	if in.TimePeriod != nil {
		if s := in.TimePeriod["Start"]; s != "" {
			start = s
		}
		if e := in.TimePeriod["End"]; e != "" {
			end = e
		}
	}

	details := h.Backend.GetSavingsPlansUtilizationDetails(start, end)
	total := h.Backend.GetSavingsPlansUtilization(start, end)

	if details == nil {
		details = []SavingsPlansUtilizationDetail{}
	}

	return &getSavingsPlansUtilizationDetailsOutput{
		SavingsPlansUtilizationDetails: details,
		Total:                          total,
		TimePeriod:                     map[string]string{"Start": start, "End": end},
	}, nil
}

type listSavingsPlansPurchaseRecommendationGenerationInput struct {
	GenerationStatus string `json:"GenerationStatus"`
	NextPageToken    string `json:"NextPageToken"`
	PageSize         int    `json:"PageSize"`
}

type listSavingsPlansPurchaseRecommendationGenerationOutput struct {
	NextPageToken         string `json:"NextPageToken,omitempty"`
	GenerationSummaryList []any  `json:"GenerationSummaryList"`
}

func (h *Handler) handleListSavingsPlansPurchaseRecommendationGeneration(
	_ context.Context,
	_ *listSavingsPlansPurchaseRecommendationGenerationInput,
) (*listSavingsPlansPurchaseRecommendationGenerationOutput, error) {
	return &listSavingsPlansPurchaseRecommendationGenerationOutput{
		GenerationSummaryList: []any{},
	}, nil
}

type startSavingsPlansPurchaseRecommendationGenerationInput struct{}

type startSavingsPlansPurchaseRecommendationGenerationOutput struct {
	GenerationID            string `json:"GenerationId,omitempty"`
	GenerationStartedTime   string `json:"GenerationStartedTime,omitempty"`
	EstimatedCompletionTime string `json:"EstimatedCompletionTime,omitempty"`
}

func (h *Handler) handleStartSavingsPlansPurchaseRecommendationGeneration(
	_ context.Context,
	_ *startSavingsPlansPurchaseRecommendationGenerationInput,
) (*startSavingsPlansPurchaseRecommendationGenerationOutput, error) {
	return &startSavingsPlansPurchaseRecommendationGenerationOutput{}, nil
}

// buildSavingsPlansOps returns the savings-plans-family op dispatch entries.
func (h *Handler) buildSavingsPlansOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetSavingsPlanPurchaseRecommendationDetails": service.WrapOp(
			h.handleGetSavingsPlanPurchaseRecommendationDetails,
		),
		"GetSavingsPlansCoverage": service.WrapOp(
			h.handleGetSavingsPlansCoverage,
		),
		"GetSavingsPlansPurchaseRecommendation": service.WrapOp(
			h.handleGetSavingsPlansPurchaseRecommendation,
		),
		"GetSavingsPlansUtilization": service.WrapOp(
			h.handleGetSavingsPlansUtilization,
		),
		"GetSavingsPlansUtilizationDetails": service.WrapOp(
			h.handleGetSavingsPlansUtilizationDetails,
		),
		"ListSavingsPlansPurchaseRecommendationGeneration": service.WrapOp(
			h.handleListSavingsPlansPurchaseRecommendationGeneration,
		),
		"StartSavingsPlansPurchaseRecommendationGeneration": service.WrapOp(
			h.handleStartSavingsPlansPurchaseRecommendationGeneration,
		),
	}
}
