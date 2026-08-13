package ce

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type getSavingsPlanPurchaseRecommendationDetailsInput struct {
	RecommendationDetailID string `json:"RecommendationDetailId"`
}

// recommendationDetailData mirrors aws-sdk-go-v2/service/costexplorer/types'
// RecommendationDetailData (a subset of its ~20 string-valued fields covering the
// hourly cost/coverage/utilization summary real AWS documents for this op).
type recommendationDetailData struct {
	AccountID                     string `json:"AccountId,omitempty"`
	CurrencyCode                  string `json:"CurrencyCode,omitempty"`
	EstimatedAverageCoverage      string `json:"EstimatedAverageCoverage,omitempty"`
	EstimatedAverageUtilization   string `json:"EstimatedAverageUtilization,omitempty"`
	EstimatedMonthlySavingsAmount string `json:"EstimatedMonthlySavingsAmount,omitempty"`
	EstimatedOnDemandCost         string `json:"EstimatedOnDemandCost,omitempty"`
	EstimatedROI                  string `json:"EstimatedROI,omitempty"`
	EstimatedSPCost               string `json:"EstimatedSPCost,omitempty"`
	EstimatedSavingsAmount        string `json:"EstimatedSavingsAmount,omitempty"`
	EstimatedSavingsPercentage    string `json:"EstimatedSavingsPercentage,omitempty"`
}

// getSavingsPlanPurchaseRecommendationDetailsOutput's field name/JSON key
// ("RecommendationDetailData", not "RecommendationDetail") is field-diffed against real
// AWS CE's GetSavingsPlanPurchaseRecommendationDetailsOutput.
type getSavingsPlanPurchaseRecommendationDetailsOutput struct {
	RecommendationDetailData *recommendationDetailData `json:"RecommendationDetailData,omitempty"`
	RecommendationDetailID   string                    `json:"RecommendationDetailId,omitempty"`
}

func (h *Handler) handleGetSavingsPlanPurchaseRecommendationDetails(
	ctx context.Context,
	in *getSavingsPlanPurchaseRecommendationDetailsInput,
) (*getSavingsPlanPurchaseRecommendationDetailsOutput, error) {
	if in.RecommendationDetailID == "" {
		return nil, fmt.Errorf("%w: RecommendationDetailId is required", ErrValidation)
	}

	spUtil := h.Backend.GetSavingsPlansUtilization(defaultStartDate, defaultEndDate)

	return &getSavingsPlanPurchaseRecommendationDetailsOutput{
		RecommendationDetailID: in.RecommendationDetailID,
		RecommendationDetailData: &recommendationDetailData{
			AccountID:                     awsmeta.Account(ctx),
			CurrencyCode:                  handlerCurrencyCode,
			EstimatedAverageCoverage:      handlerCoverPct,
			EstimatedAverageUtilization:   handlerSPUtilPct,
			EstimatedMonthlySavingsAmount: spUtil.Savings.NetSavings,
			EstimatedOnDemandCost:         spUtil.Savings.OnDemandCostEquivalent,
			EstimatedROI:                  handlerROI,
			EstimatedSPCost:               spUtil.Utilization.TotalCommitment,
			EstimatedSavingsAmount:        spUtil.Savings.NetSavings,
			EstimatedSavingsPercentage:    handlerROI,
		},
	}, nil
}

type getSavingsPlansCoverageInput struct {
	Filter      *ceExpression     `json:"Filter"`
	TimePeriod  map[string]string `json:"TimePeriod"`
	SortBy      *ceSortDefinition `json:"SortBy"`
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

// handleGetSavingsPlansCoverage computes a single synthetic coverage entry
// for the request's region -- this emulator has no per-REGION/SERVICE/
// INSTANCE_FAMILY Savings Plans coverage breakdown to filter across (see
// GetSavingsPlansUtilization), so Filter's only real (non-fabricated) effect
// is on the REGION dimension: since the entry's Region is always ceRegion(ctx),
// a REGION filter that excludes it correctly narrows the result to zero
// items instead of silently ignoring the filter. SortBy on a single-item list
// is documented as inert rather than implemented.
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

	region := ceRegion(ctx)

	if in.Filter != nil && in.Filter.Dimensions != nil && strings.EqualFold(in.Filter.Dimensions.Key, "REGION") &&
		!stringSliceContainsFold(in.Filter.Dimensions.Values, region) {
		return &getSavingsPlansCoverageOutput{SavingsPlansCoverages: []savingsPlanCoverage{}}, nil
	}

	spUtil := h.Backend.GetSavingsPlansUtilization(start, end)

	coverages := []savingsPlanCoverage{
		{
			Attributes: map[string]string{
				"SavingsPlansType": handlerSavingsPlansType,
				"Region":           region,
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
	Filter               *ceExpression `json:"Filter"`
	SavingsPlansType     string        `json:"SavingsPlansType"`
	TermInYears          string        `json:"TermInYears"`
	PaymentOption        string        `json:"PaymentOption"`
	LookbackPeriodInDays string        `json:"LookbackPeriodInDays"`
	AccountScope         string        `json:"AccountScope"`
	NextPageToken        string        `json:"NextPageToken"`
	PageSize             int           `json:"PageSize"`
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

// handleGetSavingsPlansPurchaseRecommendation always synthesizes exactly one
// recommendation for the caller's own account (there is no multi-account
// state in this emulator). Real GetSavingsPlansPurchaseRecommendation only
// documents filtering by the LINKED_ACCOUNT dimension, so that is the one
// Filter clause given a real (non-fabricated) effect here: an account that
// doesn't match the filter genuinely gets no recommendation, rather than the
// filter being silently accepted and ignored.
func (h *Handler) handleGetSavingsPlansPurchaseRecommendation(
	ctx context.Context,
	in *getSavingsPlansPurchaseRecommendationInput,
) (*getSavingsPlansPurchaseRecommendationOutput, error) {
	if !matchesLinkedAccountFilter(in.Filter, awsmeta.Account(ctx)) {
		return &getSavingsPlansPurchaseRecommendationOutput{
			Metadata: map[string]string{
				metadataRecommendationTotalCount: "0",
				"GenerationTimestamp":            "2024-01-01T00:00:00Z",
			},
		}, nil
	}

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
			metadataRecommendationTotalCount: "1",
			"GenerationTimestamp":            "2024-01-01T00:00:00Z",
			"AdditionalMetadata":             "lookback=30days",
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
	GenerationStatus  string   `json:"GenerationStatus"`
	NextPageToken     string   `json:"NextPageToken"`
	RecommendationIDs []string `json:"RecommendationIds"`
	PageSize          int      `json:"PageSize"`
}

// generationSummary mirrors aws-sdk-go-v2/service/costexplorer/types' GenerationSummary
// exactly -- the field is RecommendationId, not GenerationId (see
// api_op_StartSavingsPlansPurchaseRecommendationGeneration.go / GenerationSummary in
// types.go).
type generationSummary struct {
	EstimatedCompletionTime  string `json:"EstimatedCompletionTime,omitempty"`
	GenerationCompletionTime string `json:"GenerationCompletionTime,omitempty"`
	GenerationStartedTime    string `json:"GenerationStartedTime,omitempty"`
	GenerationStatus         string `json:"GenerationStatus,omitempty"`
	RecommendationID         string `json:"RecommendationId,omitempty"`
}

type listSavingsPlansPurchaseRecommendationGenerationOutput struct {
	NextPageToken         string              `json:"NextPageToken,omitempty"`
	GenerationSummaryList []generationSummary `json:"GenerationSummaryList"`
}

func (h *Handler) handleListSavingsPlansPurchaseRecommendationGeneration(
	_ context.Context,
	in *listSavingsPlansPurchaseRecommendationGenerationInput,
) (*listSavingsPlansPurchaseRecommendationGenerationOutput, error) {
	gens := h.Backend.ListSavingsPlansGenerations(in.GenerationStatus)

	items := make([]generationSummary, 0, len(gens))

	for _, g := range gens {
		items = append(items, generationSummary{
			EstimatedCompletionTime:  g.EstimatedCompletionTime,
			GenerationCompletionTime: g.GenerationCompletionTime,
			GenerationStartedTime:    g.GenerationStartedTime,
			GenerationStatus:         g.GenerationStatus,
			RecommendationID:         g.RecommendationID,
		})
	}

	return &listSavingsPlansPurchaseRecommendationGenerationOutput{
		GenerationSummaryList: items,
	}, nil
}

type startSavingsPlansPurchaseRecommendationGenerationInput struct{}

// startSavingsPlansPurchaseRecommendationGenerationOutput's RecommendationId field name
// (previously the invented "GenerationId") is field-diffed against real AWS CE's
// StartSavingsPlansPurchaseRecommendationGenerationOutput.
type startSavingsPlansPurchaseRecommendationGenerationOutput struct {
	EstimatedCompletionTime string `json:"EstimatedCompletionTime,omitempty"`
	GenerationStartedTime   string `json:"GenerationStartedTime,omitempty"`
	RecommendationID        string `json:"RecommendationId,omitempty"`
}

func (h *Handler) handleStartSavingsPlansPurchaseRecommendationGeneration(
	_ context.Context,
	_ *startSavingsPlansPurchaseRecommendationGenerationInput,
) (*startSavingsPlansPurchaseRecommendationGenerationOutput, error) {
	g := h.Backend.CreateSavingsPlansGeneration()

	return &startSavingsPlansPurchaseRecommendationGenerationOutput{
		RecommendationID:        g.RecommendationID,
		GenerationStartedTime:   g.GenerationStartedTime,
		EstimatedCompletionTime: g.EstimatedCompletionTime,
	}, nil
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
