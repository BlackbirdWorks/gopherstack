package ce

import (
	"context"
	"fmt"
	"sort"
	"strconv"
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

// getSavingsPlansCoverageInput.GroupBy/Metrics are accepted for wire parity
// but stay unapplied: GroupBy would need a per-INSTANCE_FAMILY/REGION/SERVICE
// coverage breakdown this emulator's ledger does not model (each bucket is
// always exactly one synthetic entry, see the handler below); Metrics'
// only real value ("SpendCoveredBySavingsPlans", confirmed against
// GetSavingsPlansCoverageInput's doc comment) does not change the Coverage
// struct's fixed shape, so there is no differing output to select between.
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

// handleGetSavingsPlansCoverage computes one synthetic coverage entry per
// Granularity time bucket (DAILY/MONTHLY, matching GetReservationCoverage's
// bucketing -- real GetSavingsPlansCoverage documents "GetSavingsPlansCoverage
// operation supports only DAILY and MONTHLY granularities" and returns one
// entry per period). This emulator has no per-REGION/SERVICE/INSTANCE_FAMILY
// Savings Plans coverage breakdown to filter across (see
// GetSavingsPlansUtilization), so Filter's only real (non-fabricated) effect
// is on the REGION dimension: since every entry's Region is always
// ceRegion(ctx), a REGION filter that excludes it correctly narrows the
// result to zero items instead of silently ignoring the filter. SortBy has no
// documented "Time" key for this op (unlike GetReservationCoverage) and stays
// inert.
func (h *Handler) handleGetSavingsPlansCoverage(
	ctx context.Context,
	in *getSavingsPlansCoverageInput,
) (*getSavingsPlansCoverageOutput, error) {
	start, end, granularity := resolveCoverageTimeRange(in.TimePeriod, in.Granularity)

	region := ceRegion(ctx)

	if in.Filter != nil && in.Filter.Dimensions != nil && strings.EqualFold(in.Filter.Dimensions.Key, "REGION") &&
		!stringSliceContainsFold(in.Filter.Dimensions.Values, region) {
		return &getSavingsPlansCoverageOutput{SavingsPlansCoverages: []savingsPlanCoverage{}}, nil
	}

	buckets := buildTimeBuckets(start, end, granularity)
	coverages := make([]savingsPlanCoverage, 0, len(buckets))

	for _, bucket := range buckets {
		spUtil := h.Backend.GetSavingsPlansUtilization(bucket.start, bucket.end)

		coverages = append(coverages, savingsPlanCoverage{
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
			TimePeriod: map[string]string{timePeriodKeyStart: bucket.start, timePeriodKeyEnd: bucket.end},
		})
	}

	page, nextToken := paginateList(coverages, in.MaxResults, in.NextToken,
		func(c savingsPlanCoverage) string { return c.TimePeriod[timePeriodKeyStart] })

	return &getSavingsPlansCoverageOutput{
		SavingsPlansCoverages: page,
		NextToken:             nextToken,
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
	// Same PAYER/LINKED validation as GetReservationPurchaseRecommendation's
	// AccountScope -- this emulator has only one account's worth of state
	// either way.
	switch in.AccountScope {
	case "", accountScopePayer, accountScopeLinked:
	default:
		return nil, fmt.Errorf("%w: AccountScope must be PAYER or LINKED", ErrValidation)
	}

	if !matchesLinkedAccountFilter(in.Filter, awsmeta.Account(ctx)) {
		// types.SavingsPlansPurchaseRecommendationMetadata has no
		// "RecommendationTotalCount" member -- AdditionalMetadata/
		// GenerationTimestamp/RecommendationId only.
		return &getSavingsPlansPurchaseRecommendationOutput{
			Metadata: map[string]string{
				"GenerationTimestamp": "2024-01-01T00:00:00Z",
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

	details := []map[string]any{
		{
			"SavingsPlansDetails": map[string]string{
				"Region":         ceRegion(ctx),
				"InstanceFamily": "m5",
				"OfferingId":     "synthetic-sp-offer-1",
			},
			"AccountId":    awsmeta.Account(ctx),
			"UpfrontCost":  handlerZeroAmount,
			"EstimatedROI": handlerROI,
			// handlerCurrencyCode's own value ("USD") was used as the
			// map key here by mistake; the real member is
			// "CurrencyCode" (costexplorer@v1.67.4 deserializers.go).
			mapKeyCurrencyCode:                           metricUnitUSD,
			"EstimatedSPCost":                            spUtil.Utilization.TotalCommitment,
			"EstimatedOnDemandCost":                      spUtil.Savings.OnDemandCostEquivalent,
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
	}

	detailsPage, nextToken := paginateList(details, in.PageSize, in.NextPageToken,
		func(map[string]any) string { return "" })

	return &getSavingsPlansPurchaseRecommendationOutput{
		NextPageToken: nextToken,
		PurchaseRecommendation: &savingsPlansPurchaseRecommendation{
			SavingsPlansType:      spType,
			TermInYears:           in.TermInYears,
			PaymentOption:         in.PaymentOption,
			LookbackPeriodInDays:  in.LookbackPeriodInDays,
			RecommendationDetails: detailsPage,
			RecommendationSummary: map[string]string{
				"EstimatedROI":                               handlerROI,
				mapKeyCurrencyCode:                           metricUnitUSD,
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
		// types.SavingsPlansPurchaseRecommendationMetadata has no
		// "RecommendationTotalCount" member.
		Metadata: map[string]string{
			"GenerationTimestamp": "2024-01-01T00:00:00Z",
			"AdditionalMetadata":  "lookback=30days",
		},
	}, nil
}

type getSavingsPlansUtilizationInput struct {
	Filter      *ceExpression     `json:"Filter"`
	SortBy      *ceSortDefinition `json:"SortBy"`
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

// savingsPlansUtilizationSortValue extracts the numeric value of one of the
// SortBy keys real GetSavingsPlansUtilization documents (TotalCommitment/
// UsedCommitment/UnusedCommitment/NetSavings/UtilizationPercentage). The
// first four genuinely vary per time bucket (derived from that bucket's
// ledger total); UtilizationPercentage is always the same fixed synthetic
// ratio (spUtilizationPct) so sorting by it ties every entry -- included for
// completeness, not fabricated significance.
func savingsPlansUtilizationSortValue(e getSavingsPlansUtilizationByTimeEntry, key string) (float64, bool) {
	var s string

	switch normalizeMetricName(key) {
	case "TOTALCOMMITMENT":
		s = e.Utilization.TotalCommitment
	case "USEDCOMMITMENT":
		s = e.Utilization.UsedCommitment
	case "UNUSEDCOMMITMENT":
		s = e.Utilization.UnusedCommitment
	case "NETSAVINGS":
		s = e.Savings.NetSavings
	case "UTILIZATIONPERCENTAGE":
		s = e.Utilization.UtilizationPercentage
	default:
		return 0, false
	}

	v, err := strconv.ParseFloat(s, 64)

	return v, err == nil
}

// resolveTimePeriod extracts start/end from tp, falling back to
// defaultStart/defaultEnd for a missing map or missing/empty members --
// shared by every Savings Plans/forecast handler that accepts an optional
// TimePeriod.
func resolveTimePeriod(tp map[string]string, defaultStart, defaultEnd string) (string, string) {
	start, end := defaultStart, defaultEnd

	if tp != nil {
		if s := tp[timePeriodKeyStart]; s != "" {
			start = s
		}

		if e := tp[timePeriodKeyEnd]; e != "" {
			end = e
		}
	}

	return start, end
}

// savingsPlansAccountOrRegionExcluded reports whether filter's REGION or
// LINKED_ACCOUNT Dimensions clause excludes this backend's single
// account/region. Real AWS documents more Filter dimensions for
// GetSavingsPlansUtilization (SAVINGS_PLAN_ARN/SAVINGS_PLANS_TYPE/
// PAYMENT_OPTION/INSTANCE_TYPE_FAMILY), but only these two have a
// non-fabricated per-entry value to exclude/include against here (same shape
// as GetReservationUtilization's Filter).
func savingsPlansAccountOrRegionExcluded(filter *ceExpression, region, accountID string) bool {
	if filter == nil || filter.Dimensions == nil {
		return false
	}

	key := filter.Dimensions.Key

	return (strings.EqualFold(key, "REGION") && !stringSliceContainsFold(filter.Dimensions.Values, region)) ||
		(strings.EqualFold(key, "LINKED_ACCOUNT") && !stringSliceContainsFold(filter.Dimensions.Values, accountID))
}

// sortSavingsPlansUtilizationByTime reorders byTime by sortBy's numeric key
// (see savingsPlansUtilizationSortValue) when sortBy names one, honoring
// SortOrder; an unrecognized key is left in its existing (chronological)
// order rather than silently matching a wrong sort.
func sortSavingsPlansUtilizationByTime(byTime []getSavingsPlansUtilizationByTimeEntry, sortBy *ceSortDefinition) {
	if sortBy == nil || len(byTime) == 0 {
		return
	}

	if _, ok := savingsPlansUtilizationSortValue(byTime[0], sortBy.Key); !ok {
		return
	}

	desc := sortDescending(sortBy.SortOrder)
	sort.SliceStable(byTime, func(i, j int) bool {
		vi, _ := savingsPlansUtilizationSortValue(byTime[i], sortBy.Key)
		vj, _ := savingsPlansUtilizationSortValue(byTime[j], sortBy.Key)

		if desc {
			return vi > vj
		}

		return vi < vj
	})
}

func (h *Handler) handleGetSavingsPlansUtilization(
	_ context.Context,
	in *getSavingsPlansUtilizationInput,
) (*getSavingsPlansUtilizationOutput, error) {
	start, end := resolveTimePeriod(in.TimePeriod, defaultStartDate, defaultEndDate)

	granularity := in.Granularity
	if granularity == "" {
		granularity = defaultGranularity
	}

	if savingsPlansAccountOrRegionExcluded(in.Filter, h.Backend.region, h.Backend.accountID) {
		return &getSavingsPlansUtilizationOutput{
			Total:                          &SavingsPlansUtilizationResult{},
			SavingsPlansUtilizationsByTime: []getSavingsPlansUtilizationByTimeEntry{},
		}, nil
	}

	total := h.Backend.GetSavingsPlansUtilization(start, end)
	buckets := buildTimeBuckets(start, end, granularity)

	byTime := make([]getSavingsPlansUtilizationByTimeEntry, 0, len(buckets))

	for _, bucket := range buckets {
		bucketUtil := h.Backend.GetSavingsPlansUtilization(bucket.start, bucket.end)
		byTime = append(byTime, getSavingsPlansUtilizationByTimeEntry{
			TimePeriod:          map[string]string{timePeriodKeyStart: bucket.start, timePeriodKeyEnd: bucket.end},
			Utilization:         bucketUtil.Utilization,
			Savings:             bucketUtil.Savings,
			AmortizedCommitment: bucketUtil.AmortizedCommitment,
		})
	}

	sortSavingsPlansUtilizationByTime(byTime, in.SortBy)

	return &getSavingsPlansUtilizationOutput{
		Total:                          total,
		SavingsPlansUtilizationsByTime: byTime,
	}, nil
}

// getSavingsPlansUtilizationDetailsInput's DataType member (real
// []types.SavingsPlansDataType) was previously declared as "Fields" -- no
// such member exists on the real GetSavingsPlansUtilizationDetailsInput, so a
// real client's DataType was silently dropped. SortBy has no documented
// effect here: this emulator's single synthetic detail item makes any
// ordering trivially a no-op (same precedent as GetSavingsPlansCoverage's
// SortBy).
type getSavingsPlansUtilizationDetailsInput struct {
	Filter     *ceExpression     `json:"Filter"`
	SortBy     any               `json:"SortBy"`
	TimePeriod map[string]string `json:"TimePeriod"`
	NextToken  string            `json:"NextToken"`
	DataType   []string          `json:"DataType"`
	MaxResults int               `json:"MaxResults"`
}

type getSavingsPlansUtilizationDetailsOutput struct {
	NextToken                      string                          `json:"NextToken,omitempty"`
	Total                          *SavingsPlansUtilizationResult  `json:"Total,omitempty"`
	TimePeriod                     map[string]string               `json:"TimePeriod,omitempty"`
	SavingsPlansUtilizationDetails []SavingsPlansUtilizationDetail `json:"SavingsPlansUtilizationDetails"`
}

// applySavingsPlansDataType nils out any of Attributes/Utilization/Savings/
// AmortizedCommitment not named in dataType, matching real AWS's per-item
// selective population; an empty dataType (the common case) leaves every
// section populated.
func applySavingsPlansDataType(d SavingsPlansUtilizationDetail, dataType []string) SavingsPlansUtilizationDetail {
	if len(dataType) == 0 {
		return d
	}

	if !stringSliceContainsFold(dataType, "ATTRIBUTES") {
		d.Attributes = nil
	}

	if !stringSliceContainsFold(dataType, "UTILIZATION") {
		d.Utilization = nil
	}

	if !stringSliceContainsFold(dataType, "SAVINGS") {
		d.Savings = nil
	}

	if !stringSliceContainsFold(dataType, "AMORTIZED_COMMITMENT") {
		d.AmortizedCommitment = nil
	}

	return d
}

// filterSavingsPlansUtilizationDetails narrows details by filter's REGION or
// SAVINGS_PLAN_ARN Dimensions clause -- the two clauses (of the five real AWS
// documents for this op: REGION/SAVINGS_PLAN_ARN/LINKED_ACCOUNT/
// PAYMENT_OPTION/INSTANCE_TYPE_FAMILY) with a real, non-fabricated
// exclude/include effect on this emulator's single synthetic detail item
// (same shape as GetSavingsPlansCoverage's Filter).
func filterSavingsPlansUtilizationDetails(
	details []SavingsPlansUtilizationDetail, filter *ceExpression, region string,
) []SavingsPlansUtilizationDetail {
	if filter == nil || filter.Dimensions == nil {
		return details
	}

	switch key := filter.Dimensions.Key; {
	case strings.EqualFold(key, "REGION"):
		if !stringSliceContainsFold(filter.Dimensions.Values, region) {
			return nil
		}
	case strings.EqualFold(key, "SAVINGS_PLAN_ARN"):
		for _, d := range details {
			if stringSliceContainsFold(filter.Dimensions.Values, d.SavingsPlanARN) {
				return details
			}
		}

		return nil
	}

	return details
}

func (h *Handler) handleGetSavingsPlansUtilizationDetails(
	_ context.Context,
	in *getSavingsPlansUtilizationDetailsInput,
) (*getSavingsPlansUtilizationDetailsOutput, error) {
	start, end := resolveTimePeriod(in.TimePeriod, defaultStartDate, defaultEndDate)

	details := h.Backend.GetSavingsPlansUtilizationDetails(start, end)
	total := h.Backend.GetSavingsPlansUtilization(start, end)

	details = filterSavingsPlansUtilizationDetails(details, in.Filter, h.Backend.region)

	for i := range details {
		details[i] = applySavingsPlansDataType(details[i], in.DataType)
	}

	if details == nil {
		details = []SavingsPlansUtilizationDetail{}
	}

	page, nextToken := paginateList(details, in.MaxResults, in.NextToken,
		func(d SavingsPlansUtilizationDetail) string { return d.SavingsPlanARN })

	return &getSavingsPlansUtilizationDetailsOutput{
		SavingsPlansUtilizationDetails: page,
		NextToken:                      nextToken,
		Total:                          total,
		TimePeriod:                     map[string]string{timePeriodKeyStart: start, timePeriodKeyEnd: end},
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
	gens := h.Backend.ListSavingsPlansGenerations(in.GenerationStatus, in.RecommendationIDs)

	// paginateOrdered, not paginateList: gens is already in
	// most-recently-started-first order, which re-sorting ascending by
	// RecommendationID would discard.
	page, nextToken := paginateOrdered(gens, in.PageSize, in.NextPageToken,
		func(g *SavingsPlansGeneration) string { return g.RecommendationID })

	items := make([]generationSummary, 0, len(page))

	for _, g := range page {
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
		NextPageToken:         nextToken,
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
