package ce

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// serviceDimensionFilter extracts the SERVICE dimension's Values from filter,
// the one Dimensions key the reservation coverage/utilization ledger can
// honor (see GetReservationCoverageFiltered/GetReservationUtilizationFiltered).
func serviceDimensionFilter(filter *ceExpression) []string {
	if filter == nil || filter.Dimensions == nil || !strings.EqualFold(filter.Dimensions.Key, "SERVICE") {
		return nil
	}

	return filter.Dimensions.Values
}

// sortByTime reorders items by their TimePeriod.Start, honoring desc. Real
// GetReservationCoverage/Utilization SortBy also supports several numeric
// metric keys (OnDemandCost, CoverageHoursPercentage, ...); only the
// documented "Time" key is applied here, so a request sorting by a numeric
// key is accepted but left in natural (chronological) order rather than
// fabricating a metric-based ordering.
func sortByTime[T any](items []T, timePeriod func(T) map[string]string, desc bool) {
	sort.SliceStable(items, func(i, j int) bool {
		ti := timePeriod(items[i])[timePeriodKeyStart]
		tj := timePeriod(items[j])[timePeriodKeyStart]

		if desc {
			return ti > tj
		}

		return ti < tj
	})
}

// buildTimeSeriesResponse is the shared shape behind
// handleGetReservationCoverage/handleGetReservationUtilization: apply
// SortBy=Time if requested, derive Total from the first (possibly reordered)
// entry, then paginate preserving whatever order sortByTime produced.
func buildTimeSeriesResponse[T, A any](
	items []T,
	timePeriod func(T) map[string]string,
	totalOf func(T) A,
	sortBy *ceSortDefinition,
	nextPageToken string,
) ([]T, *A, string) {
	if sortBy != nil && strings.EqualFold(sortBy.Key, "Time") {
		sortByTime(items, timePeriod, sortDescending(sortBy.SortOrder))
	}

	var total *A
	if len(items) > 0 {
		t := totalOf(items[0])
		total = &t
	}

	page, nextToken := paginateOrdered(items, 0, nextPageToken, func(item T) string {
		return timePeriod(item)[timePeriodKeyStart]
	})

	return page, total, nextToken
}

// resolveCoverageTimeRange extracts start/end/granularity from a
// GetReservationCoverage/Utilization-style request, applying the defaults
// both operations share.
func resolveCoverageTimeRange(timePeriod map[string]string, granularity string) (string, string, string) {
	start, end := defaultStartDate, defaultEndDate

	if timePeriod != nil {
		if s := timePeriod["Start"]; s != "" {
			start = s
		}

		if e := timePeriod["End"]; e != "" {
			end = e
		}
	}

	gran := granularity
	if gran == "" {
		gran = defaultGranularity
	}

	return start, end, gran
}

// getReservationCoverageInput.GroupBy is accepted for wire parity but stays
// unapplied: this emulator's CoveragesByTime entries never populate a
// per-group Groups breakdown (Groups is always [], see
// GetReservationCoverageFiltered) -- there is no real per-SERVICE/AZ/... RI
// coverage state to disguise a fabricated breakdown from (same documented
// shape as GetCostAndUsageWithResources.ResultsByTime).
type getReservationCoverageInput struct {
	Filter        *ceExpression     `json:"Filter"`
	TimePeriod    map[string]string `json:"TimePeriod"`
	SortBy        *ceSortDefinition `json:"SortBy"`
	Granularity   string            `json:"Granularity"`
	NextPageToken string            `json:"NextPageToken"`
	GroupBy       []groupBySpec     `json:"GroupBy"`
}

type getReservationCoverageOutput struct {
	Total           *ReservationCoverageAgg     `json:"Total,omitempty"`
	NextPageToken   string                      `json:"NextPageToken,omitempty"`
	CoveragesByTime []ReservationCoverageByTime `json:"CoveragesByTime"`
}

func (h *Handler) handleGetReservationCoverage(
	_ context.Context,
	in *getReservationCoverageInput,
) (*getReservationCoverageOutput, error) {
	start, end, granularity := resolveCoverageTimeRange(in.TimePeriod, in.Granularity)

	coverages := h.Backend.GetReservationCoverageFiltered(start, end, granularity, serviceDimensionFilter(in.Filter))

	page, total, nextToken := buildTimeSeriesResponse(
		coverages,
		func(c ReservationCoverageByTime) map[string]string { return c.TimePeriod },
		func(c ReservationCoverageByTime) ReservationCoverageAgg { return c.Total },
		in.SortBy, in.NextPageToken,
	)

	return &getReservationCoverageOutput{
		CoveragesByTime: page,
		NextPageToken:   nextToken,
		Total:           total,
	}, nil
}

type getReservationPurchaseRecommendationInput struct {
	Filter               *ceExpression `json:"Filter"`
	Service              string        `json:"Service"`
	AccountScope         string        `json:"AccountScope"`
	LookbackPeriodInDays string        `json:"LookbackPeriodInDays"`
	TermInYears          string        `json:"TermInYears"`
	PaymentOption        string        `json:"PaymentOption"`
	NextPageToken        string        `json:"NextPageToken"`
	PageSize             int           `json:"PageSize"`
}

type getReservationPurchaseRecommendationOutput struct {
	NextPageToken   string                      `json:"NextPageToken,omitempty"`
	Metadata        any                         `json:"Metadata,omitempty"`
	Recommendations []ReservationRecommendation `json:"Recommendations"`
}

// matchesLinkedAccountFilter reports whether accountID satisfies filter's
// LINKED_ACCOUNT Dimensions clause -- the only Dimensions key real AWS
// documents for GetReservationPurchaseRecommendation's Filter. This emulator
// is single-account (every recommendation is for b.accountID), so applying
// this filter is a real, non-fabricated exclude/include decision: no filter
// (or one that lists accountID) keeps the recommendation, any other
// LINKED_ACCOUNT list excludes it.
func matchesLinkedAccountFilter(filter *ceExpression, accountID string) bool {
	if filter == nil || filter.Dimensions == nil || !strings.EqualFold(filter.Dimensions.Key, "LINKED_ACCOUNT") {
		return true
	}

	return stringSliceContainsFold(filter.Dimensions.Values, accountID)
}

func (h *Handler) handleGetReservationPurchaseRecommendation(
	_ context.Context,
	in *getReservationPurchaseRecommendationInput,
) (*getReservationPurchaseRecommendationOutput, error) {
	// AccountScope distinguishes PAYER (whole-org) from LINKED
	// (single-account) recommendations on real AWS; this emulator has only
	// one account's worth of state either way, so the value is validated (an
	// unrecognized scope real AWS rejects) rather than left unchecked.
	switch in.AccountScope {
	case "", accountScopePayer, accountScopeLinked:
	default:
		return nil, fmt.Errorf("%w: AccountScope must be PAYER or LINKED", ErrValidation)
	}

	recs := h.Backend.GetReservationPurchaseRecommendations(
		in.Service, in.LookbackPeriodInDays, in.TermInYears, in.PaymentOption,
	)

	if !matchesLinkedAccountFilter(in.Filter, h.Backend.accountID) {
		recs = nil
	}

	if recs == nil {
		recs = []ReservationRecommendation{}
	}

	page, nextToken := paginateList(recs, in.PageSize, in.NextPageToken,
		func(ReservationRecommendation) string { return "" })

	// No Metadata: types.ReservationPurchaseRecommendationMetadata
	// (costexplorer@v1.67.4 types/types.go) has only
	// AdditionalMetadata/GenerationTimestamp/RecommendationId, none of which
	// this backend tracks -- "RecommendationTotalCount" and "USD" (a stray
	// use of handlerCurrencyCode's own value as a map key) were both
	// fabricated.
	return &getReservationPurchaseRecommendationOutput{
		Recommendations: page,
		NextPageToken:   nextToken,
	}, nil
}

// getReservationUtilizationInput.GroupBy has the same accepted-but-inert
// shape as getReservationCoverageInput.GroupBy above.
type getReservationUtilizationInput struct {
	Filter        *ceExpression     `json:"Filter"`
	TimePeriod    map[string]string `json:"TimePeriod"`
	SortBy        *ceSortDefinition `json:"SortBy"`
	Granularity   string            `json:"Granularity"`
	NextPageToken string            `json:"NextPageToken"`
	GroupBy       []groupBySpec     `json:"GroupBy"`
}

type getReservationUtilizationOutput struct {
	Total              *ReservationUtilizationAgg     `json:"Total,omitempty"`
	NextPageToken      string                         `json:"NextPageToken,omitempty"`
	UtilizationsByTime []ReservationUtilizationByTime `json:"UtilizationsByTime"`
}

func (h *Handler) handleGetReservationUtilization(
	_ context.Context,
	in *getReservationUtilizationInput,
) (*getReservationUtilizationOutput, error) {
	start, end, granularity := resolveCoverageTimeRange(in.TimePeriod, in.Granularity)

	utils := h.Backend.GetReservationUtilizationFiltered(start, end, granularity, serviceDimensionFilter(in.Filter))

	page, total, nextToken := buildTimeSeriesResponse(
		utils,
		func(u ReservationUtilizationByTime) map[string]string { return u.TimePeriod },
		func(u ReservationUtilizationByTime) ReservationUtilizationAgg { return u.Total },
		in.SortBy, in.NextPageToken,
	)

	return &getReservationUtilizationOutput{
		UtilizationsByTime: page,
		NextPageToken:      nextToken,
		Total:              total,
	}, nil
}

// rightsizingRecommendationConfiguration mirrors aws-sdk-go-v2/service/costexplorer/types'
// RightsizingRecommendationConfiguration exactly. Both members are always
// present on the real response (server-applied defaults: BenefitsConsidered
// defaults true, RecommendationTarget defaults SAME_INSTANCE_FAMILY -- see
// types.RightsizingRecommendationConfiguration's doc comments), not only when
// the request set them.
type rightsizingRecommendationConfiguration struct {
	RecommendationTarget string `json:"RecommendationTarget"`
	BenefitsConsidered   bool   `json:"BenefitsConsidered"`
}

type getRightsizingRecommendationInput struct {
	Service       string                                  `json:"Service"`
	Filter        *ceExpression                           `json:"Filter"`
	Configuration *rightsizingRecommendationConfiguration `json:"Configuration"`
	NextPageToken string                                  `json:"NextPageToken"`
	PageSize      int                                     `json:"PageSize"`
}

// getRightsizingRecommendationOutput's Configuration echo was previously
// missing entirely -- see aws-sdk-go-v2/service/costexplorer's
// GetRightsizingRecommendationOutput. A real client's typed .Configuration
// was nil regardless of what (if anything) it requested.
type getRightsizingRecommendationOutput struct {
	Summary                    map[string]string                      `json:"Summary,omitempty"`
	Metadata                   any                                    `json:"Metadata,omitempty"`
	Configuration              rightsizingRecommendationConfiguration `json:"Configuration"`
	NextPageToken              string                                 `json:"NextPageToken,omitempty"`
	RightsizingRecommendations []RightsizingRecommendation            `json:"RightsizingRecommendations"`
}

func (h *Handler) handleGetRightsizingRecommendation(
	_ context.Context,
	in *getRightsizingRecommendationInput,
) (*getRightsizingRecommendationOutput, error) {
	recs := h.Backend.GetRightsizingRecommendations(in.Service)

	// Real AWS documents Filter's Dimensions as limited to LINKED_ACCOUNT/
	// REGION/RIGHTSIZING_TYPE for this op. This emulator's single synthetic
	// recommendation is always for the caller's own account, so LINKED_ACCOUNT
	// is the one clause with a real, non-fabricated exclude/include effect
	// (same shape as GetReservationPurchaseRecommendation's Filter).
	if !matchesLinkedAccountFilter(in.Filter, h.Backend.accountID) {
		recs = nil
	}

	if recs == nil {
		recs = []RightsizingRecommendation{}
	}

	page, nextToken := paginateList(recs, in.PageSize, in.NextPageToken,
		func(RightsizingRecommendation) string { return "" })
	recs = page

	summary := map[string]string{
		"TotalRecommendationCount":           strconv.Itoa(len(recs)),
		"EstimatedTotalMonthlySavingsAmount": handlerZeroAmount,
		"SavingsCurrencyCode":                metricUnitUSD,
		"SavingsPercentage":                  handlerZeroAmount,
	}

	if len(recs) > 0 {
		summary["EstimatedTotalMonthlySavingsAmount"] = recs[0].CurrentInstance.MonthlyCost
		summary["SavingsPercentage"] = "50.0000"
	}

	config := rightsizingRecommendationConfiguration{
		RecommendationTarget: "SAME_INSTANCE_FAMILY",
		BenefitsConsidered:   true,
	}
	if in.Configuration != nil {
		config = *in.Configuration
	}

	return &getRightsizingRecommendationOutput{
		RightsizingRecommendations: recs,
		NextPageToken:              nextToken,
		Summary:                    summary,
		Configuration:              config,
	}, nil
}

// buildReservationOps returns the reservation-family op dispatch entries.
func (h *Handler) buildReservationOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetReservationCoverage": service.WrapOp(
			h.handleGetReservationCoverage,
		),
		"GetReservationPurchaseRecommendation": service.WrapOp(
			h.handleGetReservationPurchaseRecommendation,
		),
		"GetReservationUtilization": service.WrapOp(
			h.handleGetReservationUtilization,
		),
		"GetRightsizingRecommendation": service.WrapOp(
			h.handleGetRightsizingRecommendation,
		),
	}
}
