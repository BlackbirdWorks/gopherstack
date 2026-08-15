package ce

import (
	"context"
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

	if in.SortBy != nil && strings.EqualFold(in.SortBy.Key, "Time") {
		sortByTime(coverages, func(c ReservationCoverageByTime) map[string]string { return c.TimePeriod },
			sortDescending(in.SortBy.SortOrder))
	}

	var total *ReservationCoverageAgg
	if len(coverages) > 0 {
		agg := coverages[0].Total
		total = &agg
	}

	return &getReservationCoverageOutput{
		CoveragesByTime: coverages,
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
	recs := h.Backend.GetReservationPurchaseRecommendations(
		in.Service, in.LookbackPeriodInDays, in.TermInYears, in.PaymentOption,
	)

	if !matchesLinkedAccountFilter(in.Filter, h.Backend.accountID) {
		recs = nil
	}

	if recs == nil {
		recs = []ReservationRecommendation{}
	}

	return &getReservationPurchaseRecommendationOutput{
		Recommendations: recs,
		Metadata: map[string]string{
			metadataRecommendationTotalCount: strconv.Itoa(len(recs)),
			handlerCurrencyCode:              metricUnitUSD,
		},
	}, nil
}

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

	if in.SortBy != nil && strings.EqualFold(in.SortBy.Key, "Time") {
		sortByTime(utils, func(u ReservationUtilizationByTime) map[string]string { return u.TimePeriod },
			sortDescending(in.SortBy.SortOrder))
	}

	var total *ReservationUtilizationAgg
	if len(utils) > 0 {
		agg := utils[0].Total
		total = &agg
	}

	return &getReservationUtilizationOutput{
		UtilizationsByTime: utils,
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
	Filter        any                                     `json:"Filter"`
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

	if recs == nil {
		recs = []RightsizingRecommendation{}
	}

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
