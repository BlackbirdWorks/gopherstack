package ce

import (
	"context"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type getReservationCoverageInput struct {
	Filter        any               `json:"Filter"`
	TimePeriod    map[string]string `json:"TimePeriod"`
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

	coverages := h.Backend.GetReservationCoverage(start, end, granularity)

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
	Service              string `json:"Service"`
	AccountScope         string `json:"AccountScope"`
	LookbackPeriodInDays string `json:"LookbackPeriodInDays"`
	TermInYears          string `json:"TermInYears"`
	PaymentOption        string `json:"PaymentOption"`
	NextPageToken        string `json:"NextPageToken"`
	PageSize             int    `json:"PageSize"`
}

type getReservationPurchaseRecommendationOutput struct {
	NextPageToken   string                      `json:"NextPageToken,omitempty"`
	Metadata        any                         `json:"Metadata,omitempty"`
	Recommendations []ReservationRecommendation `json:"Recommendations"`
}

func (h *Handler) handleGetReservationPurchaseRecommendation(
	_ context.Context,
	in *getReservationPurchaseRecommendationInput,
) (*getReservationPurchaseRecommendationOutput, error) {
	recs := h.Backend.GetReservationPurchaseRecommendations(
		in.Service, in.LookbackPeriodInDays, in.TermInYears, in.PaymentOption,
	)

	if recs == nil {
		recs = []ReservationRecommendation{}
	}

	return &getReservationPurchaseRecommendationOutput{
		Recommendations: recs,
		Metadata: map[string]string{
			"RecommendationTotalCount": strconv.Itoa(len(recs)),
			handlerCurrencyCode:        metricUnitUSD,
		},
	}, nil
}

type getReservationUtilizationInput struct {
	Filter        any               `json:"Filter"`
	TimePeriod    map[string]string `json:"TimePeriod"`
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

	utils := h.Backend.GetReservationUtilization(start, end, granularity)

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

type getRightsizingRecommendationInput struct {
	Service       string `json:"Service"`
	Filter        any    `json:"Filter"`
	Configuration any    `json:"Configuration"`
	NextPageToken string `json:"NextPageToken"`
	PageSize      int    `json:"PageSize"`
}

type getRightsizingRecommendationOutput struct {
	Summary                    map[string]string           `json:"Summary,omitempty"`
	Metadata                   any                         `json:"Metadata,omitempty"`
	NextPageToken              string                      `json:"NextPageToken,omitempty"`
	RightsizingRecommendations []RightsizingRecommendation `json:"RightsizingRecommendations"`
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

	return &getRightsizingRecommendationOutput{
		RightsizingRecommendations: recs,
		Summary:                    summary,
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
