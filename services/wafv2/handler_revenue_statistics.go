package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
)

// This file implements the AI-bot pay-per-crawl monetization reporting family added to
// wafv2 in aws-sdk-go-v2/service/wafv2@v1.76.0: GetRevenueStatistics,
// GetRevenueStatisticsSummary, GetRevenueStatisticsTimeSeries, and
// ListSettlementRecords. All four report revenue/traffic analytics derived from real HTTP
// requests, AI-bot detection, and on-chain payment settlement -- none of which this
// emulator has (no real traffic passes through it, there is no bot-classification
// pipeline, and there is no billing/blockchain-settlement system). Every handler below
// therefore performs full AWS-accurate request validation and then returns an honestly
// empty result (empty lists, zero counts) rather than fabricating revenue amounts, bot
// names, path statistics, or settlement records. This mirrors the existing
// GetSampledRequests/GetTopPathStatisticsByTraffic pattern in handler_rate_based_rules.go,
// which already documents the same "no traffic exists to report" honesty constraint.

const (
	// currencyUSDC is the only member of types.Currency in the real SDK (enums.go).
	currencyUSDC = "USDC"

	// RankingStatisticType values (GetRevenueStatistics.StatisticType).
	statisticTypeTopSourcesByRevenue = "TOP_SOURCES_BY_REVENUE"
	statisticTypeTopPathsByRevenue   = "TOP_PATHS_BY_REVENUE"

	// GroupByType values (GetRevenueStatistics/GetRevenueStatisticsTimeSeries.GroupBy).
	groupByCategory     = "CATEGORY"
	groupByIntent       = "INTENT"
	groupByOrganization = "ORGANIZATION"
	groupByWebACL       = "WEBACL"

	// RankingSortBy values (GetRevenueStatistics.SortBy).
	rankingSortByRevenue    = "REVENUE"
	rankingSortByPercentage = "PERCENTAGE"

	// SortOrder values, shared by GetRevenueStatistics and ListSettlementRecords.
	sortOrderAsc  = "ASC"
	sortOrderDesc = "DESC"

	// IntervalType values (GetRevenueStatisticsTimeSeries.Interval).
	intervalMinutely     = "MINUTELY"
	intervalFiveMinutely = "FIVE_MINUTELY"
	intervalHourly       = "HOURLY"
	intervalDaily        = "DAILY"

	// TimeSeriesStatisticType values (GetRevenueStatisticsTimeSeries.StatisticType).
	timeSeriesDateHistogram  = "DATE_HISTOGRAM"
	timeSeriesPaymentTraffic = "PAYMENT_TRAFFIC"

	// SettlementSortBy values (ListSettlementRecords.SortBy).
	settlementSortByTimestamp = "TIMESTAMP"
	settlementSortByAmount    = "AMOUNT"
	settlementSortByStatus    = "STATUS"

	// enumNameValue is "NAME" -- a valid member of GroupByType, RankingSortBy, AND
	// SettlementSortBy independently (each defines its own "NAME" enum member in
	// enums.go), shared here as one constant rather than three identical literals.
	enumNameValue = "NAME"

	// MonetizationFilter.Name values that are enum-restricted server-side per the
	// types.MonetizationFilter doc comment (as opposed to free-text or ARN-validated
	// filter names, which are not enum-checked here).
	filterNameCurrencyMode     = "CurrencyMode"
	filterNameChainName        = "ChainName"
	filterNameSettlementStatus = "SettlementStatus"
	filterNameHTTPSourceName   = "HttpSourceName"

	// maxMonetizationFilterValues is the documented cap ("Maximum: 20 values per
	// filter") on types.MonetizationFilter.Values.
	maxMonetizationFilterValues = 20

	// maxRevenueTimeWindowSeconds is the documented maximum TimeWindow span ("The
	// maximum supported time window is 90 days") shared by all four ops in this file.
	maxRevenueTimeWindowSeconds = float64(90 * 24 * 60 * 60)

	// GetRevenueStatisticsTimeSeries.Limit bounds ("Minimum: 1. Maximum: 10000").
	minTimeSeriesLimit = 1
	maxTimeSeriesLimit = 10000

	// ListSettlementRecords.Limit bounds ("Minimum: 1. Maximum: 100").
	minSettlementRecordsLimit = 1
	maxSettlementRecordsLimit = 100
)

// timeWindowRequest mirrors types.TimeWindow's awsjson1.1 wire shape: StartTime/EndTime
// are epoch-seconds numbers (verified against serializeDocumentTimeWindow, which encodes
// via smithytime.FormatEpochSeconds as a JSON Double), not ISO8601 strings.
type timeWindowRequest struct {
	StartTime *float64 `json:"StartTime"`
	EndTime   *float64 `json:"EndTime"`
}

// monetizationFilterRequest mirrors types.MonetizationFilter.
type monetizationFilterRequest struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// validateRevenueScope validates Scope for the monetization-reporting family. It reuses
// validScope (store.go) -- the one existing REGIONAL/CLOUDFRONT enum helper in this
// package -- for the base enum check, then layers on the operation-specific restriction
// documented on every op in this file ("This operation is only available for CLOUDFRONT
// scope"): AI-bot monetization only exists for CloudFront distributions.
func validateRevenueScope(scope string) error {
	if scope == "" {
		return fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if !validScope(scope) {
		return fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	if scope != ScopeCloudFront {
		return fmt.Errorf(
			"%w: this operation is only available for %s scope",
			errInvalidRequest, ScopeCloudFront,
		)
	}

	return nil
}

// validateCurrency validates the required Currency field. USDC is the only currency the
// real SDK's Currency enum supports today (enums.go).
func validateCurrency(currency string) error {
	if currency == "" {
		return fmt.Errorf("%w: Currency is required", errInvalidRequest)
	}

	if currency != currencyUSDC {
		return fmt.Errorf("%w: Currency must be %s", errInvalidRequest, currencyUSDC)
	}

	return nil
}

// validateMonetizationTimeWindow validates the required TimeWindow: both bounds present,
// EndTime not before StartTime, and the span within the documented 90-day maximum.
func validateMonetizationTimeWindow(tw *timeWindowRequest) error {
	if tw == nil {
		return fmt.Errorf("%w: TimeWindow is required", errInvalidRequest)
	}

	if tw.StartTime == nil {
		return fmt.Errorf("%w: TimeWindow.StartTime is required", errInvalidRequest)
	}

	if tw.EndTime == nil {
		return fmt.Errorf("%w: TimeWindow.EndTime is required", errInvalidRequest)
	}

	if *tw.EndTime < *tw.StartTime {
		return fmt.Errorf("%w: TimeWindow.EndTime must not be before StartTime", errInvalidRequest)
	}

	if *tw.EndTime-*tw.StartTime > maxRevenueTimeWindowSeconds {
		return fmt.Errorf("%w: TimeWindow must not span more than 90 days", errInvalidRequest)
	}

	return nil
}

// validateMonetizationFilters validates the optional Filters list: each entry requires a
// non-empty Name and a non-empty Values list capped at 20 entries, and enum-restricted
// filter names (CurrencyMode/ChainName/SettlementStatus/HttpSourceName) have their values
// checked against the documented enum.
func validateMonetizationFilters(filters []monetizationFilterRequest) error {
	for i, f := range filters {
		if f.Name == "" {
			return fmt.Errorf("%w: Filters[%d].Name is required", errInvalidRequest, i)
		}

		if len(f.Values) == 0 {
			return fmt.Errorf("%w: Filters[%d].Values is required", errInvalidRequest, i)
		}

		if len(f.Values) > maxMonetizationFilterValues {
			return fmt.Errorf(
				"%w: Filters[%d].Values must not exceed %d entries",
				errInvalidRequest, i, maxMonetizationFilterValues,
			)
		}

		if err := validateMonetizationFilterEnumValues(f.Name, f.Values); err != nil {
			return err
		}
	}

	return nil
}

// monetizationFilterEnumAllowed reports whether name is one of the four enum-restricted
// MonetizationFilter.Name values, and if so, whether value is a valid member of that
// filter's enum (per the types.MonetizationFilter doc comment in the real SDK).
func monetizationFilterEnumAllowed(name, value string) (bool, bool) {
	switch name {
	case filterNameCurrencyMode:
		return true, value == "REAL" || value == "TEST"
	case filterNameChainName:
		return true, value == "BASE" || value == "SOLANA" || value == "BASE_SEPOLIA" || value == "SOLANA_DEVNET"
	case filterNameSettlementStatus:
		return true, isValidSettlementStatusFilterValue(value)
	case filterNameHTTPSourceName:
		return true, isValidHTTPSourceNameFilterValue(value)
	default:
		return false, false
	}
}

func isValidSettlementStatusFilterValue(v string) bool {
	switch v {
	case "SETTLED", "PENDING", "FAILED", "SERVICE_ERROR", "SKIPPED_ORIGIN_ERROR", "DUPLICATE":
		return true
	default:
		return false
	}
}

func isValidHTTPSourceNameFilterValue(v string) bool {
	switch v {
	case "CF", "ALB", "APIGW", "APPRUNNER", "COGNITO", "VERIFIED_ACCESS":
		return true
	default:
		return false
	}
}

func validateMonetizationFilterEnumValues(name string, values []string) error {
	for _, v := range values {
		restricted, valid := monetizationFilterEnumAllowed(name, v)
		if restricted && !valid {
			return fmt.Errorf("%w: Filters value %q is not valid for %s", errInvalidRequest, v, name)
		}
	}

	return nil
}

func isValidGroupBy(v string) bool {
	switch v {
	case groupByCategory, groupByIntent, groupByOrganization, groupByWebACL, enumNameValue:
		return true
	default:
		return false
	}
}

func isValidSortOrder(v string) bool {
	return v == sortOrderAsc || v == sortOrderDesc
}

// ---- GetRevenueStatistics ---------------------------------------------------------

// getRevenueStatisticsRequest mirrors GetRevenueStatisticsInput.
type getRevenueStatisticsRequest struct {
	Currency      string                      `json:"Currency"`
	Scope         string                      `json:"Scope"`
	StatisticType string                      `json:"StatisticType"`
	GroupBy       string                      `json:"GroupBy"`
	SortBy        string                      `json:"SortBy"`
	SortOrder     string                      `json:"SortOrder"`
	NextMarker    string                      `json:"NextMarker"`
	TimeWindow    *timeWindowRequest          `json:"TimeWindow"`
	Filters       []monetizationFilterRequest `json:"Filters"`
	Limit         int32                       `json:"Limit"`
}

func isValidRankingStatisticType(v string) bool {
	return v == statisticTypeTopSourcesByRevenue || v == statisticTypeTopPathsByRevenue
}

func isValidRankingSortBy(v string) bool {
	switch v {
	case rankingSortByRevenue, rankingSortByPercentage, enumNameValue:
		return true
	default:
		return false
	}
}

// validateGetRevenueStatisticsRequest validates the fields specific to
// GetRevenueStatistics beyond the shared Scope/Currency/TimeWindow/Filters checks:
// StatisticType (required enum), GroupBy (required exactly when StatisticType is
// TOP_SOURCES_BY_REVENUE, per the SDK doc comment: "If StatisticType is
// TOP_SOURCES_BY_REVENUE and GroupBy is omitted, the request is rejected with a
// WAFInvalidParameterException"), SortBy, and SortOrder.
func validateGetRevenueStatisticsRequest(req getRevenueStatisticsRequest) error {
	if req.StatisticType == "" {
		return fmt.Errorf("%w: StatisticType is required", errInvalidRequest)
	}

	if !isValidRankingStatisticType(req.StatisticType) {
		return fmt.Errorf(
			"%w: StatisticType must be %s or %s",
			errInvalidRequest, statisticTypeTopSourcesByRevenue, statisticTypeTopPathsByRevenue,
		)
	}

	if req.StatisticType == statisticTypeTopSourcesByRevenue && req.GroupBy == "" {
		return fmt.Errorf(
			"%w: GroupBy is required when StatisticType is %s",
			errInvalidRequest, statisticTypeTopSourcesByRevenue,
		)
	}

	if req.GroupBy != "" && !isValidGroupBy(req.GroupBy) {
		return fmt.Errorf("%w: GroupBy must be one of NAME, CATEGORY, INTENT, ORGANIZATION, WEBACL", errInvalidRequest)
	}

	if req.SortBy != "" && !isValidRankingSortBy(req.SortBy) {
		return fmt.Errorf("%w: SortBy must be one of REVENUE, PERCENTAGE, NAME", errInvalidRequest)
	}

	if req.SortOrder != "" && !isValidSortOrder(req.SortOrder) {
		return fmt.Errorf("%w: SortOrder must be ASC or DESC", errInvalidRequest)
	}

	if req.Limit < 0 {
		return fmt.Errorf("%w: Limit must not be negative", errInvalidRequest)
	}

	return nil
}

// handleGetRevenueStatistics validates the request and returns an honestly empty ranked
// statistics response: SourceStatistics (TOP_SOURCES_BY_REVENUE) or RevenuePathStatistics
// (TOP_PATHS_BY_REVENUE) are always empty because this emulator has no real HTTP traffic,
// AI-bot detection, or revenue pipeline to rank -- never a fabricated bot name, path, or
// dollar amount.
func (h *Handler) handleGetRevenueStatistics(body []byte) ([]byte, error) {
	var req getRevenueStatisticsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRevenueScope(req.Scope); err != nil {
		return nil, err
	}

	if err := validateCurrency(req.Currency); err != nil {
		return nil, err
	}

	if err := validateMonetizationTimeWindow(req.TimeWindow); err != nil {
		return nil, err
	}

	if err := validateMonetizationFilters(req.Filters); err != nil {
		return nil, err
	}

	if err := validateGetRevenueStatisticsRequest(req); err != nil {
		return nil, err
	}

	resp := map[string]any{}

	switch req.StatisticType {
	case statisticTypeTopSourcesByRevenue:
		resp["SourceStatistics"] = []any{}
	case statisticTypeTopPathsByRevenue:
		resp["RevenuePathStatistics"] = []any{}
	}

	return json.Marshal(resp)
}

// ---- GetRevenueStatisticsSummary ---------------------------------------------------

// getRevenueStatisticsSummaryRequest mirrors GetRevenueStatisticsSummaryInput.
type getRevenueStatisticsSummaryRequest struct {
	Currency   string                      `json:"Currency"`
	Scope      string                      `json:"Scope"`
	TimeWindow *timeWindowRequest          `json:"TimeWindow"`
	Filters    []monetizationFilterRequest `json:"Filters"`
}

// handleGetRevenueStatisticsSummary validates the request and returns an honestly-zero
// RevenueBreakdown: this emulator has no real HTTP traffic, AI-bot detection pipeline, or
// billing/settlement system, so every amount and count below is a real, defensible zero
// ("zero monetized traffic observed") rather than a fabricated dollar figure.
func (h *Handler) handleGetRevenueStatisticsSummary(body []byte) ([]byte, error) {
	var req getRevenueStatisticsSummaryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRevenueScope(req.Scope); err != nil {
		return nil, err
	}

	if err := validateCurrency(req.Currency); err != nil {
		return nil, err
	}

	if err := validateMonetizationTimeWindow(req.TimeWindow); err != nil {
		return nil, err
	}

	if err := validateMonetizationFilters(req.Filters); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"RevenueBreakdown": map[string]any{
			"Currency":            req.Currency,
			"TotalAmount":         "0",
			"TotalMonetizeServed": int64(0),
			"TotalSettled":        int64(0),
			"UnverifiedAmount":    "0",
			"VerifiedAmount":      "0",
		},
	})
}

// ---- GetRevenueStatisticsTimeSeries -------------------------------------------------

// getRevenueStatisticsTimeSeriesRequest mirrors GetRevenueStatisticsTimeSeriesInput.
type getRevenueStatisticsTimeSeriesRequest struct {
	Currency      string                      `json:"Currency"`
	Interval      string                      `json:"Interval"`
	Scope         string                      `json:"Scope"`
	StatisticType string                      `json:"StatisticType"`
	GroupBy       string                      `json:"GroupBy"`
	NextMarker    string                      `json:"NextMarker"`
	TimeWindow    *timeWindowRequest          `json:"TimeWindow"`
	Filters       []monetizationFilterRequest `json:"Filters"`
	Limit         int32                       `json:"Limit"`
}

func isValidInterval(v string) bool {
	switch v {
	case intervalMinutely, intervalFiveMinutely, intervalHourly, intervalDaily:
		return true
	default:
		return false
	}
}

func isValidTimeSeriesStatisticType(v string) bool {
	return v == timeSeriesDateHistogram || v == timeSeriesPaymentTraffic
}

// validateGetRevenueStatisticsTimeSeriesRequest validates the fields specific to
// GetRevenueStatisticsTimeSeries: Interval and StatisticType (both required enums),
// optional GroupBy, and Limit (when provided, 1-10000 per the SDK doc comment).
func validateGetRevenueStatisticsTimeSeriesRequest(req getRevenueStatisticsTimeSeriesRequest) error {
	if req.Interval == "" {
		return fmt.Errorf("%w: Interval is required", errInvalidRequest)
	}

	if !isValidInterval(req.Interval) {
		return fmt.Errorf("%w: Interval must be one of MINUTELY, FIVE_MINUTELY, HOURLY, DAILY", errInvalidRequest)
	}

	if req.StatisticType == "" {
		return fmt.Errorf("%w: StatisticType is required", errInvalidRequest)
	}

	if !isValidTimeSeriesStatisticType(req.StatisticType) {
		return fmt.Errorf(
			"%w: StatisticType must be %s or %s",
			errInvalidRequest, timeSeriesDateHistogram, timeSeriesPaymentTraffic,
		)
	}

	if req.GroupBy != "" && !isValidGroupBy(req.GroupBy) {
		return fmt.Errorf("%w: GroupBy must be one of NAME, CATEGORY, INTENT, ORGANIZATION, WEBACL", errInvalidRequest)
	}

	if req.Limit != 0 && (req.Limit < minTimeSeriesLimit || req.Limit > maxTimeSeriesLimit) {
		return fmt.Errorf(
			"%w: Limit must be between %d and %d",
			errInvalidRequest, minTimeSeriesLimit, maxTimeSeriesLimit,
		)
	}

	return nil
}

// handleGetRevenueStatisticsTimeSeries validates the request and returns an honestly
// empty DataPoints list: no revenue events exist in this emulator to bucket into
// time-series intervals, so there is nothing genuine to aggregate.
func (h *Handler) handleGetRevenueStatisticsTimeSeries(body []byte) ([]byte, error) {
	var req getRevenueStatisticsTimeSeriesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRevenueScope(req.Scope); err != nil {
		return nil, err
	}

	if err := validateCurrency(req.Currency); err != nil {
		return nil, err
	}

	if err := validateMonetizationTimeWindow(req.TimeWindow); err != nil {
		return nil, err
	}

	if err := validateMonetizationFilters(req.Filters); err != nil {
		return nil, err
	}

	if err := validateGetRevenueStatisticsTimeSeriesRequest(req); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"DataPoints": []any{}})
}

// ---- ListSettlementRecords ----------------------------------------------------------

// listSettlementRecordsRequest mirrors ListSettlementRecordsInput.
type listSettlementRecordsRequest struct {
	Currency   string                      `json:"Currency"`
	Scope      string                      `json:"Scope"`
	SortBy     string                      `json:"SortBy"`
	SortOrder  string                      `json:"SortOrder"`
	NextMarker string                      `json:"NextMarker"`
	TimeWindow *timeWindowRequest          `json:"TimeWindow"`
	Filters    []monetizationFilterRequest `json:"Filters"`
	Limit      int32                       `json:"Limit"`
}

func isValidSettlementSortBy(v string) bool {
	switch v {
	case settlementSortByTimestamp, settlementSortByAmount, settlementSortByStatus, enumNameValue:
		return true
	default:
		return false
	}
}

// validateListSettlementRecordsRequest validates the fields specific to
// ListSettlementRecords: Limit (when provided, 1-100 per the SDK doc comment), SortBy,
// and SortOrder.
func validateListSettlementRecordsRequest(req listSettlementRecordsRequest) error {
	if req.Limit != 0 && (req.Limit < minSettlementRecordsLimit || req.Limit > maxSettlementRecordsLimit) {
		return fmt.Errorf(
			"%w: Limit must be between %d and %d",
			errInvalidRequest, minSettlementRecordsLimit, maxSettlementRecordsLimit,
		)
	}

	if req.SortBy != "" && !isValidSettlementSortBy(req.SortBy) {
		return fmt.Errorf("%w: SortBy must be one of TIMESTAMP, AMOUNT, NAME, STATUS", errInvalidRequest)
	}

	if req.SortOrder != "" && !isValidSortOrder(req.SortOrder) {
		return fmt.Errorf("%w: SortOrder must be ASC or DESC", errInvalidRequest)
	}

	return nil
}

// handleListSettlementRecords validates the request and returns an honestly empty
// Settlements list: this emulator has no real payment or blockchain-settlement pipeline,
// so there are no settlement transactions to list -- never a fabricated PayerAddress,
// TransactionId, or Amount.
func (h *Handler) handleListSettlementRecords(body []byte) ([]byte, error) {
	var req listSettlementRecordsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRevenueScope(req.Scope); err != nil {
		return nil, err
	}

	if err := validateCurrency(req.Currency); err != nil {
		return nil, err
	}

	if err := validateMonetizationTimeWindow(req.TimeWindow); err != nil {
		return nil, err
	}

	if err := validateMonetizationFilters(req.Filters); err != nil {
		return nil, err
	}

	if err := validateListSettlementRecordsRequest(req); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Settlements": []any{}})
}

// revenueStatisticsDispatchOps returns the AI-bot monetization reporting operation
// dispatch entries. wrapNoCtx adapts these handlers, none of which need a context (they
// validate the request and return honestly-empty analytics with no backend state to
// read), to the ctx-taking dispatchFn signature the dispatch table requires -- the same
// pattern used by managedRuleCatalogDispatchOps in handler_managed_rule_catalog.go.
func (h *Handler) revenueStatisticsDispatchOps() map[string]dispatchFn {
	wrapNoCtx := func(f func([]byte) ([]byte, error)) dispatchFn {
		return func(_ context.Context, b []byte) ([]byte, error) { return f(b) }
	}

	return map[string]dispatchFn{
		"GetRevenueStatistics":           wrapNoCtx(h.handleGetRevenueStatistics),
		"GetRevenueStatisticsSummary":    wrapNoCtx(h.handleGetRevenueStatisticsSummary),
		"GetRevenueStatisticsTimeSeries": wrapNoCtx(h.handleGetRevenueStatisticsTimeSeries),
		"ListSettlementRecords":          wrapNoCtx(h.handleListSettlementRecords),
	}
}
