package ce

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type groupBySpec struct {
	Type string `json:"Type"`
	Key  string `json:"Key"`
}

type getCostAndUsageInput struct {
	Filter        any               `json:"Filter"`
	TimePeriod    map[string]string `json:"TimePeriod"`
	Granularity   string            `json:"Granularity"`
	NextPageToken string            `json:"NextPageToken"`
	Metrics       []string          `json:"Metrics"`
	GroupBy       []groupBySpec     `json:"GroupBy"`
}

// getCostAndUsageOutput's GroupDefinitions field (the groups specified by the request's
// GroupBy, echoed back -- see aws-sdk-go-v2/service/costexplorer's GetCostAndUsageOutput)
// was previously missing entirely.
type getCostAndUsageOutput struct {
	NextPageToken            string         `json:"NextPageToken,omitempty"`
	ResultsByTime            []ResultByTime `json:"ResultsByTime"`
	GroupDefinitions         []groupBySpec  `json:"GroupDefinitions"`
	DimensionValueAttributes []any          `json:"DimensionValueAttributes"`
}

func (h *Handler) handleGetCostAndUsage(
	_ context.Context,
	in *getCostAndUsageInput,
) (*getCostAndUsageOutput, error) {
	if in.Granularity == "" {
		return nil, fmt.Errorf("%w: Granularity is required", ErrValidation)
	}

	start := ""
	end := ""

	if in.TimePeriod != nil {
		start = in.TimePeriod["Start"]
		end = in.TimePeriod["End"]
	}

	if start == "" {
		start = defaultStartDate
	}

	if end == "" {
		end = defaultEndDate
	}

	granularity := in.Granularity

	groupBy := make([]GroupBySpec, len(in.GroupBy))
	for i, g := range in.GroupBy {
		groupBy[i] = GroupBySpec(g)
	}

	results := h.Backend.GetCostAndUsage(start, end, granularity, in.Metrics, groupBy)

	return &getCostAndUsageOutput{
		ResultsByTime:            results,
		GroupDefinitions:         in.GroupBy,
		DimensionValueAttributes: []any{},
	}, nil
}

type dimensionValue struct {
	Attributes map[string]string `json:"Attributes,omitempty"`
	Value      string            `json:"Value"`
}

type getDimensionValuesInput struct {
	TimePeriod    map[string]string `json:"TimePeriod"`
	Dimension     string            `json:"Dimension"`
	SearchString  string            `json:"SearchString"`
	Context       string            `json:"Context"`
	NextPageToken string            `json:"NextPageToken"`
	MaxResults    int               `json:"MaxResults"`
}

type getDimensionValuesOutput struct {
	NextPageToken   string           `json:"NextPageToken,omitempty"`
	DimensionValues []dimensionValue `json:"DimensionValues"`
	ReturnSize      int              `json:"ReturnSize"`
	TotalSize       int              `json:"TotalSize"`
}

func (h *Handler) handleGetDimensionValues(
	_ context.Context,
	in *getDimensionValuesInput,
) (*getDimensionValuesOutput, error) {
	if in.Dimension == "" {
		return nil, fmt.Errorf("%w: Dimension is required", ErrValidation)
	}

	vals := h.Backend.GetDimensionValues(in.Dimension)

	if in.SearchString != "" {
		filtered := vals[:0]
		search := strings.ToLower(in.SearchString)

		for _, v := range vals {
			if strings.Contains(strings.ToLower(v), search) {
				filtered = append(filtered, v)
			}
		}

		vals = filtered
	}

	items := make([]dimensionValue, 0, len(vals))
	for _, v := range vals {
		items = append(items, dimensionValue{Value: v})
	}

	return &getDimensionValuesOutput{
		DimensionValues: items,
		ReturnSize:      len(items),
		TotalSize:       len(items),
	}, nil
}

type getTagsInput struct {
	TimePeriod    map[string]string `json:"TimePeriod"`
	TagKey        string            `json:"TagKey"`
	SearchString  string            `json:"SearchString"`
	Filter        any               `json:"Filter"`
	NextPageToken string            `json:"NextPageToken"`
	MaxResults    int               `json:"MaxResults"`
}

type getTagsOutput struct {
	NextPageToken string   `json:"NextPageToken,omitempty"`
	Tags          []string `json:"Tags"`
	ReturnSize    int      `json:"ReturnSize"`
	TotalSize     int      `json:"TotalSize"`
}

func (h *Handler) handleGetTags(
	_ context.Context,
	in *getTagsInput,
) (*getTagsOutput, error) {
	var tags []string

	if in.TagKey != "" {
		tags = h.Backend.GetTagValues(in.TagKey)
	} else {
		tags = h.Backend.GetTagKeys()
	}

	if in.SearchString != "" {
		filtered := tags[:0]
		search := strings.ToLower(in.SearchString)

		for _, t := range tags {
			if strings.Contains(strings.ToLower(t), search) {
				filtered = append(filtered, t)
			}
		}

		tags = filtered
	}

	if tags == nil {
		tags = []string{}
	}

	return &getTagsOutput{
		Tags:       tags,
		ReturnSize: len(tags),
		TotalSize:  len(tags),
	}, nil
}

type getCostForecastInput struct {
	Filter                  any               `json:"Filter"`
	TimePeriod              map[string]string `json:"TimePeriod"`
	Granularity             string            `json:"Granularity"`
	Metric                  string            `json:"Metric"`
	PredictionIntervalLevel int               `json:"PredictionIntervalLevel"`
}

type getCostForecastOutput struct {
	Total                 *ForecastResult  `json:"Total,omitempty"`
	ForecastResultsByTime []ForecastResult `json:"ForecastResultsByTime"`
}

func (h *Handler) handleGetCostForecast(
	_ context.Context,
	in *getCostForecastInput,
) (*getCostForecastOutput, error) {
	start, end := defaultForecastStart, defaultForecastEnd
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

	level := in.PredictionIntervalLevel
	if level == 0 {
		level = 80
	}

	buckets, totalMean, totalLo, totalHi := h.Backend.GetForecastByTime(
		start,
		end,
		granularity,
		level,
	)

	return &getCostForecastOutput{
		Total: &ForecastResult{
			MeanValue:                    fmt.Sprintf("%.4f", totalMean),
			PredictionIntervalLowerBound: fmt.Sprintf("%.4f", totalLo),
			PredictionIntervalUpperBound: fmt.Sprintf("%.4f", totalHi),
		},
		ForecastResultsByTime: buckets,
	}, nil
}

type getUsageForecastInput struct {
	Filter                  any               `json:"Filter"`
	TimePeriod              map[string]string `json:"TimePeriod"`
	Granularity             string            `json:"Granularity"`
	Metric                  string            `json:"Metric"`
	PredictionIntervalLevel int               `json:"PredictionIntervalLevel"`
}

type getUsageForecastOutput struct {
	Total                 *ForecastResult  `json:"Total,omitempty"`
	ForecastResultsByTime []ForecastResult `json:"ForecastResultsByTime"`
}

func (h *Handler) handleGetUsageForecast(
	_ context.Context,
	in *getUsageForecastInput,
) (*getUsageForecastOutput, error) {
	start, end := defaultForecastStart, defaultForecastEnd
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

	level := in.PredictionIntervalLevel
	if level == 0 {
		level = 80
	}

	buckets, totalMean, totalLo, totalHi := h.Backend.GetForecastByTime(
		start,
		end,
		granularity,
		level,
	)

	return &getUsageForecastOutput{
		Total: &ForecastResult{
			MeanValue:                    fmt.Sprintf("%.4f", totalMean),
			PredictionIntervalLowerBound: fmt.Sprintf("%.4f", totalLo),
			PredictionIntervalUpperBound: fmt.Sprintf("%.4f", totalHi),
		},
		ForecastResultsByTime: buckets,
	}, nil
}

type getApproximateUsageRecordsInput struct {
	ApproximationDimension string   `json:"ApproximationDimension"`
	Granularity            string   `json:"Granularity"`
	Services               []string `json:"Services"`
}

type getApproximateUsageRecordsOutput struct {
	LookbackPeriod map[string]string `json:"LookbackPeriod,omitempty"`
	// Services and TotalRecords are wire-typed as JSON numbers in real AWS CE
	// (NonNegativeLong), not strings -- see aws-sdk-go-v2/service/costexplorer's
	// GetApproximateUsageRecordsOutput (Services map[string]int64, TotalRecords int64).
	Services     map[string]int64 `json:"Services"`
	TotalRecords int64            `json:"TotalRecords"`
}

func (h *Handler) handleGetApproximateUsageRecords(
	_ context.Context,
	in *getApproximateUsageRecordsInput,
) (*getApproximateUsageRecordsOutput, error) {
	if in.ApproximationDimension == "" {
		return nil, fmt.Errorf("%w: ApproximationDimension is required", ErrValidation)
	}

	if in.Granularity == "" {
		return nil, fmt.Errorf("%w: Granularity is required", ErrValidation)
	}

	start, end, perService, total := h.Backend.GetApproximateUsageRecords(in.Services)

	return &getApproximateUsageRecordsOutput{
		LookbackPeriod: map[string]string{timePeriodKeyStart: start, timePeriodKeyEnd: end},
		Services:       perService,
		TotalRecords:   total,
	}, nil
}

// getCostAndUsageComparisonsInput's field names/types are field-diffed against real AWS
// CE's GetCostAndUsageComparisonsInput: the request field is BaselineTimePeriod (not
// "BaseTimePeriod"), there is no Granularity member on this op, and the metric member is
// the singular, required MetricForComparison string (not a "Metrics" array).
type getCostAndUsageComparisonsInput struct {
	Filter               any               `json:"Filter"`
	BaselineTimePeriod   map[string]string `json:"BaselineTimePeriod"`
	ComparisonTimePeriod map[string]string `json:"ComparisonTimePeriod"`
	MetricForComparison  string            `json:"MetricForComparison"`
	NextPageToken        string            `json:"NextPageToken"`
	GroupBy              []groupBySpec     `json:"GroupBy"`
	MaxResults           int               `json:"MaxResults"`
}

// comparisonMetricValue mirrors aws-sdk-go-v2/service/costexplorer/types'
// ComparisonMetricValue exactly.
type comparisonMetricValue struct {
	BaselineTimePeriodAmount   string `json:"BaselineTimePeriodAmount,omitempty"`
	ComparisonTimePeriodAmount string `json:"ComparisonTimePeriodAmount,omitempty"`
	Difference                 string `json:"Difference,omitempty"`
}

// costAndUsageComparison mirrors aws-sdk-go-v2/service/costexplorer/types'
// CostAndUsageComparison (Metrics -- a map of metric name to comparison value).
type costAndUsageComparison struct {
	Metrics map[string]comparisonMetricValue `json:"Metrics,omitempty"`
}

// getCostAndUsageComparisonsOutput's field names/types are field-diffed against real AWS
// CE's GetCostAndUsageComparisonsOutput: CostAndUsageComparisons (not the previously
// invented "CostAndUsages"), and TotalCostAndUsage is a map keyed by metric name (not an
// array).
type getCostAndUsageComparisonsOutput struct {
	TotalCostAndUsage       map[string]comparisonMetricValue `json:"TotalCostAndUsage"`
	NextPageToken           string                           `json:"NextPageToken,omitempty"`
	CostAndUsageComparisons []costAndUsageComparison         `json:"CostAndUsageComparisons"`
}

// metricTotalForPeriod sums metric across the cost ledger for [start, end) by reusing
// the same DAILY-bucketed aggregation GetCostAndUsage uses, so comparisons are derived
// from real ledger state rather than a hardcoded literal.
func metricTotalForPeriod(h *Handler, start, end, metric string) float64 {
	var total float64

	for _, r := range h.Backend.GetCostAndUsage(start, end, "DAILY", []string{metric}, nil) {
		if mv, ok := r.Total[metric]; ok {
			if v, err := strconv.ParseFloat(mv.Amount, 64); err == nil {
				total += v
			}
		}
	}

	return total
}

func (h *Handler) handleGetCostAndUsageComparisons(
	_ context.Context,
	in *getCostAndUsageComparisonsInput,
) (*getCostAndUsageComparisonsOutput, error) {
	if in.BaselineTimePeriod == nil {
		return nil, fmt.Errorf("%w: BaselineTimePeriod is required", ErrValidation)
	}

	if in.ComparisonTimePeriod == nil {
		return nil, fmt.Errorf("%w: ComparisonTimePeriod is required", ErrValidation)
	}

	if in.MetricForComparison == "" {
		return nil, fmt.Errorf("%w: MetricForComparison is required", ErrValidation)
	}

	baseline := metricTotalForPeriod(
		h, in.BaselineTimePeriod["Start"], in.BaselineTimePeriod["End"], in.MetricForComparison,
	)
	comparison := metricTotalForPeriod(
		h, in.ComparisonTimePeriod["Start"], in.ComparisonTimePeriod["End"], in.MetricForComparison,
	)

	mv := comparisonMetricValue{
		BaselineTimePeriodAmount:   fmt.Sprintf("%.4f", baseline),
		ComparisonTimePeriodAmount: fmt.Sprintf("%.4f", comparison),
		Difference:                 fmt.Sprintf("%.4f", comparison-baseline),
	}
	metrics := map[string]comparisonMetricValue{in.MetricForComparison: mv}

	return &getCostAndUsageComparisonsOutput{
		CostAndUsageComparisons: []costAndUsageComparison{{Metrics: metrics}},
		TotalCostAndUsage:       metrics,
	}, nil
}

type getCostAndUsageWithResourcesInput struct {
	Filter      any               `json:"Filter"`
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	Metrics     []string          `json:"Metrics"`
	GroupBy     []groupBySpec     `json:"GroupBy"`
}

// getCostAndUsageWithResourcesOutput's GroupDefinitions field was previously missing
// entirely -- see aws-sdk-go-v2/service/costexplorer's GetCostAndUsageWithResourcesOutput.
// ResultsByTime is legitimately always empty: real AWS resource-level cost data is keyed
// by individual resource ID (e.g. a specific EC2 instance ARN), and this emulator's
// synthetic cost ledger (seedCostLedger) only models service+date granularity, not
// per-resource entries, so there is no state to derive a non-empty result from.
type getCostAndUsageWithResourcesOutput struct {
	NextPageToken            string        `json:"NextPageToken,omitempty"`
	ResultsByTime            []any         `json:"ResultsByTime"`
	GroupDefinitions         []groupBySpec `json:"GroupDefinitions"`
	DimensionValueAttributes []any         `json:"DimensionValueAttributes"`
}

func (h *Handler) handleGetCostAndUsageWithResources(
	_ context.Context,
	in *getCostAndUsageWithResourcesInput,
) (*getCostAndUsageWithResourcesOutput, error) {
	if in.Filter == nil {
		return nil, fmt.Errorf("%w: Filter is required", ErrValidation)
	}

	if in.Granularity == "" {
		return nil, fmt.Errorf("%w: Granularity is required", ErrValidation)
	}

	return &getCostAndUsageWithResourcesOutput{
		ResultsByTime:            []any{},
		GroupDefinitions:         in.GroupBy,
		DimensionValueAttributes: []any{},
	}, nil
}

type getCostComparisonDriversInput struct {
	BaselineTimePeriod   map[string]string `json:"BaselineTimePeriod"`
	ComparisonTimePeriod map[string]string `json:"ComparisonTimePeriod"`
	Metric               string            `json:"Metric"`
}

type getCostComparisonDriversOutput struct {
	NextPageToken         string `json:"NextPageToken,omitempty"`
	CostComparisonDrivers []any  `json:"CostComparisonDrivers"`
}

func (h *Handler) handleGetCostComparisonDrivers(
	_ context.Context,
	_ *getCostComparisonDriversInput,
) (*getCostComparisonDriversOutput, error) {
	return &getCostComparisonDriversOutput{
		CostComparisonDrivers: []any{},
	}, nil
}

// buildCostUsageOps returns the cost-and-usage-family op dispatch entries.
func (h *Handler) buildCostUsageOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetCostAndUsage": service.WrapOp(
			h.handleGetCostAndUsage,
		),
		"GetCostForecast": service.WrapOp(
			h.handleGetCostForecast,
		),
		"GetUsageForecast": service.WrapOp(
			h.handleGetUsageForecast,
		),
		"GetDimensionValues": service.WrapOp(
			h.handleGetDimensionValues,
		),
		"GetTags": service.WrapOp(h.handleGetTags),
		"GetApproximateUsageRecords": service.WrapOp(
			h.handleGetApproximateUsageRecords,
		),
		"GetCostAndUsageComparisons": service.WrapOp(
			h.handleGetCostAndUsageComparisons,
		),
		"GetCostAndUsageWithResources": service.WrapOp(
			h.handleGetCostAndUsageWithResources,
		),
		"GetCostComparisonDrivers": service.WrapOp(
			h.handleGetCostComparisonDrivers,
		),
	}
}
