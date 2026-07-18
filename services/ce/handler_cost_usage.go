package ce

import (
	"context"
	"fmt"
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

type getCostAndUsageOutput struct {
	NextPageToken            string         `json:"NextPageToken,omitempty"`
	ResultsByTime            []ResultByTime `json:"ResultsByTime"`
	DimensionValueAttributes []any          `json:"DimensionValueAttributes"`
}

func (h *Handler) handleGetCostAndUsage(
	_ context.Context,
	in *getCostAndUsageInput,
) (*getCostAndUsageOutput, error) {
	if in.Granularity == "" {
		return nil, fmt.Errorf("%w: Granularity is required", errInvalidRequest)
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
		return nil, fmt.Errorf("%w: Dimension is required", errInvalidRequest)
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
	Services       map[string]string `json:"Services"`
	TotalRecords   string            `json:"TotalRecords"`
}

func (h *Handler) handleGetApproximateUsageRecords(
	_ context.Context,
	_ *getApproximateUsageRecordsInput,
) (*getApproximateUsageRecordsOutput, error) {
	return &getApproximateUsageRecordsOutput{
		Services:     map[string]string{},
		TotalRecords: "0",
	}, nil
}

type getCostAndUsageComparisonsInput struct {
	BaseTimePeriod       map[string]string `json:"BaseTimePeriod"`
	ComparisonTimePeriod map[string]string `json:"ComparisonTimePeriod"`
	Granularity          string            `json:"Granularity"`
	Metrics              []string          `json:"Metrics"`
}

type getCostAndUsageComparisonsOutput struct {
	NextPageToken     string `json:"NextPageToken,omitempty"`
	CostAndUsages     []any  `json:"CostAndUsages"`
	TotalCostAndUsage []any  `json:"TotalCostAndUsage"`
}

func (h *Handler) handleGetCostAndUsageComparisons(
	_ context.Context,
	_ *getCostAndUsageComparisonsInput,
) (*getCostAndUsageComparisonsOutput, error) {
	return &getCostAndUsageComparisonsOutput{
		CostAndUsages:     []any{},
		TotalCostAndUsage: []any{},
	}, nil
}

type getCostAndUsageWithResourcesInput struct {
	Filter      any               `json:"Filter"`
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	Metrics     []string          `json:"Metrics"`
}

type getCostAndUsageWithResourcesOutput struct {
	NextPageToken            string `json:"NextPageToken,omitempty"`
	ResultsByTime            []any  `json:"ResultsByTime"`
	DimensionValueAttributes []any  `json:"DimensionValueAttributes"`
}

func (h *Handler) handleGetCostAndUsageWithResources(
	_ context.Context,
	_ *getCostAndUsageWithResourcesInput,
) (*getCostAndUsageWithResourcesOutput, error) {
	return &getCostAndUsageWithResourcesOutput{
		ResultsByTime:            []any{},
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
