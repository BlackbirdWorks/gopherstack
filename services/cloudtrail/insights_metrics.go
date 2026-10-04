package cloudtrail

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"
)

const (
	insightTypeCallRate  = "ApiCallRateInsight"
	insightTypeErrorRate = "ApiErrorRateInsight"

	metricDataNonZero   = "NonZeroData"
	metricDataFillZeros = "FillWithZeros"

	metricPeriodMinute    = 60
	metricPeriodFiveMin   = 300
	metricPeriodHour      = 3600
	defaultMetricPeriod   = metricPeriodHour
	maxMetricResults      = 21600
	defaultMetricLookback = eventHistoryRetention
)

// InsightsMetricInput is the ListInsightsMetricData request.
type InsightsMetricInput struct {
	Start       *time.Time
	End         *time.Time
	EventName   string
	EventSource string
	InsightType string
	ErrorCode   string
	DataType    string
	NextToken   string
	Period      int
	MaxResults  int
}

// InsightsMetricOutput is the ListInsightsMetricData time series (epoch-second timestamps).
type InsightsMetricOutput struct {
	NextToken  string
	Timestamps []float64
	Values     []float64
}

func validateInsightsMetricInput(in *InsightsMetricInput) error {
	switch in.InsightType {
	case insightTypeCallRate, insightTypeErrorRate:
	default:
		return fmt.Errorf("%w: InsightType must be ApiCallRateInsight or ApiErrorRateInsight", ErrValidation)
	}

	if in.InsightType == insightTypeErrorRate && in.ErrorCode == "" {
		return fmt.Errorf("%w: ErrorCode is required for ApiErrorRateInsight", ErrValidation)
	}

	switch in.DataType {
	case "", metricDataNonZero, metricDataFillZeros:
	default:
		return fmt.Errorf("%w: DataType must be NonZeroData or FillWithZeros", ErrValidation)
	}

	switch in.Period {
	case 0, metricPeriodMinute, metricPeriodFiveMin, metricPeriodHour:
	default:
		return fmt.Errorf("%w: Period must be 60, 300 or 3600", ErrValidation)
	}

	if in.MaxResults < 0 || in.MaxResults > maxMetricResults {
		return fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidation, maxMetricResults)
	}

	return nil
}

// eventErrorCode extracts errorCode from a recorded event's CloudTrailEvent JSON.
func eventErrorCode(ev Event) string {
	var rec struct {
		ErrorCode string `json:"errorCode"`
	}

	if ev.CloudTrailEvent == "" || json.Unmarshal([]byte(ev.CloudTrailEvent), &rec) != nil {
		return ""
	}

	return rec.ErrorCode
}

// ListInsightsMetricData returns the per-period count of recorded management API calls (or errors) for an
// event source/name, the series ApiCallRateInsight/ApiErrorRateInsight analyze.
func (b *InMemoryBackend) ListInsightsMetricData(in *InsightsMetricInput) (*InsightsMetricOutput, error) {
	if err := validateInsightsMetricInput(in); err != nil {
		return nil, err
	}

	period := int64(in.Period)
	if period == 0 {
		period = defaultMetricPeriod
	}

	end := time.Now().UTC()
	if in.End != nil {
		end = in.End.UTC()
	}

	start := end.Add(-defaultMetricLookback)
	if in.Start != nil {
		start = in.Start.UTC()
	}

	if !start.Before(end) {
		return nil, fmt.Errorf("%w: StartTime must be before EndTime", ErrValidation)
	}

	counts := b.metricBucketCounts(in, start, end, period)

	firstBucket := start.Unix() / period
	lastBucket := (end.Unix() - 1) / period

	var buckets []int64

	if in.DataType == metricDataFillZeros {
		for k := firstBucket; k <= lastBucket; k++ {
			buckets = append(buckets, k)
		}
	} else {
		for k := range counts {
			buckets = append(buckets, k)
		}

		slices.Sort(buckets)
	}

	return pageMetricBuckets(buckets, counts, period, in), nil
}

func (b *InMemoryBackend) metricBucketCounts(
	in *InsightsMetricInput,
	start, end time.Time,
	period int64,
) map[int64]float64 {
	b.mu.RLock("ListInsightsMetricData")
	defer b.mu.RUnlock()

	counts := make(map[int64]float64)

	for _, ev := range b.events {
		if ev.EventSource != in.EventSource || ev.EventName != in.EventName ||
			ev.EventCategory != eventCategoryManagement ||
			ev.EventTime.Before(start) || !ev.EventTime.Before(end) {
			continue
		}

		if in.InsightType == insightTypeErrorRate && eventErrorCode(ev) != in.ErrorCode {
			continue
		}

		counts[ev.EventTime.Unix()/period]++
	}

	return counts
}

func pageMetricBuckets(
	buckets []int64,
	counts map[int64]float64,
	period int64,
	in *InsightsMetricInput,
) *InsightsMetricOutput {
	offset := 0

	if in.NextToken != "" {
		if n, err := strconv.Atoi(in.NextToken); err == nil && n >= 0 && n <= len(buckets) {
			offset = n
		}
	}

	limit := in.MaxResults
	if limit <= 0 {
		limit = maxMetricResults
	}

	end := min(offset+limit, len(buckets))
	out := &InsightsMetricOutput{Timestamps: []float64{}, Values: []float64{}}

	for _, k := range buckets[offset:end] {
		out.Timestamps = append(out.Timestamps, float64(k*period))
		out.Values = append(out.Values, counts[k])
	}

	if end < len(buckets) {
		out.NextToken = strconv.Itoa(end)
	}

	return out
}
