package sesv2

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type batchGetMetricDataInput struct {
	Queries []struct {
		Dimensions map[string]string `json:"Dimensions"`
		ID         string            `json:"Id"`
		Namespace  string            `json:"Namespace"`
		Metric     string            `json:"Metric"`
		StartDate  float64           `json:"StartDate"`
		EndDate    float64           `json:"EndDate"`
	} `json:"Queries"`
}

type batchGetMetricDataResultOutput struct {
	ID         string  `json:"Id"`
	Timestamps []any   `json:"Timestamps"`
	Values     []int64 `json:"Values"`
}

type batchGetMetricDataOutput struct {
	Results []batchGetMetricDataResultOutput `json:"Results"`
}

// epochSecondsToTime decodes a JSON-protocol unixTimestamp number (see
// pkgs/awstime's doc comment on the encode direction) back into a
// time.Time. A zero value decodes to the zero time.Time, matching an absent
// field.
func epochSecondsToTime(f float64) time.Time {
	if f == 0 {
		return time.Time{}
	}

	sec := int64(f)
	nsec := int64((f - float64(sec)) * float64(time.Second))

	return time.Unix(sec, nsec).UTC()
}

func (h *Handler) handleBatchGetMetricData(c *echo.Context) (any, error) {
	var in batchGetMetricDataInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	queries := make([]MetricDataQuery, 0, len(in.Queries))
	for _, q := range in.Queries {
		queries = append(queries, MetricDataQuery{
			ID:         q.ID,
			Namespace:  q.Namespace,
			Metric:     q.Metric,
			StartDate:  epochSecondsToTime(q.StartDate),
			EndDate:    epochSecondsToTime(q.EndDate),
			Dimensions: q.Dimensions,
		})
	}

	results, err := h.Backend.BatchGetMetricData(queries)
	if err != nil {
		return nil, err
	}

	out := batchGetMetricDataOutput{Results: make([]batchGetMetricDataResultOutput, 0, len(results))}

	for _, r := range results {
		timestamps := make([]any, 0, len(r.Timestamps))
		for _, ts := range r.Timestamps {
			timestamps = append(timestamps, awstime.Epoch(ts))
		}

		out.Results = append(out.Results, batchGetMetricDataResultOutput{
			ID:         r.ID,
			Timestamps: timestamps,
			Values:     r.Values,
		})
	}

	return &out, nil
}

func (h *Handler) handleGetMessageInsights(messageID string) (any, error) {
	result, err := h.Backend.GetMessageInsights(messageID)
	if err != nil {
		return nil, err
	}

	return result, nil
}
