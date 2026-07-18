package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type batchGetMetricDataInput struct {
	Queries []struct {
		ID        string `json:"Id"`
		Namespace string `json:"Namespace"`
		Metric    string `json:"Metric"`
	} `json:"Queries"`
}

type batchGetMetricDataOutput struct {
	Results []struct {
		ID         string    `json:"Id"`
		Timestamps []any     `json:"Timestamps"`
		Values     []float64 `json:"Values"`
	} `json:"Results"`
}

func (h *Handler) handleBatchGetMetricData(c *echo.Context) (any, error) {
	var in batchGetMetricDataInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	queries := make([]MetricDataQuery, 0, len(in.Queries))
	for _, q := range in.Queries {
		queries = append(
			queries,
			MetricDataQuery{ID: q.ID, Namespace: q.Namespace, Metric: q.Metric},
		)
	}

	results, err := h.Backend.BatchGetMetricData(queries)
	if err != nil {
		return nil, err
	}

	out := batchGetMetricDataOutput{}
	for _, r := range results {
		timestamps := make([]any, 0, len(r.Timestamps))
		for _, ts := range r.Timestamps {
			timestamps = append(timestamps, awstime.Epoch(ts))
		}

		out.Results = append(out.Results, struct {
			ID         string    `json:"Id"`
			Timestamps []any     `json:"Timestamps"`
			Values     []float64 `json:"Values"`
		}{
			ID:         r.ID,
			Timestamps: timestamps,
			Values:     r.Values,
		})
	}

	if out.Results == nil {
		out.Results = []struct {
			ID         string    `json:"Id"`
			Timestamps []any     `json:"Timestamps"`
			Values     []float64 `json:"Values"`
		}{}
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
