package xray

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type getServiceGraphInput struct {
	NextToken string  `json:"NextToken"`
	GroupName string  `json:"GroupName"`
	GroupARN  string  `json:"GroupARN"`
	StartTime float64 `json:"StartTime"`
	EndTime   float64 `json:"EndTime"`
}

func (h *Handler) handleGetServiceGraph(_ context.Context, body []byte) ([]byte, error) {
	var in getServiceGraphInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.StartTime == 0 || in.EndTime == 0 {
		return nil, fmt.Errorf("%w: StartTime and EndTime are required", errInvalidRequest)
	}

	services := h.Backend.GetServiceGraph(time.Unix(int64(in.StartTime), 0), time.Unix(int64(in.EndTime), 0))

	pg := page.New(services, in.NextToken, 0, defaultServiceGraphPageSize)

	return json.Marshal(map[string]any{
		keyServices:                pg.Data,
		keyNextToken:               pg.Next,
		"ContainsOldGroupVersions": false,
		keyStartTime:               in.StartTime,
		keyEndTime:                 in.EndTime,
	})
}

type getTimeSeriesServiceStatisticsInput struct {
	NextToken                string  `json:"NextToken"`
	EntitySelectorExpression string  `json:"EntitySelectorExpression"`
	GroupName                string  `json:"GroupName"`
	GroupARN                 string  `json:"GroupARN"`
	StartTime                float64 `json:"StartTime"`
	EndTime                  float64 `json:"EndTime"`
	Period                   int     `json:"Period"`
	ForecastStatistics       bool    `json:"ForecastStatistics"`
}

func (h *Handler) handleGetTimeSeriesServiceStatistics(_ context.Context, body []byte) ([]byte, error) {
	var in getTimeSeriesServiceStatisticsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.StartTime == 0 || in.EndTime == 0 {
		return nil, fmt.Errorf("%w: StartTime and EndTime are required", errInvalidRequest)
	}

	period := in.Period
	if period <= 0 {
		period = 60
	}

	// AWS docs: Period must be 60 or 300 seconds.
	if period != 60 && period != 300 {
		return nil, fmt.Errorf("%w: Period must be 60 or 300 seconds, got %d", errInvalidRequest, period)
	}

	stats := h.Backend.GetTimeSeriesServiceStatistics(
		time.Unix(int64(in.StartTime), 0),
		time.Unix(int64(in.EndTime), 0),
		period,
	)

	pg := page.New(stats, in.NextToken, 0, defaultTimeSeriesPageSize)

	return json.Marshal(map[string]any{
		"TimeSeriesServiceStatistics": pg.Data,
		"ContainsOldGroupVersions":    false,
		keyNextToken:                  pg.Next,
	})
}

type getTraceGraphInput struct {
	NextToken string   `json:"NextToken"`
	TraceIDs  []string `json:"TraceIds"`
}

func (h *Handler) handleGetTraceGraph(_ context.Context, body []byte) ([]byte, error) {
	var in getTraceGraphInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if len(in.TraceIDs) == 0 {
		return nil, fmt.Errorf("%w: TraceIds is required", errInvalidRequest)
	}

	services := h.Backend.GetTraceGraph(in.TraceIDs)

	pg := page.New(services, in.NextToken, 0, defaultServiceGraphPageSize)

	return json.Marshal(map[string]any{
		keyServices:  pg.Data,
		keyNextToken: pg.Next,
	})
}

const (
	keyStartTime = "StartTime"
)

const (
	keyEndTime = "EndTime"
)

const (
	defaultServiceGraphPageSize = 100
)

const (
	defaultTimeSeriesPageSize = 100
)
