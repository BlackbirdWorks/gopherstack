package xray

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	keyStartTime = "StartTime"
	keyEndTime   = "EndTime"

	defaultTracesPageSize       = 100
	defaultServiceGraphPageSize = 100
	defaultTimeSeriesPageSize   = 100
	defaultTagsPageSize         = 50
)

// segmentDoc is used to extract timing and ID from a raw segment JSON for the retrieval response.
type segmentDoc struct {
	ID        string  `json:"id"`
	StartTime float64 `json:"start_time"`
	EndTime   float64 `json:"end_time"`
}

// --- GetServiceGraph ---

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

// --- GetTimeSeriesServiceStatistics ---

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

// --- GetTraceGraph ---

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

// --- GetTraceSegmentDestination ---

func (h *Handler) handleGetTraceSegmentDestination(_ context.Context, _ []byte) ([]byte, error) {
	dest := h.Backend.GetTraceSegmentDestination()

	return json.Marshal(map[string]any{
		"Destination": dest,
		"Status":      statusActive,
	})
}

// --- ListRetrievedTraces ---

type listRetrievedTracesInput struct {
	RetrievalToken string `json:"RetrievalToken"`
	NextToken      string `json:"NextToken"`
	MaxResults     int    `json:"MaxResults"`
}

// buildTraceView converts a raw Trace into the map shape returned by ListRetrievedTraces.
func buildTraceView(t *Trace) map[string]any {
	segs := make([]any, 0, len(t.Segments))

	var minStart, maxEnd float64

	for _, rawSeg := range t.Segments {
		var doc segmentDoc
		if err := json.Unmarshal([]byte(rawSeg), &doc); err == nil {
			segs = append(segs, map[string]any{
				"Document": rawSeg,
				"Id":       doc.ID,
			})

			if doc.StartTime > 0 && (minStart == 0 || doc.StartTime < minStart) {
				minStart = doc.StartTime
			}

			if doc.EndTime > maxEnd {
				maxEnd = doc.EndTime
			}
		}
	}

	duration := 0.0
	if maxEnd > minStart && minStart > 0 {
		duration = maxEnd - minStart
	}

	return map[string]any{
		"Id":       t.TraceID,
		"Duration": duration,
		"Segments": segs,
	}
}

func (h *Handler) handleListRetrievedTraces(_ context.Context, body []byte) ([]byte, error) {
	var in listRetrievedTracesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.RetrievalToken == "" {
		return nil, fmt.Errorf("%w: RetrievalToken is required", errInvalidRequest)
	}

	status, traces := h.Backend.ListRetrievedTraces(in.RetrievalToken)

	traceViews := make([]map[string]any, 0, len(traces))
	for _, t := range traces {
		traceViews = append(traceViews, buildTraceView(t))
	}

	pg := page.New(traceViews, in.NextToken, in.MaxResults, defaultTracesPageSize)
	resp := map[string]any{
		"RetrievalStatus": status,
		"Traces":          pg.Data,
	}
	if pg.Next != "" {
		resp[keyNextToken] = pg.Next
	}

	return json.Marshal(resp)
}

// --- ListTagsForResource ---

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	NextToken   string `json:"NextToken"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var in listTagsForResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags := h.Backend.ListTagsForResource(in.ResourceARN)

	pg := page.New(tags, in.NextToken, 0, defaultTagsPageSize)

	return json.Marshal(map[string]any{
		"Tags":       pg.Data,
		keyNextToken: pg.Next,
	})
}

// --- StartTraceRetrieval ---

type startTraceRetrievalInput struct {
	TraceIDs  []string `json:"TraceIds"`
	StartTime float64  `json:"StartTime"`
	EndTime   float64  `json:"EndTime"`
}

func (h *Handler) handleStartTraceRetrieval(_ context.Context, body []byte) ([]byte, error) {
	var in startTraceRetrievalInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if len(in.TraceIDs) == 0 {
		return nil, fmt.Errorf("%w: TraceIds is required", errInvalidRequest)
	}

	token := h.Backend.StartTraceRetrieval(in.TraceIDs)

	return json.Marshal(map[string]any{
		"RetrievalToken": token,
	})
}

// --- TagResource ---

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceARN string            `json:"ResourceARN"`
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	var in tagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	h.Backend.TagResource(in.ResourceARN, in.Tags)

	return json.Marshal(map[string]any{})
}

// --- UntagResource ---

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) ([]byte, error) {
	var in untagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	h.Backend.UntagResource(in.ResourceARN, in.TagKeys)

	return json.Marshal(map[string]any{})
}

// --- UpdateIndexingRule ---

type updateIndexingRuleInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateIndexingRule(_ context.Context, body []byte) ([]byte, error) {
	var in updateIndexingRuleInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	rule, err := h.Backend.UpdateIndexingRule(in.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"IndexingRule": map[string]any{
			"Name":       rule.Name,
			"ModifiedAt": rule.ModifiedAt,
		},
	})
}

// --- UpdateTraceSegmentDestination ---

type updateTraceSegmentDestinationInput struct {
	Destination string `json:"Destination"`
}

func (h *Handler) handleUpdateTraceSegmentDestination(_ context.Context, body []byte) ([]byte, error) {
	var in updateTraceSegmentDestinationInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.Destination == "" {
		return nil, fmt.Errorf("%w: Destination is required", errInvalidRequest)
	}

	dest := h.Backend.UpdateTraceSegmentDestination(in.Destination)

	return json.Marshal(map[string]any{
		"Destination": dest,
		"Status":      statusActive,
	})
}
