package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type getInsightInput struct {
	InsightID string `json:"InsightId"`
}

type insightView struct {
	InsightID  string   `json:"InsightId"`
	GroupARN   string   `json:"GroupARN"`
	GroupName  string   `json:"GroupName"`
	State      string   `json:"State"`
	Summary    string   `json:"Summary"`
	Categories []string `json:"Categories,omitempty"`
	StartTime  float64  `json:"StartTime"`
	EndTime    float64  `json:"EndTime,omitempty"`
}

func toInsightView(i *Insight) insightView {
	v := insightView{
		InsightID: i.InsightID,
		GroupARN:  i.GroupARN,
		GroupName: i.GroupName,
		State:     i.State,
		Summary:   i.Summary,
		StartTime: float64(i.StartTime.Unix()),
	}
	if !i.EndTime.IsZero() {
		v.EndTime = float64(i.EndTime.Unix())
	}

	return v
}

func (h *Handler) handleGetInsight(_ context.Context, body []byte) ([]byte, error) {
	var in getInsightInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.InsightID == "" {
		return nil, fmt.Errorf("%w: InsightId is required", errInvalidRequest)
	}

	i, err := h.Backend.GetInsight(in.InsightID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Insight": toInsightView(i),
	})
}

type getInsightEventsInput struct {
	InsightID  string `json:"InsightId"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type insightEventView struct {
	Summary   string  `json:"Summary"`
	EventTime float64 `json:"EventTime"`
}

func (h *Handler) handleGetInsightEvents(_ context.Context, body []byte) ([]byte, error) {
	var in getInsightEventsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.InsightID == "" {
		return nil, fmt.Errorf("%w: InsightId is required", errInvalidRequest)
	}

	events, err := h.Backend.GetInsightEvents(in.InsightID)
	if err != nil {
		return nil, err
	}

	views := make([]insightEventView, 0, len(events))
	for _, e := range events {
		views = append(views, insightEventView{
			Summary:   e.Summary,
			EventTime: float64(e.EventTime.Unix()),
		})
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultInsightEventsPageSize)

	return json.Marshal(map[string]any{
		"InsightEvents": pg.Data,
		keyNextToken:    pg.Next,
	})
}

type getInsightImpactGraphInput struct {
	InsightID string  `json:"InsightId"`
	NextToken string  `json:"NextToken"`
	StartTime float64 `json:"StartTime"`
	EndTime   float64 `json:"EndTime"`
}

func (h *Handler) handleGetInsightImpactGraph(_ context.Context, body []byte) ([]byte, error) {
	var in getInsightImpactGraphInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.InsightID == "" {
		return nil, fmt.Errorf("%w: InsightId is required", errInvalidRequest)
	}

	// Validate the insight exists.
	if _, err := h.Backend.GetInsight(in.InsightID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"InsightId":             in.InsightID,
		keyServices:             []any{},
		"StartTime":             in.StartTime,
		"EndTime":               in.EndTime,
		"ServiceGraphStartTime": in.StartTime,
		"ServiceGraphEndTime":   in.EndTime,
		keyNextToken:            "",
	})
}

type getInsightSummariesInput struct {
	GroupARN   string   `json:"GroupARN"`
	GroupName  string   `json:"GroupName"`
	NextToken  string   `json:"NextToken"`
	States     []string `json:"States"`
	StartTime  float64  `json:"StartTime"`
	EndTime    float64  `json:"EndTime"`
	MaxResults int32    `json:"MaxResults"`
}

func (h *Handler) handleGetInsightSummaries(_ context.Context, body []byte) ([]byte, error) {
	var in getInsightSummariesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	summaries, err := h.Backend.GetInsightSummaries(in.States)
	if err != nil {
		return nil, err
	}

	views := make([]insightView, 0, len(summaries))

	for i := range summaries {
		views = append(views, toInsightView(&summaries[i]))
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultInsightSummariesPageSize)

	return json.Marshal(map[string]any{
		"InsightSummaries": pg.Data,
		keyNextToken:       pg.Next,
	})
}
