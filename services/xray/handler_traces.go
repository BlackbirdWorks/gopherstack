package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type getTraceSummariesInput struct {
	FilterExpression string  `json:"FilterExpression"`
	TimeRangeType    string  `json:"TimeRangeType"`
	NextToken        string  `json:"NextToken"`
	StartTime        float64 `json:"StartTime"`
	EndTime          float64 `json:"EndTime"`
	MaxResults       int32   `json:"MaxResults"`
	Sampling         bool    `json:"Sampling"`
}

type traceSummaryHTTPView struct {
	HTTPURL    string `json:"HttpURL,omitempty"`
	HTTPMethod string `json:"HttpMethod,omitempty"`
	ClientIP   string `json:"ClientIp,omitempty"`
	UserAgent  string `json:"UserAgent,omitempty"`
	HTTPStatus int    `json:"HttpStatus,omitempty"`
}

type traceSummaryServiceIDView struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

type traceSummaryForecastView struct{}

type traceSummary struct {
	HTTP               *traceSummaryHTTPView       `json:"Http,omitempty"`
	Annotations        map[string]any              `json:"Annotations,omitempty"`
	ForecastStatistics *traceSummaryForecastView   `json:"ForecastStatistics,omitempty"`
	EntryPoint         string                      `json:"EntryPoint,omitempty"`
	ID                 string                      `json:"Id"`
	ServiceIds         []traceSummaryServiceIDView `json:"ServiceIds,omitempty"` //nolint:revive // AWS API field name
	Users              []string                    `json:"Users,omitempty"`
	Duration           float64                     `json:"Duration"`
	ResponseTime       float64                     `json:"ResponseTime"`
	ApproximateTime    float64                     `json:"ApproximateTime"`
	Revision           int                         `json:"Revision"`
	HasFault           bool                        `json:"HasFault"`
	HasError           bool                        `json:"HasError"`
	HasThrottle        bool                        `json:"HasThrottle"`
	IsPartial          bool                        `json:"IsPartial"`
}

// buildTraceSummaryView converts a TraceSummaryData to the JSON view struct.
func buildTraceSummaryView(traceID string, sd TraceSummaryData) traceSummary {
	s := traceSummary{
		ID:              traceID,
		Duration:        sd.Duration,
		ResponseTime:    sd.ResponseTime,
		ApproximateTime: sd.ApproxTime,
		HasFault:        sd.HasFault,
		HasError:        sd.HasError,
		HasThrottle:     sd.HasThrottle,
		IsPartial:       sd.IsPartial,
		EntryPoint:      sd.EntryPoint,
		Revision:        sd.Revision,
		// ForecastStatistics is always present per AWS API (even as empty object).
		ForecastStatistics: &traceSummaryForecastView{},
	}

	if len(sd.Users) > 0 {
		s.Users = sd.Users
	}

	if sd.HTTP != nil {
		s.HTTP = &traceSummaryHTTPView{
			HTTPStatus: sd.HTTP.HTTPStatus,
			HTTPURL:    sd.HTTP.HTTPURL,
			HTTPMethod: sd.HTTP.HTTPMethod,
			ClientIP:   sd.HTTP.ClientIP,
			UserAgent:  sd.HTTP.UserAgent,
		}
	}

	if len(sd.Annotations) > 0 {
		s.Annotations = sd.Annotations
	}

	if len(sd.ServiceIDs) > 0 {
		s.ServiceIds = make([]traceSummaryServiceIDView, 0, len(sd.ServiceIDs))
		for _, svc := range sd.ServiceIDs {
			s.ServiceIds = append(s.ServiceIds, traceSummaryServiceIDView(svc))
		}
	}

	return s
}

func (h *Handler) handleGetTraceSummaries(_ context.Context, body []byte) ([]byte, error) {
	var in getTraceSummariesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.TimeRangeType != "" &&
		in.TimeRangeType != timeRangeTypeTraceID &&
		in.TimeRangeType != timeRangeTypeEvent &&
		in.TimeRangeType != timeRangeTypeService {
		return nil, fmt.Errorf("%w: TimeRangeType must be %q, %q, or %q, got %q",
			errInvalidRequest, timeRangeTypeTraceID, timeRangeTypeEvent, timeRangeTypeService, in.TimeRangeType)
	}

	traces := h.Backend.GetTraceSummaries()
	allSegs := h.Backend.GetAllParsedSegments()

	summaries := make([]traceSummary, 0, len(traces))

	for i := range traces {
		// Apply optional time window filter when both bounds are provided.
		if in.StartTime > 0 && in.EndTime > 0 {
			ts := float64(traces[i].StartTime.Unix())
			if ts < in.StartTime || ts > in.EndTime {
				continue
			}
		}

		segs := allSegs[traces[i].TraceID]
		sd := BuildTraceSummary(traces[i].TraceID, segs)

		if !evaluateFilter(in.FilterExpression, sd) {
			continue
		}

		summaries = append(summaries, buildTraceSummaryView(traces[i].TraceID, sd))
	}

	pg := page.New(summaries, in.NextToken, int(in.MaxResults), defaultTraceSummariesPageSize)

	return json.Marshal(map[string]any{
		"TraceSummaries":       pg.Data,
		"TracesProcessedCount": len(summaries),
		keyNextToken:           pg.Next,
	})
}

type batchGetTracesInput struct {
	TraceIDs []string `json:"TraceIds"`
}

type batchSegmentOutput struct {
	ID       string `json:"Id"`
	Document string `json:"Document"`
}

type traceOutput struct {
	ID       string               `json:"Id"`
	Segments []batchSegmentOutput `json:"Segments"`
	Duration float64              `json:"Duration"`
}

func (h *Handler) handleBatchGetTraces(_ context.Context, body []byte) ([]byte, error) {
	var in batchGetTracesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if len(in.TraceIDs) > maxBatchGetTraces {
		return nil, fmt.Errorf("%w: BatchGetTraces supports at most %d trace IDs, got %d",
			ErrBatchGetTracesLimit, maxBatchGetTraces, len(in.TraceIDs))
	}

	traces := make([]traceOutput, 0, len(in.TraceIDs))
	unprocessed := make([]string, 0, len(in.TraceIDs))

	for _, id := range in.TraceIDs {
		t := h.Backend.GetTrace(id)
		if t == nil {
			unprocessed = append(unprocessed, id)

			continue
		}

		segs := h.Backend.GetParsedSegments(id)
		sd := BuildTraceSummary(id, segs)

		segViews := make([]batchSegmentOutput, 0, len(segs))
		for _, seg := range segs {
			segViews = append(segViews, batchSegmentOutput{
				ID:       seg.ID,
				Document: seg.Document,
			})
		}

		// Fall back to raw segments when parsed segments are unavailable.
		if len(segViews) == 0 {
			for _, rawDoc := range t.Segments {
				var hdr struct {
					ID string `json:"id"`
				}

				if err := json.Unmarshal([]byte(rawDoc), &hdr); err == nil {
					segViews = append(segViews, batchSegmentOutput{
						ID:       hdr.ID,
						Document: rawDoc,
					})
				}
			}
		}

		traces = append(traces, traceOutput{
			ID:       t.TraceID,
			Duration: sd.Duration,
			Segments: segViews,
		})
	}

	return json.Marshal(map[string]any{
		"Traces":              traces,
		"UnprocessedTraceIds": unprocessed,
	})
}
