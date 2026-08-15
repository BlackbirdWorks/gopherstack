package xray

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

// annotationValueView is the wire shape for a single annotation value: a tagged
// union with exactly one of StringValue/NumberValue/BooleanValue set, selected by
// the value's Go kind. X-Ray segment document "annotations" values are only ever
// string, number, or boolean (the segment document spec disallows nested
// objects/arrays there), matching the real API's AnnotationValue union exactly.
type annotationValueView struct {
	StringValue  *string  `json:"StringValue,omitempty"`
	NumberValue  *float64 `json:"NumberValue,omitempty"`
	BooleanValue *bool    `json:"BooleanValue,omitempty"`
}

func toAnnotationValueView(v any) annotationValueView {
	switch tv := v.(type) {
	case string:
		return annotationValueView{StringValue: &tv}
	case bool:
		return annotationValueView{BooleanValue: &tv}
	case float64:
		return annotationValueView{NumberValue: &tv}
	default:
		// Defensive fallback for a caller-supplied annotation value outside the
		// documented string/number/boolean set (e.g. a stray nested object) --
		// stringify rather than drop it silently.
		s := fmt.Sprintf("%v", tv)

		return annotationValueView{StringValue: &s}
	}
}

// valueWithServiceIDsView is the wire shape for one distinct value observed for
// an annotation key, per the real API's ValueWithServiceIds type -- NOT a flat
// key->value map. See buildTraceSummaryView.
type valueWithServiceIDsView struct {
	AnnotationValue annotationValueView         `json:"AnnotationValue"`
	ServiceIds      []traceSummaryServiceIDView `json:"ServiceIds"` //nolint:revive // AWS API field name
}

func toValueWithServiceIDsViews(occs []AnnotationOccurrence) []valueWithServiceIDsView {
	views := make([]valueWithServiceIDsView, 0, len(occs))

	for _, occ := range occs {
		svcViews := make([]traceSummaryServiceIDView, 0, len(occ.ServiceIDs))
		for _, s := range occ.ServiceIDs {
			svcViews = append(svcViews, traceSummaryServiceIDView(s))
		}

		views = append(views, valueWithServiceIDsView{
			AnnotationValue: toAnnotationValueView(occ.Value),
			ServiceIds:      svcViews,
		})
	}

	return views
}

// traceSummary is the wire view for a single entry of GetTraceSummariesOutput's
// TraceSummaries list. EntryPoint is a ServiceId object per the real SDK (types.ServiceId),
// not a plain string -- a real client fails to parse this field otherwise, since
// awsRestjson1_deserializeDocumentServiceId expects a JSON object. There is deliberately
// no per-item "ApproximateTime" field: the real API only has "ApproximateTime" at the
// GetTraceSummariesOutput envelope level (the start time of the results page), not per
// TraceSummary -- see handleGetTraceSummaries. Annotations is a map to a LIST of
// {AnnotationValue,ServiceIds} objects per key (types.ValueWithServiceIds), not a flat
// key->value map -- a real client's deserializer hard-errors on a flat value here
// (awsRestjson1_deserializeDocumentAnnotations expects each map value to be a JSON array).
type traceSummary struct {
	HTTP               *traceSummaryHTTPView                `json:"Http,omitempty"`
	Annotations        map[string][]valueWithServiceIDsView `json:"Annotations,omitempty"`
	ForecastStatistics *traceSummaryForecastView            `json:"ForecastStatistics,omitempty"`
	EntryPoint         *traceSummaryServiceIDView           `json:"EntryPoint,omitempty"`
	ID                 string                               `json:"Id"`
	ServiceIds         []traceSummaryServiceIDView          `json:"ServiceIds,omitempty"` //nolint:revive // AWS field name
	Users              []string                             `json:"Users,omitempty"`
	Duration           float64                              `json:"Duration"`
	ResponseTime       float64                              `json:"ResponseTime"`
	StartTime          float64                              `json:"StartTime"`
	Revision           int                                  `json:"Revision"`
	HasFault           bool                                 `json:"HasFault"`
	HasError           bool                                 `json:"HasError"`
	HasThrottle        bool                                 `json:"HasThrottle"`
	IsPartial          bool                                 `json:"IsPartial"`
}

// buildTraceSummaryView converts a TraceSummaryData to the JSON view struct.
// startTime is the trace's earliest-observed segment start time (tracked on the
// backend's Trace record), surfaced as the real API's required "StartTime" field.
func buildTraceSummaryView(traceID string, sd TraceSummaryData, startTime time.Time) traceSummary {
	s := traceSummary{
		ID:           traceID,
		Duration:     sd.Duration,
		ResponseTime: sd.ResponseTime,
		StartTime:    float64(startTime.Unix()),
		HasFault:     sd.HasFault,
		HasError:     sd.HasError,
		HasThrottle:  sd.HasThrottle,
		IsPartial:    sd.IsPartial,
		Revision:     sd.Revision,
		// ForecastStatistics is always present per AWS API (even as empty object).
		ForecastStatistics: &traceSummaryForecastView{},
	}

	if sd.EntryPoint != nil {
		s.EntryPoint = &traceSummaryServiceIDView{Name: sd.EntryPoint.Name, Type: sd.EntryPoint.Type}
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
		s.Annotations = make(map[string][]valueWithServiceIDsView, len(sd.Annotations))
		for k, occs := range sd.Annotations {
			s.Annotations[k] = toValueWithServiceIDsViews(occs)
		}
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

		summaries = append(summaries, buildTraceSummaryView(traces[i].TraceID, sd, traces[i].StartTime))
	}

	pg := page.New(summaries, in.NextToken, int(in.MaxResults), defaultTraceSummariesPageSize)

	return json.Marshal(map[string]any{
		"TraceSummaries":       pg.Data,
		"TracesProcessedCount": len(summaries),
		// ApproximateTime is the start time of this page of results (per the real
		// GetTraceSummariesOutput shape); it is an envelope-level field, not a
		// per-TraceSummary field.
		"ApproximateTime": float64(time.Now().Unix()),
		keyNextToken:      pg.Next,
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
	ID            string               `json:"Id"`
	Segments      []batchSegmentOutput `json:"Segments"`
	Duration      float64              `json:"Duration"`
	LimitExceeded bool                 `json:"LimitExceeded"`
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
