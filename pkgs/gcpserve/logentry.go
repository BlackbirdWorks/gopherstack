package gcpserve

import (
	"encoding/json"
	"maps"
	"strconv"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/structpb"
)

// Special JSON fields the structured-logging agent lifts out of jsonPayload.
const (
	fieldSeverity  = "severity"
	fieldMessage   = "message"
	fieldTrace     = "logging.googleapis.com/trace"
	fieldSpanID    = "logging.googleapis.com/spanId"
	fieldSampled   = "logging.googleapis.com/trace_sampled"
	fieldLabels    = "logging.googleapis.com/labels"
	fieldSourceLoc = "logging.googleapis.com/sourceLocation"
)

// LineEntry builds the entry for one output line: textPayload for plain text, jsonPayload for a JSON object.
func (t LogTarget) LineEntry(stream, line string) *loggingpb.LogEntry {
	e := &loggingpb.LogEntry{
		LogName:  LogName(t.Project, "run.googleapis.com/"+stream),
		Resource: t.Resource,
		Labels:   maps.Clone(t.Labels),
		Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: line},
	}

	if stream == StreamStderr {
		e.Severity = ltype.LogSeverity_ERROR
	}

	if obj, ok := jsonObject(line); ok {
		t.applyStructured(e, obj)
	}

	return e
}

func jsonObject(line string) (map[string]any, bool) {
	s := strings.TrimSpace(line)
	if !strings.HasPrefix(s, "{") {
		return nil, false
	}

	var obj map[string]any
	if err := json.Unmarshal([]byte(s), &obj); err != nil || obj == nil {
		return nil, false
	}

	return obj, true
}

func (t LogTarget) applyStructured(e *loggingpb.LogEntry, obj map[string]any) {
	if s, ok := obj[fieldSeverity].(string); ok {
		if v, known := ltype.LogSeverity_value[strings.ToUpper(s)]; known {
			e.Severity = ltype.LogSeverity(v)

			delete(obj, fieldSeverity)
		}
	}

	t.liftTrace(e, obj)
	liftLabels(e, obj)
	liftSourceLocation(e, obj)

	if len(obj) == 0 {
		return
	}

	if msg, ok := obj[fieldMessage].(string); ok && len(obj) == 1 {
		e.Payload = &loggingpb.LogEntry_TextPayload{TextPayload: msg}

		return
	}

	if st, err := structpb.NewStruct(obj); err == nil {
		e.Payload = &loggingpb.LogEntry_JsonPayload{JsonPayload: st}
	}
}

func (t LogTarget) liftTrace(e *loggingpb.LogEntry, obj map[string]any) {
	if s, ok := obj[fieldTrace].(string); ok {
		if !strings.HasPrefix(s, "projects/") {
			s = "projects/" + t.Project + "/traces/" + s
		}

		e.Trace = s

		delete(obj, fieldTrace)
	}

	if s, ok := obj[fieldSpanID].(string); ok {
		e.SpanId = s

		delete(obj, fieldSpanID)
	}

	if b, ok := obj[fieldSampled].(bool); ok {
		e.TraceSampled = b

		delete(obj, fieldSampled)
	}
}

func liftLabels(e *loggingpb.LogEntry, obj map[string]any) {
	raw, ok := obj[fieldLabels].(map[string]any)
	if !ok {
		return
	}

	delete(obj, fieldLabels)

	if e.Labels == nil {
		e.Labels = map[string]string{}
	}

	for k, v := range raw {
		if s, isStr := v.(string); isStr {
			e.Labels[k] = s
		}
	}
}

func liftSourceLocation(e *loggingpb.LogEntry, obj map[string]any) {
	raw, ok := obj[fieldSourceLoc].(map[string]any)
	if !ok {
		return
	}

	delete(obj, fieldSourceLoc)

	loc := &loggingpb.LogEntrySourceLocation{}
	loc.File, _ = raw["file"].(string)
	loc.Function, _ = raw["function"].(string)

	switch v := raw["line"].(type) {
	case string:
		loc.Line, _ = strconv.ParseInt(v, 10, 64)
	case float64:
		loc.Line = int64(v)
	}

	e.SourceLocation = loc
}
