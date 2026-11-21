package gcpworkflows

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

// Log ID and payload shape: docs.cloud.google.com/logging/docs/api/platform-logs and
// google/cloud/workflows/type/engine_call.proto (EngineCallLog, rendered as jsonPayload).
const (
	engineCallLogID  = "workflows.googleapis.com/engine_call"
	maxCallLogText   = 8192
	maxCallLogReason = 1024
)

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n]
}

func callWanted(level executionspb.Execution_CallLogLevel, state string) bool {
	switch level {
	case executionspb.Execution_LOG_ALL_CALLS:
		return true
	case executionspb.Execution_LOG_ERRORS_ONLY:
		return state == wfinterp.CallExceptionRaised
	default:
		return false
	}
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func callPayload(info runInfo, e wfinterp.CallEvent) map[string]any {
	p := map[string]any{
		"executionId":  lastSegment(info.exec),
		"activityTime": stamp(e.Time),
		fieldState:     e.State,
		keyStep:        truncate(e.Step, maxCallLogReason),
		"callee":       truncate(e.Callee, maxCallLogReason),
	}

	switch e.State {
	case wfinterp.CallBegun:
		p["begun"] = map[string]any{"namedArgs": namedArgs(e.Args)}
	case wfinterp.CallSucceeded:
		p["succeeded"] = map[string]any{
			"callStartTime": stamp(e.CallStart), "response": truncate(e.Response, maxCallLogText),
		}
	case wfinterp.CallExceptionRaised:
		p["exceptionRaised"] = exceptionDetails(e)
	case wfinterp.CallExceptionHandled:
		p["exceptionHandled"] = exceptionDetails(e)
	}

	return p
}

func exceptionDetails(e wfinterp.CallEvent) map[string]any {
	return map[string]any{
		"callStartTime": stamp(e.CallStart),
		"exception":     truncate(e.Exception, maxCallLogReason),
		"origin":        truncate(e.Origin, maxCallLogReason),
	}
}

// namedArgs keeps JSON-encodable arguments and falls back to their serialised text, which may be truncated.
func namedArgs(args map[string]any) map[string]any {
	out := make(map[string]any, len(args))

	safe, _ := redact(args, 0).(map[string]any)

	for k, v := range safe {
		raw, err := wfinterp.ToJSON(v)
		if err != nil {
			out[k] = truncate(wfinterpText(v), maxCallLogText)

			continue
		}

		if len(raw) > maxCallLogText {
			out[k] = truncate(string(raw), maxCallLogText)

			continue
		}

		var val structpb.Value
		if protojson.Unmarshal(raw, &val) != nil {
			out[k] = truncate(string(raw), maxCallLogText)

			continue
		}

		out[k] = val.AsInterface()
	}

	return out
}

func wfinterpText(v any) string {
	raw, err := wfinterp.ToJSON(v)
	if err != nil {
		return "null"
	}

	return string(raw)
}

func (b *InMemoryBackend) emitCall(info runInfo, level executionspb.Execution_CallLogLevel, e wfinterp.CallEvent) {
	if !callWanted(level, e.State) {
		return
	}

	b.mu.RLock("emitCall")
	sink := b.logs
	b.mu.RUnlock()

	if sink == nil {
		return
	}

	s, err := structpb.NewStruct(callPayload(info, e))
	if err != nil {
		return
	}

	sev := ltype.LogSeverity_INFO
	if e.State == wfinterp.CallExceptionRaised {
		sev = ltype.LogSeverity_ERROR
	}

	entry := &loggingpb.LogEntry{
		LogName:   gcpserve.LogName(info.project, engineCallLogID),
		Resource:  info.resource(),
		Labels:    map[string]string{labelExecutionID: lastSegment(info.exec)},
		Severity:  sev,
		Payload:   &loggingpb.LogEntry_JsonPayload{JsonPayload: s},
		Timestamp: timestamppb.New(e.Time),
	}

	_ = sink.WriteLogEntries(context.Background(), &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{entry},
	})
}

const (
	redactedValue = "[REDACTED]"
	redactDepth   = 32
)

// secretKey names the headers and arguments whose values never reach the log; Authorization is documented at
// docs.cloud.google.com/workflows/docs/log-workflow, the rest are credential carriers of the same kind.
func secretKey(k string) bool {
	switch strings.ToLower(k) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie", "x-goog-api-key", "x-api-key":
		return true
	}

	return false
}

// redact hides credential headers; structure nested deeper than redactDepth is dropped rather than logged raw.
func redact(v any, depth int) any {
	if depth > redactDepth {
		return redactedValue
	}

	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))

		for k, e := range x {
			if secretKey(k) {
				out[k] = redactedValue

				continue
			}

			out[k] = redact(e, depth+1)
		}

		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = redact(e, depth+1)
		}

		return out
	default:
		return v
	}
}
