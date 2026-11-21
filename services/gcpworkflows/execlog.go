package gcpworkflows

import (
	"context"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	wftype "google.golang.org/genproto/googleapis/cloud/workflows/type"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

// Log ID and payload: google/cloud/workflows/type/executions_system.proto (ExecutionsSystemLog as jsonPayload).
const (
	executionsSystemLogID = "workflows.googleapis.com/executions_system"
	maxSystemLogMessage   = 5 << 10
)

func (b *InMemoryBackend) writeSystemLog(
	info runInfo, sev ltype.LogSeverity, msg *wftype.ExecutionsSystemLog,
) {
	b.mu.RLock("writeSystemLog")
	sink := b.logs
	b.mu.RUnlock()

	if sink == nil {
		return
	}

	raw, err := protojson.Marshal(msg)
	if err != nil {
		return
	}

	payload := &structpb.Struct{}
	if payload.UnmarshalJSON(raw) != nil {
		return
	}

	entry := &loggingpb.LogEntry{
		LogName:   gcpserve.LogName(info.project, executionsSystemLogID),
		Resource:  info.resource(),
		Labels:    map[string]string{labelExecutionID: lastSegment(info.exec)},
		Severity:  sev,
		Payload:   &loggingpb.LogEntry_JsonPayload{JsonPayload: payload},
		Timestamp: msg.GetActivityTime(),
	}

	_ = sink.WriteLogEntries(context.Background(), &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{entry},
	})
}

func (b *InMemoryBackend) logExecutionStart(info runInfo, argument string) {
	b.writeSystemLog(info, ltype.LogSeverity_INFO, &wftype.ExecutionsSystemLog{
		Message:      "Execution started.",
		ActivityTime: b.ts(),
		State:        wftype.ExecutionsSystemLog_ACTIVE,
		Details: &wftype.ExecutionsSystemLog_Start_{
			Start: &wftype.ExecutionsSystemLog_Start{Argument: truncate(argument, maxCallLogText)},
		},
	})
}

// logExecutionEnd writes the finish entry for the execution's final state; an execution that is gone or not
// finished writes nothing.
func (b *InMemoryBackend) logExecutionEnd(info runInfo) {
	b.mu.RLock("logExecutionEnd")

	rec, ok := b.execs[info.exec]
	if !ok {
		b.mu.RUnlock()

		return
	}

	pb := clone(rec.pb)

	b.mu.RUnlock()

	at := pb.GetEndTime()
	if at == nil {
		at = timestamppb.New(b.now())
	}

	msg := &wftype.ExecutionsSystemLog{ActivityTime: at}
	sev := ltype.LogSeverity_INFO

	switch pb.GetState() {
	case executionspb.Execution_SUCCEEDED:
		msg.Message, msg.State = "Execution finished successfully.", wftype.ExecutionsSystemLog_SUCCEEDED
		msg.Details = &wftype.ExecutionsSystemLog_Success_{
			Success: &wftype.ExecutionsSystemLog_Success{Result: truncate(pb.GetResult(), maxCallLogText)},
		}
	case executionspb.Execution_FAILED:
		sev = ltype.LogSeverity_ERROR
		msg.Message, msg.State = "Execution failed.", wftype.ExecutionsSystemLog_FAILED
		msg.Details = &wftype.ExecutionsSystemLog_Failure_{Failure: failureDetails(pb.GetError())}
	case executionspb.Execution_CANCELLED:
		msg.Message, msg.State = "Execution was cancelled.", wftype.ExecutionsSystemLog_CANCELLED
	default:
		return
	}

	msg.Message = truncate(msg.GetMessage(), maxSystemLogMessage)
	b.writeSystemLog(info, sev, msg)
}

func failureDetails(e *executionspb.Execution_Error) *wftype.ExecutionsSystemLog_Failure {
	out := &wftype.ExecutionsSystemLog_Failure{Exception: truncate(e.GetContext(), maxCallLogReason)}

	if els := e.GetStackTrace().GetElements(); len(els) > 0 {
		out.Source = truncate(els[0].GetRoutine()+"."+els[0].GetStep(), maxCallLogReason)
	}

	return out
}
