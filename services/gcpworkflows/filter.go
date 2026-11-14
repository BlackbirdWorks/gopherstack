package gcpworkflows

import (
	"strings"
	"time"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	fieldExecID   = "executionID"
	fieldStart    = "startTime"
	fieldEnd      = "endTime"
	fieldState    = "state"
	fieldRevision = "workflowRevisionID"
	fieldDuration = "duration"
	fieldStepName = "stepName"
	fieldWfName   = "name"
	dirDescending = "desc"
)

func one(s string) []string {
	if s == "" {
		return nil
	}

	return []string{s}
}

func tsValue(t *timestamppb.Timestamp) []string {
	if t == nil {
		return nil
	}

	return []string{t.AsTime().Format(time.RFC3339Nano)}
}

func labelField[T any](key string, labels func(T) map[string]string) fieldAccess[T] {
	return fieldAccess[T]{kind: kindString, get: func(v T) []string {
		if val, ok := labels(v)[key]; ok {
			return []string{val}
		}

		return nil
	}}
}

func labelKey(field string) (string, bool) {
	for _, p := range []string{"labels.", "label."} {
		if k, ok := strings.CutPrefix(field, p); ok && k != "" {
			return k, true
		}
	}

	return "", false
}

func execFilterField(field string) (fieldAccess[*executionspb.Execution], bool) {
	if k, ok := labelKey(field); ok {
		return labelField(k, (*executionspb.Execution).GetLabels), true
	}

	str := func(get func(*executionspb.Execution) string) fieldAccess[*executionspb.Execution] {
		return fieldAccess[*executionspb.Execution]{kind: kindString, get: func(e *executionspb.Execution) []string {
			return one(get(e))
		}}
	}

	switch field {
	case fieldExecID:
		return str(func(e *executionspb.Execution) string { return lastSegment(e.GetName()) }), true
	case fieldRevision:
		return str((*executionspb.Execution).GetWorkflowRevisionId), true
	case fieldState:
		return fieldAccess[*executionspb.Execution]{
			kind: kindEnum,
			valid: func(s string) bool {
				_, ok := executionspb.Execution_State_value[s]

				return ok
			},
			get: func(e *executionspb.Execution) []string { return []string{e.GetState().String()} },
		}, true
	case fieldStart:
		return fieldAccess[*executionspb.Execution]{kind: kindTime, get: func(e *executionspb.Execution) []string {
			return tsValue(e.GetStartTime())
		}}, true
	case fieldEnd:
		return fieldAccess[*executionspb.Execution]{kind: kindTime, get: func(e *executionspb.Execution) []string {
			return tsValue(e.GetEndTime())
		}}, true
	case fieldDuration:
		return fieldAccess[*executionspb.Execution]{kind: kindDuration, get: func(e *executionspb.Execution) []string {
			if e.GetDuration() == nil {
				return nil
			}

			return []string{e.GetDuration().AsDuration().String()}
		}}, true
	case fieldStepName:
		return fieldAccess[*executionspb.Execution]{kind: kindString, get: func(e *executionspb.Execution) []string {
			steps := e.GetStatus().GetCurrentSteps()
			out := make([]string, 0, len(steps))

			for _, s := range steps {
				out = append(out, s.GetStep())
			}

			return out
		}}, true
	default:
		return fieldAccess[*executionspb.Execution]{}, false
	}
}

func parseExecFilter(filter string) (func(*executionspb.Execution) bool, error) {
	return parseFilter(filter, execFilterField)
}

func workflowFilterField(field string) (fieldAccess[*workflowspb.Workflow], bool) {
	if k, ok := labelKey(field); ok {
		return labelField(k, (*workflowspb.Workflow).GetLabels), true
	}

	str := func(get func(*workflowspb.Workflow) string) fieldAccess[*workflowspb.Workflow] {
		return fieldAccess[*workflowspb.Workflow]{kind: kindString, get: func(w *workflowspb.Workflow) []string {
			return one(get(w))
		}}
	}

	ts := func(get func(*workflowspb.Workflow) *timestamppb.Timestamp) fieldAccess[*workflowspb.Workflow] {
		return fieldAccess[*workflowspb.Workflow]{kind: kindTime, get: func(w *workflowspb.Workflow) []string {
			return tsValue(get(w))
		}}
	}

	switch field {
	case fieldWfName:
		return str((*workflowspb.Workflow).GetName), true
	case "description":
		return str((*workflowspb.Workflow).GetDescription), true
	case "revisionId":
		return str((*workflowspb.Workflow).GetRevisionId), true
	case "serviceAccount":
		return str((*workflowspb.Workflow).GetServiceAccount), true
	case "cryptoKeyName":
		return str((*workflowspb.Workflow).GetCryptoKeyName), true
	case "createTime":
		return ts((*workflowspb.Workflow).GetCreateTime), true
	case "updateTime":
		return ts((*workflowspb.Workflow).GetUpdateTime), true
	case "revisionCreateTime":
		return ts((*workflowspb.Workflow).GetRevisionCreateTime), true
	case "state":
		return fieldAccess[*workflowspb.Workflow]{
			kind: kindEnum,
			get:  func(w *workflowspb.Workflow) []string { return []string{w.GetState().String()} },
		}, true
	case "callLogLevel":
		return fieldAccess[*workflowspb.Workflow]{
			kind: kindEnum,
			get:  func(w *workflowspb.Workflow) []string { return []string{w.GetCallLogLevel().String()} },
		}, true
	default:
		return fieldAccess[*workflowspb.Workflow]{}, false
	}
}

func parseWorkflowFilter(filter string) (func(*workflowspb.Workflow) bool, error) {
	return parseFilter(filter, workflowFilterField)
}
