package swf

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"
)

const (
	defaultLambdaStartToCloseSeconds = 900
	maxOpenLambdaFunctions           = 1000
)

// LambdaInvoker runs a Lambda function synchronously for ScheduleLambdaFunction decisions.
type LambdaInvoker interface {
	// InvokeLambda returns the response payload and, when the function failed, the Lambda function-error kind.
	InvokeLambda(ctx context.Context, name string, payload []byte) (result []byte, functionError string, err error)
}

// SetLambdaInvoker wires the Lambda accessor used by ScheduleLambdaFunction decisions.
func (b *InMemoryBackend) SetLambdaInvoker(l LambdaInvoker) {
	b.mu.Lock("SetLambdaInvoker")
	defer b.mu.Unlock()

	b.lambda = l
}

func (b *InMemoryBackend) lambdaInvoker() LambdaInvoker {
	b.mu.RLock("lambdaInvoker")
	defer b.mu.RUnlock()

	return b.lambda
}

// SetLambdaInvoker wires l on the home backend and every region sibling.
func (h *Handler) SetLambdaInvoker(l LambdaInvoker) {
	for _, bk := range h.RegionBackends() {
		if mem, ok := bk.(*InMemoryBackend); ok {
			mem.SetLambdaInvoker(l)
		}
	}
}

// ScheduleLambdaFunctionDecisionAttrs holds attributes for ScheduleLambdaFunction.
type ScheduleLambdaFunctionDecisionAttrs struct {
	ID                  string
	Name                string
	Control             string
	Input               string
	StartToCloseTimeout string
}

type lambdaRun struct {
	domain, workflowID, runID string
	id, name, input           string
	scheduledEventID          int64
	startedEventID            int64
	timeout                   time.Duration
}

func (b *InMemoryBackend) handleScheduleLambdaFunctionDecision(dc decisionCtx) {
	attrs := dc.decision.ScheduleLambdaFunctionAttrs
	if attrs == nil {
		return
	}

	failed := func(cause string) {
		b.appendHistoryEventLocked(dc.domain, dc.workflowID, dc.runID, "ScheduleLambdaFunctionFailed", map[string]any{
			eventAttrKey("ScheduleLambdaFunctionFailed"): map[string]any{
				attrDTCEventID: dc.decisionTaskCompletedEventID,
				"id":           attrs.ID,
				attrName:       attrs.Name,
				attrCause:      cause,
			},
		})
	}

	switch {
	case slices.Contains(dc.exec.OpenLambdaIDs, attrs.ID):
		failed("ID_ALREADY_IN_USE")

		return
	case len(dc.exec.OpenLambdaIDs) >= maxOpenLambdaFunctions:
		failed("OPEN_LAMBDA_FUNCTIONS_LIMIT_EXCEEDED")

		return
	case b.lambda == nil:
		failed("LAMBDA_SERVICE_NOT_AVAILABLE_IN_REGION")

		return
	}

	scheduled := map[string]any{
		attrDTCEventID: dc.decisionTaskCompletedEventID,
		"id":           attrs.ID,
		attrName:       attrs.Name,
		attrInput:      attrs.Input,
	}
	if attrs.Control != "" {
		scheduled["control"] = attrs.Control
	}

	secs := defaultLambdaStartToCloseSeconds
	if n, err := strconv.Atoi(attrs.StartToCloseTimeout); err == nil && n > 0 {
		secs = n
	}

	scheduled["startToCloseTimeout"] = strconv.Itoa(secs)

	scheduledID := b.appendHistoryEventLocked(
		dc.domain,
		dc.workflowID,
		dc.runID,
		"LambdaFunctionScheduled",
		map[string]any{
			eventAttrKey("LambdaFunctionScheduled"): scheduled,
		},
	)
	startedID := b.appendHistoryEventLocked(dc.domain, dc.workflowID, dc.runID, "LambdaFunctionStarted", map[string]any{
		eventAttrKey("LambdaFunctionStarted"): map[string]any{"scheduledEventId": scheduledID},
	})
	dc.exec.OpenLambdaIDs = append(dc.exec.OpenLambdaIDs, attrs.ID)

	run := lambdaRun{
		domain: dc.domain, workflowID: dc.workflowID, runID: dc.runID,
		id: attrs.ID, name: attrs.Name, input: attrs.Input,
		scheduledEventID: scheduledID, startedEventID: startedID,
		timeout: time.Duration(secs) * time.Second,
	}

	go b.runLambda(b.lambda, run)
}

// runLambda invokes the function outside the backend lock and records its outcome in the execution history.
func (b *InMemoryBackend) runLambda(invoker LambdaInvoker, run lambdaRun) {
	ctx, cancel := context.WithTimeout(context.Background(), run.timeout)
	defer cancel()

	result, functionError, err := invoker.InvokeLambda(ctx, run.name, []byte(run.input))

	b.mu.Lock("LambdaFunctionDone")
	defer b.mu.Unlock()

	exec, ok := b.executions.Get(executionKey(run.domain, run.workflowID, run.runID))
	if !ok || exec.Status != statusRunning {
		return
	}

	idx := slices.Index(exec.OpenLambdaIDs, run.id)
	if idx == -1 {
		return
	}

	exec.OpenLambdaIDs = slices.Delete(exec.OpenLambdaIDs, idx, idx+1)

	ids := map[string]any{"scheduledEventId": run.scheduledEventID, "startedEventId": run.startedEventID}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		ids["timeoutType"] = "START_TO_CLOSE"
		b.appendLambdaOutcomeLocked(run, "LambdaFunctionTimedOut", ids)
	case err != nil:
		ids[attrReason] = "LambdaInvocationFailed"
		ids[attrDetails] = err.Error()
		b.appendLambdaOutcomeLocked(run, "LambdaFunctionFailed", ids)
	case functionError != "":
		ids[attrReason], ids[attrDetails] = lambdaFailureDetails(functionError, result)
		b.appendLambdaOutcomeLocked(run, "LambdaFunctionFailed", ids)
	default:
		ids[attrResult] = string(result)
		b.appendLambdaOutcomeLocked(run, "LambdaFunctionCompleted", ids)
	}
}

func (b *InMemoryBackend) appendLambdaOutcomeLocked(run lambdaRun, eventType string, attrs map[string]any) {
	b.appendHistoryEventLocked(run.domain, run.workflowID, run.runID, eventType, map[string]any{
		eventAttrKey(eventType): attrs,
	})
	b.enqueueDecisionTaskLocked(run.domain, run.workflowID, run.runID)
}

// lambdaFailureDetails returns the failure reason (the function's errorType when reported, else its kind) and the
// raw error payload.
func lambdaFailureDetails(functionError string, payload []byte) (string, string) {
	var body struct {
		ErrorType string `json:"errorType"`
	}

	if json.Unmarshal(payload, &body) == nil && body.ErrorType != "" {
		return body.ErrorType, string(payload)
	}

	return functionError, string(payload)
}
