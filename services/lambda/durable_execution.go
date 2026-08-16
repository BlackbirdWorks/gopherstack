package lambda

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// durableExecutionRootOperationID is the well-known Id of the implicit
// Type=EXECUTION operation every durable execution has (index 0 of
// DurableExecution.Operations) — see durableExecutionStore's doc comment.
const durableExecutionRootOperationID = "execution"

// DurableExecutionStatus mirrors types.ExecutionStatus
// (aws-sdk-go-v2/service/lambda@v1.101.2/types/enums.go).
type DurableExecutionStatus string

// Enum values for DurableExecutionStatus — field-verified against
// types.ExecutionStatus. TIMED_OUT was previously missing entirely.
const (
	DurableExecutionStatusRunning   DurableExecutionStatus = "RUNNING"
	DurableExecutionStatusSucceeded DurableExecutionStatus = "SUCCEEDED"
	DurableExecutionStatusFailed    DurableExecutionStatus = "FAILED"
	DurableExecutionStatusTimedOut  DurableExecutionStatus = "TIMED_OUT"
	DurableExecutionStatusStopped   DurableExecutionStatus = "STOPPED"
)

// DurableOperationStatus mirrors types.OperationStatus. Only the subset this
// emulator's checkpoint-driven state machine produces is declared as named
// constants (PENDING/READY are real enum values but nothing in this store
// emits them).
type DurableOperationStatus string

const (
	DurableOperationStatusStarted   DurableOperationStatus = "STARTED"
	DurableOperationStatusSucceeded DurableOperationStatus = "SUCCEEDED"
	DurableOperationStatusFailed    DurableOperationStatus = "FAILED"
	DurableOperationStatusCancelled DurableOperationStatus = "CANCELLED"
	DurableOperationStatusStopped   DurableOperationStatus = "STOPPED"
)

// DurableOperationType mirrors types.OperationType.
type DurableOperationType string

const (
	DurableOperationTypeExecution     DurableOperationType = "EXECUTION"
	DurableOperationTypeContext       DurableOperationType = "CONTEXT"
	DurableOperationTypeStep          DurableOperationType = "STEP"
	DurableOperationTypeWait          DurableOperationType = "WAIT"
	DurableOperationTypeCallback      DurableOperationType = "CALLBACK"
	DurableOperationTypeChainedInvoke DurableOperationType = "CHAINED_INVOKE"
)

// Real OperationAction enum values (types.OperationAction), used as raw
// strings in DurableOperationUpdate.Action / checkpointEventTypes below —
// not worth a named type since they never round-trip through a response.
const (
	operationActionStart   = "START"
	operationActionSucceed = "SUCCEED"
	operationActionFail    = "FAIL"
	operationActionCancel  = "CANCEL"
	operationActionRetry   = "RETRY"
)

// Real EventType enum values (types.EventType) this emulator's checkpoint-
// driven state machine can produce. InvocationCompleted and the
// ChainedInvoke Stopped/TimedOut variants are real enum values but nothing
// in this store emits them (no chained-invoke timeout/stop modeling).
const (
	eventTypeExecutionStarted       = "ExecutionStarted"
	eventTypeExecutionSucceeded     = "ExecutionSucceeded"
	eventTypeExecutionFailed        = "ExecutionFailed"
	eventTypeExecutionTimedOut      = "ExecutionTimedOut"
	eventTypeExecutionStopped       = "ExecutionStopped"
	eventTypeStepStarted            = "StepStarted"
	eventTypeStepSucceeded          = "StepSucceeded"
	eventTypeStepFailed             = "StepFailed"
	eventTypeWaitStarted            = "WaitStarted"
	eventTypeWaitSucceeded          = "WaitSucceeded"
	eventTypeWaitCancelled          = "WaitCancelled"
	eventTypeCallbackStarted        = "CallbackStarted"
	eventTypeCallbackSucceeded      = "CallbackSucceeded"
	eventTypeCallbackFailed         = "CallbackFailed"
	eventTypeContextStarted         = "ContextStarted"
	eventTypeContextSucceeded       = "ContextSucceeded"
	eventTypeContextFailed          = "ContextFailed"
	eventTypeChainedInvokeStarted   = "ChainedInvokeStarted"
	eventTypeChainedInvokeSucceeded = "ChainedInvokeSucceeded"
	eventTypeChainedInvokeFailed    = "ChainedInvokeFailed"
)

// checkpointEventTypes maps (OperationType, Action) pairs to the EventType
// they produce in the execution history. Only combinations that actually
// exist in types.EventType are present — e.g. WAIT has no "FAIL" event (only
// WaitCancelled), CALLBACK/CONTEXT/CHAINED_INVOKE have no "CANCEL" event.
// Combinations absent from this table produce no history event.
//
//nolint:gochecknoglobals // static lookup table mirroring a fixed SDK enum
var checkpointEventTypes = map[[2]string]string{
	{string(DurableOperationTypeStep), operationActionStart}:            eventTypeStepStarted,
	{string(DurableOperationTypeStep), operationActionSucceed}:          eventTypeStepSucceeded,
	{string(DurableOperationTypeStep), operationActionFail}:             eventTypeStepFailed,
	{string(DurableOperationTypeWait), operationActionStart}:            eventTypeWaitStarted,
	{string(DurableOperationTypeWait), operationActionSucceed}:          eventTypeWaitSucceeded,
	{string(DurableOperationTypeWait), operationActionCancel}:           eventTypeWaitCancelled,
	{string(DurableOperationTypeCallback), operationActionStart}:        eventTypeCallbackStarted,
	{string(DurableOperationTypeCallback), operationActionSucceed}:      eventTypeCallbackSucceeded,
	{string(DurableOperationTypeCallback), operationActionFail}:         eventTypeCallbackFailed,
	{string(DurableOperationTypeContext), operationActionStart}:         eventTypeContextStarted,
	{string(DurableOperationTypeContext), operationActionSucceed}:       eventTypeContextSucceeded,
	{string(DurableOperationTypeContext), operationActionFail}:          eventTypeContextFailed,
	{string(DurableOperationTypeChainedInvoke), operationActionStart}:   eventTypeChainedInvokeStarted,
	{string(DurableOperationTypeChainedInvoke), operationActionSucceed}: eventTypeChainedInvokeSucceeded,
	{string(DurableOperationTypeChainedInvoke), operationActionFail}:    eventTypeChainedInvokeFailed,
}

func checkpointEventType(opType, action string) (string, bool) {
	evType, ok := checkpointEventTypes[[2]string{opType, action}]

	return evType, ok
}

// ErrorObject mirrors types.ErrorObject — the wire shape for durable
// execution / event / operation error details.
type ErrorObject struct {
	ErrorData    *string  `json:"ErrorData,omitempty"`
	ErrorMessage *string  `json:"ErrorMessage,omitempty"`
	ErrorType    *string  `json:"ErrorType,omitempty"`
	StackTrace   []string `json:"StackTrace,omitempty"`
}

// TraceHeader mirrors types.TraceHeader.
type TraceHeader struct {
	XAmznTraceID *string `json:"XAmznTraceId,omitempty"`
}

// EventInput mirrors types.EventInput.
type EventInput struct {
	Payload   *string `json:"Payload,omitempty"`
	Truncated *bool   `json:"Truncated,omitempty"`
}

// EventResult mirrors types.EventResult.
type EventResult struct {
	Payload   *string `json:"Payload,omitempty"`
	Truncated *bool   `json:"Truncated,omitempty"`
}

// EventError mirrors types.EventError.
type EventError struct {
	Payload   *ErrorObject `json:"Payload,omitempty"`
	Truncated *bool        `json:"Truncated,omitempty"`
}

// ExecutionStartedDetails mirrors types.ExecutionStartedDetails. This, and
// the other four Execution*Details types below, are the ONLY Event subtype
// Details this emulator ever populates: it has no step-function-style
// replay engine, so the Context/Step/Wait/Callback/ChainedInvoke *Details
// fields declared on the real types.Event (~19 of them) are intentionally
// not declared here — leaving them permanently nil would not improve wire
// fidelity, only add dead surface. Generic Event fields
// (Id/Name/ParentId/SubType/EventType) ARE populated for every event,
// including STEP/WAIT/CALLBACK/CONTEXT/CHAINED_INVOKE ones driven by
// CheckpointDurableExecution's Updates.
type ExecutionStartedDetails struct {
	ExecutionTimeout *int32      `json:"ExecutionTimeout,omitempty"`
	Input            *EventInput `json:"Input,omitempty"`
}

// ExecutionSucceededDetails mirrors types.ExecutionSucceededDetails.
type ExecutionSucceededDetails struct {
	Result *EventResult `json:"Result,omitempty"`
}

// ExecutionFailedDetails mirrors types.ExecutionFailedDetails.
type ExecutionFailedDetails struct {
	Error *EventError `json:"Error,omitempty"`
}

// ExecutionStoppedDetails mirrors types.ExecutionStoppedDetails.
type ExecutionStoppedDetails struct {
	Error *EventError `json:"Error,omitempty"`
}

// ExecutionTimedOutDetails mirrors types.ExecutionTimedOutDetails.
type ExecutionTimedOutDetails struct {
	Error *EventError `json:"Error,omitempty"`
}

// DurableExecutionEvent mirrors types.Event.
type DurableExecutionEvent struct {
	EventID                   *int32                     `json:"EventId,omitempty"`
	ID                        *string                    `json:"Id,omitempty"`
	Name                      *string                    `json:"Name,omitempty"`
	ParentID                  *string                    `json:"ParentId,omitempty"`
	SubType                   *string                    `json:"SubType,omitempty"`
	ExecutionStartedDetails   *ExecutionStartedDetails   `json:"ExecutionStartedDetails,omitempty"`
	ExecutionSucceededDetails *ExecutionSucceededDetails `json:"ExecutionSucceededDetails,omitempty"`
	ExecutionFailedDetails    *ExecutionFailedDetails    `json:"ExecutionFailedDetails,omitempty"`
	ExecutionStoppedDetails   *ExecutionStoppedDetails   `json:"ExecutionStoppedDetails,omitempty"`
	ExecutionTimedOutDetails  *ExecutionTimedOutDetails  `json:"ExecutionTimedOutDetails,omitempty"`
	EventType                 string                     `json:"EventType"`
	EventTimestamp            float64                    `json:"EventTimestamp"`
}

// DurableOperationExecutionDetails mirrors types.ExecutionDetails.
type DurableOperationExecutionDetails struct {
	InputPayload *string `json:"InputPayload,omitempty"`
}

// DurableOperation mirrors types.Operation. This emulator only ever
// produces Type=EXECUTION (the implicit root, see durableExecutionStore's
// doc comment) and whatever CONTEXT/STEP/WAIT/CALLBACK/CHAINED_INVOKE
// operations the caller submits via CheckpointDurableExecution's Updates —
// the type-specific *Details fields on the real type (CallbackDetails,
// StepDetails, WaitDetails, ContextDetails, ChainedInvokeDetails) are
// therefore not declared, matching the Event Details scoping above.
type DurableOperation struct {
	Name             *string                           `json:"Name,omitempty"`
	ParentID         *string                           `json:"ParentId,omitempty"`
	SubType          *string                           `json:"SubType,omitempty"`
	ExecutionDetails *DurableOperationExecutionDetails `json:"ExecutionDetails,omitempty"`
	EndTimestamp     *float64                          `json:"EndTimestamp,omitempty"`
	ID               string                            `json:"Id"`
	Status           DurableOperationStatus            `json:"Status"`
	Type             DurableOperationType              `json:"Type"`
	StartTimestamp   float64                           `json:"StartTimestamp"`
}

// DurableOperationUpdate is the accepted shape of one entry in
// CheckpointDurableExecutionInput.Updates (mirrors the generic fields of
// types.OperationUpdate). The type-specific *Options fields on the real type
// (CallbackOptions/ChainedInvokeOptions/ContextOptions/StepOptions/
// WaitOptions — timeout/retry-policy configuration knobs) are accepted-and-
// discarded: this emulator's checkpoint state machine tracks operation
// status transitions, not timing/retry behavior, so declaring those fields
// would only add always-empty struct surface.
type DurableOperationUpdate struct {
	Error    *ErrorObject `json:"Error,omitempty"`
	Name     *string      `json:"Name,omitempty"`
	ParentID *string      `json:"ParentId,omitempty"`
	Payload  *string      `json:"Payload,omitempty"`
	SubType  *string      `json:"SubType,omitempty"`
	Action   string       `json:"Action"`
	ID       string       `json:"Id"`
	Type     string       `json:"Type"`
}

// CheckpointDurableExecutionInput mirrors the CheckpointDurableExecution
// JSON body (DurableExecutionArn is a URI member, not part of the body, per
// the real serializer).
type CheckpointDurableExecutionInput struct {
	CheckpointToken *string                  `json:"CheckpointToken,omitempty"`
	ClientToken     *string                  `json:"ClientToken,omitempty"`
	Updates         []DurableOperationUpdate `json:"Updates,omitempty"`
}

// CheckpointUpdatedExecutionState mirrors types.CheckpointUpdatedExecutionState.
type CheckpointUpdatedExecutionState struct {
	NextMarker *string             `json:"NextMarker,omitempty"`
	Operations []*DurableOperation `json:"Operations,omitempty"`
}

// CheckpointDurableExecutionOutput mirrors the real response shape.
type CheckpointDurableExecutionOutput struct {
	NewExecutionState *CheckpointUpdatedExecutionState `json:"NewExecutionState"`
	CheckpointToken   *string                          `json:"CheckpointToken,omitempty"`
}

// GetDurableExecutionOutput mirrors api_op_GetDurableExecution.go's
// GetDurableExecutionOutput field-for-field (verified against
// aws-sdk-go-v2/service/lambda@v1.101.2/deserializers.go). Previously
// gopherstack emitted a single merged "ExecutionArn" field and ISO8601
// StartTime/StopTime strings; the real shape splits DurableExecutionArn from
// DurableExecutionName and uses Unix-epoch StartTimestamp/EndTimestamp.
type GetDurableExecutionOutput struct {
	DurableConfig         *DurableConfig         `json:"DurableConfig,omitempty"`
	Error                 *ErrorObject           `json:"Error,omitempty"`
	ExecutionDataIncluded *bool                  `json:"ExecutionDataIncluded,omitempty"`
	InputPayload          *string                `json:"InputPayload,omitempty"`
	Result                *string                `json:"Result,omitempty"`
	TraceHeader           *TraceHeader           `json:"TraceHeader,omitempty"`
	Version               *string                `json:"Version,omitempty"`
	EndTimestamp          *float64               `json:"EndTimestamp,omitempty"`
	DurableExecutionArn   string                 `json:"DurableExecutionArn"`
	DurableExecutionName  string                 `json:"DurableExecutionName"`
	FunctionArn           string                 `json:"FunctionArn"`
	Status                DurableExecutionStatus `json:"Status"`
	StartTimestamp        float64                `json:"StartTimestamp"`
}

// GetDurableExecutionHistoryOutput mirrors api_op_GetDurableExecutionHistory.go's output.
type GetDurableExecutionHistoryOutput struct {
	NextMarker *string                 `json:"NextMarker,omitempty"`
	Events     []DurableExecutionEvent `json:"Events"`
}

// GetDurableExecutionStateOutput mirrors api_op_GetDurableExecutionState.go's output.
type GetDurableExecutionStateOutput struct {
	NextMarker *string             `json:"NextMarker,omitempty"`
	Operations []*DurableOperation `json:"Operations"`
}

// DurableExecutionSummary mirrors types.Execution — the (lighter) list-item
// shape ListDurableExecutionsByFunction returns, distinct from
// GetDurableExecutionOutput (no Error/InputPayload/Result/DurableConfig
// echo; just an optional KMSKeyArn).
type DurableExecutionSummary struct {
	KMSKeyArn            *string                `json:"KMSKeyArn,omitempty"`
	EndTimestamp         *float64               `json:"EndTimestamp,omitempty"`
	DurableExecutionArn  string                 `json:"DurableExecutionArn"`
	DurableExecutionName string                 `json:"DurableExecutionName"`
	FunctionArn          string                 `json:"FunctionArn"`
	Status               DurableExecutionStatus `json:"Status"`
	StartTimestamp       float64                `json:"StartTimestamp"`
}

// ListDurableExecutionsByFunctionOutput mirrors api_op_ListDurableExecutionsByFunction.go's output.
type ListDurableExecutionsByFunctionOutput struct {
	NextMarker        *string                   `json:"NextMarker,omitempty"`
	DurableExecutions []DurableExecutionSummary `json:"DurableExecutions"`
}

// StopDurableExecutionOutput mirrors api_op_StopDurableExecution.go's output.
type StopDurableExecutionOutput struct {
	StopTimestamp float64 `json:"StopTimestamp"`
}

// SendDurableExecutionCallbackSuccessOutput mirrors the real (empty-body) output.
type SendDurableExecutionCallbackSuccessOutput struct{}

// SendDurableExecutionCallbackFailureOutput mirrors the real (empty-body) output.
type SendDurableExecutionCallbackFailureOutput struct{}

// SendDurableExecutionCallbackHeartbeatOutput mirrors the real (empty-body) output.
type SendDurableExecutionCallbackHeartbeatOutput struct{}

// DurableExecution is gopherstack's internal record for a Lambda durable
// execution.
type DurableExecution struct {
	EndTime         time.Time
	StartTime       time.Time
	DurableConfig   *DurableConfig
	Error           *ErrorObject
	TraceHeader     *TraceHeader
	opIndex         map[string]int
	InputPayload    string
	Version         string
	FunctionARN     string
	Result          string
	CheckpointToken string
	Status          DurableExecutionStatus
	Name            string
	ARN             string
	Events          []DurableExecutionEvent
	Operations      []*DurableOperation
	eventSeq        int32
}

// deriveDurableExecutionName returns the DurableExecutionName to use when the
// caller never supplied one explicitly: real AWS returns "a system-generated
// unique identifier" in that case (GetDurableExecutionOutput doc comment).
// This emulator derives it from the last ":"- or "/"-delimited segment of the
// (client-opaque, server-never-parses-structure-from-it) DurableExecutionArn
// — stable and unique per execution without inventing unrelated data.
func deriveDurableExecutionName(arn string) string {
	if idx := strings.LastIndexAny(arn, ":/"); idx >= 0 {
		return arn[idx+1:]
	}

	return arn
}

func epochPtr(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	ts := awstime.Epoch(t)

	return &ts
}

// newDurableExecution creates a fresh execution record, seeding its
// history/operations with the implicit ExecutionStarted event and root
// Type=EXECUTION operation every durable execution has.
func newDurableExecution(arn string) *DurableExecution {
	now := time.Now().UTC()
	ex := &DurableExecution{
		ARN:       arn,
		Name:      deriveDurableExecutionName(arn),
		Status:    DurableExecutionStatusRunning,
		StartTime: now,
		opIndex:   make(map[string]int),
	}

	ex.appendEvent(DurableExecutionEvent{
		EventType: eventTypeExecutionStarted,
		ID:        ptrconv.NilIfEmpty(durableExecutionRootOperationID),
		ExecutionStartedDetails: &ExecutionStartedDetails{
			Input: &EventInput{Payload: ptrconv.NilIfEmpty(ex.InputPayload)},
		},
	})

	ex.opIndex[durableExecutionRootOperationID] = len(ex.Operations)
	ex.Operations = append(ex.Operations, &DurableOperation{
		ID:               durableExecutionRootOperationID,
		Type:             DurableOperationTypeExecution,
		Status:           DurableOperationStatusStarted,
		StartTimestamp:   awstime.Epoch(now),
		ExecutionDetails: &DurableOperationExecutionDetails{},
	})

	return ex
}

// appendEvent stamps ev with the next sequential EventId and the current
// timestamp, then appends it to the execution's history. Callers must hold
// the owning store's write lock.
func (ex *DurableExecution) appendEvent(ev DurableExecutionEvent) {
	ex.eventSeq++
	id := ex.eventSeq
	ev.EventID = &id
	ev.EventTimestamp = awstime.Epoch(time.Now().UTC())
	ex.Events = append(ex.Events, ev)
}

// applyUpdate applies one CheckpointDurableExecution Update to the
// execution's operation set: Action=START creates a new operation (ignoring
// updates that reference an unknown Id with any other action — malformed
// input is ignored rather than erroring, matching this store's "accept,
// don't pedantically validate the internal replay protocol" simplification),
// any other action transitions the existing operation in place. Returns the
// affected operation (nil if the update was ignored). Callers must hold the
// owning store's write lock.
func (ex *DurableExecution) applyUpdate(u DurableOperationUpdate) *DurableOperation {
	op := ex.operationFor(u)
	if op == nil {
		return nil
	}

	ex.transitionOperation(op, u)

	if evType, ok := checkpointEventType(u.Type, u.Action); ok {
		ex.appendEvent(DurableExecutionEvent{
			EventType: evType,
			ID:        ptrconv.NilIfEmpty(u.ID),
			Name:      u.Name,
			ParentID:  u.ParentID,
			SubType:   u.SubType,
		})
	}

	return op
}

// operationFor returns the existing operation for u.Id, creating one when
// u.Action is START and no such operation exists yet. Returns nil for any
// other action against an unknown Id.
func (ex *DurableExecution) operationFor(u DurableOperationUpdate) *DurableOperation {
	if idx, ok := ex.opIndex[u.ID]; ok {
		return ex.Operations[idx]
	}

	if u.Action != operationActionStart {
		return nil
	}

	op := &DurableOperation{
		ID:             u.ID,
		Type:           DurableOperationType(u.Type),
		StartTimestamp: awstime.Epoch(time.Now().UTC()),
		Name:           u.Name,
		ParentID:       u.ParentID,
		SubType:        u.SubType,
	}
	ex.opIndex[u.ID] = len(ex.Operations)
	ex.Operations = append(ex.Operations, op)

	return op
}

// transitionOperation applies u.Action's status transition to op in place.
func (ex *DurableExecution) transitionOperation(op *DurableOperation, u DurableOperationUpdate) {
	now := time.Now().UTC()

	switch u.Action {
	case operationActionStart:
		op.Status = DurableOperationStatusStarted
	case operationActionSucceed:
		op.Status = DurableOperationStatusSucceeded
		op.EndTimestamp = epochPtr(now)
	case operationActionFail:
		op.Status = DurableOperationStatusFailed
		op.EndTimestamp = epochPtr(now)
	case operationActionCancel:
		op.Status = DurableOperationStatusCancelled
		op.EndTimestamp = epochPtr(now)
	case operationActionRetry:
		op.Status = DurableOperationStatusStarted
		op.EndTimestamp = nil
	}
}

// toGetOutput converts ex into the GetDurableExecution wire response. When
// includeData is false, InputPayload/Result/Error are omitted and
// ExecutionDataIncluded is explicitly false, matching the real API's
// documented IncludeExecutionData=false "compact response" behavior.
// Callers must hold the owning store's read (or write) lock.
func (ex *DurableExecution) toGetOutput(includeData bool) *GetDurableExecutionOutput {
	out := &GetDurableExecutionOutput{
		DurableExecutionArn:   ex.ARN,
		DurableExecutionName:  ex.Name,
		FunctionArn:           ex.FunctionARN,
		Status:                ex.Status,
		StartTimestamp:        awstime.Epoch(ex.StartTime),
		EndTimestamp:          epochPtr(ex.EndTime),
		DurableConfig:         ex.DurableConfig,
		TraceHeader:           ex.TraceHeader,
		Version:               ptrconv.NilIfEmpty(ex.Version),
		ExecutionDataIncluded: &includeData,
	}

	if includeData {
		out.InputPayload = ptrconv.NilIfEmpty(ex.InputPayload)
		out.Result = ptrconv.NilIfEmpty(ex.Result)
		out.Error = ex.Error
	}

	return out
}

// toSummary converts ex into the ListDurableExecutionsByFunction list-item
// shape. Callers must hold the owning store's read (or write) lock.
func (ex *DurableExecution) toSummary() DurableExecutionSummary {
	sum := DurableExecutionSummary{
		DurableExecutionArn:  ex.ARN,
		DurableExecutionName: ex.Name,
		FunctionArn:          ex.FunctionARN,
		Status:               ex.Status,
		StartTimestamp:       awstime.Epoch(ex.StartTime),
		EndTimestamp:         epochPtr(ex.EndTime),
	}
	if ex.DurableConfig != nil {
		sum.KMSKeyArn = ptrconv.NilIfEmpty(ex.DurableConfig.KMSKeyArn)
	}

	return sum
}

// redactEventData replaces ev's execution-data-bearing Details fields with a
// payload-free copy (EventType/Id/timestamps are unaffected). Always
// allocates fresh Details objects rather than mutating through the shared
// pointer from the stored event, so it is safe to call on a shallow-copied
// event without corrupting the store's data or racing a concurrent reader.
func redactEventData(ev *DurableExecutionEvent) {
	if ev.ExecutionStartedDetails != nil {
		d := *ev.ExecutionStartedDetails
		d.Input = nil
		ev.ExecutionStartedDetails = &d
	}

	if ev.ExecutionSucceededDetails != nil {
		ev.ExecutionSucceededDetails = &ExecutionSucceededDetails{}
	}

	if ev.ExecutionFailedDetails != nil {
		ev.ExecutionFailedDetails = &ExecutionFailedDetails{}
	}

	if ev.ExecutionStoppedDetails != nil {
		ev.ExecutionStoppedDetails = &ExecutionStoppedDetails{}
	}

	if ev.ExecutionTimedOutDetails != nil {
		ev.ExecutionTimedOutDetails = &ExecutionTimedOutDetails{}
	}
}

func reverseEventsInPlace(events []DurableExecutionEvent) {
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
}

// durableExecutionStore is gopherstack's in-memory simulator for the Lambda
// Durable Functions family: GetDurableExecution, GetDurableExecutionHistory,
// GetDurableExecutionState, ListDurableExecutionsByFunction,
// CheckpointDurableExecution, StopDurableExecution, and
// SendDurableExecutionCallback{Success,Failure,Heartbeat}.
//
// Entry-point scope note: real AWS starts a durable execution implicitly
// when a function configured with DurableConfig is invoked — there is no
// "StartDurableExecution" API. Gopherstack's Invoke path (invocation.go)
// does not model durable-execution semantics, so this store instead
// auto-creates the execution record on its first CheckpointDurableExecution
// call (pre-existing behavior of this file, kept as-is — a simplification of
// the entry point, not a wire-shape bug). Consequently FunctionArn/
// DurableConfig/InputPayload/Version are only ever populated when a caller
// threads them through explicitly (there is currently no such caller); every
// RESPONSE FIELD SHAPE below is nonetheless field-verified against
// aws-sdk-go-v2/service/lambda@v1.101.2's deserializers.go, so they round-trip
// correctly through the real SDK client the moment they ARE populated.
//
// Locking: every exported read/write method builds its full wire-response
// (or a deep copy of any *DurableOperation it returns) WHILE HOLDING the
// lock — never returns a live internal pointer for a caller to read
// unsynchronized. This matters because DurableOperation.Status/EndTimestamp
// and DurableExecution.Status/Error/etc. are mutated in place by later
// Checkpoint/Stop/SendCallback calls.
type durableExecutionStore struct {
	mu            *lockmetrics.RWMutex
	executions    map[string]*DurableExecution // key: DurableExecutionArn
	callbackOwner map[string]string            // key: CallbackId (== a CALLBACK operation's Id) -> DurableExecutionArn
}

func newDurableExecutionStore() *durableExecutionStore {
	return &durableExecutionStore{
		mu:            lockmetrics.New("lambda.durable_executions"),
		executions:    make(map[string]*DurableExecution),
		callbackOwner: make(map[string]string),
	}
}

// getOutput returns the GetDurableExecution response for arn, or (nil,
// false) when it does not exist.
func (s *durableExecutionStore) getOutput(arn string, includeData bool) (*GetDurableExecutionOutput, bool) {
	s.mu.RLock("GetOutput")
	defer s.mu.RUnlock()

	ex, ok := s.executions[arn]
	if !ok {
		return nil, false
	}

	return ex.toGetOutput(includeData), true
}

// historyOutput returns the (paginated, optionally reversed/redacted)
// GetDurableExecutionHistory response for arn, or (nil, false) when it does
// not exist.
func (s *durableExecutionStore) historyOutput(
	arn string, includeData, reverseOrder bool, marker string, maxItems int,
) (*GetDurableExecutionHistoryOutput, bool) {
	s.mu.RLock("HistoryOutput")
	defer s.mu.RUnlock()

	ex, ok := s.executions[arn]
	if !ok {
		return nil, false
	}

	events := make([]DurableExecutionEvent, len(ex.Events))
	copy(events, ex.Events)

	if !includeData {
		for i := range events {
			redactEventData(&events[i])
		}
	}

	if reverseOrder {
		reverseEventsInPlace(events)
	}

	p := page.New(events, marker, maxItems, lambdaDefaultMaxItems)

	return &GetDurableExecutionHistoryOutput{Events: p.Data, NextMarker: ptrconv.NilIfEmpty(p.Next)}, true
}

// stateOutput returns the (paginated) GetDurableExecutionState response for
// arn, or (nil, false) when it does not exist.
func (s *durableExecutionStore) stateOutput(arn, marker string, maxItems int) (*GetDurableExecutionStateOutput, bool) {
	s.mu.RLock("StateOutput")
	defer s.mu.RUnlock()

	ex, ok := s.executions[arn]
	if !ok {
		return nil, false
	}

	ops := make([]*DurableOperation, len(ex.Operations))
	for i, op := range ex.Operations {
		cp := *op
		ops[i] = &cp
	}

	p := page.New(ops, marker, maxItems, lambdaDefaultMaxItems)

	return &GetDurableExecutionStateOutput{Operations: p.Data, NextMarker: ptrconv.NilIfEmpty(p.Next)}, true
}

// listSummaries returns DurableExecutionSummary values (in start-time order,
// or reverse when reverseOrder is set) for every execution matching the
// given filters. functionARN == "" matches every execution (used only
// internally by tests; the wire-facing handler always resolves a concrete
// function ARN from the {FunctionName} URI segment first).
func (s *durableExecutionStore) listSummaries(
	functionARN, nameFilter string,
	statuses []DurableExecutionStatus,
	startedAfter, startedBefore time.Time,
	reverseOrder bool,
) []DurableExecutionSummary {
	s.mu.RLock("ListSummaries")
	defer s.mu.RUnlock()

	statusSet := make(map[DurableExecutionStatus]bool, len(statuses))
	for _, st := range statuses {
		statusSet[st] = true
	}

	var matched []*DurableExecution

	for _, ex := range s.executions {
		if !matchesListFilter(ex, functionARN, nameFilter, statusSet, startedAfter, startedBefore) {
			continue
		}

		matched = append(matched, ex)
	}

	sort.Slice(matched, func(i, k int) bool {
		if reverseOrder {
			return matched[i].StartTime.After(matched[k].StartTime)
		}

		return matched[i].StartTime.Before(matched[k].StartTime)
	})

	out := make([]DurableExecutionSummary, len(matched))
	for i, ex := range matched {
		out[i] = ex.toSummary()
	}

	return out
}

func matchesListFilter(
	ex *DurableExecution,
	functionARN, nameFilter string,
	statusSet map[DurableExecutionStatus]bool,
	startedAfter, startedBefore time.Time,
) bool {
	if functionARN != "" && ex.FunctionARN != functionARN {
		return false
	}

	if nameFilter != "" && ex.Name != nameFilter {
		return false
	}

	if len(statusSet) > 0 && !statusSet[ex.Status] {
		return false
	}

	if !startedAfter.IsZero() && !ex.StartTime.After(startedAfter) {
		return false
	}

	if !startedBefore.IsZero() && !ex.StartTime.Before(startedBefore) {
		return false
	}

	return true
}

// checkpoint applies updates to the execution identified by arn — creating
// it on first use, see the store's doc comment — rotates the checkpoint
// token, and returns the response. Any update whose Type is CALLBACK and
// Action is START registers a CallbackId -> arn mapping so a later
// SendDurableExecutionCallback{Success,Failure,Heartbeat} call can resolve
// it (those ops address the callback by CallbackId alone, per the real API,
// not by DurableExecutionArn).
func (s *durableExecutionStore) checkpoint(
	arn string, updates []DurableOperationUpdate,
) *CheckpointDurableExecutionOutput {
	s.mu.Lock("Checkpoint")
	defer s.mu.Unlock()

	ex, ok := s.executions[arn]
	if !ok {
		ex = newDurableExecution(arn)
		s.executions[arn] = ex
	}

	var touched []*DurableOperation

	for _, u := range updates {
		op := ex.applyUpdate(u)
		if op == nil {
			continue
		}

		cp := *op
		touched = append(touched, &cp)

		if u.Type == string(DurableOperationTypeCallback) && u.Action == operationActionStart {
			s.callbackOwner[u.ID] = arn
		}
	}

	ex.CheckpointToken = uuid.New().String()
	token := ex.CheckpointToken

	return &CheckpointDurableExecutionOutput{
		NewExecutionState: &CheckpointUpdatedExecutionState{Operations: touched},
		CheckpointToken:   &token,
	}
}

// stopOutput stops the running execution identified by arn (idempotent: a
// second Stop on an already-terminal execution just reports its existing
// StopTimestamp/EndTime rather than erroring or re-mutating state), or
// returns ErrDurableExecutionNotFound when arn is unknown.
func (s *durableExecutionStore) stopOutput(arn string, errObj *ErrorObject) (*StopDurableExecutionOutput, error) {
	s.mu.Lock("Stop")
	defer s.mu.Unlock()

	ex, ok := s.executions[arn]
	if !ok {
		return nil, ErrDurableExecutionNotFound
	}

	if ex.Status != DurableExecutionStatusRunning {
		return &StopDurableExecutionOutput{StopTimestamp: awstime.Epoch(ex.EndTime)}, nil
	}

	now := time.Now().UTC()
	ex.Status = DurableExecutionStatusStopped
	ex.EndTime = now
	ex.Error = errObj

	// ExecutionStoppedDetails.Error is a required member on the real type;
	// synthesize a default reason when the caller's StopDurableExecutionInput
	// (where Error is optional) didn't supply one.
	effErr := errObj
	if effErr == nil {
		effErr = &ErrorObject{
			ErrorType:    ptrconv.NilIfEmpty("Stopped"),
			ErrorMessage: ptrconv.NilIfEmpty("The durable execution was stopped by StopDurableExecution."),
		}
	}

	ex.appendEvent(DurableExecutionEvent{
		EventType:               eventTypeExecutionStopped,
		ID:                      ptrconv.NilIfEmpty(durableExecutionRootOperationID),
		ExecutionStoppedDetails: &ExecutionStoppedDetails{Error: &EventError{Payload: effErr}},
	})

	if idx, found := ex.opIndex[durableExecutionRootOperationID]; found {
		ex.Operations[idx].Status = DurableOperationStatusStopped
		ex.Operations[idx].EndTimestamp = epochPtr(now)
	}

	return &StopDurableExecutionOutput{StopTimestamp: awstime.Epoch(now)}, nil
}

// sendCallback resolves callbackID (via the callbackOwner index populated by
// checkpoint) to its owning execution and applies action ("SUCCEED", "FAIL",
// or "HEARTBEAT") to the corresponding CALLBACK operation. HEARTBEAT is a
// real API call with no corresponding OperationStatus transition or history
// event (CallbackStarted/Succeeded/Failed are the only CALLBACK EventType
// values — there is no "CallbackHeartbeat"); real AWS just extends an
// internal timeout, which this in-memory store has no timer to extend, so
// it is accepted as a no-op. Returns ErrCallbackNotFound when callbackID is
// unknown.
func (s *durableExecutionStore) sendCallback(callbackID, action string, errObj *ErrorObject, result []byte) error {
	s.mu.Lock("SendCallback")
	defer s.mu.Unlock()

	arn, ok := s.callbackOwner[callbackID]
	if !ok {
		return ErrCallbackNotFound
	}

	ex, ok := s.executions[arn]
	if !ok {
		return ErrCallbackNotFound
	}

	if action == "HEARTBEAT" {
		return nil
	}

	update := DurableOperationUpdate{
		ID: callbackID, Type: string(DurableOperationTypeCallback), Action: action, Error: errObj,
	}
	if result != nil {
		payload := string(result)
		update.Payload = &payload
	}

	ex.applyUpdate(update)

	return nil
}

// reset clears all durable executions and callback registrations.
func (s *durableExecutionStore) reset() {
	s.mu.Lock("Reset")
	defer s.mu.Unlock()

	s.executions = make(map[string]*DurableExecution)
	s.callbackOwner = make(map[string]string)
}
