package stepfunctions

import (
	"encoding/json"
)

type startExecutionInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	Name            string `json:"name"`
	Input           string `json:"input"`
	TraceHeader     string `json:"traceHeader"`
}

type startSyncExecutionInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	Name            string `json:"name"`
	Input           string `json:"input"`
}

type stopExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
	Error        string `json:"error"`
	Cause        string `json:"cause"`
}

type describeExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
}

type listExecutionsInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	StatusFilter    string `json:"statusFilter"`
	NextToken       string `json:"nextToken"`
	MaxResults      int    `json:"maxResults"`
}

type getExecutionHistoryInput struct {
	ExecutionArn string `json:"executionArn"`
	NextToken    string `json:"nextToken"`
	MaxResults   int    `json:"maxResults"`
	ReverseOrder bool   `json:"reverseOrder"`
}

type startExecutionOutput struct {
	ExecutionArn string  `json:"executionArn"`
	StartDate    float64 `json:"startDate"`
}

type stopExecutionOutput struct {
	StopDate *float64 `json:"stopDate"`
}

type listExecutionsOutput struct {
	NextToken  string              `json:"nextToken,omitempty"`
	Executions []executionListItem `json:"executions"`
}

// executionListItem mirrors AWS's ExecutionListItem, which -- unlike the
// full Execution shape DescribeExecution returns -- has no input, output,
// error, or cause (types.go, sfn@v1.45.4). ItemCount/MapRunArn are also
// declared on ExecutionListItem but are not tracked on the domain Execution
// struct here; that is a separate (missing-field, not over-wide) gap.
type executionListItem struct {
	RedriveDate            *float64 `json:"redriveDate,omitempty"`
	StopDate               *float64 `json:"stopDate,omitempty"`
	ExecutionArn           string   `json:"executionArn"`
	Name                   string   `json:"name"`
	StateMachineArn        string   `json:"stateMachineArn"`
	Status                 string   `json:"status"`
	StateMachineAliasArn   string   `json:"stateMachineAliasArn,omitempty"`
	StateMachineVersionArn string   `json:"stateMachineVersionArn,omitempty"`
	StartDate              float64  `json:"startDate"`
	RedriveCount           int      `json:"redriveCount,omitempty"`
}

func newExecutionListItem(e *Execution) executionListItem {
	return executionListItem{
		ExecutionArn:           e.ExecutionArn,
		Name:                   e.Name,
		StartDate:              e.StartDate,
		StateMachineArn:        e.StateMachineArn,
		Status:                 e.Status,
		RedriveCount:           e.RedriveCount,
		RedriveDate:            e.RedriveDate,
		StateMachineAliasArn:   e.StateMachineAliasArn,
		StateMachineVersionArn: e.StateMachineVersionArn,
		StopDate:               e.StopDate,
	}
}

type getExecutionHistoryOutput struct {
	NextToken string         `json:"nextToken,omitempty"`
	Events    []HistoryEvent `json:"events"`
}

type redriveExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
}

type redriveExecutionOutput struct {
	RedriveDate float64 `json:"redriveDate"`
}

type describeStateMachineForExecutionInput struct {
	ExecutionArn string `json:"executionArn"`
}

// describeStateMachineForExecutionOutput mirrors AWS's
// DescribeStateMachineForExecutionOutput shape, which -- unlike StateMachine
// -- has no creationDate and names its only timestamp "updateDate" rather
// than "updatedDate".
type describeStateMachineForExecutionOutput struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	Definition              string                   `json:"definition"`
	Name                    string                   `json:"name"`
	RoleArn                 string                   `json:"roleArn"`
	StateMachineArn         string                   `json:"stateMachineArn"`
	UpdateDate              float64                  `json:"updateDate"`
}

func (h *Handler) executionActions() map[string]actionFn {
	return map[string]actionFn{
		"StartExecution":                   h.handleStartExecution,
		"StartSyncExecution":               h.handleStartSyncExecution,
		"StopExecution":                    h.handleStopExecution,
		"RedriveExecution":                 h.handleRedriveExecution,
		"DescribeExecution":                h.handleDescribeExecution,
		"DescribeStateMachineForExecution": h.handleDescribeStateMachineForExecution,
		"ListExecutions":                   h.handleListExecutions,
		"GetExecutionHistory":              h.handleGetExecutionHistory,
	}
}

func (h *Handler) handleRedriveExecution(b []byte) (any, error) {
	var input redriveExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	exec, err := h.Backend.RedriveExecution(input.ExecutionArn)
	if err != nil {
		return nil, err
	}

	return &redriveExecutionOutput{RedriveDate: exec.StartDate}, nil
}

func (h *Handler) handleDescribeStateMachineForExecution(b []byte) (any, error) {
	var input describeStateMachineForExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	sm, err := h.Backend.DescribeStateMachineForExecution(input.ExecutionArn)
	if err != nil {
		return nil, err
	}

	return &describeStateMachineForExecutionOutput{
		EncryptionConfiguration: sm.EncryptionConfiguration,
		Definition:              sm.Definition,
		Name:                    sm.Name,
		RoleArn:                 sm.RoleArn,
		StateMachineArn:         sm.StateMachineArn,
		UpdateDate:              sm.UpdatedDate,
	}, nil
}

func (h *Handler) handleStartExecution(b []byte) (any, error) {
	var input startExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	exec, err := h.Backend.StartExecutionWithTrace(
		input.StateMachineArn,
		input.Name,
		input.Input,
		input.TraceHeader,
	)
	if err != nil {
		return nil, err
	}

	return &startExecutionOutput{ExecutionArn: exec.ExecutionArn, StartDate: exec.StartDate}, nil
}

func (h *Handler) handleStartSyncExecution(b []byte) (any, error) {
	var input startSyncExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	return h.Backend.StartSyncExecution(input.StateMachineArn, input.Name, input.Input)
}

func (h *Handler) handleStopExecution(b []byte) (any, error) {
	var input stopExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.StopExecution(input.ExecutionArn, input.Error, input.Cause); err != nil {
		return nil, err
	}

	exec, err := h.Backend.DescribeExecution(input.ExecutionArn)
	if err != nil {
		return nil, err
	}

	return &stopExecutionOutput{StopDate: exec.StopDate}, nil
}

func (h *Handler) handleDescribeExecution(b []byte) (any, error) {
	var input describeExecutionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	return h.Backend.DescribeExecution(input.ExecutionArn)
}

func (h *Handler) handleListExecutions(b []byte) (any, error) {
	var input listExecutionsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	execs, next, err := h.Backend.ListExecutions(
		input.StateMachineArn, input.StatusFilter, input.NextToken, input.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	items := make([]executionListItem, len(execs))
	for i := range execs {
		items[i] = newExecutionListItem(&execs[i])
	}

	return &listExecutionsOutput{Executions: items, NextToken: next}, nil
}

func (h *Handler) handleGetExecutionHistory(b []byte) (any, error) {
	var input getExecutionHistoryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	events, next, err := h.Backend.GetExecutionHistory(
		input.ExecutionArn, input.NextToken, input.MaxResults, input.ReverseOrder,
	)
	if err != nil {
		return nil, err
	}

	return &getExecutionHistoryOutput{Events: events, NextToken: next}, nil
}
