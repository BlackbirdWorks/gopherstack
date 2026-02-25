package asl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"time"
)

// ErrExecutionFailed is returned when a Fail state is reached.
var ErrExecutionFailed = errors.New("ExecutionFailed")

// ErrChoiceNoMatch is returned when a Choice state has no matching rule and no Default.
var ErrChoiceNoMatch = errors.New("States.NoChoiceMatched")

// Sentinel errors for executor internals.
var (
	ErrStateNotFound         = errors.New("state not found")
	ErrMaxTransitions        = errors.New("state machine exceeded maximum transitions")
	ErrUnsupportedStateType  = errors.New("unsupported state type")
	ErrChoiceNoNext          = errors.New("choice rule has no Next")
	ErrLambdaNotConfigured   = errors.New("lambda invoker not configured")
	ErrLambdaStatusError     = errors.New("lambda returned non-2xx status")
	ErrMapRequiresIterator   = errors.New("map state requires Iterator")
	ErrUnsupportedPathExpr   = errors.New("unsupported path expression")
	ErrUnsupportedResultPath = errors.New("unsupported ResultPath")
	ErrCannotIndexNonObject  = errors.New("cannot index non-object with path")
	ErrFieldNotFound         = errors.New("field not found")
	ErrMapInputNotArray      = errors.New("input is not an array for Map state")
	ErrItemsPathNotArray     = errors.New("ItemsPath does not point to an array")
)

// LambdaInvoker can invoke a Lambda function.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// HistoryRecorder is called during execution to record state transition events.
type HistoryRecorder interface {
	RecordStateEntered(executionARN, stateName, stateType string, input any)
	RecordStateExited(executionARN, stateName, stateType string, output any)
	RecordTaskScheduled(executionARN, stateName, resource string)
	RecordTaskSucceeded(executionARN, stateName string, output any)
	RecordTaskFailed(executionARN, stateName, errCode, cause string)
}

// ExecutionResult holds the final output and status of a state machine execution.
type ExecutionResult struct {
	Output any
	Error  string
	Cause  string
}

// Executor runs an ASL state machine.
type Executor struct {
	sm      *StateMachine
	lambda  LambdaInvoker
	history HistoryRecorder
}

// NewExecutor creates an Executor for the given state machine.
func NewExecutor(sm *StateMachine, lambda LambdaInvoker, history HistoryRecorder) *Executor {
	return &Executor{sm: sm, lambda: lambda, history: history}
}

// Execute runs the state machine with the given input JSON and returns the result.
func (e *Executor) Execute(ctx context.Context, executionARN, inputJSON string) (*ExecutionResult, error) {
	var input any
	if inputJSON != "" {
		if err := json.Unmarshal([]byte(inputJSON), &input); err != nil {
			return nil, fmt.Errorf("invalid input JSON: %w", err)
		}
	}

	output, err := e.runStates(ctx, executionARN, e.sm.States, e.sm.StartAt, input)
	if err != nil {
		var failErr *FailError
		if errors.As(err, &failErr) {
			return &ExecutionResult{Error: failErr.ErrCode, Cause: failErr.Cause}, nil
		}

		return nil, err
	}

	return &ExecutionResult{Output: output}, nil
}

// runStates executes states starting from startAt in the provided states map.
func (e *Executor) runStates(
	ctx context.Context,
	executionARN string,
	states map[string]*State,
	startAt string,
	input any,
) (any, error) {
	current := startAt
	value := input

	const maxTransitions = 10000

	for range maxTransitions {
		state, ok := states[current]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrStateNotFound, current)
		}

		if e.history != nil {
			e.history.RecordStateEntered(executionARN, current, state.Type, value)
		}

		// Apply InputPath.
		effectiveInput, err := applyPath(state.InputPath, value)
		if err != nil {
			return nil, fmt.Errorf("InputPath error in state %q: %w", current, err)
		}

		var result any
		var nextState string

		nextState, result, err = e.executeState(ctx, executionARN, current, state, effectiveInput)
		if err != nil {
			return nil, err
		}

		// Apply ResultPath: merge result into original input.
		finalOutput, err := applyResultPath(state.ResultPath, value, result)
		if err != nil {
			return nil, fmt.Errorf("ResultPath error in state %q: %w", current, err)
		}

		// Apply OutputPath.
		finalOutput, err = applyPath(state.OutputPath, finalOutput)
		if err != nil {
			return nil, fmt.Errorf("OutputPath error in state %q: %w", current, err)
		}

		if e.history != nil {
			e.history.RecordStateExited(executionARN, current, state.Type, finalOutput)
		}

		if nextState == "" {
			return finalOutput, nil
		}

		value = finalOutput
		current = nextState
	}

	return nil, ErrMaxTransitions
}

// executeState executes a single state and returns (nextStateName, output, error).
func (e *Executor) executeState(
	ctx context.Context,
	executionARN, stateName string,
	state *State,
	input any,
) (string, any, error) {
	switch state.Type {
	case "Pass":
		return e.executePass(state, input)
	case "Succeed":
		return "", input, nil
	case "Fail":
		return "", nil, &FailError{ErrCode: state.Error, Cause: state.Cause}
	case "Wait":
		return e.executeWait(ctx, state, input)
	case "Choice":
		return e.executeChoice(state, input)
	case "Task":
		return e.executeTask(ctx, executionARN, stateName, state, input)
	case "Parallel":
		return e.executeParallel(ctx, executionARN, state, input)
	case "Map":
		return e.executeMap(ctx, executionARN, state, input)
	default:
		return "", nil, fmt.Errorf("%w: %q in state %q", ErrUnsupportedStateType, state.Type, stateName)
	}
}

// executePass handles Pass state.
func (e *Executor) executePass(state *State, input any) (string, any, error) {
	if len(state.Result) > 0 {
		var result any
		if err := json.Unmarshal(state.Result, &result); err != nil {
			return "", nil, fmt.Errorf("invalid Pass Result: %w", err)
		}

		return state.Next, result, nil
	}

	return state.Next, input, nil
}

// executeWait handles Wait state.
func (e *Executor) executeWait(ctx context.Context, state *State, input any) (string, any, error) {
	if state.Seconds > 0 {
		select {
		case <-time.After(time.Duration(state.Seconds) * time.Second):
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}

	// Timestamp and SecondsPath/TimestampPath are not fully implemented here.
	// For test purposes, we skip the actual wait.

	return state.Next, input, nil
}

// executeChoice handles Choice state.
func (e *Executor) executeChoice(state *State, input any) (string, any, error) {
	for _, rule := range state.Choices {
		if evaluateChoiceRule(&rule, input) {
			if rule.Next == "" {
				return "", nil, ErrChoiceNoNext
			}

			return rule.Next, input, nil
		}
	}

	if state.Default != "" {
		return state.Default, input, nil
	}

	return "", nil, &FailError{
		ErrCode: ErrChoiceNoMatch.Error(),
		Cause:   "No choice matched and no Default was provided",
	}
}

// executeTask handles Task state (Lambda integration and placeholder for other types).
func (e *Executor) executeTask(
	ctx context.Context,
	executionARN, stateName string,
	state *State,
	input any,
) (string, any, error) {
	if e.history != nil {
		e.history.RecordTaskScheduled(executionARN, stateName, state.Resource)
	}

	result, taskErr := e.invokeTask(ctx, state, input)
	if taskErr != nil {
		// Check Catch clauses.
		for _, catcher := range state.Catch {
			if catchesError(catcher.ErrorEquals, taskErr) {
				errorResult := map[string]any{
					"Error": taskErr.Error(),
				}
				out, _ := applyResultPath(catcher.ResultPath, input, errorResult)

				if e.history != nil {
					e.history.RecordTaskFailed(executionARN, stateName, taskErr.Error(), "")
				}

				return catcher.Next, out, nil
			}
		}

		if e.history != nil {
			e.history.RecordTaskFailed(executionARN, stateName, taskErr.Error(), "")
		}

		return "", nil, &FailError{ErrCode: "TaskFailed", Cause: taskErr.Error()}
	}

	if e.history != nil {
		e.history.RecordTaskSucceeded(executionARN, stateName, result)
	}

	return state.Next, result, nil
}

// invokeTask performs the actual task invocation.
func (e *Executor) invokeTask(ctx context.Context, state *State, input any) (any, error) {
	if isLambdaResource(state.Resource) {
		return e.invokeLambdaTask(ctx, state, input)
	}

	// For unsupported resource types, pass input through (permissive stub).
	return input, nil
}

// invokeLambdaTask invokes a Lambda function as a Task state.
func (e *Executor) invokeLambdaTask(ctx context.Context, state *State, input any) (any, error) {
	if e.lambda == nil {
		return nil, ErrLambdaNotConfigured
	}

	payload, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal task input: %w", err)
	}

	respBytes, statusCode, err := e.lambda.InvokeFunction(ctx, state.Resource, "RequestResponse", payload)
	if err != nil {
		return nil, err
	}

	const statusOK = 200
	if statusCode >= 400 || statusCode < statusOK {
		return nil, fmt.Errorf("%w: %d", ErrLambdaStatusError, statusCode)
	}

	var result any
	if unmarshalErr := json.Unmarshal(respBytes, &result); unmarshalErr != nil {
		// If not JSON, return raw string as the output — the error is expected and intentional.
		return string(respBytes), nil //nolint:nilerr // non-JSON Lambda response is valid; return as string
	}

	return result, nil
}

// executeParallel handles Parallel state: runs all branches concurrently.
func (e *Executor) executeParallel(
	ctx context.Context,
	executionARN string,
	state *State,
	input any,
) (string, any, error) {
	results := make([]any, len(state.Branches))
	errs := make([]error, len(state.Branches))

	var wg sync.WaitGroup
	for i, branch := range state.Branches {
		wg.Add(1)

		go func(idx int, b Branch) {
			defer wg.Done()
			branchSM := &StateMachine{
				StartAt: b.StartAt,
				States:  b.States,
			}
			exec := NewExecutor(branchSM, e.lambda, e.history)
			res, err := exec.Execute(ctx, executionARN, marshalInput(input))
			if err != nil {
				errs[idx] = err

				return
			}
			results[idx] = res.Output
		}(i, branch)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return "", nil, err
		}
	}

	return state.Next, results, nil
}

// executeMap handles Map state: iterates over an array.
func (e *Executor) executeMap(ctx context.Context, executionARN string, state *State, input any) (string, any, error) {
	if state.Iterator == nil {
		return "", nil, ErrMapRequiresIterator
	}

	// Resolve the items to iterate over.
	items, err := resolveItems(state.ItemsPath, input)
	if err != nil {
		return "", nil, fmt.Errorf("map ItemsPath error: %w", err)
	}

	results := make([]any, len(items))
	errs := make([]error, len(items))

	concurrency := state.MaxConcurrency
	if concurrency <= 0 {
		concurrency = len(items)
	}

	sem := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	for i, item := range items {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int, it any) {
			defer wg.Done()
			defer func() { <-sem }()

			exec := NewExecutor(state.Iterator, e.lambda, e.history)
			res, execErr := exec.Execute(ctx, executionARN, marshalInput(it))
			if execErr != nil {
				errs[idx] = execErr

				return
			}
			results[idx] = res.Output
		}(i, item)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return "", nil, err
		}
	}

	return state.Next, results, nil
}

// FailError represents the error from a Fail state.
type FailError struct {
	ErrCode string
	Cause   string
}

func (e *FailError) Error() string {
	if e.Cause != "" {
		return fmt.Sprintf("%s: %s", e.ErrCode, e.Cause)
	}

	return e.ErrCode
}

// applyPath applies an InputPath or OutputPath to a value.
// If path is "" or "$", returns value unchanged.
// If path starts with "$.", it's a JSONPath reference.
func applyPath(path string, value any) (any, error) {
	if path == "" || path == "$" {
		return value, nil
	}

	if path == "$$" {
		return value, nil
	}

	// Simple dot-notation JSONPath: $.field.subfield
	if strings.HasPrefix(path, "$.") {
		return jsonPathGet(path[2:], value)
	}

	return nil, fmt.Errorf("%w: %q", ErrUnsupportedPathExpr, path)
}

// applyResultPath merges result into input according to ResultPath.
// If ResultPath is "", result replaces input entirely.
// If ResultPath is "$", result replaces input entirely (default).
// If ResultPath is "$.field", result is written to input[field].
// If ResultPath is "null", result is discarded (input passes through).
func applyResultPath(resultPath string, input, result any) (any, error) {
	if resultPath == "null" {
		return input, nil
	}

	if resultPath == "" || resultPath == "$" {
		return result, nil
	}

	if !strings.HasPrefix(resultPath, "$.") {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedResultPath, resultPath)
	}

	field := resultPath[2:]
	inputMap, ok := input.(map[string]any)
	if !ok {
		// If input is not a map, create a new one.
		inputMap = make(map[string]any)
	} else {
		// Copy to avoid mutation.
		inputCopy := make(map[string]any, len(inputMap))
		maps.Copy(inputCopy, inputMap)
		inputMap = inputCopy
	}

	// Support nested paths like $.a.b.
	const splitFieldParts = 2
	parts := strings.SplitN(field, ".", splitFieldParts)
	if len(parts) == 1 {
		inputMap[field] = result
	} else {
		sub, subErr := applyResultPath("$."+parts[1], inputMap[parts[0]], result)
		if subErr != nil {
			return nil, subErr
		}
		inputMap[parts[0]] = sub
	}

	return inputMap, nil
}

// jsonPathGet performs a simple dot-notation JSONPath get on a value.
func jsonPathGet(path string, value any) (any, error) {
	if path == "" {
		return value, nil
	}

	const splitPathParts = 2
	parts := strings.SplitN(path, ".", splitPathParts)
	m, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrCannotIndexNonObject, path)
	}

	child, ok := m[parts[0]]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrFieldNotFound, parts[0])
	}

	if len(parts) == 1 {
		return child, nil
	}

	return jsonPathGet(parts[1], child)
}

// evaluateChoiceRule checks whether a ChoiceRule matches the given input.
//
//nolint:gocognit,cyclop // choice rule evaluation requires checking many condition types
func evaluateChoiceRule(rule *ChoiceRule, input any) bool {
	// Logical operators.
	if len(rule.And) > 0 {
		for i := range rule.And {
			if !evaluateChoiceRule(&rule.And[i], input) {
				return false
			}
		}

		return true
	}

	if len(rule.Or) > 0 {
		for i := range rule.Or {
			if evaluateChoiceRule(&rule.Or[i], input) {
				return true
			}
		}

		return false
	}

	if rule.Not != nil {
		return !evaluateChoiceRule(rule.Not, input)
	}

	// Variable-based comparison.
	if rule.Variable == "" {
		return false
	}

	varVal, err := applyPath(rule.Variable, input)
	if err != nil {
		// Variable not found.
		if rule.IsPresent != nil {
			return !*rule.IsPresent
		}

		return false
	}

	if rule.IsPresent != nil {
		return *rule.IsPresent
	}

	if rule.IsNull != nil {
		return (varVal == nil) == *rule.IsNull
	}

	if rule.StringEquals != nil {
		s, ok := varVal.(string)

		return ok && s == *rule.StringEquals
	}

	if rule.StringLessThan != nil {
		s, ok := varVal.(string)

		return ok && s < *rule.StringLessThan
	}

	if rule.StringGreaterThan != nil {
		s, ok := varVal.(string)

		return ok && s > *rule.StringGreaterThan
	}

	if rule.NumericEquals != nil {
		n, ok := toFloat(varVal)

		return ok && n == *rule.NumericEquals
	}

	if rule.NumericLessThan != nil {
		n, ok := toFloat(varVal)

		return ok && n < *rule.NumericLessThan
	}

	if rule.NumericGreaterThan != nil {
		n, ok := toFloat(varVal)

		return ok && n > *rule.NumericGreaterThan
	}

	if rule.BooleanEquals != nil {
		b, ok := varVal.(bool)

		return ok && b == *rule.BooleanEquals
	}

	return false
}

// toFloat converts a numeric value to float64.
func toFloat(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	default:
		return 0, false
	}
}

// catchesError checks if a set of error equals values matches the given error.
func catchesError(errorEquals []string, err error) bool {
	for _, e := range errorEquals {
		if e == "States.ALL" || e == err.Error() {
			return true
		}
	}

	return false
}

// isLambdaResource returns true if the resource ARN is a Lambda function.
func isLambdaResource(resource string) bool {
	return strings.Contains(resource, ":lambda:") ||
		strings.HasPrefix(resource, "arn:aws:lambda:")
}

// resolveItems returns the array of items for a Map state.
func resolveItems(itemsPath string, input any) ([]any, error) {
	if itemsPath == "" || itemsPath == "$" {
		arr, ok := input.([]any)
		if !ok {
			return nil, ErrMapInputNotArray
		}

		return arr, nil
	}

	val, err := applyPath(itemsPath, input)
	if err != nil {
		return nil, err
	}

	arr, ok := val.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrItemsPathNotArray, itemsPath)
	}

	return arr, nil
}

// marshalInput marshals a value to JSON string for sub-execution input.
func marshalInput(v any) string {
	if v == nil {
		return "{}"
	}

	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}

	return string(b)
}
