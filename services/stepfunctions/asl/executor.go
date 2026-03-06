package asl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
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
	ErrStateNotFound                    = errors.New("state not found")
	ErrMaxTransitions                   = errors.New("state machine exceeded maximum transitions")
	ErrUnsupportedStateType             = errors.New("unsupported state type")
	ErrChoiceNoNext                     = errors.New("choice rule has no Next")
	ErrLambdaNotConfigured              = errors.New("lambda invoker not configured")
	ErrLambdaStatusError                = errors.New("lambda returned non-2xx status")
	ErrMapRequiresIterator              = errors.New("map state requires Iterator")
	ErrUnsupportedPathExpr              = errors.New("unsupported path expression")
	ErrUnsupportedResultPath            = errors.New("unsupported ResultPath")
	ErrCannotIndexNonObject             = errors.New("cannot index non-object with path")
	ErrFieldNotFound                    = errors.New("field not found")
	ErrMapInputNotArray                 = errors.New("input is not an array for Map state")
	ErrItemsPathNotArray                = errors.New("ItemsPath does not point to an array")
	ErrStatesTimeout                    = errors.New("States.Timeout")
	ErrSecondsPathNotNumber             = errors.New("SecondsPath did not resolve to a number")
	ErrTimestampPathNotString           = errors.New("TimestampPath did not resolve to a string")
	ErrNotAString                       = errors.New("not a string")
	ErrReferenceKeyNotString            = errors.New("value for reference key must be a string")
	ErrSQSIntegrationNotConfigured      = errors.New("SQS integration not configured")
	ErrSNSIntegrationNotConfigured      = errors.New("SNS integration not configured")
	ErrDynamoDBIntegrationNotConfigured = errors.New("DynamoDB integration not configured")
	ErrUnsupportedSQSAction             = errors.New("unsupported SQS action")
	ErrUnsupportedSNSAction             = errors.New("unsupported SNS action")
	ErrUnsupportedDynamoDBAction        = errors.New("unsupported DynamoDB action")
)

// LambdaInvoker can invoke a Lambda function.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// SQSIntegration handles Step Functions SQS service integration.
type SQSIntegration interface {
	SFNSendMessage(
		ctx context.Context,
		queueURL, messageBody, groupID, deduplicationID string,
		delaySeconds int,
	) (messageID string, md5 string, err error)
}

// SNSIntegration handles Step Functions SNS service integration.
type SNSIntegration interface {
	SFNPublish(ctx context.Context, topicARN, message, subject string) (messageID string, err error)
}

// DynamoDBIntegration handles Step Functions DynamoDB service integration.
type DynamoDBIntegration interface {
	SFNPutItem(ctx context.Context, input any) (any, error)
	SFNGetItem(ctx context.Context, input any) (any, error)
	SFNDeleteItem(ctx context.Context, input any) (any, error)
	SFNUpdateItem(ctx context.Context, input any) (any, error)
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
	sm       *StateMachine
	lambda   LambdaInvoker
	sqs      SQSIntegration
	sns      SNSIntegration
	dynamodb DynamoDBIntegration
	history  HistoryRecorder
}

// NewExecutor creates an Executor for the given state machine.
func NewExecutor(sm *StateMachine, lambda LambdaInvoker, history HistoryRecorder) *Executor {
	return &Executor{sm: sm, lambda: lambda, history: history}
}

// SetSQSIntegration configures the SQS integration for Task states.
func (e *Executor) SetSQSIntegration(sqs SQSIntegration) { e.sqs = sqs }

// SetSNSIntegration configures the SNS integration for Task states.
func (e *Executor) SetSNSIntegration(sns SNSIntegration) { e.sns = sns }

// SetDynamoDBIntegration configures the DynamoDB integration for Task states.
func (e *Executor) SetDynamoDBIntegration(ddb DynamoDBIntegration) { e.dynamodb = ddb }

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

		// Apply Parameters to transform the effective input for this state.
		taskInput := effectiveInput
		if len(state.Parameters) > 0 {
			taskInput, err = applyParametersTemplate(state.Parameters, effectiveInput)
			if err != nil {
				return nil, fmt.Errorf("parameters error in state %q: %w", current, err)
			}
		}

		var result any
		var nextState string

		nextState, result, err = e.executeState(ctx, executionARN, current, state, taskInput)
		if err != nil {
			return nil, err
		}

		finalOutput, err := applyStateOutputTransforms(state, value, result, current)
		if err != nil {
			return nil, err
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

// applyStateOutputTransforms applies ResultSelector, ResultPath, and OutputPath to produce the final state output.
func applyStateOutputTransforms(state *State, input, result any, stateName string) (any, error) {
	// Apply ResultSelector to filter the state output before ResultPath merge.
	if len(state.ResultSelector) > 0 {
		var err error

		result, err = applyParametersTemplate(state.ResultSelector, result)
		if err != nil {
			return nil, fmt.Errorf("ResultSelector error in state %q: %w", stateName, err)
		}
	}

	// Apply ResultPath: merge result into original input.
	finalOutput, err := applyResultPath(state.ResultPath, input, result)
	if err != nil {
		return nil, fmt.Errorf("ResultPath error in state %q: %w", stateName, err)
	}

	// Apply OutputPath.
	finalOutput, err = applyPath(state.OutputPath, finalOutput)
	if err != nil {
		return nil, fmt.Errorf("OutputPath error in state %q: %w", stateName, err)
	}

	return finalOutput, nil
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
//
//nolint:gocognit,cyclop // inherently complex due to multiple wait modes
func (e *Executor) executeWait(ctx context.Context, state *State, input any) (string, any, error) {
	var waitDuration time.Duration

	switch {
	case state.Seconds > 0:
		waitDuration = time.Duration(state.Seconds) * time.Second

	case state.SecondsPath != "":
		val, err := applyPath(state.SecondsPath, input)
		if err != nil {
			return "", nil, fmt.Errorf("SecondsPath error: %w", err)
		}

		secs, ok := toFloat(val)
		if !ok {
			return "", nil, ErrSecondsPathNotNumber
		}

		if secs > 0 {
			waitDuration = time.Duration(secs * float64(time.Second))
		}

	case state.Timestamp != "":
		t, err := time.Parse(time.RFC3339, state.Timestamp)
		if err != nil {
			return "", nil, fmt.Errorf("timestamp parse error: %w", err)
		}

		if d := time.Until(t); d > 0 {
			waitDuration = d
		}

	case state.TimestampPath != "":
		val, err := applyPath(state.TimestampPath, input)
		if err != nil {
			return "", nil, fmt.Errorf("TimestampPath error: %w", err)
		}

		tsStr, ok := val.(string)
		if !ok {
			return "", nil, ErrTimestampPathNotString
		}

		t, err := time.Parse(time.RFC3339, tsStr)
		if err != nil {
			return "", nil, fmt.Errorf("TimestampPath value parse error: %w", err)
		}

		if d := time.Until(t); d > 0 {
			waitDuration = d
		}
	}

	if waitDuration > 0 {
		select {
		case <-time.After(waitDuration):
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}

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
	// Enforce TimeoutSeconds by wrapping the context.
	if state.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(state.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	if e.history != nil {
		e.history.RecordTaskScheduled(executionARN, stateName, state.Resource)
	}

	// retryAttempts tracks how many times each retrier entry has been used.
	retryAttempts := make([]int, len(state.Retry))

	for {
		result, taskErr := e.invokeTask(ctx, state, input)
		if taskErr == nil {
			e.recordTaskSucceeded(executionARN, stateName, result)

			return state.Next, result, nil
		}

		if errors.Is(taskErr, context.DeadlineExceeded) {
			taskErr = ErrStatesTimeout
		} else if errors.Is(taskErr, context.Canceled) {
			// External cancellation: propagate immediately without retrying.
			return "", nil, taskErr
		}

		retried, retryErr := tryRetry(ctx, state, retryAttempts, taskErr)
		if retryErr != nil {
			if !errors.Is(retryErr, ErrStatesTimeout) {
				return "", nil, retryErr
			}

			taskErr = ErrStatesTimeout
		} else if retried {
			continue
		}

		if next, out, matched := e.checkCatchers(executionARN, stateName, state, input, taskErr); matched {
			return next, out, nil
		}

		e.recordTaskFailed(executionARN, stateName, taskErr.Error())

		return "", nil, &FailError{ErrCode: "TaskFailed", Cause: taskErr.Error()}
	}
}

// waitForRetry waits for the retry delay, or returns ctx.Err() if the context is done.
// It checks for immediate cancellation first to avoid non-deterministic select behavior.
func waitForRetry(ctx context.Context, delay time.Duration) error {
	// Fail fast if context is already done.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tryRetry attempts to schedule a retry for the failed task.
// Returns (scheduled=true, nil) if a retry delay was served; the caller should retry the task.
// Returns (false, ErrStatesTimeout) if the context deadline fired during the delay.
// Returns (false, ctx.Err()) for other context cancellations.
// Returns (false, nil) if no matching, non-exhausted retrier was found.
func tryRetry(ctx context.Context, state *State, retryAttempts []int, taskErr error) (bool, error) {
	const defaultMaxAttempts = 3
	const defaultIntervalSeconds = 1
	const defaultBackoffRate = 2.0

	for i := range state.Retry {
		retrier := &state.Retry[i]

		if !catchesError(retrier.ErrorEquals, taskErr) {
			continue
		}

		maxAttempts := defaultMaxAttempts
		if retrier.MaxAttempts != nil {
			maxAttempts = *retrier.MaxAttempts
		}

		if retryAttempts[i] >= maxAttempts {
			continue
		}

		intervalSeconds := defaultIntervalSeconds
		if retrier.IntervalSeconds != nil {
			intervalSeconds = *retrier.IntervalSeconds
		}

		backoffRate := retrier.BackoffRate
		if backoffRate <= 0 {
			backoffRate = defaultBackoffRate
		}

		delay := time.Duration(
			float64(intervalSeconds) *
				math.Pow(backoffRate, float64(retryAttempts[i])) *
				float64(time.Second),
		)
		retryAttempts[i]++

		if err := waitForRetry(ctx, delay); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return false, ErrStatesTimeout
			}

			return false, err
		}

		return true, nil
	}

	return false, nil
}

// checkCatchers checks Catch clauses and returns (nextState, output, matched).
// If a catcher matches, the task failure is recorded in the history.
func (e *Executor) checkCatchers(
	executionARN, stateName string,
	state *State,
	input any,
	taskErr error,
) (string, any, bool) {
	for _, catcher := range state.Catch {
		if catchesError(catcher.ErrorEquals, taskErr) {
			errorResult := map[string]any{
				"Error": taskErr.Error(),
			}
			out, _ := applyResultPath(catcher.ResultPath, input, errorResult)

			e.recordTaskFailed(executionARN, stateName, taskErr.Error())

			return catcher.Next, out, true
		}
	}

	return "", nil, false
}

// recordTaskSucceeded records a task success event if a history recorder is configured.
func (e *Executor) recordTaskSucceeded(executionARN, stateName string, result any) {
	if e.history != nil {
		e.history.RecordTaskSucceeded(executionARN, stateName, result)
	}
}

// recordTaskFailed records a task failure event if a history recorder is configured.
func (e *Executor) recordTaskFailed(executionARN, stateName, errCode string) {
	if e.history != nil {
		e.history.RecordTaskFailed(executionARN, stateName, errCode, "")
	}
}

// invokeTask performs the actual task invocation.
func (e *Executor) invokeTask(ctx context.Context, state *State, input any) (any, error) {
	if isLambdaResource(state.Resource) {
		return e.invokeLambdaTask(ctx, state, input)
	}
	if isSQSResource(state.Resource) {
		return e.invokeSQSTask(ctx, state, input)
	}
	if isSNSResource(state.Resource) {
		return e.invokeSNSTask(ctx, state, input)
	}
	if isDynamoDBResource(state.Resource) {
		return e.invokeDynamoDBTask(ctx, state, input)
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

// invokeSQSTask invokes an SQS action as a Task state.
func (e *Executor) invokeSQSTask(ctx context.Context, state *State, input any) (any, error) {
	if e.sqs == nil {
		return nil, ErrSQSIntegrationNotConfigured
	}

	action := serviceAction(state.Resource)
	m, _ := input.(map[string]any)

	switch action {
	case "sendMessage":
		queueURL, _ := m["QueueUrl"].(string)
		messageBody, _ := m["MessageBody"].(string)
		groupID, _ := m["MessageGroupId"].(string)
		dedupID, _ := m["MessageDeduplicationId"].(string)
		var delaySeconds int
		if d, ok := toFloat(m["DelaySeconds"]); ok {
			delaySeconds = int(d)
		}
		msgID, md5, err := e.sqs.SFNSendMessage(ctx, queueURL, messageBody, groupID, dedupID, delaySeconds)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			"MD5OfMessageBody": md5,
			"MessageId":        msgID,
		}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedSQSAction, action)
	}
}

// invokeSNSTask invokes an SNS action as a Task state.
func (e *Executor) invokeSNSTask(ctx context.Context, state *State, input any) (any, error) {
	if e.sns == nil {
		return nil, ErrSNSIntegrationNotConfigured
	}

	action := serviceAction(state.Resource)
	m, _ := input.(map[string]any)

	switch action {
	case "publish":
		topicARN, _ := m["TopicArn"].(string)
		message, _ := m["Message"].(string)
		subject, _ := m["Subject"].(string)
		msgID, err := e.sns.SFNPublish(ctx, topicARN, message, subject)
		if err != nil {
			return nil, err
		}

		return map[string]any{"MessageId": msgID}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedSNSAction, action)
	}
}

// invokeDynamoDBTask invokes a DynamoDB action as a Task state.
func (e *Executor) invokeDynamoDBTask(ctx context.Context, state *State, input any) (any, error) {
	if e.dynamodb == nil {
		return nil, ErrDynamoDBIntegrationNotConfigured
	}

	action := serviceAction(state.Resource)

	switch action {
	case "putItem":
		return e.dynamodb.SFNPutItem(ctx, input)
	case "getItem":
		return e.dynamodb.SFNGetItem(ctx, input)
	case "deleteItem":
		return e.dynamodb.SFNDeleteItem(ctx, input)
	case "updateItem":
		return e.dynamodb.SFNUpdateItem(ctx, input)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedDynamoDBAction, action)
	}
}

// serviceAction extracts the action name from a States service integration ARN,
// stripping any .sync or .waitForTaskToken suffix.
// Example: "arn:aws:states:::sqs:sendMessage.sync" → "sendMessage".
func serviceAction(resource string) string {
	parts := strings.Split(resource, ":")
	action := parts[len(parts)-1]
	if i := strings.IndexByte(action, '.'); i >= 0 {
		action = action[:i]
	}

	return action
}

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
	// Support both Iterator (legacy) and ItemProcessor (current ASL spec).
	iterator := state.Iterator
	if iterator == nil {
		iterator = state.ItemProcessor
	}

	if iterator == nil {
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

			exec := NewExecutor(iterator, e.lambda, e.history)
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
func evaluateChoiceRule(rule *ChoiceRule, input any) bool {
	if result, handled := evaluateLogicalOp(rule, input); handled {
		return result
	}

	if rule.Variable == "" {
		return false
	}

	return evaluateVariableComparison(rule, input)
}

// evaluateLogicalOp handles And/Or/Not logical operators.
// Returns (result, true) if a logical operator was found, or (false, false) otherwise.
func evaluateLogicalOp(rule *ChoiceRule, input any) (bool, bool) {
	if len(rule.And) > 0 {
		for i := range rule.And {
			if !evaluateChoiceRule(&rule.And[i], input) {
				return false, true
			}
		}

		return true, true
	}

	if len(rule.Or) > 0 {
		for i := range rule.Or {
			if evaluateChoiceRule(&rule.Or[i], input) {
				return true, true
			}
		}

		return false, true
	}

	if rule.Not != nil {
		return !evaluateChoiceRule(rule.Not, input), true
	}

	return false, false
}

// evaluateVariableComparison resolves the variable and checks condition comparisons.
func evaluateVariableComparison(rule *ChoiceRule, input any) bool {
	varVal, err := applyPath(rule.Variable, input)
	if err != nil {
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

	// Type checks do not require IsNull handling above.
	if rule.IsString != nil {
		_, ok := varVal.(string)

		return ok == *rule.IsString
	}

	if rule.IsNumeric != nil {
		_, ok := toFloat(varVal)

		return ok == *rule.IsNumeric
	}

	if rule.IsBoolean != nil {
		_, ok := varVal.(bool)

		return ok == *rule.IsBoolean
	}

	if rule.IsTimestamp != nil {
		s, ok := varVal.(string)
		if !ok {
			return !*rule.IsTimestamp
		}
		_, parseErr := time.Parse(time.RFC3339, s)

		return (parseErr == nil) == *rule.IsTimestamp
	}

	return matchVariableCondition(rule, varVal, input)
}

// matchVariableCondition checks all value-comparison conditions by delegating to
// type-specific sub-functions.
func matchVariableCondition(rule *ChoiceRule, varVal, input any) bool {
	if result, matched := matchStringCondition(rule, varVal, input); matched {
		return result
	}

	if result, matched := matchNumericCondition(rule, varVal, input); matched {
		return result
	}

	if result, matched := matchBooleanCondition(rule, varVal, input); matched {
		return result
	}

	if result, matched := matchTimestampCondition(rule, varVal, input); matched {
		return result
	}

	return false
}

// matchStringCondition checks string comparison conditions.
//
//nolint:gocognit,gocyclo,cyclop,funlen // many string comparison operators
func matchStringCondition(rule *ChoiceRule, varVal, input any) (bool, bool) {
	if rule.StringEquals != nil {
		s, ok := varVal.(string)

		return ok && s == *rule.StringEquals, true
	}

	if rule.StringLessThan != nil {
		s, ok := varVal.(string)

		return ok && s < *rule.StringLessThan, true
	}

	if rule.StringGreaterThan != nil {
		s, ok := varVal.(string)

		return ok && s > *rule.StringGreaterThan, true
	}

	if rule.StringLessThanEquals != nil {
		s, ok := varVal.(string)

		return ok && s <= *rule.StringLessThanEquals, true
	}

	if rule.StringGreaterThanEquals != nil {
		s, ok := varVal.(string)

		return ok && s >= *rule.StringGreaterThanEquals, true
	}

	if rule.StringEqualsPath != nil {
		ref, err := applyPath(*rule.StringEqualsPath, input)
		if err != nil {
			return false, true
		}

		s1, ok1 := varVal.(string)
		s2, ok2 := ref.(string)

		return ok1 && ok2 && s1 == s2, true
	}

	if rule.StringLessThanPath != nil {
		ref, err := applyPath(*rule.StringLessThanPath, input)
		if err != nil {
			return false, true
		}

		s1, ok1 := varVal.(string)
		s2, ok2 := ref.(string)

		return ok1 && ok2 && s1 < s2, true
	}

	if rule.StringGreaterThanPath != nil {
		ref, err := applyPath(*rule.StringGreaterThanPath, input)
		if err != nil {
			return false, true
		}

		s1, ok1 := varVal.(string)
		s2, ok2 := ref.(string)

		return ok1 && ok2 && s1 > s2, true
	}

	if rule.StringLessThanEqualsPath != nil {
		ref, err := applyPath(*rule.StringLessThanEqualsPath, input)
		if err != nil {
			return false, true
		}

		s1, ok1 := varVal.(string)
		s2, ok2 := ref.(string)

		return ok1 && ok2 && s1 <= s2, true
	}

	if rule.StringGreaterThanEqualsPath != nil {
		ref, err := applyPath(*rule.StringGreaterThanEqualsPath, input)
		if err != nil {
			return false, true
		}

		s1, ok1 := varVal.(string)
		s2, ok2 := ref.(string)

		return ok1 && ok2 && s1 >= s2, true
	}

	return false, false
}

// matchNumericCondition checks numeric comparison conditions.
//
//nolint:gocognit,gocyclo,cyclop,funlen // many numeric comparison operators
func matchNumericCondition(rule *ChoiceRule, varVal, input any) (bool, bool) {
	if rule.NumericEquals != nil {
		n, ok := toFloat(varVal)

		return ok && n == *rule.NumericEquals, true
	}

	if rule.NumericLessThan != nil {
		n, ok := toFloat(varVal)

		return ok && n < *rule.NumericLessThan, true
	}

	if rule.NumericGreaterThan != nil {
		n, ok := toFloat(varVal)

		return ok && n > *rule.NumericGreaterThan, true
	}

	if rule.NumericLessThanEquals != nil {
		n, ok := toFloat(varVal)

		return ok && n <= *rule.NumericLessThanEquals, true
	}

	if rule.NumericGreaterThanEquals != nil {
		n, ok := toFloat(varVal)

		return ok && n >= *rule.NumericGreaterThanEquals, true
	}

	if rule.NumericEqualsPath != nil {
		ref, err := applyPath(*rule.NumericEqualsPath, input)
		if err != nil {
			return false, true
		}

		n1, ok1 := toFloat(varVal)
		n2, ok2 := toFloat(ref)

		return ok1 && ok2 && n1 == n2, true
	}

	if rule.NumericLessThanPath != nil {
		ref, err := applyPath(*rule.NumericLessThanPath, input)
		if err != nil {
			return false, true
		}

		n1, ok1 := toFloat(varVal)
		n2, ok2 := toFloat(ref)

		return ok1 && ok2 && n1 < n2, true
	}

	if rule.NumericGreaterThanPath != nil {
		ref, err := applyPath(*rule.NumericGreaterThanPath, input)
		if err != nil {
			return false, true
		}

		n1, ok1 := toFloat(varVal)
		n2, ok2 := toFloat(ref)

		return ok1 && ok2 && n1 > n2, true
	}

	if rule.NumericLessThanEqualsPath != nil {
		ref, err := applyPath(*rule.NumericLessThanEqualsPath, input)
		if err != nil {
			return false, true
		}

		n1, ok1 := toFloat(varVal)
		n2, ok2 := toFloat(ref)

		return ok1 && ok2 && n1 <= n2, true
	}

	if rule.NumericGreaterThanEqualsPath != nil {
		ref, err := applyPath(*rule.NumericGreaterThanEqualsPath, input)
		if err != nil {
			return false, true
		}

		n1, ok1 := toFloat(varVal)
		n2, ok2 := toFloat(ref)

		return ok1 && ok2 && n1 >= n2, true
	}

	return false, false
}

// matchBooleanCondition checks boolean comparison conditions.
func matchBooleanCondition(rule *ChoiceRule, varVal, input any) (bool, bool) {
	if rule.BooleanEquals != nil {
		b, ok := varVal.(bool)

		return ok && b == *rule.BooleanEquals, true
	}

	if rule.BooleanEqualsPath != nil {
		ref, err := applyPath(*rule.BooleanEqualsPath, input)
		if err != nil {
			return false, true
		}

		b1, ok1 := varVal.(bool)
		b2, ok2 := ref.(bool)

		return ok1 && ok2 && b1 == b2, true
	}

	return false, false
}

// matchTimestampCondition checks timestamp comparison conditions.
//
//nolint:cyclop // many timestamp comparison operators
func matchTimestampCondition(rule *ChoiceRule, varVal, input any) (bool, bool) {
	if rule.TimestampEquals != nil {
		t1, err1 := parseTimestamp(varVal)
		t2, err2 := time.Parse(time.RFC3339, *rule.TimestampEquals)

		return err1 == nil && err2 == nil && t1.Equal(t2), true
	}

	if rule.TimestampLessThan != nil {
		t1, err1 := parseTimestamp(varVal)
		t2, err2 := time.Parse(time.RFC3339, *rule.TimestampLessThan)

		return err1 == nil && err2 == nil && t1.Before(t2), true
	}

	if rule.TimestampGreaterThan != nil {
		t1, err1 := parseTimestamp(varVal)
		t2, err2 := time.Parse(time.RFC3339, *rule.TimestampGreaterThan)

		return err1 == nil && err2 == nil && t1.After(t2), true
	}

	if rule.TimestampLessThanEquals != nil {
		t1, err1 := parseTimestamp(varVal)
		t2, err2 := time.Parse(time.RFC3339, *rule.TimestampLessThanEquals)

		return err1 == nil && err2 == nil && !t1.After(t2), true
	}

	if rule.TimestampGreaterThanEquals != nil {
		t1, err1 := parseTimestamp(varVal)
		t2, err2 := time.Parse(time.RFC3339, *rule.TimestampGreaterThanEquals)

		return err1 == nil && err2 == nil && !t1.Before(t2), true
	}

	if rule.TimestampEqualsPath != nil {
		t1, t2, err := resolveTimestampPath(varVal, *rule.TimestampEqualsPath, input)

		return err == nil && t1.Equal(t2), true
	}

	if rule.TimestampLessThanPath != nil {
		t1, t2, err := resolveTimestampPath(varVal, *rule.TimestampLessThanPath, input)

		return err == nil && t1.Before(t2), true
	}

	if rule.TimestampGreaterThanPath != nil {
		t1, t2, err := resolveTimestampPath(varVal, *rule.TimestampGreaterThanPath, input)

		return err == nil && t1.After(t2), true
	}

	if rule.TimestampLessThanEqualsPath != nil {
		t1, t2, err := resolveTimestampPath(varVal, *rule.TimestampLessThanEqualsPath, input)

		return err == nil && !t1.After(t2), true
	}

	if rule.TimestampGreaterThanEqualsPath != nil {
		t1, t2, err := resolveTimestampPath(varVal, *rule.TimestampGreaterThanEqualsPath, input)

		return err == nil && !t1.Before(t2), true
	}

	return false, false
}

// parseTimestamp converts any value to a [time.Time] by expecting a RFC3339 string.
func parseTimestamp(v any) (time.Time, error) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, ErrNotAString
	}

	return time.Parse(time.RFC3339, s)
}

// resolveTimestampPath resolves a path reference and returns both timestamps.
func resolveTimestampPath(varVal any, path string, input any) (time.Time, time.Time, error) {
	t1, err := parseTimestamp(varVal)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	ref, err := applyPath(path, input)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	t2, err := parseTimestamp(ref)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	return t1, t2, nil
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
		strings.HasPrefix(resource, "arn:aws:lambda:") ||
		strings.HasPrefix(resource, "arn:aws:states:::aws-sdk:lambda:")
}

func isSQSResource(resource string) bool {
	return strings.HasPrefix(resource, "arn:aws:states:::sqs:") ||
		strings.HasPrefix(resource, "arn:aws:states:::aws-sdk:sqs:")
}

func isSNSResource(resource string) bool {
	return strings.HasPrefix(resource, "arn:aws:states:::sns:") ||
		strings.HasPrefix(resource, "arn:aws:states:::aws-sdk:sns:")
}

func isDynamoDBResource(resource string) bool {
	return strings.HasPrefix(resource, "arn:aws:states:::dynamodb:") ||
		strings.HasPrefix(resource, "arn:aws:states:::aws-sdk:dynamodb:")
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

// applyParametersTemplate evaluates a Parameters or ResultSelector template
// (json.RawMessage) against the given input context.
// Keys ending in ".$" are evaluated as JSONPath or intrinsic function references.
func applyParametersTemplate(template json.RawMessage, input any) (any, error) {
	var tmpl any
	if err := json.Unmarshal(template, &tmpl); err != nil {
		return nil, fmt.Errorf("invalid template: %w", err)
	}

	return evalTemplate(tmpl, input)
}

// evalTemplate recursively evaluates a template structure against the input context.
//
//nolint:gocognit // inherently complex due to multiple template types
func evalTemplate(tmpl, input any) (any, error) {
	switch v := tmpl.(type) {
	case map[string]any:
		result := make(map[string]any, len(v))

		for key, val := range v {
			if strings.HasSuffix(key, ".$") {
				actualKey := key[:len(key)-2]

				strVal, ok := val.(string)
				if !ok {
					return nil, fmt.Errorf("%w: %q", ErrReferenceKeyNotString, key)
				}

				evaluated, err := evaluateReference(strVal, input)
				if err != nil {
					return nil, fmt.Errorf("evaluating reference %q: %w", key, err)
				}

				result[actualKey] = evaluated
			} else {
				sub, err := evalTemplate(val, input)
				if err != nil {
					return nil, err
				}

				result[key] = sub
			}
		}

		return result, nil

	case []any:
		result := make([]any, len(v))

		for i, item := range v {
			sub, err := evalTemplate(item, input)
			if err != nil {
				return nil, err
			}

			result[i] = sub
		}

		return result, nil

	default:
		return tmpl, nil
	}
}

// evaluateReference resolves a JSONPath expression or intrinsic function call.
func evaluateReference(ref string, input any) (any, error) {
	if strings.HasPrefix(ref, "States.") {
		return evaluateIntrinsicFunction(ref, input)
	}

	return applyPath(ref, input)
}
