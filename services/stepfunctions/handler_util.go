package stepfunctions

import (
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

type validateStateMachineDefinitionOutput struct {
	Result      string `json:"result"`
	Diagnostics []any  `json:"diagnostics"`
	Truncated   bool   `json:"truncated"`
}

type validateStateMachineDefinitionInput struct {
	Definition string `json:"definition"`
	Severity   string `json:"severity"`
	Type       string `json:"type"`
	MaxResults int32  `json:"maxResults"`
}

// validateStateMachineDefinitionMaxResultsDefault is AWS's documented
// default and max diagnostics-per-call value; a caller-supplied 0 also
// falls back to it (api_op_ValidateStateMachineDefinition.go MaxResults doc).
const validateStateMachineDefinitionMaxResultsDefault = 100

const (
	validateStateMachineDefinitionSeverityError   = "ERROR"
	validateStateMachineDefinitionSeverityWarning = "WARNING"
	validateStateMachineDefinitionTypeStandard    = "STANDARD"
	validateStateMachineDefinitionTypeExpress     = "EXPRESS"
)

// utilActions returns utility operations like definition validation.
func (h *Handler) utilActions() map[string]actionFn {
	return map[string]actionFn{
		"ValidateStateMachineDefinition": func(b []byte) (any, error) {
			var input validateStateMachineDefinitionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			if input.Severity != "" &&
				input.Severity != validateStateMachineDefinitionSeverityError &&
				input.Severity != validateStateMachineDefinitionSeverityWarning {
				return nil, fmt.Errorf(
					"%w: severity %q is not ERROR or WARNING", ErrValidation, input.Severity)
			}
			if input.Type != "" &&
				input.Type != validateStateMachineDefinitionTypeStandard &&
				input.Type != validateStateMachineDefinitionTypeExpress {
				return nil, fmt.Errorf(
					"%w: type %q is not STANDARD or EXPRESS", ErrValidation, input.Type)
			}

			maxResults := input.MaxResults
			if maxResults <= 0 {
				maxResults = validateStateMachineDefinitionMaxResultsDefault
			}

			result := "OK"

			diagnostics := []any{}
			if _, err := asl.Parse(input.Definition); err != nil {
				result = "FAIL"
				diagnostics = []any{map[string]string{
					"message":  err.Error(),
					"code":     "SCHEMA_VALIDATION_FAILED",
					"severity": validateStateMachineDefinitionSeverityError,
				}}
			}

			var truncated bool
			if len(diagnostics) > int(maxResults) {
				diagnostics = diagnostics[:maxResults]
				truncated = true
			}

			return &validateStateMachineDefinitionOutput{
				Result:      result,
				Diagnostics: diagnostics,
				Truncated:   truncated,
			}, nil
		},
	}
}

// bareStateName names the state when TestState is given a bare state definition.
const bareStateName = "TestStateName"

type testStateInput struct {
	Mock               *testStateMock          `json:"mock,omitempty"`
	StateConfiguration *testStateConfiguration `json:"stateConfiguration,omitempty"`
	Context            *string                 `json:"context,omitempty"`
	Definition         string                  `json:"definition"`
	Input              string                  `json:"input"`
	RoleArn            string                  `json:"roleArn,omitempty"`
	InspectionLevel    string                  `json:"inspectionLevel,omitempty"`
}

type testStateMock struct {
	Result      *string `json:"result,omitempty"`
	ErrorOutput *struct {
		Error *string `json:"error,omitempty"`
		Cause *string `json:"cause,omitempty"`
	} `json:"errorOutput,omitempty"`
}

type testStateConfiguration struct {
	RetrierRetryCount *int `json:"retrierRetryCount,omitempty"`
}

// testStateMockRun turns a TestState mock into a one-shot MockRun for stateName.
func testStateMockRun(m *testStateMock, stateName string) (*asl.MockRun, error) {
	if (m.Result == nil) == (m.ErrorOutput == nil) {
		return nil, fmt.Errorf("%w: mock needs exactly one of result or errorOutput", ErrValidation)
	}

	var step map[string]any

	if m.Result != nil {
		if !json.Valid([]byte(*m.Result)) {
			return nil, fmt.Errorf("%w: mock result is not valid JSON", ErrValidation)
		}

		step = map[string]any{"Return": json.RawMessage(*m.Result)}
	} else {
		if m.ErrorOutput.Error == nil || *m.ErrorOutput.Error == "" {
			return nil, fmt.Errorf("%w: mock errorOutput.error is required", ErrValidation)
		}

		step = map[string]any{"Throw": map[string]string{
			"Error": *m.ErrorOutput.Error, "Cause": ptrconv.String(m.ErrorOutput.Cause),
		}}
	}

	cfg, err := json.Marshal(map[string]any{
		"MockedResponses": map[string]any{"m": map[string]any{"0": step}},
		"StateMachines": map[string]any{
			"s": map[string]any{"TestCases": map[string]any{"t": map[string]string{stateName: "m"}}},
		},
	})
	if err != nil {
		return nil, err
	}

	parsed, err := asl.ParseMockConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	return parsed.TestCase("s", "t")
}

type testStateOutput struct {
	InspectionData map[string]string `json:"inspectionData,omitempty"`
	Output         string            `json:"output,omitempty"`
	Error          string            `json:"error,omitempty"`
	Cause          string            `json:"cause,omitempty"`
	Status         string            `json:"status"`
	NextState      string            `json:"nextState,omitempty"`
}

// inspectionDataFor renders the executor's recorded stage data as the
// JSON-string members of types.InspectionData. INFO (the default) returns none.
func inspectionDataFor(level string, executor *asl.Executor) map[string]string {
	if level == "" || level == "INFO" {
		return nil
	}

	out := map[string]string{}

	for key, value := range executor.Inspection() {
		if raw, err := json.Marshal(value); err == nil {
			out[key] = string(raw)
		}
	}

	return out
}

func validInspectionLevel(level string) bool {
	return level == "" || level == "INFO" || level == "DEBUG" || level == "TRACE"
}

// detachNext swaps Next for End:true so TestState can run a non-terminal
// state without a synthetic next state, returning the new definition and Next.
func detachNext(definition, stateName string, raw json.RawMessage) (string, string) {
	var (
		next     string
		rawState map[string]json.RawMessage
	)

	if json.Unmarshal(raw, &rawState) != nil {
		return definition, next
	}

	if nextRaw, hasNext := rawState["Next"]; hasNext {
		_ = json.Unmarshal(nextRaw, &next)
		delete(rawState, "Next")
		rawState["End"] = json.RawMessage(`true`)
	}

	if modified, err := json.Marshal(map[string]any{stateName: rawState}); err == nil {
		definition = string(modified)
	}

	return definition, next
}

// handleTestState executes a single state definition in isolation and returns its output.
func (h *Handler) handleTestState(body []byte) (any, error) {
	var input testStateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	if !validInspectionLevel(input.InspectionLevel) {
		return nil, fmt.Errorf("%w: inspectionLevel must be INFO, DEBUG or TRACE", ErrValidation)
	}

	// Wrap the state definition in a minimal state machine.
	var states map[string]json.RawMessage
	if err := json.Unmarshal([]byte(input.Definition), &states); err != nil {
		return nil, fmt.Errorf("%w: invalid state definition JSON: %w", ErrInvalidDefinition, err)
	}

	if _, bare := states["Type"]; bare {
		states = map[string]json.RawMessage{bareStateName: json.RawMessage(input.Definition)}
		input.Definition = fmt.Sprintf(`{%q:%s}`, bareStateName, input.Definition)
	}

	if len(states) != 1 {
		return nil, fmt.Errorf(
			"%w: TestState definition must contain exactly one state",
			ErrInvalidDefinition,
		)
	}

	var stateName string

	for k := range states {
		stateName = k
	}

	var nextStateName string

	input.Definition, nextStateName = detachNext(input.Definition, stateName, states[stateName])

	smDef := fmt.Sprintf(`{"StartAt":%q,"States":%s}`, stateName, input.Definition)

	sm, err := asl.Parse(smDef)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}

	var lambdaInvoker asl.LambdaInvoker

	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		bk.mu.RLock("TestState")
		lambdaInvoker = bk.lambdaInvoker
		bk.mu.RUnlock()
	}

	executor := asl.NewExecutor(sm, lambdaInvoker, nil)
	executor.EnableInspection()
	executor.EnableSingleState()

	if cfgErr := configureTestStateExecutor(executor, &input, stateName, states[stateName]); cfgErr != nil {
		return nil, cfgErr
	}

	stateInput := input.Input
	if stateInput == "" {
		stateInput = "{}"
	}

	result, execErr := executor.Execute(h.svcCtx, "test-state", stateInput)
	if execErr != nil {
		out := &testStateOutput{
			Status:         "FAILED",
			Error:          execErr.Error(),
			InspectionData: inspectionDataFor(input.InspectionLevel, executor),
		}

		return out, nil //nolint:nilerr // execution errors are encoded in the response body
	}

	if result.Failed {
		status := "FAILED"
		if result.Retriable {
			status = "RETRIABLE"
		}

		return &testStateOutput{
			Status: status, Error: result.Error, Cause: result.Cause,
			InspectionData: inspectionDataFor(input.InspectionLevel, executor),
		}, nil
	}

	outputBytes, _ := json.Marshal(result.Output)

	out := &testStateOutput{
		Status:         "SUCCEEDED",
		Output:         string(outputBytes),
		NextState:      nextStateName,
		InspectionData: inspectionDataFor(input.InspectionLevel, executor),
	}

	if result.Caught {
		out.Status, out.Error, out.Cause, out.NextState = "CAUGHT_ERROR", result.Error, result.Cause, result.NextState
	}

	return out, nil
}

// configureTestStateExecutor applies the request's mock, context and
// stateConfiguration to executor, rejecting combinations AWS rejects.
func configureTestStateExecutor(
	executor *asl.Executor, input *testStateInput, stateName string, rawState json.RawMessage,
) error {
	if input.Context != nil && input.Mock == nil {
		return fmt.Errorf("%w: context may only be specified together with a mock", ErrValidation)
	}

	if input.Context != nil {
		var ctxObj map[string]any
		if err := json.Unmarshal([]byte(*input.Context), &ctxObj); err != nil {
			return fmt.Errorf("%w: context must be a JSON object", ErrValidation)
		}

		executor.SetContextOverride(ctxObj)
	}

	if input.StateConfiguration != nil && input.StateConfiguration.RetrierRetryCount != nil {
		executor.SetRetrierRetryCount(*input.StateConfiguration.RetrierRetryCount)
	}

	if input.Mock == nil {
		return nil
	}

	var typed struct {
		Type string `json:"Type"`
	}

	_ = json.Unmarshal(rawState, &typed)

	if typed.Type != stateTypeTask {
		return fmt.Errorf("%w: mock is supported for Task states only", ErrValidation)
	}

	run, err := testStateMockRun(input.Mock, stateName)
	if err != nil {
		return err
	}

	executor.SetMockRun(run)

	return nil
}
