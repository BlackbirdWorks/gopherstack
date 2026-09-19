package stepfunctions

import (
	"encoding/json"
	"fmt"

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

type testStateInput struct {
	Definition string `json:"definition"`
	Input      string `json:"input"`
	RoleArn    string `json:"roleArn,omitempty"`
}

type testStateOutput struct {
	Output    string `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
	Cause     string `json:"cause,omitempty"`
	Status    string `json:"status"`
	NextState string `json:"nextState,omitempty"`
}

// handleTestState executes a single state definition in isolation and returns its output.
// The definition is a JSON object mapping a single state name to its state definition.
func (h *Handler) handleTestState(body []byte) (any, error) {
	var input testStateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	// Wrap the state definition in a minimal state machine.
	var states map[string]json.RawMessage
	if err := json.Unmarshal([]byte(input.Definition), &states); err != nil {
		return nil, fmt.Errorf("%w: invalid state definition JSON: %w", ErrInvalidDefinition, err)
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

	// Extract Next field and replace with End:true so TestState can run
	// non-terminal states without a synthetic next state in the SM.
	var nextStateName string

	var rawState map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(states[stateName], &rawState); unmarshalErr == nil {
		if nextRaw, hasNext := rawState["Next"]; hasNext {
			_ = json.Unmarshal(nextRaw, &nextStateName)
			delete(rawState, "Next")
			rawState["End"] = json.RawMessage(`true`)
		}

		if modifiedDef, marshalErr := json.Marshal(map[string]any{stateName: rawState}); marshalErr == nil {
			input.Definition = string(modifiedDef)
		}
	}

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

	stateInput := input.Input
	if stateInput == "" {
		stateInput = "{}"
	}

	result, execErr := executor.Execute(h.svcCtx, "test-state", stateInput)
	if execErr != nil {
		out := &testStateOutput{Status: "FAILED", Error: execErr.Error()}

		return out, nil //nolint:nilerr // execution errors are encoded in the response body
	}

	if result.Failed {
		return &testStateOutput{Status: "FAILED", Error: result.Error, Cause: result.Cause}, nil
	}

	outputBytes, _ := json.Marshal(result.Output)

	return &testStateOutput{
		Status:    "SUCCEEDED",
		Output:    string(outputBytes),
		NextState: nextStateName,
	}, nil
}
