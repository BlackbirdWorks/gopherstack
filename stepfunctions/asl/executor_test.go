package asl_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/stepfunctions/asl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test sentinel errors — used as mock return values in Lambda error tests.
var (
	errLambdaService   = errors.New("Lambda.ServiceException")
	errMyError         = errors.New("MyError")
	errMySpecific      = errors.New("MySpecificError")
	errUnhandled       = errors.New("UnhandledError")
	errTransientError  = errors.New("TransientError")
	errPersistentError = errors.New("PersistentError")
	errSomeError       = errors.New("SomeError")
)

func TestParse_Valid(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Hello",
		"States": {
			"Hello": {
				"Type": "Pass",
				"End": true
			}
		}
	}`
	sm, err := asl.Parse(def)
	require.NoError(t, err)
	assert.Equal(t, "Hello", sm.StartAt)
	assert.Len(t, sm.States, 1)
}

func TestParse_MissingStartAt(t *testing.T) {
	t.Parallel()

	def := `{"States": {"S": {"Type": "Pass", "End": true}}}`
	_, err := asl.Parse(def)
	require.Error(t, err)
	assert.ErrorIs(t, err, asl.ErrParseError)
}

func TestParse_MissingStates(t *testing.T) {
	t.Parallel()

	def := `{"StartAt": "S"}`
	_, err := asl.Parse(def)
	require.Error(t, err)
}

func TestParse_StartAtNotInStates(t *testing.T) {
	t.Parallel()

	def := `{"StartAt": "Missing", "States": {"S": {"Type": "Pass", "End": true}}}`
	_, err := asl.Parse(def)
	require.Error(t, err)
}

func TestParse_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, err := asl.Parse("{invalid json}")
	require.Error(t, err)
}

// --- Executor tests ---

func execute(t *testing.T, def, input string) *asl.ExecutionResult {
	t.Helper()
	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, nil, nil)
	result, err := exec.Execute(t.Context(), "test-exec", input)
	require.NoError(t, err)

	return result
}

func TestExecutor_PassState_PassThrough(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "P",
		"States": {
			"P": {"Type": "Pass", "End": true}
		}
	}`
	result := execute(t, def, `{"x": 1}`)
	assert.Empty(t, result.Error)

	m, ok := result.Output.(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(1), m["x"], 1e-9)
}

func TestExecutor_PassState_WithResult(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "P",
		"States": {
			"P": {"Type": "Pass", "Result": {"msg": "hello"}, "End": true}
		}
	}`
	result := execute(t, def, `{}`)
	assert.Empty(t, result.Error)

	m, ok := result.Output.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hello", m["msg"])
}

func TestExecutor_SucceedState(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Done",
		"States": {
			"Done": {"Type": "Succeed"}
		}
	}`
	result := execute(t, def, `{"result": "ok"}`)
	assert.Empty(t, result.Error)
}

func TestExecutor_FailState(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Oops",
		"States": {
			"Oops": {"Type": "Fail", "Error": "MyError", "Cause": "something bad"}
		}
	}`
	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, nil, nil)
	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err) // Fail state is NOT a Go error; it's captured in result
	assert.Equal(t, "MyError", result.Error)
	assert.Equal(t, "something bad", result.Cause)
}

func TestExecutor_WaitState_ZeroSeconds(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Wait",
		"States": {
			"Wait": {"Type": "Wait", "Seconds": 0, "Next": "Done"},
			"Done": {"Type": "Succeed"}
		}
	}`
	result := execute(t, def, `{}`)
	assert.Empty(t, result.Error)
}

func TestExecutor_ChoiceState_StringEquals(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Check",
		"States": {
			"Check": {
				"Type": "Choice",
				"Choices": [
					{
						"Variable": "$.status",
						"StringEquals": "active",
						"Next": "Active"
					}
				],
				"Default": "Inactive"
			},
			"Active": {"Type": "Pass", "End": true, "Result": {"branch": "active"}},
			"Inactive": {"Type": "Pass", "End": true, "Result": {"branch": "inactive"}}
		}
	}`

	result := execute(t, def, `{"status": "active"}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, "active", m["branch"])

	result2 := execute(t, def, `{"status": "other"}`)
	m2 := result2.Output.(map[string]any)
	assert.Equal(t, "inactive", m2["branch"])
}

func TestExecutor_ChoiceState_NumericGreaterThan(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Check",
		"States": {
			"Check": {
				"Type": "Choice",
				"Choices": [
					{
						"Variable": "$.count",
						"NumericGreaterThan": 5,
						"Next": "High"
					}
				],
				"Default": "Low"
			},
			"High": {"Type": "Pass", "End": true, "Result": {"level": "high"}},
			"Low": {"Type": "Pass", "End": true, "Result": {"level": "low"}}
		}
	}`

	result := execute(t, def, `{"count": 10}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, "high", m["level"])

	result2 := execute(t, def, `{"count": 2}`)
	m2 := result2.Output.(map[string]any)
	assert.Equal(t, "low", m2["level"])
}

func TestExecutor_ChoiceState_AndCondition(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Check",
		"States": {
			"Check": {
				"Type": "Choice",
				"Choices": [
					{
						"And": [
							{"Variable": "$.x", "NumericGreaterThan": 0},
							{"Variable": "$.y", "StringEquals": "yes"}
						],
						"Next": "Both"
					}
				],
				"Default": "Neither"
			},
			"Both": {"Type": "Pass", "End": true, "Result": "both"},
			"Neither": {"Type": "Pass", "End": true, "Result": "neither"}
		}
	}`

	result := execute(t, def, `{"x": 5, "y": "yes"}`)
	assert.Equal(t, "both", result.Output)

	result2 := execute(t, def, `{"x": 5, "y": "no"}`)
	assert.Equal(t, "neither", result2.Output)
}

func TestExecutor_ParallelState(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Par",
		"States": {
			"Par": {
				"Type": "Parallel",
				"End": true,
				"Branches": [
					{
						"StartAt": "A",
						"States": {"A": {"Type": "Pass", "End": true, "Result": "branch-a"}}
					},
					{
						"StartAt": "B",
						"States": {"B": {"Type": "Pass", "End": true, "Result": "branch-b"}}
					}
				]
			}
		}
	}`

	result := execute(t, def, `{}`)
	assert.Empty(t, result.Error)
	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 2)
}

func TestExecutor_MapState(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "Map",
		"States": {
			"Map": {
				"Type": "Map",
				"End": true,
				"Iterator": {
					"StartAt": "Double",
					"States": {
						"Double": {"Type": "Pass", "End": true}
					}
				}
			}
		}
	}`

	result := execute(t, def, `[1, 2, 3]`)
	assert.Empty(t, result.Error)
	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 3)
}

func TestExecutor_ResultPath(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "P",
		"States": {
			"P": {
				"Type": "Pass",
				"Result": {"computed": true},
				"ResultPath": "$.result",
				"End": true
			}
		}
	}`

	result := execute(t, def, `{"original": "data"}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, "data", m["original"])
	sub := m["result"].(map[string]any)
	assert.Equal(t, true, sub["computed"])
}

func TestExecutor_InputPath(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "P",
		"States": {
			"P": {
				"Type": "Pass",
				"InputPath": "$.inner",
				"End": true
			}
		}
	}`

	result := execute(t, def, `{"inner": {"key": "value"}}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, "value", m["key"])
}

func TestExecutor_ChainedStates(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "A",
		"States": {
			"A": {"Type": "Pass", "Next": "B"},
			"B": {"Type": "Pass", "Next": "C"},
			"C": {"Type": "Succeed"}
		}
	}`

	result := execute(t, def, `{"value": 42}`)
	assert.Empty(t, result.Error)
}

func TestExecutor_TaskState_NoLambda_PassThrough(t *testing.T) {
	t.Parallel()

	// DynamoDB is now a recognized integration; without a configured backend it fails.
	def := `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:aws:states:::dynamodb:getItem",
				"End": true
			}
		}
	}`

	result := execute(t, def, `{"pk": "123"}`)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Contains(t, result.Cause, "DynamoDB integration not configured")
}

func TestExecutor_TaskState_Lambda(t *testing.T) {
	t.Parallel()

	mockLambda := &mockLambda{
		response: `{"processed": true}`,
	}

	def := `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
				"End": true
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, mockLambda, nil)
	result, err := exec.Execute(t.Context(), "test-exec", `{"input": "data"}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)

	m, ok := result.Output.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, m["processed"])
}

func TestExecutor_TaskState_LambdaError_Catch(t *testing.T) {
	t.Parallel()

	mockLambda := &mockLambda{
		returnErr: errLambdaService,
	}

	def := `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
				"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Handled"}],
				"End": true
			},
			"Handled": {
				"Type": "Pass",
				"End": true,
				"Result": "caught"
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, mockLambda, nil)
	result, err := exec.Execute(t.Context(), "test-exec", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)
	assert.Equal(t, "caught", result.Output)
}

// mockLambda implements asl.LambdaInvoker for testing.
type mockLambda struct {
	returnErr  error
	response   string
	statusCode int
}

func (m *mockLambda) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	if m.returnErr != nil {
		return nil, 500, m.returnErr
	}

	sc := m.statusCode
	if sc == 0 {
		sc = 200
	}

	if m.response != "" {
		return []byte(m.response), sc, nil
	}

	return []byte(`{}`), sc, nil
}

// TestExecutor_ResultPath_Null verifies that "null" ResultPath discards result.
func TestExecutor_ResultPath_Null(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "P",
		"States": {
			"P": {
				"Type": "Pass",
				"Result": {"discarded": true},
				"ResultPath": "null",
				"End": true
			}
		}
	}`

	result := execute(t, def, `{"original": "kept"}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, "kept", m["original"])
	_, hasDiscarded := m["discarded"]
	assert.False(t, hasDiscarded)
}

func TestExecutor_FailError_Method(t *testing.T) {
	t.Parallel()

	err := &asl.FailError{ErrCode: "E1", Cause: "cause1"}
	assert.Equal(t, "E1: cause1", err.Error())

	err2 := &asl.FailError{ErrCode: "E2"}
	assert.Equal(t, "E2", err2.Error())
}

func TestExecutor_ChoiceState_NoMatchNoDefault(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [
{"Variable": "$.x", "StringEquals": "yes", "Next": "Done"}
]
},
"Done": {"Type": "Succeed"}
}
}`
	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, nil, nil)
	result, err := exec.Execute(t.Context(), "test", `{"x": "no"}`)
	require.NoError(t, err)
	// No match and no default → FailError with States.NoChoiceMatched
	assert.NotEmpty(t, result.Error)
}

func TestExecutor_Choice_Or(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [
{
"Or": [
{"Variable": "$.x", "StringEquals": "a"},
{"Variable": "$.x", "StringEquals": "b"}
],
"Next": "Match"
}
],
"Default": "NoMatch"
},
"Match": {"Type": "Pass", "End": true, "Result": "match"},
"NoMatch": {"Type": "Pass", "End": true, "Result": "no-match"}
}
}`

	result := execute(t, def, `{"x": "a"}`)
	assert.Equal(t, "match", result.Output)

	result2 := execute(t, def, `{"x": "c"}`)
	assert.Equal(t, "no-match", result2.Output)
}

func TestExecutor_Choice_Not(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [
{
"Not": {"Variable": "$.status", "StringEquals": "inactive"},
"Next": "Active"
}
],
"Default": "Inactive"
},
"Active": {"Type": "Pass", "End": true, "Result": "active"},
"Inactive": {"Type": "Pass", "End": true, "Result": "inactive"}
}
}`

	result := execute(t, def, `{"status": "running"}`)
	assert.Equal(t, "active", result.Output)

	result2 := execute(t, def, `{"status": "inactive"}`)
	assert.Equal(t, "inactive", result2.Output)
}

func TestExecutor_Choice_BooleanEquals(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [
{"Variable": "$.flag", "BooleanEquals": true, "Next": "Yes"}
],
"Default": "No"
},
"Yes": {"Type": "Pass", "End": true, "Result": "yes"},
"No": {"Type": "Pass", "End": true, "Result": "no"}
}
}`

	result := execute(t, def, `{"flag": true}`)
	assert.Equal(t, "yes", result.Output)

	result2 := execute(t, def, `{"flag": false}`)
	assert.Equal(t, "no", result2.Output)
}

func TestExecutor_Choice_NumericLessThan(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [
{"Variable": "$.n", "NumericLessThan": 10, "Next": "Low"}
],
"Default": "High"
},
"Low": {"Type": "Pass", "End": true, "Result": "low"},
"High": {"Type": "Pass", "End": true, "Result": "high"}
}
}`

	result := execute(t, def, `{"n": 5}`)
	assert.Equal(t, "low", result.Output)

	result2 := execute(t, def, `{"n": 15}`)
	assert.Equal(t, "high", result2.Output)
}

func TestExecutor_Choice_NumericEquals(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [
{"Variable": "$.n", "NumericEquals": 42, "Next": "Match"}
],
"Default": "NoMatch"
},
"Match": {"Type": "Pass", "End": true, "Result": "match"},
"NoMatch": {"Type": "Pass", "End": true, "Result": "no-match"}
}
}`

	result := execute(t, def, `{"n": 42}`)
	assert.Equal(t, "match", result.Output)
}

func TestExecutor_Choice_StringGreaterThan(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [
{"Variable": "$.s", "StringGreaterThan": "m", "Next": "High"}
],
"Default": "Low"
},
"High": {"Type": "Pass", "End": true, "Result": "high"},
"Low": {"Type": "Pass", "End": true, "Result": "low"}
}
}`

	result := execute(t, def, `{"s": "z"}`)
	assert.Equal(t, "high", result.Output)

	result2 := execute(t, def, `{"s": "a"}`)
	assert.Equal(t, "low", result2.Output)
}

func TestExecutor_Choice_IsPresent(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [
{"Variable": "$.optional", "IsPresent": true, "Next": "HasIt"}
],
"Default": "NoIt"
},
"HasIt": {"Type": "Pass", "End": true, "Result": "has"},
"NoIt": {"Type": "Pass", "End": true, "Result": "missing"}
}
}`

	result := execute(t, def, `{"optional": "value"}`)
	assert.Equal(t, "has", result.Output)

	result2 := execute(t, def, `{}`)
	assert.Equal(t, "missing", result2.Output)
}

func TestExecutor_MapState_WithItemsPath(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "Map",
"States": {
"Map": {
"Type": "Map",
"End": true,
"ItemsPath": "$.items",
"Iterator": {
"StartAt": "P",
"States": {
"P": {"Type": "Pass", "End": true}
}
}
}
}
}`

	result := execute(t, def, `{"items": ["a", "b", "c"]}`)
	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 3)
}

func TestExecutor_MapState_MaxConcurrency(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "Map",
"States": {
"Map": {
"Type": "Map",
"End": true,
"MaxConcurrency": 2,
"Iterator": {
"StartAt": "P",
"States": {
"P": {"Type": "Pass", "End": true}
}
}
}
}
}`

	result := execute(t, def, `[1, 2, 3, 4, 5]`)
	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 5)
}

func TestExecutor_LambdaTask_StatusError(t *testing.T) {
	t.Parallel()

	lam := &mockLambda{
		response:   `{"error": "bad request"}`,
		statusCode: 400,
	}

	def := `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:bad-fn",
				"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
				"End": true
			},
			"Fallback": {"Type": "Succeed"}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lam, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	// Error should be caught and execution succeeds via Fallback.
	assert.Empty(t, result.Error)
}

func TestExecutor_OutputPath(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"End": true,
"OutputPath": "$.key"
}
}
}`

	result := execute(t, def, `{"key": "extracted"}`)
	assert.Equal(t, "extracted", result.Output)
}

func TestExecutor_ContextCancellation(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "W",
"States": {
"W": {
"Type": "Wait",
"Seconds": 10,
"End": true
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately.

	exec := asl.NewExecutor(sm, nil, nil)
	_, err = exec.Execute(ctx, "test", `{}`)
	require.Error(t, err)
}

func TestExecutor_TaskState_CatchWithResultPath(t *testing.T) {
	t.Parallel()

	lam := &mockLambda{returnErr: errMyError}

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback", "ResultPath": "$.error"}],
"End": true
},
"Fallback": {"Type": "Succeed"}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lam, nil)
	result, err := exec.Execute(t.Context(), "test", `{"original": "data"}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)
}

func TestExecutor_catchesError_SpecificError(t *testing.T) {
	t.Parallel()

	// Test that a specific error name in Catch catches that specific error.
	lam := &mockLambda{returnErr: errMySpecific}

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"Catch": [{"ErrorEquals": ["MySpecificError"], "Next": "Caught"}],
"End": true
},
"Caught": {"Type": "Pass", "End": true, "Result": "caught"}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lam, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "caught", result.Output)
}

func TestExecutor_TaskState_FailWithNoCatch(t *testing.T) {
	t.Parallel()

	lam := &mockLambda{returnErr: errUnhandled}

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"End": true
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lam, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
}

func TestExecutor_MapState_EmptyArray(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "Map",
"States": {
"Map": {
"Type": "Map",
"End": true,
"Iterator": {
"StartAt": "P",
"States": {"P": {"Type": "Succeed"}}
}
}
}
}`

	result := execute(t, def, `[]`)
	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Empty(t, arr)
}

func TestExecutor_LambdaTask_NonJSONResponse(t *testing.T) {
	t.Parallel()

	lam := &mockLambda{response: "plain text response"}

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"End": true
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lam, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)
}

func TestExecutor_InputPath_Invalid(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"InputPath": "invalid-path",
"End": true
}
}
}`
	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, nil, nil)
	_, err = exec.Execute(t.Context(), "test", `{"x": 1}`)
	require.Error(t, err)
}

func TestExecutor_MapState_NotArray(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "Map",
"States": {
"Map": {
"Type": "Map",
"End": true,
"Iterator": {
"StartAt": "P",
"States": {"P": {"Type": "Succeed"}}
}
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, nil, nil)
	_, err = exec.Execute(t.Context(), "test", `{"not": "array"}`)
	require.Error(t, err)
}

func TestExecutor_MapState_ItemsPath_NotArray(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "Map",
"States": {
"Map": {
"Type": "Map",
"End": true,
"ItemsPath": "$.count",
"Iterator": {
"StartAt": "P",
"States": {"P": {"Type": "Succeed"}}
}
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, nil, nil)
	_, err = exec.Execute(t.Context(), "test", `{"count": 5}`)
	require.Error(t, err)
}

func TestExecutor_ResultPath_NestedCreate(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Result": "computed",
"ResultPath": "$.nested.value",
"End": true
}
}
}`

	result := execute(t, def, `{"data": "original"}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, "original", m["data"])
}

func TestExecutor_UnknownStateType(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "X",
"States": {
"X": {"Type": "Unknown", "End": true}
}
}`

	// Parse will succeed but execute will fail on unknown type.
	var sm asl.StateMachine
	require.NoError(t, json.Unmarshal([]byte(def), &sm))
	exec := asl.NewExecutor(&sm, nil, nil)
	_, err := exec.Execute(t.Context(), "test", `{}`)
	require.Error(t, err)
}

func TestExecutor_TaskNoLambdaInvoker(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"End": true
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	// No lambda invoker - should fail
	exec := asl.NewExecutor(sm, nil, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Error)
}

// --- Parameters and ResultSelector tests ---

func TestExecutor_Parameters_StaticAndDynamic(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"static": "hello", "dynamic.$": "$.name"},
"End": true
}
}
}`

	result := execute(t, def, `{"name": "world"}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, "hello", m["static"])
	assert.Equal(t, "world", m["dynamic"])
}

func TestExecutor_ResultSelector_FiltersOutput(t *testing.T) {
	t.Parallel()

	mockLambda := &mockLambda{response: `{"status": "ok", "noise": "ignored"}`}

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"ResultSelector": {"result.$": "$.status"},
"End": true
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, mockLambda, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)

	m, ok := result.Output.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ok", m["result"])
	_, hasNoise := m["noise"]
	assert.False(t, hasNoise)
}

// --- Retry tests ---

func TestExecutor_Retry_SucceedsAfterRetry(t *testing.T) {
	t.Parallel()

	callCount := 0
	lam := &mockLambdaFn{fn: func() ([]byte, int, error) {
		callCount++
		if callCount < 3 {
			return nil, 500, errTransientError
		}

		return []byte(`{"done": true}`), 200, nil
	}}

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"Retry": [{"ErrorEquals": ["TransientError"], "IntervalSeconds": 0, "MaxAttempts": 5}],
"End": true
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lam, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	assert.Empty(t, result.Error)
	assert.Equal(t, 3, callCount)
}

func TestExecutor_Retry_ExhaustedFallsThroughToCatch(t *testing.T) {
	t.Parallel()

	lam := &mockLambda{returnErr: errPersistentError}

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"Retry": [{"ErrorEquals": ["PersistentError"], "IntervalSeconds": 0, "MaxAttempts": 2}],
"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Handled"}],
"End": true
},
"Handled": {"Type": "Pass", "End": true, "Result": "caught"}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lam, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "caught", result.Output)
}

func TestExecutor_Retry_MaxAttemptsZeroNoRetry(t *testing.T) {
	t.Parallel()

	callCount := 0
	lam := &mockLambdaFn{fn: func() ([]byte, int, error) {
		callCount++

		return nil, 500, errSomeError
	}}

	def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"Retry": [{"ErrorEquals": ["SomeError"], "MaxAttempts": 0}],
"End": true
}
}
}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lam, nil)
	result, err := exec.Execute(t.Context(), "test", `{}`)
	require.NoError(t, err)
	assert.Equal(t, "TaskFailed", result.Error)
	assert.Equal(t, 1, callCount, "should only try once with MaxAttempts=0")
}

// mockLambdaFn is a flexible mock that calls a function for each invocation.
type mockLambdaFn struct {
	fn func() ([]byte, int, error)
}

func (m *mockLambdaFn) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	return m.fn()
}

// mockLambdaFnCtx is a context-aware mock that passes the context to the function.
type mockLambdaFnCtx struct {
	fn func(ctx context.Context) ([]byte, int, error)
}

func (m *mockLambdaFnCtx) InvokeFunction(ctx context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	return m.fn(ctx)
}

// --- TimeoutSeconds test ---

func TestExecutor_Task_TimeoutSeconds(t *testing.T) {
	t.Parallel()

	t.Run("timeout is caught by States.Timeout catcher", func(t *testing.T) {
		t.Parallel()

		lam := &mockLambdaFnCtx{fn: func(ctx context.Context) ([]byte, int, error) {
			<-ctx.Done()

			return nil, 0, ctx.Err()
		}}

		def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"TimeoutSeconds": 1,
"Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "TimedOut"}],
"End": true
},
"TimedOut": {"Type": "Pass", "End": true, "Result": "timeout"}
}
}`

		sm, err := asl.Parse(def)
		require.NoError(t, err)
		exec := asl.NewExecutor(sm, lam, nil)
		result, err := exec.Execute(t.Context(), "test", `{}`)
		require.NoError(t, err)
		assert.Empty(t, result.Error)
		assert.Equal(t, "timeout", result.Output)
	})

	t.Run("timeout is not retried even with States.ALL retry", func(t *testing.T) {
		t.Parallel()

		callCount := 0
		lam := &mockLambdaFnCtx{fn: func(ctx context.Context) ([]byte, int, error) {
			callCount++
			<-ctx.Done()

			return nil, 0, ctx.Err()
		}}

		def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"TimeoutSeconds": 1,
"Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 3, "IntervalSeconds": 0}],
"Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "TimedOut"}],
"End": true
},
"TimedOut": {"Type": "Pass", "End": true, "Result": "timeout"}
}
}`

		sm, err := asl.Parse(def)
		require.NoError(t, err)
		exec := asl.NewExecutor(sm, lam, nil)
		result, err := exec.Execute(t.Context(), "test", `{}`)
		require.NoError(t, err)
		assert.Empty(t, result.Error)
		assert.Equal(t, "timeout", result.Output)
		assert.Equal(t, 1, callCount, "task should not be retried after timeout")
	})

	t.Run("zero TimeoutSeconds means no timeout enforcement", func(t *testing.T) {
		t.Parallel()

		lam := &mockLambdaFn{fn: func() ([]byte, int, error) {
			time.Sleep(50 * time.Millisecond)

			return []byte(`{}`), 200, nil
		}}

		def := `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"TimeoutSeconds": 0,
"End": true
}
}
}`

		sm, err := asl.Parse(def)
		require.NoError(t, err)
		exec := asl.NewExecutor(sm, lam, nil)
		result, err := exec.Execute(t.Context(), "test", `{}`)
		require.NoError(t, err)
		assert.Empty(t, result.Error)
	})
}

// --- Wait state improvements ---

func TestExecutor_Wait_SecondsPath(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "W",
"States": {
"W": {"Type": "Wait", "SecondsPath": "$.delay", "Next": "Done"},
"Done": {"Type": "Succeed"}
}
}`

	result := execute(t, def, `{"delay": 0}`)
	assert.Empty(t, result.Error)
}

func TestExecutor_Wait_Timestamp_Past(t *testing.T) {
	t.Parallel()

	// A past timestamp should not actually wait.
	def := `{
"StartAt": "W",
"States": {
"W": {"Type": "Wait", "Timestamp": "2000-01-01T00:00:00Z", "Next": "Done"},
"Done": {"Type": "Succeed"}
}
}`

	result := execute(t, def, `{}`)
	assert.Empty(t, result.Error)
}

func TestExecutor_Wait_TimestampPath(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "W",
"States": {
"W": {"Type": "Wait", "TimestampPath": "$.ts", "Next": "Done"},
"Done": {"Type": "Succeed"}
}
}`

	result := execute(t, def, `{"ts": "2000-01-01T00:00:00Z"}`)
	assert.Empty(t, result.Error)
}

// --- Choice state new operators ---

func TestExecutor_Choice_StringLessThanEquals(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.s", "StringLessThanEquals": "m", "Next": "Low"}],
"Default": "High"
},
"Low": {"Type": "Pass", "End": true, "Result": "low"},
"High": {"Type": "Pass", "End": true, "Result": "high"}
}
}`

	assert.Equal(t, "low", execute(t, def, `{"s": "a"}`).Output)
	assert.Equal(t, "low", execute(t, def, `{"s": "m"}`).Output)
	assert.Equal(t, "high", execute(t, def, `{"s": "z"}`).Output)
}

func TestExecutor_Choice_NumericGreaterThanEquals(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.n", "NumericGreaterThanEquals": 10, "Next": "High"}],
"Default": "Low"
},
"High": {"Type": "Pass", "End": true, "Result": "high"},
"Low": {"Type": "Pass", "End": true, "Result": "low"}
}
}`

	assert.Equal(t, "high", execute(t, def, `{"n": 10}`).Output)
	assert.Equal(t, "high", execute(t, def, `{"n": 15}`).Output)
	assert.Equal(t, "low", execute(t, def, `{"n": 9}`).Output)
}

func TestExecutor_Choice_NumericLessThanEquals(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.n", "NumericLessThanEquals": 5, "Next": "Low"}],
"Default": "High"
},
"Low": {"Type": "Pass", "End": true, "Result": "low"},
"High": {"Type": "Pass", "End": true, "Result": "high"}
}
}`

	assert.Equal(t, "low", execute(t, def, `{"n": 5}`).Output)
	assert.Equal(t, "low", execute(t, def, `{"n": 3}`).Output)
	assert.Equal(t, "high", execute(t, def, `{"n": 6}`).Output)
}

func TestExecutor_Choice_StringEqualsPath(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.a", "StringEqualsPath": "$.b", "Next": "Match"}],
"Default": "NoMatch"
},
"Match": {"Type": "Pass", "End": true, "Result": "match"},
"NoMatch": {"Type": "Pass", "End": true, "Result": "no-match"}
}
}`

	assert.Equal(t, "match", execute(t, def, `{"a": "hello", "b": "hello"}`).Output)
	assert.Equal(t, "no-match", execute(t, def, `{"a": "hello", "b": "world"}`).Output)
}

func TestExecutor_Choice_NumericEqualsPath(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.x", "NumericEqualsPath": "$.y", "Next": "Equal"}],
"Default": "NotEqual"
},
"Equal": {"Type": "Pass", "End": true, "Result": "equal"},
"NotEqual": {"Type": "Pass", "End": true, "Result": "not-equal"}
}
}`

	assert.Equal(t, "equal", execute(t, def, `{"x": 42, "y": 42}`).Output)
	assert.Equal(t, "not-equal", execute(t, def, `{"x": 42, "y": 43}`).Output)
}

func TestExecutor_Choice_IsString(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.v", "IsString": true, "Next": "Str"}],
"Default": "NotStr"
},
"Str": {"Type": "Pass", "End": true, "Result": "string"},
"NotStr": {"Type": "Pass", "End": true, "Result": "not-string"}
}
}`

	assert.Equal(t, "string", execute(t, def, `{"v": "hello"}`).Output)
	assert.Equal(t, "not-string", execute(t, def, `{"v": 42}`).Output)
}

func TestExecutor_Choice_IsNumeric(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.v", "IsNumeric": true, "Next": "Num"}],
"Default": "NotNum"
},
"Num": {"Type": "Pass", "End": true, "Result": "numeric"},
"NotNum": {"Type": "Pass", "End": true, "Result": "not-numeric"}
}
}`

	assert.Equal(t, "numeric", execute(t, def, `{"v": 3.14}`).Output)
	assert.Equal(t, "not-numeric", execute(t, def, `{"v": "text"}`).Output)
}

func TestExecutor_Choice_IsBoolean(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.v", "IsBoolean": true, "Next": "Bool"}],
"Default": "NotBool"
},
"Bool": {"Type": "Pass", "End": true, "Result": "boolean"},
"NotBool": {"Type": "Pass", "End": true, "Result": "not-boolean"}
}
}`

	assert.Equal(t, "boolean", execute(t, def, `{"v": true}`).Output)
	assert.Equal(t, "not-boolean", execute(t, def, `{"v": "true"}`).Output)
}

func TestExecutor_Choice_IsTimestamp(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.v", "IsTimestamp": true, "Next": "Ts"}],
"Default": "NotTs"
},
"Ts": {"Type": "Pass", "End": true, "Result": "timestamp"},
"NotTs": {"Type": "Pass", "End": true, "Result": "not-timestamp"}
}
}`

	assert.Equal(t, "timestamp", execute(t, def, `{"v": "2024-01-15T12:00:00Z"}`).Output)
	assert.Equal(t, "not-timestamp", execute(t, def, `{"v": "not-a-timestamp"}`).Output)
	assert.Equal(t, "not-timestamp", execute(t, def, `{"v": 42}`).Output)
}

func TestExecutor_Choice_TimestampLessThan(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "C",
"States": {
"C": {
"Type": "Choice",
"Choices": [{"Variable": "$.ts", "TimestampLessThan": "2024-06-01T00:00:00Z", "Next": "Before"}],
"Default": "After"
},
"Before": {"Type": "Pass", "End": true, "Result": "before"},
"After": {"Type": "Pass", "End": true, "Result": "after"}
}
}`

	assert.Equal(t, "before", execute(t, def, `{"ts": "2024-01-01T00:00:00Z"}`).Output)
	assert.Equal(t, "after", execute(t, def, `{"ts": "2024-12-01T00:00:00Z"}`).Output)
}

// --- Map state ItemProcessor ---

func TestExecutor_MapState_ItemProcessor(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "Map",
"States": {
"Map": {
"Type": "Map",
"End": true,
"ItemProcessor": {
"StartAt": "P",
"States": {
"P": {"Type": "Pass", "End": true}
}
}
}
}
}`

	result := execute(t, def, `[1, 2, 3]`)
	assert.Empty(t, result.Error)
	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 3)
}

// --- Intrinsic function tests ---

func TestIntrinsic_Format(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"msg.$": "States.Format('Hello, {}!', $.name)"},
"End": true
}
}
}`

	result := execute(t, def, `{"name": "World"}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, "Hello, World!", m["msg"])
}

func TestIntrinsic_StringToJson(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"parsed.$": "States.StringToJson($.json)"},
"End": true
}
}
}`

	result := execute(t, def, `{"json": "{\"key\": \"value\"}"}`)
	m := result.Output.(map[string]any)
	inner, ok := m["parsed"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "value", inner["key"])
}

func TestIntrinsic_JsonToString(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"serialized.$": "States.JsonToString($.obj)"},
"End": true
}
}
}`

	result := execute(t, def, `{"obj": {"x": 1}}`)
	m := result.Output.(map[string]any)
	s, ok := m["serialized"].(string)
	require.True(t, ok)
	assert.Contains(t, s, "\"x\"")
}

func TestIntrinsic_Array(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"arr.$": "States.Array($.a, $.b)"},
"End": true
}
}
}`

	result := execute(t, def, `{"a": 1, "b": 2}`)
	m := result.Output.(map[string]any)
	arr, ok := m["arr"].([]any)
	require.True(t, ok)
	assert.Len(t, arr, 2)
}

func TestIntrinsic_ArrayLength(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"len.$": "States.ArrayLength($.items)"},
"End": true
}
}
}`

	result := execute(t, def, `{"items": [1, 2, 3]}`)
	m := result.Output.(map[string]any)
	assert.InDelta(t, float64(3), m["len"], 1e-9)
}

func TestIntrinsic_ArrayContains(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"found.$": "States.ArrayContains($.arr, $.target)"},
"End": true
}
}
}`

	result := execute(t, def, `{"arr": ["a", "b", "c"], "target": "b"}`)
	m := result.Output.(map[string]any)
	assert.Equal(t, true, m["found"])

	result2 := execute(t, def, `{"arr": ["a", "b", "c"], "target": "z"}`)
	m2 := result2.Output.(map[string]any)
	assert.Equal(t, false, m2["found"])
}

func TestIntrinsic_Base64EncodeAndDecode(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"encoded.$": "States.Base64Encode($.text)"},
"End": true
}
}
}`

	result := execute(t, def, `{"text": "hello"}`)
	m := result.Output.(map[string]any)
	encoded, ok := m["encoded"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, encoded)

	// Decode it back.
	def2 := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"decoded.$": "States.Base64Decode($.enc)"},
"End": true
}
}
}`
	result2 := execute(t, def2, `{"enc": "`+encoded+`"}`)
	m2 := result2.Output.(map[string]any)
	assert.Equal(t, "hello", m2["decoded"])
}

func TestIntrinsic_Hash(t *testing.T) {
	t.Parallel()

	def := `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"h.$": "States.Hash($.data, 'SHA-256')"},
"End": true
}
}
}`

	result := execute(t, def, `{"data": "hello"}`)
	m := result.Output.(map[string]any)
	h, ok := m["h"].(string)
	require.True(t, ok)
	assert.Len(t, h, 64) // SHA-256 hex = 64 chars
}
