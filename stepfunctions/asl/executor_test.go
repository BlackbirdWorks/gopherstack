package asl_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/blackbirdworks/gopherstack/stepfunctions/asl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test sentinel errors — used as mock return values in Lambda error tests.
var (
	errLambdaService = errors.New("Lambda.ServiceException")
	errMyError       = errors.New("MyError")
	errMySpecific    = errors.New("MySpecificError")
	errUnhandled     = errors.New("UnhandledError")
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
	result, err := exec.Execute(context.Background(), "test-exec", input)
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
	result, err := exec.Execute(context.Background(), "test-exec", `{}`)
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
	assert.Empty(t, result.Error)
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
	result, err := exec.Execute(context.Background(), "test-exec", `{"input": "data"}`)
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
	result, err := exec.Execute(context.Background(), "test-exec", `{}`)
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
	result, err := exec.Execute(context.Background(), "test", `{"x": "no"}`)
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
	result, err := exec.Execute(context.Background(), "test", `{}`)
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

	ctx, cancel := context.WithCancel(context.Background())
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
	result, err := exec.Execute(context.Background(), "test", `{"original": "data"}`)
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
	result, err := exec.Execute(context.Background(), "test", `{}`)
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
	result, err := exec.Execute(context.Background(), "test", `{}`)
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
	result, err := exec.Execute(context.Background(), "test", `{}`)
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
	_, err = exec.Execute(context.Background(), "test", `{"x": 1}`)
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
	_, err = exec.Execute(context.Background(), "test", `{"not": "array"}`)
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
	_, err = exec.Execute(context.Background(), "test", `{"count": 5}`)
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
	_, err := exec.Execute(context.Background(), "test", `{}`)
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
	result, err := exec.Execute(context.Background(), "test", `{}`)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Error)
}
