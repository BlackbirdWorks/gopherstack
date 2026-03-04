package asl_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
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

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs     error
		name          string
		def           string
		wantStartAt   string
		wantStatesLen int
		wantErr       bool
	}{
		{
			name: "valid",
			def: `{
				"StartAt": "Hello",
				"States": {
					"Hello": {
						"Type": "Pass",
						"End": true
					}
				}
			}`,
			wantStartAt:   "Hello",
			wantStatesLen: 1,
		},
		{
			name:      "missing_start_at",
			def:       `{"States": {"S": {"Type": "Pass", "End": true}}}`,
			wantErr:   true,
			wantErrIs: asl.ErrParseError,
		},
		{
			name:    "missing_states",
			def:     `{"StartAt": "S"}`,
			wantErr: true,
		},
		{
			name:    "start_at_not_in_states",
			def:     `{"StartAt": "Missing", "States": {"S": {"Type": "Pass", "End": true}}}`,
			wantErr: true,
		},
		{
			name:    "invalid_json",
			def:     `{invalid json}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)

			if tt.wantErr {
				require.Error(t, err)

				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStartAt, sm.StartAt)
			assert.Len(t, sm.States, tt.wantStatesLen)
		})
	}
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

func TestExecutor_PassState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assertFn func(*testing.T, any)
		name     string
		def      string
		input    string
	}{
		{
			name: "pass_through",
			def: `{
				"StartAt": "P",
				"States": {
					"P": {"Type": "Pass", "End": true}
				}
			}`,
			input: `{"x": 1}`,
			assertFn: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.InDelta(t, float64(1), m["x"], 1e-9)
			},
		},
		{
			name: "with_result",
			def: `{
				"StartAt": "P",
				"States": {
					"P": {"Type": "Pass", "Result": {"msg": "hello"}, "End": true}
				}
			}`,
			input: `{}`,
			assertFn: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "hello", m["msg"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := execute(t, tt.def, tt.input)
			assert.Empty(t, result.Error)
			tt.assertFn(t, result.Output)
		})
	}
}

func TestExecutor_TerminalStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		def             string
		input           string
		wantResultError string
		wantCause       string
	}{
		{
			name: "succeed_state",
			def: `{
				"StartAt": "Done",
				"States": {
					"Done": {"Type": "Succeed"}
				}
			}`,
			input: `{"result": "ok"}`,
		},
		{
			name: "fail_state",
			def: `{
				"StartAt": "Oops",
				"States": {
					"Oops": {"Type": "Fail", "Error": "MyError", "Cause": "something bad"}
				}
			}`,
			input:           `{}`,
			wantResultError: "MyError",
			wantCause:       "something bad",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, nil, nil)
			result, err := exec.Execute(t.Context(), "test-exec", tt.input)
			require.NoError(t, err)

			if tt.wantResultError != "" {
				assert.Equal(t, tt.wantResultError, result.Error)
				assert.Equal(t, tt.wantCause, result.Cause)
			} else {
				assert.Empty(t, result.Error)
			}
		})
	}
}

func TestExecutor_WaitState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		def   string
		input string
	}{
		{
			name: "zero_seconds",
			def: `{
				"StartAt": "Wait",
				"States": {
					"Wait": {"Type": "Wait", "Seconds": 0, "Next": "Done"},
					"Done": {"Type": "Succeed"}
				}
			}`,
			input: `{}`,
		},
		{
			name: "seconds_path",
			def: `{
				"StartAt": "W",
				"States": {
					"W": {"Type": "Wait", "SecondsPath": "$.delay", "Next": "Done"},
					"Done": {"Type": "Succeed"}
				}
			}`,
			input: `{"delay": 0}`,
		},
		{
			name: "timestamp_past",
			def: `{
				"StartAt": "W",
				"States": {
					"W": {"Type": "Wait", "Timestamp": "2000-01-01T00:00:00Z", "Next": "Done"},
					"Done": {"Type": "Succeed"}
				}
			}`,
			input: `{}`,
		},
		{
			name: "timestamp_path",
			def: `{
				"StartAt": "W",
				"States": {
					"W": {"Type": "Wait", "TimestampPath": "$.ts", "Next": "Done"},
					"Done": {"Type": "Succeed"}
				}
			}`,
			input: `{"ts": "2000-01-01T00:00:00Z"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := execute(t, tt.def, tt.input)
			assert.Empty(t, result.Error)
		})
	}
}

func TestExecutor_ChoiceState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantOutput            any
		name                  string
		def                   string
		input                 string
		wantResultErrNotEmpty bool
	}{
		// StringEquals
		{
			name: "string_equals_match",
			def: `{
				"StartAt": "Check",
				"States": {
					"Check": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.status", "StringEquals": "active", "Next": "Active"}],
						"Default": "Inactive"
					},
					"Active": {"Type": "Pass", "End": true, "Result": "active"},
					"Inactive": {"Type": "Pass", "End": true, "Result": "inactive"}
				}
			}`,
			input:      `{"status": "active"}`,
			wantOutput: "active",
		},
		{
			name: "string_equals_default",
			def: `{
				"StartAt": "Check",
				"States": {
					"Check": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.status", "StringEquals": "active", "Next": "Active"}],
						"Default": "Inactive"
					},
					"Active": {"Type": "Pass", "End": true, "Result": "active"},
					"Inactive": {"Type": "Pass", "End": true, "Result": "inactive"}
				}
			}`,
			input:      `{"status": "other"}`,
			wantOutput: "inactive",
		},
		// NumericGreaterThan
		{
			name: "numeric_greater_than_match",
			def: `{
				"StartAt": "Check",
				"States": {
					"Check": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.count", "NumericGreaterThan": 5, "Next": "High"}],
						"Default": "Low"
					},
					"High": {"Type": "Pass", "End": true, "Result": "high"},
					"Low": {"Type": "Pass", "End": true, "Result": "low"}
				}
			}`,
			input:      `{"count": 10}`,
			wantOutput: "high",
		},
		{
			name: "numeric_greater_than_default",
			def: `{
				"StartAt": "Check",
				"States": {
					"Check": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.count", "NumericGreaterThan": 5, "Next": "High"}],
						"Default": "Low"
					},
					"High": {"Type": "Pass", "End": true, "Result": "high"},
					"Low": {"Type": "Pass", "End": true, "Result": "low"}
				}
			}`,
			input:      `{"count": 2}`,
			wantOutput: "low",
		},
		// And condition
		{
			name: "and_condition_both_true",
			def: `{
				"StartAt": "Check",
				"States": {
					"Check": {
						"Type": "Choice",
						"Choices": [{
							"And": [
								{"Variable": "$.x", "NumericGreaterThan": 0},
								{"Variable": "$.y", "StringEquals": "yes"}
							],
							"Next": "Both"
						}],
						"Default": "Neither"
					},
					"Both": {"Type": "Pass", "End": true, "Result": "both"},
					"Neither": {"Type": "Pass", "End": true, "Result": "neither"}
				}
			}`,
			input:      `{"x": 5, "y": "yes"}`,
			wantOutput: "both",
		},
		{
			name: "and_condition_one_false",
			def: `{
				"StartAt": "Check",
				"States": {
					"Check": {
						"Type": "Choice",
						"Choices": [{
							"And": [
								{"Variable": "$.x", "NumericGreaterThan": 0},
								{"Variable": "$.y", "StringEquals": "yes"}
							],
							"Next": "Both"
						}],
						"Default": "Neither"
					},
					"Both": {"Type": "Pass", "End": true, "Result": "both"},
					"Neither": {"Type": "Pass", "End": true, "Result": "neither"}
				}
			}`,
			input:      `{"x": 5, "y": "no"}`,
			wantOutput: "neither",
		},
		// No match, no default
		{
			name: "no_match_no_default",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.x", "StringEquals": "yes", "Next": "Done"}]
					},
					"Done": {"Type": "Succeed"}
				}
			}`,
			input:                 `{"x": "no"}`,
			wantResultErrNotEmpty: true,
		},
		// Or condition
		{
			name: "or_condition_first_matches",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{
							"Or": [
								{"Variable": "$.x", "StringEquals": "a"},
								{"Variable": "$.x", "StringEquals": "b"}
							],
							"Next": "Match"
						}],
						"Default": "NoMatch"
					},
					"Match": {"Type": "Pass", "End": true, "Result": "match"},
					"NoMatch": {"Type": "Pass", "End": true, "Result": "no-match"}
				}
			}`,
			input:      `{"x": "a"}`,
			wantOutput: "match",
		},
		{
			name: "or_condition_no_match",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{
							"Or": [
								{"Variable": "$.x", "StringEquals": "a"},
								{"Variable": "$.x", "StringEquals": "b"}
							],
							"Next": "Match"
						}],
						"Default": "NoMatch"
					},
					"Match": {"Type": "Pass", "End": true, "Result": "match"},
					"NoMatch": {"Type": "Pass", "End": true, "Result": "no-match"}
				}
			}`,
			input:      `{"x": "c"}`,
			wantOutput: "no-match",
		},
		// Not condition
		{
			name: "not_condition_matches",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Not": {"Variable": "$.status", "StringEquals": "inactive"}, "Next": "Active"}],
						"Default": "Inactive"
					},
					"Active": {"Type": "Pass", "End": true, "Result": "active"},
					"Inactive": {"Type": "Pass", "End": true, "Result": "inactive"}
				}
			}`,
			input:      `{"status": "running"}`,
			wantOutput: "active",
		},
		{
			name: "not_condition_default",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Not": {"Variable": "$.status", "StringEquals": "inactive"}, "Next": "Active"}],
						"Default": "Inactive"
					},
					"Active": {"Type": "Pass", "End": true, "Result": "active"},
					"Inactive": {"Type": "Pass", "End": true, "Result": "inactive"}
				}
			}`,
			input:      `{"status": "inactive"}`,
			wantOutput: "inactive",
		},
		// BooleanEquals
		{
			name: "boolean_equals_true",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.flag", "BooleanEquals": true, "Next": "Yes"}],
						"Default": "No"
					},
					"Yes": {"Type": "Pass", "End": true, "Result": "yes"},
					"No": {"Type": "Pass", "End": true, "Result": "no"}
				}
			}`,
			input:      `{"flag": true}`,
			wantOutput: "yes",
		},
		{
			name: "boolean_equals_false_default",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.flag", "BooleanEquals": true, "Next": "Yes"}],
						"Default": "No"
					},
					"Yes": {"Type": "Pass", "End": true, "Result": "yes"},
					"No": {"Type": "Pass", "End": true, "Result": "no"}
				}
			}`,
			input:      `{"flag": false}`,
			wantOutput: "no",
		},
		// NumericLessThan
		{
			name: "numeric_less_than_match",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.n", "NumericLessThan": 10, "Next": "Low"}],
						"Default": "High"
					},
					"Low": {"Type": "Pass", "End": true, "Result": "low"},
					"High": {"Type": "Pass", "End": true, "Result": "high"}
				}
			}`,
			input:      `{"n": 5}`,
			wantOutput: "low",
		},
		{
			name: "numeric_less_than_default",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.n", "NumericLessThan": 10, "Next": "Low"}],
						"Default": "High"
					},
					"Low": {"Type": "Pass", "End": true, "Result": "low"},
					"High": {"Type": "Pass", "End": true, "Result": "high"}
				}
			}`,
			input:      `{"n": 15}`,
			wantOutput: "high",
		},
		// NumericEquals
		{
			name: "numeric_equals_match",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.n", "NumericEquals": 42, "Next": "Match"}],
						"Default": "NoMatch"
					},
					"Match": {"Type": "Pass", "End": true, "Result": "match"},
					"NoMatch": {"Type": "Pass", "End": true, "Result": "no-match"}
				}
			}`,
			input:      `{"n": 42}`,
			wantOutput: "match",
		},
		// StringGreaterThan
		{
			name: "string_greater_than_match",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.s", "StringGreaterThan": "m", "Next": "High"}],
						"Default": "Low"
					},
					"High": {"Type": "Pass", "End": true, "Result": "high"},
					"Low": {"Type": "Pass", "End": true, "Result": "low"}
				}
			}`,
			input:      `{"s": "z"}`,
			wantOutput: "high",
		},
		{
			name: "string_greater_than_default",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.s", "StringGreaterThan": "m", "Next": "High"}],
						"Default": "Low"
					},
					"High": {"Type": "Pass", "End": true, "Result": "high"},
					"Low": {"Type": "Pass", "End": true, "Result": "low"}
				}
			}`,
			input:      `{"s": "a"}`,
			wantOutput: "low",
		},
		// IsPresent
		{
			name: "is_present_match",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.optional", "IsPresent": true, "Next": "HasIt"}],
						"Default": "NoIt"
					},
					"HasIt": {"Type": "Pass", "End": true, "Result": "has"},
					"NoIt": {"Type": "Pass", "End": true, "Result": "missing"}
				}
			}`,
			input:      `{"optional": "value"}`,
			wantOutput: "has",
		},
		{
			name: "is_present_default",
			def: `{
				"StartAt": "C",
				"States": {
					"C": {
						"Type": "Choice",
						"Choices": [{"Variable": "$.optional", "IsPresent": true, "Next": "HasIt"}],
						"Default": "NoIt"
					},
					"HasIt": {"Type": "Pass", "End": true, "Result": "has"},
					"NoIt": {"Type": "Pass", "End": true, "Result": "missing"}
				}
			}`,
			input:      `{}`,
			wantOutput: "missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, nil, nil)
			result, err := exec.Execute(t.Context(), "test", tt.input)
			require.NoError(t, err)

			if tt.wantResultErrNotEmpty {
				assert.NotEmpty(t, result.Error)

				return
			}

			assert.Empty(t, result.Error)
			assert.Equal(t, tt.wantOutput, result.Output)
		})
	}
}

func TestExecutor_ParallelState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		def        string
		input      string
		wantArrLen int
	}{
		{
			name: "two_branches",
			def: `{
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
			}`,
			input:      `{}`,
			wantArrLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := execute(t, tt.def, tt.input)
			assert.Empty(t, result.Error)
			arr, ok := result.Output.([]any)
			require.True(t, ok)
			assert.Len(t, arr, tt.wantArrLen)
		})
	}
}

func TestExecutor_MapState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		def           string
		input         string
		wantGoErr     bool
		wantOutputLen int
	}{
		{
			name: "basic",
			def: `{
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
			}`,
			input:         `[1, 2, 3]`,
			wantOutputLen: 3,
		},
		{
			name: "with_items_path",
			def: `{
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
			}`,
			input:         `{"items": ["a", "b", "c"]}`,
			wantOutputLen: 3,
		},
		{
			name: "max_concurrency",
			def: `{
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
			}`,
			input:         `[1, 2, 3, 4, 5]`,
			wantOutputLen: 5,
		},
		{
			name: "empty_array",
			def: `{
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
			}`,
			input:         `[]`,
			wantOutputLen: 0,
		},
		{
			name: "not_array",
			def: `{
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
			}`,
			input:     `{"not": "array"}`,
			wantGoErr: true,
		},
		{
			name: "items_path_not_array",
			def: `{
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
			}`,
			input:     `{"count": 5}`,
			wantGoErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, nil, nil)
			result, execErr := exec.Execute(t.Context(), "test", tt.input)

			if tt.wantGoErr {
				require.Error(t, execErr)

				return
			}

			require.NoError(t, execErr)
			assert.Empty(t, result.Error)
			arr, ok := result.Output.([]any)
			require.True(t, ok)
			assert.Len(t, arr, tt.wantOutputLen)
		})
	}
}

func TestExecutor_PathTransforms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assertFn  func(*testing.T, *asl.ExecutionResult)
		name      string
		def       string
		input     string
		wantGoErr bool
	}{
		{
			name: "result_path",
			def: `{
				"StartAt": "P",
				"States": {
					"P": {
						"Type": "Pass",
						"Result": {"computed": true},
						"ResultPath": "$.result",
						"End": true
					}
				}
			}`,
			input: `{"original": "data"}`,
			assertFn: func(t *testing.T, result *asl.ExecutionResult) {
				t.Helper()
				m := result.Output.(map[string]any)
				assert.Equal(t, "data", m["original"])
				sub := m["result"].(map[string]any)
				assert.Equal(t, true, sub["computed"])
			},
		},
		{
			name: "input_path",
			def: `{
				"StartAt": "P",
				"States": {
					"P": {
						"Type": "Pass",
						"InputPath": "$.inner",
						"End": true
					}
				}
			}`,
			input: `{"inner": {"key": "value"}}`,
			assertFn: func(t *testing.T, result *asl.ExecutionResult) {
				t.Helper()
				m := result.Output.(map[string]any)
				assert.Equal(t, "value", m["key"])
			},
		},
		{
			name: "result_path_null_discards_result",
			def: `{
				"StartAt": "P",
				"States": {
					"P": {
						"Type": "Pass",
						"Result": {"discarded": true},
						"ResultPath": "null",
						"End": true
					}
				}
			}`,
			input: `{"original": "kept"}`,
			assertFn: func(t *testing.T, result *asl.ExecutionResult) {
				t.Helper()
				m := result.Output.(map[string]any)
				assert.Equal(t, "kept", m["original"])
				_, hasDiscarded := m["discarded"]
				assert.False(t, hasDiscarded)
			},
		},
		{
			name: "output_path",
			def: `{
				"StartAt": "P",
				"States": {
					"P": {
						"Type": "Pass",
						"End": true,
						"OutputPath": "$.key"
					}
				}
			}`,
			input: `{"key": "extracted"}`,
			assertFn: func(t *testing.T, result *asl.ExecutionResult) {
				t.Helper()
				assert.Equal(t, "extracted", result.Output)
			},
		},
		{
			name: "result_path_nested_create",
			def: `{
				"StartAt": "P",
				"States": {
					"P": {
						"Type": "Pass",
						"Result": "computed",
						"ResultPath": "$.nested.value",
						"End": true
					}
				}
			}`,
			input: `{"data": "original"}`,
			assertFn: func(t *testing.T, result *asl.ExecutionResult) {
				t.Helper()
				m := result.Output.(map[string]any)
				assert.Equal(t, "original", m["data"])
			},
		},
		{
			name: "input_path_invalid",
			def: `{
				"StartAt": "P",
				"States": {
					"P": {
						"Type": "Pass",
						"InputPath": "invalid-path",
						"End": true
					}
				}
			}`,
			input:     `{"x": 1}`,
			wantGoErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, nil, nil)
			result, execErr := exec.Execute(t.Context(), "test", tt.input)

			if tt.wantGoErr {
				require.Error(t, execErr)

				return
			}

			require.NoError(t, execErr)
			tt.assertFn(t, result)
		})
	}
}

func TestExecutor_ChainedStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		def   string
		input string
	}{
		{
			name: "three_chained_states",
			def: `{
				"StartAt": "A",
				"States": {
					"A": {"Type": "Pass", "Next": "B"},
					"B": {"Type": "Pass", "Next": "C"},
					"C": {"Type": "Succeed"}
				}
			}`,
			input: `{"value": 42}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := execute(t, tt.def, tt.input)
			assert.Empty(t, result.Error)
		})
	}
}

func TestExecutor_TaskState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lambda                asl.LambdaInvoker
		wantOutput            any
		name                  string
		def                   string
		input                 string
		wantResultError       string
		wantCauseContains     string
		wantResultErrNotEmpty bool
	}{
		{
			name: "no_lambda_dynamodb_resource",
			def: `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:states:::dynamodb:getItem",
						"End": true
					}
				}
			}`,
			input:             `{"pk": "123"}`,
			wantResultError:   "TaskFailed",
			wantCauseContains: "DynamoDB integration not configured",
		},
		{
			name:   "lambda_success",
			lambda: &mockLambda{response: `{"processed": true}`},
			def: `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
						"End": true
					}
				}
			}`,
			input: `{"input": "data"}`,
		},
		{
			name:   "lambda_error_catch",
			lambda: &mockLambda{returnErr: errLambdaService},
			def: `{
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
			}`,
			input:      `{}`,
			wantOutput: "caught",
		},
		{
			name:   "lambda_status_error_caught",
			lambda: &mockLambda{response: `{"error": "bad request"}`, statusCode: 400},
			def: `{
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
			}`,
			input: `{}`,
		},
		{
			name:   "catch_with_result_path",
			lambda: &mockLambda{returnErr: errMyError},
			def: `{
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
			}`,
			input: `{"original": "data"}`,
		},
		{
			name:   "specific_error_catch",
			lambda: &mockLambda{returnErr: errMySpecific},
			def: `{
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
			}`,
			input:      `{}`,
			wantOutput: "caught",
		},
		{
			name:   "fail_with_no_catch",
			lambda: &mockLambda{returnErr: errUnhandled},
			def: `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
						"End": true
					}
				}
			}`,
			input:           `{}`,
			wantResultError: "TaskFailed",
		},
		{
			name:   "non_json_response",
			lambda: &mockLambda{response: "plain text response"},
			def: `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
						"End": true
					}
				}
			}`,
			input: `{}`,
		},
		{
			name: "no_lambda_invoker",
			def: `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
						"End": true
					}
				}
			}`,
			input:                 `{}`,
			wantResultErrNotEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, tt.lambda, nil)
			result, err := exec.Execute(t.Context(), "test-exec", tt.input)
			require.NoError(t, err)

			if tt.wantResultErrNotEmpty {
				assert.NotEmpty(t, result.Error)

				return
			}

			if tt.wantResultError != "" {
				assert.Equal(t, tt.wantResultError, result.Error)
			} else {
				assert.Empty(t, result.Error)
			}

			if tt.wantCauseContains != "" {
				assert.Contains(t, result.Cause, tt.wantCauseContains)
			}

			if tt.wantOutput != nil {
				assert.Equal(t, tt.wantOutput, result.Output)
			}
		})
	}
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

func TestExecutor_FailError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		errCode    string
		cause      string
		wantErrStr string
	}{
		{
			name:       "with_cause",
			errCode:    "E1",
			cause:      "cause1",
			wantErrStr: "E1: cause1",
		},
		{
			name:       "without_cause",
			errCode:    "E2",
			cause:      "",
			wantErrStr: "E2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := &asl.FailError{ErrCode: tt.errCode, Cause: tt.cause}
			assert.Equal(t, tt.wantErrStr, err.Error())
		})
	}
}

func TestExecutor_ContextCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		def   string
		input string
	}{
		{
			name: "cancelled_context_returns_error",
			def: `{
"StartAt": "W",
"States": {
"W": {
"Type": "Wait",
"Seconds": 10,
"End": true
}
}
}`,
			input: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			cancel() // Cancel immediately.

			exec := asl.NewExecutor(sm, nil, nil)
			_, err = exec.Execute(ctx, "test", tt.input)
			require.Error(t, err)
		})
	}
}

// --- Parameters and ResultSelector tests ---

func TestExecutor_Parameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assertFn func(*testing.T, any)
		lambda   asl.LambdaInvoker
		name     string
		def      string
		input    string
	}{
		{
			name: "static_and_dynamic",
			def: `{
"StartAt": "P",
"States": {
"P": {
"Type": "Pass",
"Parameters": {"static": "hello", "dynamic.$": "$.name"},
"End": true
}
}
}`,
			input: `{"name": "world"}`,
			assertFn: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "hello", m["static"])
				assert.Equal(t, "world", m["dynamic"])
			},
		},
		{
			name:   "result_selector_filters_output",
			lambda: &mockLambda{response: `{"status": "ok", "noise": "ignored"}`},
			def: `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"ResultSelector": {"result.$": "$.status"},
"End": true
}
}
}`,
			input: `{}`,
			assertFn: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "ok", m["result"])
				_, hasNoise := m["noise"]
				assert.False(t, hasNoise)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, tt.lambda, nil)
			result, err := exec.Execute(t.Context(), "test", tt.input)
			require.NoError(t, err)
			assert.Empty(t, result.Error)
			tt.assertFn(t, result.Output)
		})
	}
}

// --- Retry tests ---

func TestExecutor_Retry(t *testing.T) {
	t.Parallel()

	var retryCount, noRetryCount atomic.Int64

	tests := []struct {
		lambda        asl.LambdaInvoker
		wantOutput    any
		counter       *atomic.Int64
		name          string
		def           string
		input         string
		wantResultErr string
		wantCallCount int64
	}{
		{
			name: "succeeds_after_retry",
			lambda: &mockLambdaFn{fn: func() ([]byte, int, error) {
				c := retryCount.Add(1)
				if c < 3 {
					return nil, 500, errTransientError
				}

				return []byte(`{"done": true}`), 200, nil
			}},
			def: `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"Retry": [{"ErrorEquals": ["TransientError"], "IntervalSeconds": 0, "MaxAttempts": 5}],
"End": true
}
}
}`,
			input:         `{}`,
			counter:       &retryCount,
			wantCallCount: 3,
		},
		{
			name:   "exhausted_falls_through_to_catch",
			lambda: &mockLambda{returnErr: errPersistentError},
			def: `{
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
}`,
			input:      `{}`,
			wantOutput: "caught",
		},
		{
			name: "max_attempts_zero_no_retry",
			lambda: &mockLambdaFn{fn: func() ([]byte, int, error) {
				noRetryCount.Add(1)

				return nil, 500, errSomeError
			}},
			def: `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"Retry": [{"ErrorEquals": ["SomeError"], "MaxAttempts": 0}],
"End": true
}
}
}`,
			input:         `{}`,
			wantResultErr: "TaskFailed",
			counter:       &noRetryCount,
			wantCallCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, tt.lambda, nil)
			result, err := exec.Execute(t.Context(), "test", tt.input)
			require.NoError(t, err)

			if tt.wantResultErr != "" {
				assert.Equal(t, tt.wantResultErr, result.Error)
			} else {
				assert.Empty(t, result.Error)
			}

			if tt.wantOutput != nil {
				assert.Equal(t, tt.wantOutput, result.Output)
			}

			if tt.counter != nil {
				assert.Equal(t, tt.wantCallCount, tt.counter.Load())
			}
		})
	}
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

	var timeoutCallCount atomic.Int64

	tests := []struct {
		lambda        asl.LambdaInvoker
		counter       *atomic.Int64
		wantOutput    any
		name          string
		def           string
		input         string
		wantCallCount int64
	}{
		{
			name: "timeout_caught_by_states_timeout",
			lambda: &mockLambdaFnCtx{fn: func(ctx context.Context) ([]byte, int, error) {
				<-ctx.Done()

				return nil, 0, ctx.Err()
			}},
			def: `{
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
}`,
			input:      `{}`,
			wantOutput: "timeout",
		},
		{
			name: "timeout_not_retried_with_states_all_retry",
			lambda: &mockLambdaFnCtx{fn: func(ctx context.Context) ([]byte, int, error) {
				timeoutCallCount.Add(1)
				<-ctx.Done()

				return nil, 0, ctx.Err()
			}},
			def: `{
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
}`,
			input:         `{}`,
			wantOutput:    "timeout",
			counter:       &timeoutCallCount,
			wantCallCount: 1,
		},
		{
			name: "zero_timeout_no_enforcement",
			lambda: &mockLambdaFn{fn: func() ([]byte, int, error) {
				time.Sleep(50 * time.Millisecond)

				return []byte(`{}`), 200, nil
			}},
			def: `{
"StartAt": "T",
"States": {
"T": {
"Type": "Task",
"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
"TimeoutSeconds": 0,
"End": true
}
}
}`,
			input: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, tt.lambda, nil)
			result, err := exec.Execute(t.Context(), "test", tt.input)
			require.NoError(t, err)
			assert.Empty(t, result.Error)

			if tt.wantOutput != nil {
				assert.Equal(t, tt.wantOutput, result.Output)
			}

			if tt.counter != nil {
				assert.Equal(t, tt.wantCallCount, tt.counter.Load())
			}
		})
	}
}

func TestExecutor_UnknownStateType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		def   string
		input string
	}{
		{
			name: "unknown_type_returns_error",
			def: `{
"StartAt": "X",
"States": {
"X": {"Type": "Unknown", "End": true}
}
}`,
			input: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Parse will succeed but execute will fail on unknown type.
			var sm asl.StateMachine
			require.NoError(t, json.Unmarshal([]byte(tt.def), &sm))
			exec := asl.NewExecutor(&sm, nil, nil)
			_, err := exec.Execute(t.Context(), "test", tt.input)
			require.Error(t, err)
		})
	}
}

// makeChoiceDef builds a minimal ASL definition with a single choice condition.
// variable is a JSONPath (e.g. "$.s"), operator is the ASL operator name,
// and valueJSON must be a valid JSON fragment for the comparison value
// (e.g. `"m"` for strings, `10` for numbers, `"$.ref"` for path references).
func makeChoiceDef(variable, operator, valueJSON string) string {
	return `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"` +
		variable + `","` + operator + `":` + valueJSON + `,"Next":"Yes"}],"Default":"No"},` +
		`"Yes":{"Type":"Pass","End":true,"Result":"yes"},` +
		`"No":{"Type":"Pass","End":true,"Result":"no"}}}`
}

func TestExecutor_Choice_InclusiveStringAndNumericOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  any
		name  string
		def   string
	}{
		{
			name:  "StringLessThanEquals_less",
			def:   makeChoiceDef("$.s", "StringLessThanEquals", `"m"`),
			input: `{"s":"a"}`,
			want:  "yes",
		},
		{
			name:  "StringLessThanEquals_equal",
			def:   makeChoiceDef("$.s", "StringLessThanEquals", `"m"`),
			input: `{"s":"m"}`,
			want:  "yes",
		},
		{
			name:  "StringLessThanEquals_greater",
			def:   makeChoiceDef("$.s", "StringLessThanEquals", `"m"`),
			input: `{"s":"z"}`,
			want:  "no",
		},
		{
			name:  "NumericGreaterThanEquals_equal",
			def:   makeChoiceDef("$.n", "NumericGreaterThanEquals", `10`),
			input: `{"n":10}`,
			want:  "yes",
		},
		{
			name:  "NumericGreaterThanEquals_greater",
			def:   makeChoiceDef("$.n", "NumericGreaterThanEquals", `10`),
			input: `{"n":15}`,
			want:  "yes",
		},
		{
			name:  "NumericGreaterThanEquals_less",
			def:   makeChoiceDef("$.n", "NumericGreaterThanEquals", `10`),
			input: `{"n":9}`,
			want:  "no",
		},
		{
			name:  "NumericLessThanEquals_equal",
			def:   makeChoiceDef("$.n", "NumericLessThanEquals", `5`),
			input: `{"n":5}`,
			want:  "yes",
		},
		{
			name:  "NumericLessThanEquals_less",
			def:   makeChoiceDef("$.n", "NumericLessThanEquals", `5`),
			input: `{"n":3}`,
			want:  "yes",
		},
		{
			name:  "NumericLessThanEquals_greater",
			def:   makeChoiceDef("$.n", "NumericLessThanEquals", `5`),
			input: `{"n":6}`,
			want:  "no",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, execute(t, tt.def, tt.input).Output)
		})
	}
}

func TestExecutor_Choice_PathOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  any
		name  string
		def   string
	}{
		{
			name:  "StringEqualsPath_match",
			def:   makeChoiceDef("$.a", "StringEqualsPath", `"$.b"`),
			input: `{"a":"hello","b":"hello"}`,
			want:  "yes",
		},
		{
			name:  "StringEqualsPath_nomatch",
			def:   makeChoiceDef("$.a", "StringEqualsPath", `"$.b"`),
			input: `{"a":"hello","b":"world"}`,
			want:  "no",
		},
		{
			name:  "NumericEqualsPath_match",
			def:   makeChoiceDef("$.x", "NumericEqualsPath", `"$.y"`),
			input: `{"x":42,"y":42}`,
			want:  "yes",
		},
		{
			name:  "NumericEqualsPath_nomatch",
			def:   makeChoiceDef("$.x", "NumericEqualsPath", `"$.y"`),
			input: `{"x":42,"y":43}`,
			want:  "no",
		},
		{
			name:  "StringLessThanPath_match",
			def:   makeChoiceDef("$.a", "StringLessThanPath", `"$.b"`),
			input: `{"a":"apple","b":"banana"}`,
			want:  "yes",
		},
		{
			name:  "StringLessThanPath_nomatch",
			def:   makeChoiceDef("$.a", "StringLessThanPath", `"$.b"`),
			input: `{"a":"zebra","b":"banana"}`,
			want:  "no",
		},
		{
			name:  "StringGreaterThanPath_match",
			def:   makeChoiceDef("$.a", "StringGreaterThanPath", `"$.b"`),
			input: `{"a":"zebra","b":"banana"}`,
			want:  "yes",
		},
		{
			name:  "StringGreaterThanPath_nomatch",
			def:   makeChoiceDef("$.a", "StringGreaterThanPath", `"$.b"`),
			input: `{"a":"apple","b":"banana"}`,
			want:  "no",
		},
		{
			name:  "StringLessThanEqualsPath_equal",
			def:   makeChoiceDef("$.a", "StringLessThanEqualsPath", `"$.b"`),
			input: `{"a":"apple","b":"apple"}`,
			want:  "yes",
		},
		{
			name:  "StringGreaterThanEqualsPath_equal",
			def:   makeChoiceDef("$.a", "StringGreaterThanEqualsPath", `"$.b"`),
			input: `{"a":"apple","b":"apple"}`,
			want:  "yes",
		},
		{
			name:  "NumericLessThanPath_match",
			def:   makeChoiceDef("$.x", "NumericLessThanPath", `"$.y"`),
			input: `{"x":3,"y":5}`,
			want:  "yes",
		},
		{
			name:  "NumericLessThanPath_nomatch",
			def:   makeChoiceDef("$.x", "NumericLessThanPath", `"$.y"`),
			input: `{"x":7,"y":5}`,
			want:  "no",
		},
		{
			name:  "NumericGreaterThanPath_match",
			def:   makeChoiceDef("$.x", "NumericGreaterThanPath", `"$.y"`),
			input: `{"x":7,"y":5}`,
			want:  "yes",
		},
		{
			name:  "NumericGreaterThanPath_nomatch",
			def:   makeChoiceDef("$.x", "NumericGreaterThanPath", `"$.y"`),
			input: `{"x":3,"y":5}`,
			want:  "no",
		},
		{
			name:  "NumericLessThanEqualsPath_equal",
			def:   makeChoiceDef("$.x", "NumericLessThanEqualsPath", `"$.y"`),
			input: `{"x":5,"y":5}`,
			want:  "yes",
		},
		{
			name:  "NumericGreaterThanEqualsPath_equal",
			def:   makeChoiceDef("$.x", "NumericGreaterThanEqualsPath", `"$.y"`),
			input: `{"x":5,"y":5}`,
			want:  "yes",
		},
		{
			name:  "BooleanEqualsPath_match",
			def:   makeChoiceDef("$.a", "BooleanEqualsPath", `"$.b"`),
			input: `{"a":true,"b":true}`,
			want:  "yes",
		},
		{
			name:  "BooleanEqualsPath_nomatch",
			def:   makeChoiceDef("$.a", "BooleanEqualsPath", `"$.b"`),
			input: `{"a":true,"b":false}`,
			want:  "no",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, execute(t, tt.def, tt.input).Output)
		})
	}
}

func TestExecutor_Choice_TypeChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  any
		name  string
		def   string
	}{
		{
			name:  "IsString_match",
			def:   makeChoiceDef("$.v", "IsString", `true`),
			input: `{"v":"hello"}`,
			want:  "yes",
		},
		{
			name:  "IsString_nomatch",
			def:   makeChoiceDef("$.v", "IsString", `true`),
			input: `{"v":42}`,
			want:  "no",
		},
		{
			name:  "IsString_false_inverted",
			def:   makeChoiceDef("$.v", "IsString", `false`),
			input: `{"v":42}`,
			want:  "yes",
		},
		{
			name:  "IsNumeric_match",
			def:   makeChoiceDef("$.v", "IsNumeric", `true`),
			input: `{"v":3.14}`,
			want:  "yes",
		},
		{
			name:  "IsNumeric_nomatch",
			def:   makeChoiceDef("$.v", "IsNumeric", `true`),
			input: `{"v":"text"}`,
			want:  "no",
		},
		{
			name:  "IsBoolean_match",
			def:   makeChoiceDef("$.v", "IsBoolean", `true`),
			input: `{"v":true}`,
			want:  "yes",
		},
		{
			name:  "IsBoolean_nomatch_string",
			def:   makeChoiceDef("$.v", "IsBoolean", `true`),
			input: `{"v":"true"}`,
			want:  "no",
		},
		{
			name:  "IsTimestamp_match",
			def:   makeChoiceDef("$.v", "IsTimestamp", `true`),
			input: `{"v":"2024-01-15T12:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "IsTimestamp_nomatch_string",
			def:   makeChoiceDef("$.v", "IsTimestamp", `true`),
			input: `{"v":"not-a-timestamp"}`,
			want:  "no",
		},
		{
			name:  "IsTimestamp_nomatch_number",
			def:   makeChoiceDef("$.v", "IsTimestamp", `true`),
			input: `{"v":42}`,
			want:  "no",
		},
		{
			name:  "IsNull_match",
			def:   makeChoiceDef("$.v", "IsNull", `true`),
			input: `{"v":null}`,
			want:  "yes",
		},
		{
			name:  "IsNull_nomatch",
			def:   makeChoiceDef("$.v", "IsNull", `true`),
			input: `{"v":"something"}`,
			want:  "no",
		},
		{
			name:  "IsPresent_absent",
			def:   makeChoiceDef("$.missing", "IsPresent", `false`),
			input: `{}`,
			want:  "yes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, execute(t, tt.def, tt.input).Output)
		})
	}
}

func TestExecutor_Choice_TimestampOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  any
		name  string
		def   string
	}{
		{
			name:  "TimestampLessThan_before",
			def:   makeChoiceDef("$.ts", "TimestampLessThan", `"2024-06-01T00:00:00Z"`),
			input: `{"ts":"2024-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampLessThan_after",
			def:   makeChoiceDef("$.ts", "TimestampLessThan", `"2024-06-01T00:00:00Z"`),
			input: `{"ts":"2024-12-01T00:00:00Z"}`,
			want:  "no",
		},
		{
			name:  "TimestampGreaterThan_after",
			def:   makeChoiceDef("$.ts", "TimestampGreaterThan", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2024-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampGreaterThan_before",
			def:   makeChoiceDef("$.ts", "TimestampGreaterThan", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2019-01-01T00:00:00Z"}`,
			want:  "no",
		},
		{
			name:  "TimestampEquals_match",
			def:   makeChoiceDef("$.ts", "TimestampEquals", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2020-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampEquals_nomatch",
			def:   makeChoiceDef("$.ts", "TimestampEquals", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2021-01-01T00:00:00Z"}`,
			want:  "no",
		},
		{
			name:  "TimestampLessThanEquals_equal",
			def:   makeChoiceDef("$.ts", "TimestampLessThanEquals", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2020-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampLessThanEquals_before",
			def:   makeChoiceDef("$.ts", "TimestampLessThanEquals", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2019-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampGreaterThanEquals_equal",
			def:   makeChoiceDef("$.ts", "TimestampGreaterThanEquals", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2020-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampGreaterThanEquals_after",
			def:   makeChoiceDef("$.ts", "TimestampGreaterThanEquals", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2021-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampGreaterThanEquals_before",
			def:   makeChoiceDef("$.ts", "TimestampGreaterThanEquals", `"2020-01-01T00:00:00Z"`),
			input: `{"ts":"2019-01-01T00:00:00Z"}`,
			want:  "no",
		},
		{
			name:  "TimestampEqualsPath_match",
			def:   makeChoiceDef("$.ts", "TimestampEqualsPath", `"$.ref"`),
			input: `{"ts":"2020-01-01T00:00:00Z","ref":"2020-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampEqualsPath_nomatch",
			def:   makeChoiceDef("$.ts", "TimestampEqualsPath", `"$.ref"`),
			input: `{"ts":"2020-01-01T00:00:00Z","ref":"2021-01-01T00:00:00Z"}`,
			want:  "no",
		},
		{
			name:  "TimestampLessThanPath_match",
			def:   makeChoiceDef("$.ts", "TimestampLessThanPath", `"$.ref"`),
			input: `{"ts":"2019-01-01T00:00:00Z","ref":"2020-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampGreaterThanPath_match",
			def:   makeChoiceDef("$.ts", "TimestampGreaterThanPath", `"$.ref"`),
			input: `{"ts":"2021-01-01T00:00:00Z","ref":"2020-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampLessThanEqualsPath_equal",
			def:   makeChoiceDef("$.ts", "TimestampLessThanEqualsPath", `"$.ref"`),
			input: `{"ts":"2020-01-01T00:00:00Z","ref":"2020-01-01T00:00:00Z"}`,
			want:  "yes",
		},
		{
			name:  "TimestampGreaterThanEqualsPath_equal",
			def:   makeChoiceDef("$.ts", "TimestampGreaterThanEqualsPath", `"$.ref"`),
			input: `{"ts":"2020-01-01T00:00:00Z","ref":"2020-01-01T00:00:00Z"}`,
			want:  "yes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, execute(t, tt.def, tt.input).Output)
		})
	}
}

func TestExecutor_Choice_BooleanEquals_False(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  any
		name  string
	}{
		{name: "match_false", input: `{"v":false}`, want: "yes"},
		{name: "nomatch_true", input: `{"v":true}`, want: "no"},
	}

	def := makeChoiceDef("$.v", "BooleanEquals", `false`)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, execute(t, def, tt.input).Output)
		})
	}
}

func TestExecutor_Choice_StringGreaterThanEquals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  any
		name  string
	}{
		{name: "equal", input: `{"s":"m"}`, want: "yes"},
		{name: "greater", input: `{"s":"zebra"}`, want: "yes"},
		{name: "less", input: `{"s":"apple"}`, want: "no"},
	}

	def := makeChoiceDef("$.s", "StringGreaterThanEquals", `"m"`)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, execute(t, def, tt.input).Output)
		})
	}
}

// --- Map state ItemProcessor ---

func TestExecutor_MapState_ItemProcessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		def     string
		input   string
		wantLen int
	}{
		{
			name: "uses_ItemProcessor_field",
			def: `{"StartAt":"Map","States":{"Map":{"Type":"Map","End":true,"ItemProcessor":{` +
				`"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}}}}`,
			input:   `[1,2,3]`,
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := execute(t, tt.def, tt.input)
			assert.Empty(t, result.Error)
			arr, ok := result.Output.([]any)
			require.True(t, ok)
			assert.Len(t, arr, tt.wantLen)
		})
	}
}

// --- Intrinsic functions ---

func TestIntrinsic_FormatAndConversions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assertFunc func(t *testing.T, output any)
		name       string
		params     string
		input      string
	}{
		{
			name:   "Format_basic",
			params: `{"msg.$":"States.Format('Hello {}', $.name)"}`,
			input:  `{"name":"world"}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Hello world", m["msg"])
			},
		},
		{
			name:   "Format_multiple_placeholders",
			params: `{"msg.$":"States.Format('{} is {}', $.a, $.b)"}`,
			input:  `{"a":"sky","b":"blue"}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "sky is blue", m["msg"])
			},
		},
		{
			name:   "StringToJson",
			params: `{"parsed.$":"States.StringToJson($.s)"}`,
			input:  `{"s":"{\"k\":1}"}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				parsed, ok := m["parsed"].(map[string]any)
				require.True(t, ok)
				assert.InDelta(t, float64(1), parsed["k"], 1e-9)
			},
		},
		{
			name:   "JsonToString",
			params: `{"s.$":"States.JsonToString($.obj)"}`,
			input:  `{"obj":{"k":1}}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				s, ok := m["s"].(string)
				require.True(t, ok)
				assert.Contains(t, s, "k")
			},
		},
		{
			name:   "Array_builds_array",
			params: `{"arr.$":"States.Array($.a, $.b)"}`,
			input:  `{"a":1,"b":2}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				arr, ok := m["arr"].([]any)
				require.True(t, ok)
				assert.Len(t, arr, 2)
			},
		},
		{
			name:   "ArrayLength",
			params: `{"len.$":"States.ArrayLength($.arr)"}`,
			input:  `{"arr":[1,2,3]}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.InDelta(t, float64(3), m["len"], 1e-9)
			},
		},
		{
			name:   "ArrayContains_true",
			params: `{"has.$":"States.ArrayContains($.arr, $.val)"}`,
			input:  `{"arr":[1,2,3],"val":2}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, m["has"])
			},
		},
		{
			name:   "ArrayContains_false",
			params: `{"has.$":"States.ArrayContains($.arr, $.val)"}`,
			input:  `{"arr":[1,2,3],"val":99}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, false, m["has"])
			},
		},
		{
			name:   "ArrayPartition",
			params: `{"chunks.$":"States.ArrayPartition($.arr, 2)"}`,
			input:  `{"arr":[1,2,3,4,5]}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				chunks, ok := m["chunks"].([]any)
				require.True(t, ok)
				assert.Len(t, chunks, 3)
			},
		},
		{
			name:   "MathRandom_in_range",
			params: `{"r.$":"States.MathRandom(1, 10)"}`,
			input:  `{}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				r, ok := m["r"].(float64)
				require.True(t, ok)
				assert.GreaterOrEqual(t, r, float64(1))
				assert.LessOrEqual(t, r, float64(10))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":` +
				tt.params + `,"End":true}}}`
			result := execute(t, def, tt.input)
			require.Empty(t, result.Error)
			tt.assertFunc(t, result.Output)
		})
	}
}

func TestIntrinsic_Base64AndHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assertFunc func(t *testing.T, output any)
		name       string
		params     string
		input      string
	}{
		{
			name:   "Base64Encode_and_Decode",
			params: `{"encoded.$":"States.Base64Encode($.text)"}`,
			input:  `{"text":"hello"}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				encoded, ok := m["encoded"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, encoded)
			},
		},
		{
			name:   "Hash_SHA256_length",
			params: `{"h.$":"States.Hash($.data, 'SHA-256')"}`,
			input:  `{"data":"hello"}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				h, ok := m["h"].(string)
				require.True(t, ok)
				assert.Len(t, h, 64)
			},
		},
		{
			name:   "Hash_MD5_length",
			params: `{"h.$":"States.Hash($.data, 'MD5')"}`,
			input:  `{"data":"hello"}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				h, ok := m["h"].(string)
				require.True(t, ok)
				assert.Len(t, h, 32)
			},
		},
		{
			name:   "Hash_SHA1_length",
			params: `{"h.$":"States.Hash($.data, 'SHA-1')"}`,
			input:  `{"data":"hello"}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				h, ok := m["h"].(string)
				require.True(t, ok)
				assert.Len(t, h, 40)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":` +
				tt.params + `,"End":true}}}`
			result := execute(t, def, tt.input)
			require.Empty(t, result.Error)
			tt.assertFunc(t, result.Output)
		})
	}
}

func TestExecutor_Parameters_TemplateEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assertFunc func(t *testing.T, output any)
		name       string
		params     string
		input      string
	}{
		{
			name:   "nested_object",
			params: `{"outer":{"inner.$":"$.value","static":42}}`,
			input:  `{"value":"hello"}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				outer, ok := m["outer"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "hello", outer["inner"])
				assert.InDelta(t, float64(42), outer["static"], 1e-9)
			},
		},
		{
			name:   "array_value",
			params: `{"items":[1,2,3]}`,
			input:  `{}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				items, ok := m["items"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 3)
			},
		},
		{
			name:   "pass_output_path",
			params: "",
			input:  `{"a":1,"b":2}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				assert.InDelta(t, float64(1), output, 1e-9)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var def string
			if tt.name == "pass_output_path" {
				def = `{"StartAt":"P","States":{"P":{"Type":"Pass",` +
					`"Result":{"a":1,"b":2},"OutputPath":"$.a","End":true}}}`
			} else {
				def = `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":` +
					tt.params + `,"End":true}}}`
			}
			result := execute(t, def, tt.input)
			tt.assertFunc(t, result.Output)
		})
	}
}

// mockHistoryRecorder captures history events for assertions.
type mockHistoryRecorder struct {
	entered   []string
	exited    []string
	scheduled []string
	succeeded []string
	failed    []string
}

func (r *mockHistoryRecorder) RecordStateEntered(_, stateName, _ string, _ any) {
	r.entered = append(r.entered, stateName)
}

func (r *mockHistoryRecorder) RecordStateExited(_, stateName, _ string, _ any) {
	r.exited = append(r.exited, stateName)
}

func (r *mockHistoryRecorder) RecordTaskScheduled(_, stateName, _ string) {
	r.scheduled = append(r.scheduled, stateName)
}

func (r *mockHistoryRecorder) RecordTaskSucceeded(_, stateName string, _ any) {
	r.succeeded = append(r.succeeded, stateName)
}

func (r *mockHistoryRecorder) RecordTaskFailed(_, stateName, _, _ string) {
	r.failed = append(r.failed, stateName)
}

func executeWithHistory(
	t *testing.T,
	def, input string,
	lambda asl.LambdaInvoker,
	rec *mockHistoryRecorder,
) *asl.ExecutionResult {
	t.Helper()
	sm, err := asl.Parse(def)
	require.NoError(t, err)
	exec := asl.NewExecutor(sm, lambda, rec)
	result, err := exec.Execute(t.Context(), "test-exec", input)
	require.NoError(t, err)

	return result
}

func TestExecutor_TaskHistoryRecording(t *testing.T) {
	t.Parallel()

	const taskDef = `{"StartAt":"T","States":{"T":{"Type":"Task",` +
		`"Resource":"arn:aws:lambda:us-east-1:123456789012:function:fn","End":true}}}`

	tests := []struct {
		lambda        asl.LambdaInvoker
		name          string
		wantScheduled []string
		wantSucceeded []string
		wantFailed    []string
		wantErr       bool
	}{
		{
			name:          "success_records_scheduled_and_succeeded",
			lambda:        &mockLambda{response: `{"ok":true}`},
			wantErr:       false,
			wantScheduled: []string{"T"},
			wantSucceeded: []string{"T"},
			wantFailed:    nil,
		},
		{
			name:          "failure_records_scheduled_and_failed",
			lambda:        &mockLambda{returnErr: errLambdaService},
			wantErr:       true,
			wantScheduled: []string{"T"},
			wantSucceeded: nil,
			wantFailed:    []string{"T"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := &mockHistoryRecorder{}
			result := executeWithHistory(t, taskDef, `{}`, tt.lambda, rec)
			if tt.wantErr {
				assert.NotEmpty(t, result.Error)
			} else {
				assert.Empty(t, result.Error)
			}
			assert.Equal(t, tt.wantScheduled, rec.scheduled)
			assert.Equal(t, tt.wantSucceeded, rec.succeeded)
			assert.Equal(t, tt.wantFailed, rec.failed)
		})
	}
}

// TestIntrinsic_Base64Decode tests the Base64Decode intrinsic function.
func TestIntrinsic_Base64Decode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assertFunc func(t *testing.T, output any)
		name       string
		params     string
		input      string
	}{
		{
			name:   "decode_valid",
			params: `{"decoded.$":"States.Base64Decode($.enc)"}`,
			input:  `{"enc":"aGVsbG8="}`,
			assertFunc: func(t *testing.T, output any) {
				t.Helper()
				m, ok := output.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "hello", m["decoded"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":` +
				tt.params + `,"End":true}}}`
			result := execute(t, def, tt.input)
			require.Empty(t, result.Error)
			tt.assertFunc(t, result.Output)
		})
	}
}

func TestExecutor_Wait_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		def     string
		input   string
		wantErr bool
	}{
		{
			name: "SecondsPath_invalid_jsonpath",
			def: `{"StartAt":"W","States":{"W":{"Type":"Wait",` +
				`"SecondsPath":"$.missing","End":true}}}`,
			input:   `{}`,
			wantErr: true,
		},
		{
			name: "SecondsPath_not_number",
			def: `{"StartAt":"W","States":{"W":{"Type":"Wait",` +
				`"SecondsPath":"$.v","End":true}}}`,
			input:   `{"v":"not-a-number"}`,
			wantErr: true,
		},
		{
			name: "TimestampPath_not_string",
			def: `{"StartAt":"W","States":{"W":{"Type":"Wait",` +
				`"TimestampPath":"$.v","End":true}}}`,
			input:   `{"v":42}`,
			wantErr: true,
		},
		{
			name: "TimestampPath_invalid_timestamp",
			def: `{"StartAt":"W","States":{"W":{"Type":"Wait",` +
				`"TimestampPath":"$.v","End":true}}}`,
			input:   `{"v":"not-a-timestamp"}`,
			wantErr: true,
		},
		{
			name: "Timestamp_invalid_format",
			def: `{"StartAt":"W","States":{"W":{"Type":"Wait",` +
				`"Timestamp":"bad-timestamp","End":true}}}`,
			input:   `{}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			exec := asl.NewExecutor(sm, nil, nil)
			_, execErr := exec.Execute(t.Context(), "test", tt.input)
			if tt.wantErr {
				require.Error(t, execErr)
			} else {
				require.NoError(t, execErr)
			}
		})
	}
}

func TestExecutor_OutputTransforms_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		def     string
		input   string
		wantErr bool
	}{
		{
			name: "ResultSelector_invalid_jsonpath",
			def: `{"StartAt":"T","States":{"T":{"Type":"Task",` +
				`"Resource":"arn:aws:lambda:us-east-1:123:function:fn",` +
				`"ResultSelector":{"v.$":"$.missing"},"End":true}}}`,
			input:   `{}`,
			wantErr: true,
		},
		{
			name: "OutputPath_bad_expression",
			def: `{"StartAt":"P","States":{"P":{"Type":"Pass",` +
				`"Result":{"a":1},"OutputPath":"not-a-path","End":true}}}`,
			input:   `{}`,
			wantErr: true,
		},
		{
			name:    "Pass_Result_valid",
			def:     `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"x":42},"End":true}}}`,
			input:   `{}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sm, err := asl.Parse(tt.def)
			require.NoError(t, err)
			lambdaFn := &mockLambda{response: `{"ok":true}`}
			exec := asl.NewExecutor(sm, lambdaFn, nil)
			_, execErr := exec.Execute(t.Context(), "test", tt.input)
			if tt.wantErr {
				require.Error(t, execErr)
			} else {
				require.NoError(t, execErr)
			}
		})
	}
}
