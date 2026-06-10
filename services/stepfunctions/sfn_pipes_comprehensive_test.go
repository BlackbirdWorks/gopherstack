package stepfunctions_test

// Comprehensive tests for Step Functions and EventBridge Pipes improvements:
//   - Parallel state with Catch error handling
//   - Execution history events
//   - State machine versions lifecycle
//   - Wait state with Timestamp/TimestampPath

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// parallelWithCatchDef is a state machine where one Parallel branch always fails,
// and the Parallel state has a Catch clause that routes to a recovery Pass state.
const parallelWithCatchDef = `{
"StartAt": "ParallelStep",
"States": {
  "ParallelStep": {
    "Type": "Parallel",
    "Branches": [
      {
        "StartAt": "GoodBranch",
        "States": {
          "GoodBranch": {"Type": "Pass", "End": true}
        }
      },
      {
        "StartAt": "BadBranch",
        "States": {
          "BadBranch": {"Type": "Fail", "Error": "BranchError", "Cause": "branch failed"}
        }
      }
    ],
    "Catch": [
      {
        "ErrorEquals": ["States.ALL"],
        "Next": "Recovery",
        "ResultPath": "$.error"
      }
    ],
    "Next": "Done"
  },
  "Recovery": {"Type": "Pass", "End": true},
  "Done": {"Type": "Pass", "End": true}
}
}`

// parallelSuccessDef is a state machine with two successful parallel branches.
const parallelSuccessDef = `{
"StartAt": "ParallelStep",
"States": {
  "ParallelStep": {
    "Type": "Parallel",
    "Branches": [
      {
        "StartAt": "A",
        "States": {"A": {"Type": "Pass", "Result": {"val": 1}, "End": true}}
      },
      {
        "StartAt": "B",
        "States": {"B": {"Type": "Pass", "Result": {"val": 2}, "End": true}}
      }
    ],
    "End": true
  }
}
}`

// waitTimestampPastDef uses a Timestamp in the past so the wait completes immediately.
const waitTimestampPastDef = `{
"StartAt": "Wait",
"States": {
  "Wait": {
    "Type": "Wait",
    "Timestamp": "2020-01-01T00:00:00Z",
    "Next": "Done"
  },
  "Done": {"Type": "Pass", "End": true}
}
}`

// choiceAndOrDef tests And/Or combiners in Choice state.
const choiceAndOrDef = `{
"StartAt": "Choose",
"States": {
  "Choose": {
    "Type": "Choice",
    "Choices": [
      {
        "And": [
          {"Variable": "$.x", "NumericGreaterThan": 0},
          {"Variable": "$.y", "StringEquals": "foo"}
        ],
        "Next": "Both"
      },
      {
        "Or": [
          {"Variable": "$.a", "BooleanEquals": true},
          {"Variable": "$.b", "BooleanEquals": true}
        ],
        "Next": "Either"
      }
    ],
    "Default": "Neither"
  },
  "Both":    {"Type": "Pass", "Result": {"match": "both"},   "End": true},
  "Either":  {"Type": "Pass", "Result": {"match": "either"}, "End": true},
  "Neither": {"Type": "Pass", "Result": {"match": "none"},   "End": true}
}
}`

func TestParallelState_WithCatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		definition  string
		input       string
		wantStatus  string
		wantRecover bool
	}{
		{
			name:        "branch_error_caught_by_catch_clause",
			definition:  parallelWithCatchDef,
			input:       `{"x": 1}`,
			wantStatus:  "SUCCEEDED",
			wantRecover: true,
		},
		{
			name:       "all_branches_succeed",
			definition: parallelSuccessDef,
			input:      `{}`,
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"parallel-catch-"+tt.name,
				tt.definition,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "exec-"+tt.name, tt.input)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				d, e := b.DescribeExecution(exec.ExecutionArn)

				return e == nil && d.Status != "RUNNING"
			}, 10*time.Second, 25*time.Millisecond)

			d, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, d.Status, "unexpected execution status")
		})
	}
}

func TestExecutionHistory_Events(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		definition     string
		wantEventTypes []string
	}{
		{
			name:       "pass_state_records_entered_exited",
			definition: exprPassDef,
			wantEventTypes: []string{
				"ExecutionStarted",
				"PassStateEntered",
				"PassStateExited",
				"ExecutionSucceeded",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"hist-"+tt.name,
				tt.definition,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "hist-exec", "{}")
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				d, e := b.DescribeExecution(exec.ExecutionArn)

				return e == nil && d.Status != "RUNNING"
			}, 10*time.Second, 25*time.Millisecond)

			events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 100, false)
			require.NoError(t, err)
			require.NotEmpty(t, events)

			// Collect event types.
			types := make([]string, len(events))
			for i, e := range events {
				types[i] = e.Type
			}

			for _, wantType := range tt.wantEventTypes {
				assert.Contains(t, types, wantType, "expected event type %q in history", wantType)
			}

			// Verify IDs are monotonically increasing.
			for i := 1; i < len(events); i++ {
				assert.Greater(t, events[i].ID, events[i-1].ID, "event IDs should increase")
			}
		})
	}
}

func TestExecutionHistory_ReverseOrder(t *testing.T) {
	t.Parallel()

	b := newSFBackend()
	sm, err := b.CreateStateMachine(context.Background(), "hist-rev", exprPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "rev-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status != "RUNNING"
	}, 10*time.Second, 25*time.Millisecond)

	forward, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 100, false)
	require.NoError(t, err)

	reverse, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 100, true)
	require.NoError(t, err)

	require.Len(t, reverse, len(forward))

	// Reverse order means IDs should decrease.
	for i := 1; i < len(reverse); i++ {
		assert.Less(t, reverse[i].ID, reverse[i-1].ID)
	}
}

func TestStateMachineVersions_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		publishN    int
		deleteFirst bool
	}{
		{
			name:     "publish_two_versions",
			publishN: 2,
		},
		{
			name:        "publish_and_delete_version",
			publishN:    1,
			deleteFirst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"ver-sm-"+tt.name,
				exprPassDef,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			var lastVersionARN string

			for i := range tt.publishN {
				v, pubErr := b.PublishStateMachineVersion(sm.StateMachineArn,
					"version "+string(rune('A'+i)), "")
				require.NoError(t, pubErr)
				require.NotEmpty(t, v.StateMachineVersionArn)
				assert.Contains(t, v.StateMachineVersionArn, sm.StateMachineArn)
				lastVersionARN = v.StateMachineVersionArn
			}

			// List versions.
			versions, _, err := b.ListStateMachineVersions(sm.StateMachineArn, "", 100)
			require.NoError(t, err)
			assert.Len(t, versions, tt.publishN)

			// Describe version.
			described, err := b.DescribeStateMachineVersion(lastVersionARN)
			require.NoError(t, err)
			assert.Equal(t, sm.StateMachineArn, described.StateMachineArn)
			assert.JSONEq(t, exprPassDef, described.Definition)

			if tt.deleteFirst {
				err = b.DeleteStateMachineVersion(lastVersionARN)
				require.NoError(t, err)

				versions, _, err = b.ListStateMachineVersions(sm.StateMachineArn, "", 100)
				require.NoError(t, err)
				assert.Empty(t, versions)

				_, err = b.DescribeStateMachineVersion(lastVersionARN)
				require.ErrorIs(t, err, stepfunctions.ErrStateMachineVersionDoesNotExist)
			}
		})
	}
}

func TestWaitState_TimestampPast(t *testing.T) {
	t.Parallel()

	b := newSFBackend()
	sm, err := b.CreateStateMachine(context.Background(), "wait-ts", waitTimestampPastDef, "arn:role", "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "wait-exec", `{"x": 1}`)
	require.NoError(t, err)

	// Past timestamp → should complete quickly.
	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 25*time.Millisecond)
}

func TestChoiceState_AndOrCombiners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantMatch string
	}{
		{
			name:      "and_combiner_both_conditions_true",
			input:     `{"x": 5, "y": "foo", "a": false, "b": false}`,
			wantMatch: "both",
		},
		{
			name:      "or_combiner_one_condition_true",
			input:     `{"x": -1, "y": "bar", "a": true, "b": false}`,
			wantMatch: "either",
		},
		{
			name:      "default_when_no_match",
			input:     `{"x": -1, "y": "bar", "a": false, "b": false}`,
			wantMatch: "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			sm, err := b.CreateStateMachine(
				context.Background(),
				"choice-"+tt.name,
				choiceAndOrDef,
				"arn:role",
				"EXPRESS",
			)
			require.NoError(t, err)

			result, err := b.StartSyncExecution(sm.StateMachineArn, "", tt.input)
			require.NoError(t, err)
			require.Equal(t, "SUCCEEDED", result.Status)
			assert.Contains(t, result.Output, tt.wantMatch)
		})
	}
}
