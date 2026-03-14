package lambda_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestHandler_FISActions verifies that the Lambda handler declares exactly the two
// documented FIS actions and that their fields are populated.
func TestHandler_FISActions(t *testing.T) {
	t.Parallel()

	h := lambda.NewHandler(newSimpleBackend())

	actions := h.FISActions()
	require.Len(t, actions, 2)

	ids := make([]string, 0, len(actions))
	for _, a := range actions {
		ids = append(ids, a.ActionID)
	}

	assert.Contains(t, ids, "aws:lambda:invocation-error")
	assert.Contains(t, ids, "aws:lambda:invocation-add-delay")

	for _, a := range actions {
		assert.NotEmpty(t, a.Description, "action %s must have a description", a.ActionID)
		assert.NotEmpty(t, a.TargetType, "action %s must have a target type", a.ActionID)
		assert.NotEmpty(t, a.Parameters, "action %s must declare parameters", a.ActionID)
	}
}

// TestHandler_ExecuteFISAction_InvocationError verifies that executing the
// invocation-error action installs a FIS fault on the named function and that
// the fault is cleared when the action's duration expires or the context is
// cancelled, whichever comes first.
func TestHandler_ExecuteFISAction_InvocationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		parameters map[string]string
		name       string
		targets    []string
		duration   time.Duration
		wantFault  bool
	}{
		{
			name:       "installs_fault_100pct",
			targets:    []string{"my-function"},
			parameters: map[string]string{"percentage": "100"},
			duration:   10 * time.Second,
			wantFault:  true,
		},
		{
			name:       "installs_fault_arn_target",
			targets:    []string{"arn:aws:lambda:us-east-1:123456789012:function:my-fn"},
			parameters: map[string]string{"percentage": "50"},
			duration:   10 * time.Second,
			wantFault:  true,
		},
		{
			name:       "unknown_action_is_no_op",
			targets:    []string{"my-function"},
			parameters: map[string]string{},
			duration:   10 * time.Second,
			wantFault:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newSimpleBackend()
			h := lambda.NewHandler(bk)

			actionID := "aws:lambda:invocation-error"
			if !tt.wantFault {
				actionID = "aws:lambda:unknown-action"
			}

			err := h.ExecuteFISAction(t.Context(), service.FISActionExecution{
				ActionID:   actionID,
				Targets:    tt.targets,
				Parameters: tt.parameters,
				Duration:   tt.duration,
			})
			require.NoError(t, err)

			// Resolve the function name from the first target.
			funcName := lambda.FunctionNamesFromARNs([]string{tt.targets[0]})
			require.NotEmpty(t, funcName)

			fault := lambda.CheckFISFault(bk, funcName[0])
			if tt.wantFault {
				assert.NotNil(t, fault, "fault should be installed after ExecuteFISAction")
			} else {
				assert.Nil(t, fault, "no fault should be installed for unknown action")
			}
		})
	}
}

// TestHandler_ExecuteFISAction_InvocationDelay verifies that executing the
// invocation-add-delay action installs a FIS delay fault on the named function.
func TestHandler_ExecuteFISAction_InvocationDelay(t *testing.T) {
	t.Parallel()

	bk := newSimpleBackend()
	h := lambda.NewHandler(bk)

	err := h.ExecuteFISAction(t.Context(), service.FISActionExecution{
		ActionID: "aws:lambda:invocation-add-delay",
		Targets:  []string{"delay-fn"},
		Parameters: map[string]string{
			"invocationDelayMilliseconds": "50",
			"percentage":                  "100",
		},
		Duration: 10 * time.Second,
	})
	require.NoError(t, err)

	fault := lambda.CheckFISFault(bk, "delay-fn")
	require.NotNil(t, fault, "fault should be installed")
	assert.Equal(t, 50, fault.AddDelayMs)
}

// TestHandler_ExecuteFISAction_FaultExpiry verifies that a fault installed with
// a duration is automatically removed after the duration elapses.
func TestHandler_ExecuteFISAction_FaultExpiry(t *testing.T) {
	t.Parallel()

	bk := newSimpleBackend()
	h := lambda.NewHandler(bk)

	err := h.ExecuteFISAction(t.Context(), service.FISActionExecution{
		ActionID:   "aws:lambda:invocation-error",
		Targets:    []string{"expiry-fn"},
		Parameters: map[string]string{"percentage": "100"},
		Duration:   50 * time.Millisecond,
	})
	require.NoError(t, err)

	require.NotNil(t, lambda.CheckFISFault(bk, "expiry-fn"), "fault must be installed immediately")

	// Wait for the goroutine that clears the fault.
	require.Eventually(t, func() bool {
		return lambda.CheckFISFault(bk, "expiry-fn") == nil
	}, 2*time.Second, 10*time.Millisecond, "fault should be cleared after duration")
}

// TestHandler_ExecuteFISAction_FaultClearedOnContextCancel verifies that a fault
// installed with a duration is cleared when the action context is cancelled.
func TestHandler_ExecuteFISAction_FaultClearedOnContextCancel(t *testing.T) {
	t.Parallel()

	bk := newSimpleBackend()
	h := lambda.NewHandler(bk)

	ctx, cancel := context.WithCancel(t.Context())

	err := h.ExecuteFISAction(ctx, service.FISActionExecution{
		ActionID:   "aws:lambda:invocation-error",
		Targets:    []string{"ctx-cancel-fn"},
		Parameters: map[string]string{"percentage": "100"},
		Duration:   30 * time.Second, // long duration, cleared by cancel
	})
	require.NoError(t, err)

	require.NotNil(t, lambda.CheckFISFault(bk, "ctx-cancel-fn"), "fault must be installed initially")

	cancel()

	require.Eventually(t, func() bool {
		return lambda.CheckFISFault(bk, "ctx-cancel-fn") == nil
	}, 2*time.Second, 10*time.Millisecond, "fault should be cleared after context cancel")
}

// TestFIS_SetAndClearFault verifies direct set/clear/check of FIS faults on the backend.
func TestFIS_SetAndClearFault(t *testing.T) {
	t.Parallel()

	bk := newSimpleBackend()

	assert.Nil(t, lambda.CheckFISFault(bk, "fn"), "no fault initially")

	fault := &lambda.FISInvocationFault{ErrorProbability: 1.0}
	lambda.SetFISFault(bk, "fn", fault)

	got := lambda.CheckFISFault(bk, "fn")
	require.NotNil(t, got)
	assert.InDelta(t, 1.0, got.ErrorProbability, 0.001)

	lambda.ClearFISFault(bk, "fn")

	assert.Nil(t, lambda.CheckFISFault(bk, "fn"), "fault should be cleared")
}

// TestFIS_CheckFault_ExpiredFaultRemoved verifies that an expired fault is automatically
// cleared and nil is returned by checkFISFault.
func TestFIS_CheckFault_ExpiredFaultRemoved(t *testing.T) {
	t.Parallel()

	bk := newSimpleBackend()

	// Install a fault that expired in the past.
	lambda.SetFISFault(bk, "fn", &lambda.FISInvocationFault{
		ErrorProbability: 1.0,
		Expiry:           time.Now().Add(-1 * time.Second), // already expired
	})

	assert.Nil(t, lambda.CheckFISFault(bk, "fn"), "expired fault should return nil")
}

// TestFIS_FunctionNamesFromARNs verifies extraction of function names from ARNs and bare names.
func TestFIS_FunctionNamesFromARNs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arns []string
		want []string
	}{
		{
			name: "full_arn",
			arns: []string{"arn:aws:lambda:us-east-1:123456789012:function:my-fn"},
			want: []string{"my-fn"},
		},
		{
			name: "bare_name",
			arns: []string{"my-function"},
			want: []string{"my-function"},
		},
		{
			name: "mixed",
			arns: []string{
				"arn:aws:lambda:us-east-1:123456789012:function:fn-one",
				"fn-two",
			},
			want: []string{"fn-one", "fn-two"},
		},
		{
			name: "empty_string_skipped",
			arns: []string{"", "valid"},
			want: []string{"valid"},
		},
		{
			name: "arn_with_empty_name_treated_as_bare_name",
			arns: []string{"arn:aws:lambda:us-east-1:123456789012:function:"},
			// Malformed ARN: the function name part is empty, so the whole ARN string
			// falls through to the bare-name path and is returned unchanged.
			want: []string{"arn:aws:lambda:us-east-1:123456789012:function:"},
		},
		{
			name: "empty_slice",
			arns: []string{},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lambda.FunctionNamesFromARNs(tt.arns)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFIS_ParseInvocationPercentage verifies the percentage→probability conversion.
func TestFIS_ParseInvocationPercentage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  float64
	}{
		{name: "empty_defaults_to_100pct", input: "", want: 1.0},
		{name: "100_returns_1.0", input: "100", want: 1.0},
		{name: "50_returns_0.5", input: "50", want: 0.5},
		{name: "0_returns_0.0", input: "0", want: 0.0},
		{name: "negative_defaults_to_1.0", input: "-1", want: 1.0},
		{name: "invalid_string_defaults_to_1.0", input: "abc", want: 1.0},
		{name: "10_returns_0.1", input: "10", want: 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lambda.ParseInvocationPercentage(tt.input)
			assert.InDelta(t, tt.want, got, 0.0001)
		})
	}
}

// TestFIS_ParseInvocationDelayMs verifies parsing of delay milliseconds.
func TestFIS_ParseInvocationDelayMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  int
	}{
		{name: "empty_returns_0", input: "", want: 0},
		{name: "valid_100ms", input: "100", want: 100},
		{name: "zero", input: "0", want: 0},
		{name: "invalid_returns_0", input: "abc", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lambda.ParseInvocationDelayMs(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFIS_ParseIntSafe verifies integer parsing with error propagation.
func TestFIS_ParseIntSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    int
		wantErr bool
	}{
		{name: "valid", input: "42", want: 42},
		{name: "zero", input: "0", want: 0},
		{name: "negative", input: "-5", want: -5},
		{name: "not_an_integer", input: "abc", wantErr: true},
		{name: "float_string", input: "3.14", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out int
			err := lambda.ParseIntSafe(tt.input, &out)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, out)
		})
	}
}

// TestFIS_ExpiryFromDuration verifies duration-to-expiry-time conversion.
func TestFIS_ExpiryFromDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		d        time.Duration
		wantZero bool
	}{
		{name: "zero_duration_returns_zero_time", d: 0, wantZero: true},
		{name: "negative_returns_zero_time", d: -1 * time.Second, wantZero: true},
		{name: "positive_returns_future_time", d: 5 * time.Minute, wantZero: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			before := time.Now()
			expiry := lambda.ExpiryFromDuration(tt.d)

			if tt.wantZero {
				assert.True(t, expiry.IsZero(), "expected zero time for d=%v", tt.d)

				return
			}

			assert.True(t, expiry.After(before), "expiry must be after the call time")
		})
	}
}

// TestBackend_ConcurrencySlot verifies acquireConcurrencySlot and releaseConcurrencySlot behavior.
func TestBackend_ConcurrencySlot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		invocationType  lambda.InvocationType
		reservedLimit   int
		acquireTimes    int
		wantAcquired    bool
		wantErrOnExceed bool
	}{
		{
			name:           "no_limit_always_returns_false",
			reservedLimit:  -1, // skip PutFunctionConcurrency
			invocationType: lambda.InvocationTypeRequestResponse,
			acquireTimes:   1,
			wantAcquired:   false,
		},
		{
			name:            "reserved_zero_returns_too_many_requests",
			reservedLimit:   0,
			invocationType:  lambda.InvocationTypeRequestResponse,
			acquireTimes:    1,
			wantAcquired:    false,
			wantErrOnExceed: true,
		},
		{
			name:           "event_type_never_acquires_slot",
			reservedLimit:  5,
			invocationType: lambda.InvocationTypeEvent,
			acquireTimes:   1,
			wantAcquired:   false,
		},
		{
			name:            "exceeds_reserved_concurrency_limit",
			reservedLimit:   1,
			invocationType:  lambda.InvocationTypeRequestResponse,
			acquireTimes:    2,
			wantAcquired:    true,
			wantErrOnExceed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newSimpleBackend()

			const fnName = "test-fn"

			// Create the function first so PutFunctionConcurrency can find it.
			require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: fnName}))

			if tt.reservedLimit >= 0 {
				_, err := bk.PutFunctionConcurrency(fnName, tt.reservedLimit)
				require.NoError(t, err)
			}

			// Acquire slots up to acquireTimes-1 (expected to succeed) to reach the boundary.
			for range tt.acquireTimes - 1 {
				_, priorErr := lambda.AcquireConcurrencySlot(bk, fnName, tt.invocationType)
				require.NoError(t, priorErr, "setup acquires must not fail")
			}

			// The final acquisition is the one under test.
			lastAcquired, lastErr := lambda.AcquireConcurrencySlot(bk, fnName, tt.invocationType)

			if tt.wantErrOnExceed {
				require.Error(t, lastErr)

				return
			}

			require.NoError(t, lastErr)
			assert.Equal(t, tt.wantAcquired, lastAcquired)
		})
	}
}

// TestBackend_ReleaseConcurrencySlot verifies that releasing a slot decrements the counter.
func TestBackend_ReleaseConcurrencySlot(t *testing.T) {
	t.Parallel()

	bk := newSimpleBackend()

	const fnName = "release-fn"

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: fnName}))

	_, err := bk.PutFunctionConcurrency(fnName, 2)
	require.NoError(t, err)

	// Acquire one slot.
	acquired, acquireErr := lambda.AcquireConcurrencySlot(bk, fnName, lambda.InvocationTypeRequestResponse)
	require.NoError(t, acquireErr)
	assert.True(t, acquired, "first acquire should succeed")

	// Release it.
	lambda.ReleaseConcurrencySlot(bk, fnName)

	// After release, we should still be able to acquire up to the limit again.
	acquired2, acquireErr2 := lambda.AcquireConcurrencySlot(bk, fnName, lambda.InvocationTypeRequestResponse)
	require.NoError(t, acquireErr2)
	assert.True(t, acquired2, "acquire after release should succeed")
}

// TestBackend_ReleaseConcurrencySlot_NopOnNoLimit verifies that calling
// releaseConcurrencySlot when no limit is set does not panic.
func TestBackend_ReleaseConcurrencySlot_NopOnNoLimit(t *testing.T) {
	t.Parallel()

	bk := newSimpleBackend()

	// Should be a no-op without panicking.
	assert.NotPanics(t, func() {
		lambda.ReleaseConcurrencySlot(bk, "no-limit-fn")
	})
}
