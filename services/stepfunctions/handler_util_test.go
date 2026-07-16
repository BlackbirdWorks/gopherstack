package stepfunctions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ValidateStateMachineDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		wantResult string
		wantCode   int
		wantDiags  bool
	}{
		{
			name: "valid definition returns OK result with no diagnostics",
			definition: `{
"StartAt": "S",
"States": {"S": {"Type": "Pass", "End": true}}
}`,
			wantCode:   http.StatusOK,
			wantResult: "OK",
			wantDiags:  false,
		},
		{
			name:       "invalid definition returns FAIL result with diagnostics",
			definition: `{"StartAt": "Missing", "States": {"S": {"Type": "Pass", "End": true}}}`,
			wantCode:   http.StatusOK,
			wantResult: "FAIL",
			wantDiags:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			reqBody, err := json.Marshal(map[string]string{"definition": tt.definition})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "ValidateStateMachineDefinition", string(reqBody))
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantResult, resp["result"])

			diag, ok := resp["diagnostics"].([]any)
			require.True(t, ok)

			if tt.wantDiags {
				assert.NotEmpty(t, diag)
			} else {
				assert.Empty(t, diag)
			}
		})
	}
}

func TestValidateStateMachineDefinition_Valid(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body, err := json.Marshal(map[string]any{"definition": validPassDef})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "ValidateStateMachineDefinition", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestValidateStateMachineDefinition_Invalid(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body, err := json.Marshal(map[string]any{"definition": `{"not":"valid-asl"}`})
	require.NoError(t, err)

	// AWS returns 200 with Result:"FAIL" and diagnostics — not a 4xx.
	rec := sfnPost(ctx, t, h, e, "ValidateStateMachineDefinition", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "FAIL", out["result"])
}

// ─── MapRun stubs ─────────────────────────────────────────────────────────────

// TestParity_TestStateNextState verifies that TestState returns NextState
// for non-terminal states and an empty nextState for terminal (End:true) states.
func TestTestState_NextState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		definition    string
		input         string
		wantStatus    string
		wantNextState string
		wantError     string
		wantHTTPCode  int
	}{
		{
			name:          "End:true yields empty nextState",
			definition:    `{"MyState":{"Type":"Pass","End":true}}`,
			input:         `{"x":1}`,
			wantHTTPCode:  http.StatusOK,
			wantStatus:    "SUCCEEDED",
			wantNextState: "",
		},
		{
			name:          "Next:AnotherState yields nextState",
			definition:    `{"MyState":{"Type":"Pass","Next":"AnotherState"}}`,
			input:         `{"x":1}`,
			wantHTTPCode:  http.StatusOK,
			wantStatus:    "SUCCEEDED",
			wantNextState: "AnotherState",
		},
		{
			name:         "Fail state yields status FAILED",
			definition:   `{"FailState":{"Type":"Fail","Error":"MyError","Cause":"test"}}`,
			input:        `{}`,
			wantHTTPCode: http.StatusOK,
			wantStatus:   "FAILED",
			wantError:    "MyError",
		},
		{
			name: "Two states in definition yields error",
			// Two states is invalid for TestState (must be exactly one).
			definition:   `{"State1":{"Type":"Pass","End":true},"State2":{"Type":"Pass","End":true}}`,
			input:        `{}`,
			wantHTTPCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newSFNHandler(t)
			ctx := context.Background()

			body, err := json.Marshal(map[string]any{
				"definition": tt.definition,
				"input":      tt.input,
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "TestState", string(body))
			require.Equal(t, tt.wantHTTPCode, rec.Code)

			if tt.wantHTTPCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.Equal(t, tt.wantStatus, resp["status"])

			// nextState may be absent (nil) or empty string — both are fine for "".
			gotNextState, _ := resp["nextState"].(string)
			assert.Equal(t, tt.wantNextState, gotNextState)

			if tt.wantError != "" {
				assert.Equal(t, tt.wantError, resp["error"])
			}
		})
	}
}

// TestRefinement1_ValidateStateMachineDefinition tests the ValidateStateMachineDefinition handler.
func TestValidateStateMachineDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		def        string
		wantResult string
		wantStatus int
	}{
		{
			name:       "valid_definition",
			def:        validPassDef,
			wantResult: "OK",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_definition",
			def:        `{"StartAt":"Missing"}`,
			wantResult: "FAIL",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newSFNHandler(t)
			body, err := json.Marshal(map[string]string{"definition": tt.def})
			require.NoError(t, err)

			rec := sfnPost(t.Context(), t, h, e, "ValidateStateMachineDefinition", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantResult, resp["result"])
		})
	}
}
