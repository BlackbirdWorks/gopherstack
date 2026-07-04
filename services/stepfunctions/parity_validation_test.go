package stepfunctions_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateStateMachine_NameValidation asserts InvalidName for malformed names.
func TestCreateStateMachine_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		smName   string
		wantType string
		wantCode int
	}{
		{name: "valid_name", smName: "my-state-machine", wantCode: http.StatusOK},
		{name: "empty_name", smName: "", wantCode: http.StatusBadRequest, wantType: "InvalidName"},
		{
			name:     "too_long_81_chars",
			smName:   strings.Repeat("a", 81),
			wantCode: http.StatusBadRequest,
			wantType: "InvalidName",
		},
		{name: "invalid_chars", smName: "bad<name>!", wantCode: http.StatusBadRequest, wantType: "InvalidName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newSFNHandler(t)
			ctx := t.Context()

			body, err := json.Marshal(map[string]any{
				"name":       tt.smName,
				"definition": validPassDef,
				"roleArn":    "arn:aws:iam::123456789012:role/role",
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}

// TestUpdateStateMachine_ArnValidation asserts ValidationException for missing/empty ARN.
func TestUpdateStateMachine_ArnValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		wantType string
		wantCode int
	}{
		{
			name:     "empty_arn_returns_ValidationException",
			arn:      "",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "nonexistent_arn_returns_StateMachineDoesNotExist",
			arn:      "arn:aws:states:us-east-1:123456789012:stateMachine:no-such",
			wantCode: http.StatusNotFound,
			wantType: "StateMachineDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSFNHandler(t)
			e := echo.New()
			ctx := t.Context()

			body, err := json.Marshal(map[string]any{
				"stateMachineArn": tt.arn,
				"definition":      validPassDef,
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "UpdateStateMachine", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}
