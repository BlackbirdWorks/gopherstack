package stepfunctions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CreateStateMachineRequiresRoleArn verifies that CreateStateMachine
// rejects requests with a missing or empty roleArn.
// Real AWS returns ValidationException for this case; the emulator previously
// accepted an empty roleArn, silently creating a state machine without a role.
func TestParity_CreateStateMachineRequiresRoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name: "absent_roleArn_rejected",
			body: map[string]any{
				"name":       "my-sm",
				"definition": validPassDef,
			},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name: "empty_roleArn_rejected",
			body: map[string]any{
				"name":       "my-sm",
				"definition": validPassDef,
				"roleArn":    "",
			},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name: "valid_roleArn_accepted",
			body: map[string]any{
				"name":       "my-sm",
				"definition": validPassDef,
				"roleArn":    "arn:aws:iam::123456789012:role/MyRole",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newSFNHandler(t)
			ctx := context.Background()

			body, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateStateMachine status for case %q", tt.name)

			if tt.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["__type"],
					"error type for case %q", tt.name)
			}
		})
	}
}
