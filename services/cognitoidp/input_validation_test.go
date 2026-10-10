package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInputValidation_ErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     func(poolID, clientID string) map[string]any
		name     string
		action   string
		wantType string
	}{
		{
			name: "malformed pool id", action: "AdminGetUser", wantType: "InvalidParameterException",
			body: func(_, _ string) map[string]any {
				return map[string]any{"UserPoolId": "bogus", "Username": "u"}
			},
		},
		{
			name: "well formed unknown pool", action: "AdminGetUser", wantType: "ResourceNotFoundException",
			body: func(_, _ string) map[string]any {
				return map[string]any{"UserPoolId": "us-east-1_zzzzzzzzz", "Username": "u"}
			},
		},
		{
			name: "malformed client id", action: "SignUp", wantType: "InvalidParameterException",
			body: func(_, _ string) map[string]any {
				return map[string]any{"ClientId": "bad client!", "Username": "u", "Password": "Passw0rd!x"}
			},
		},
		{
			name: "sign up weak password default policy", action: "SignUp", wantType: "InvalidPasswordException",
			body: func(_, clientID string) map[string]any {
				return map[string]any{"ClientId": clientID, "Username": "u", "Password": "short"}
			},
		},
		{
			name: "sign up missing symbol", action: "SignUp", wantType: "InvalidPasswordException",
			body: func(_, clientID string) map[string]any {
				return map[string]any{"ClientId": clientID, "Username": "u", "Password": "Passw0rdxxx"}
			},
		},
		{
			name: "admin create weak temp password", action: "AdminCreateUser", wantType: "InvalidPasswordException",
			body: func(poolID, _ string) map[string]any {
				return map[string]any{"UserPoolId": poolID, "Username": "u", "TemporaryPassword": "abc"}
			},
		},
		{
			name: "admin set weak password", action: "AdminSetUserPassword", wantType: "InvalidPasswordException",
			body: func(poolID, _ string) map[string]any {
				return map[string]any{"UserPoolId": poolID, "Username": "seed", "Password": "short", "Permanent": true}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupHandlerPoolAndClient(t, h, "iv-pool")

			seed := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId": poolID, "Username": "seed", "TemporaryPassword": "Passw0rd!x",
			})
			require.Equal(t, http.StatusOK, seed.Code)

			rec := doCognitoRequest(t, h, tc.action, tc.body(poolID, clientID))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tc.wantType, errResp.Type)
		})
	}
}
