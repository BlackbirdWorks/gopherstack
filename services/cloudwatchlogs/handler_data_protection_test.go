package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestHandler_DeleteAccountPolicy_NonExistentSucceeds(t *testing.T) {
	t.Parallel()

	// DeleteAccountPolicy is idempotent: deleting a non-existent policy
	// should succeed (no error).
	rec := makeLogsRequest(t, "DeleteAccountPolicy",
		`{"policyName":"does-not-exist","policyType":"DATA_PROTECTION_POLICY"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteAccountPolicy_InvalidPolicyType(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "DeleteAccountPolicy",
		`{"policyName":"my-policy","policyType":"INVALID_TYPE"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DataProtectionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutDataProtectionPolicy/OK",
			action: "PutDataProtectionPolicy",
			body: map[string]any{
				"logGroupIdentifier": "/aws/lambda/fn",
				"policyDocument":     `{"Name":"protect","Version":"2021-06-01","Statement":[]}`,
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GetDataProtectionPolicy/OK",
			action: "GetDataProtectionPolicy",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutDataProtectionPolicy",
					`{"logGroupIdentifier":"/aws/lambda/fn","policyDocument":"{\"Name\":\"protect\"}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDataProtectionPolicy/OK",
			action: "DeleteDataProtectionPolicy",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutDataProtectionPolicy",
					`{"logGroupIdentifier":"/aws/lambda/fn","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteAccountPolicyOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantKey  string
		wantVal  string
		wantCode int
	}{
		// DeleteAccountPolicy
		{
			name:   "DeleteAccountPolicy/OK",
			action: "DeleteAccountPolicy",
			body: map[string]any{
				"policyName": "my-policy",
				"policyType": "DATA_PROTECTION_POLICY",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteAccountPolicy/MissingPolicyName",
			action:   "DeleteAccountPolicy",
			body:     map[string]any{"policyType": "DATA_PROTECTION_POLICY"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteAccountPolicy/MissingPolicyType",
			action:   "DeleteAccountPolicy",
			body:     map[string]any{"policyName": "my-policy"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.wantKey != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				if tt.wantVal != "" {
					assert.Equal(t, tt.wantVal, out[tt.wantKey])
				} else {
					assert.NotEmpty(t, out[tt.wantKey], "expected non-empty %s", tt.wantKey)
				}
			}
		})
	}
}
