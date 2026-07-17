package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body          map[string]any
		name          string
		action        string
		wantBodyField string
		wantCode      int
	}{
		{
			name:     "PutResourcePolicy/OK",
			action:   "PutResourcePolicy",
			body:     map[string]any{"policyName": "my-policy", "policyDocument": `{"Version":"2012-10-17"}`},
			wantCode: http.StatusOK,
		},
		{
			name:     "PutResourcePolicy/EmptyName",
			action:   "PutResourcePolicy",
			body:     map[string]any{"policyName": "", "policyDocument": `{}`},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeResourcePolicies/Empty",
			action:   "DescribeResourcePolicies",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeResourcePolicies/WithEntries",
			action: "DescribeResourcePolicies",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutResourcePolicy",
					`{"policyName":"p1","policyDocument":"{}"}`)
				doLogsRequest(t, h, e, "PutResourcePolicy",
					`{"policyName":"p2","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteResourcePolicy/OK",
			action: "DeleteResourcePolicy",
			body:   map[string]any{"policyName": "my-policy"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutResourcePolicy",
					`{"policyName":"my-policy","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteResourcePolicy/NotFound",
			action:   "DeleteResourcePolicy",
			body:     map[string]any{"policyName": "ghost"},
			wantCode: http.StatusNotFound,
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

func TestHandler_ResourcePolicyResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body       map[string]any
		name       string
		action     string
		wantFields []string
		wantCode   int
	}{
		{
			name:       "PutResourcePolicy/HasResourcePolicy",
			action:     "PutResourcePolicy",
			body:       map[string]any{"policyName": "p", "policyDocument": `{}`},
			wantFields: []string{"resourcePolicy"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "DescribeResourcePolicies/HasResourcePolicies",
			action:     "DescribeResourcePolicies",
			body:       map[string]any{},
			wantFields: []string{"resourcePolicies"},
			wantCode:   http.StatusOK,
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
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "response should contain field %q", field)
			}
		})
	}
}
