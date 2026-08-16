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

func TestHandler_IndexPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutIndexPolicy/OK",
			action: "PutIndexPolicy",
			body: map[string]any{
				"logGroupIdentifier": "/aws/lambda/fn",
				"policyDocument":     `{"fields":["@message"]}`,
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutIndexPolicy/EmptyIdentifier",
			action: "PutIndexPolicy",
			body: map[string]any{
				"logGroupIdentifier": "",
				"policyDocument":     `{}`,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeIndexPolicies/WithEntries",
			action: "DescribeIndexPolicies",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIndexPolicy",
					`{"logGroupIdentifier":"/grp1","policyDocument":"{}"}`)
				doLogsRequest(t, h, e, "PutIndexPolicy",
					`{"logGroupIdentifier":"/grp2","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteIndexPolicy/OK",
			action: "DeleteIndexPolicy",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIndexPolicy",
					`{"logGroupIdentifier":"/aws/lambda/fn","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteIndexPolicy/NotFound",
			action:   "DeleteIndexPolicy",
			body:     map[string]any{"logGroupIdentifier": "ghost"},
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

func TestHandler_ConfigTemplatesAndFieldIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		action        string
		wantListField string
		wantCode      int
	}{
		{
			name:          "DescribeConfigurationTemplates/ReturnsEmpty",
			action:        "DescribeConfigurationTemplates",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "configurationTemplates",
		},
		{
			name:          "DescribeFieldIndexes/ReturnsEmpty",
			action:        "DescribeFieldIndexes",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "fieldIndexes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := makeLogsRequest(t, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				list, ok := resp[tt.wantListField].([]any)
				require.True(t, ok, "expected list field %q in response", tt.wantListField)
				assert.Empty(t, list)
			}
		})
	}
}

func TestHandler_IndexPolicyResponseShape(t *testing.T) {
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
			name:       "PutIndexPolicy/HasIndexPolicy",
			action:     "PutIndexPolicy",
			body:       map[string]any{"logGroupIdentifier": "/grp", "policyDocument": `{}`},
			wantFields: []string{"indexPolicy"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "DescribeIndexPolicies/HasIndexPolicies",
			action:     "DescribeIndexPolicies",
			body:       map[string]any{},
			wantFields: []string{"indexPolicies"},
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
