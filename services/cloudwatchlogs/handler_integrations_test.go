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

func TestHandler_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutIntegration/OK",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "my-opensearch",
				"integrationType": "OPENSEARCH",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutIntegration/EmptyName",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "",
				"integrationType": "OPENSEARCH",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetIntegration/OK",
			action: "GetIntegration",
			body:   map[string]any{"integrationName": "my-opensearch"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration",
					`{"integrationName":"my-opensearch","integrationType":"OPENSEARCH"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "GetIntegration/NotFound",
			action:   "GetIntegration",
			body:     map[string]any{"integrationName": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListIntegrations/WithEntries",
			action: "ListIntegrations",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration", `{"integrationName":"ig1","integrationType":"OPENSEARCH"}`)
				doLogsRequest(t, h, e, "PutIntegration", `{"integrationName":"ig2","integrationType":"OPENSEARCH"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteIntegration/OK",
			action: "DeleteIntegration",
			body:   map[string]any{"integrationName": "my-opensearch"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration",
					`{"integrationName":"my-opensearch","integrationType":"OPENSEARCH"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteIntegration/NotFound",
			action:   "DeleteIntegration",
			body:     map[string]any{"integrationName": "ghost"},
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

func TestHandler_AssociateSourceToS3TableIntegrationOperations(t *testing.T) {
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
		// AssociateSourceToS3TableIntegration
		{
			name:   "AssociateSourceToS3TableIntegration/OK",
			action: "AssociateSourceToS3TableIntegration",
			body: map[string]any{
				"integrationArn": "arn:aws:s3tables:us-east-1:123:integration/my-int",
				"dataSource":     map[string]any{"name": "source1", "type": "CloudWatchLogs"},
			},
			wantCode: http.StatusOK,
			wantKey:  "identifier",
		},
		{
			name:     "AssociateSourceToS3TableIntegration/MissingArn",
			action:   "AssociateSourceToS3TableIntegration",
			body:     map[string]any{},
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

func TestHandler_S3TableIntegrationSourceOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		action        string
		wantListField string
		wantCode      int
	}{
		{
			name:          "ListSourcesForS3TableIntegration/ReturnsEmpty",
			action:        "ListSourcesForS3TableIntegration",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "sources",
		},
		{
			name:     "DisassociateSourceFromS3TableIntegration/OK",
			action:   "DisassociateSourceFromS3TableIntegration",
			body:     map[string]any{},
			wantCode: http.StatusOK,
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

func TestHandler_IntegrationResponseShape(t *testing.T) {
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
			name:   "PutIntegration/HasIntegrationName",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "ig",
				"integrationType": "OPENSEARCH",
			},
			wantFields: []string{"integrationName"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "ListIntegrations/HasIntegrationSummaries",
			action:     "ListIntegrations",
			body:       map[string]any{},
			wantFields: []string{"integrationSummaries"},
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
