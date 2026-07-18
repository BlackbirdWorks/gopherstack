package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectorsV2(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any) string
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Register Delete ConnectorV2",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/connectorsv2",
					body: map[string]any{
						"Name":        "TestConnector",
						"Description": "A test connector",
						"Provider": map[string]any{
							"Type": "JIRA",
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						id, _ := resp["ConnectorId"].(string)
						assert.NotEmpty(t, id)
						assert.Equal(t, "TestConnector", resp["Name"])
						assert.Equal(t, "ACTIVE", resp["ConnectorStatus"])

						return id
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/connectorsv2",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						connectors, _ := resp["Connectors"].([]any)
						assert.Len(t, connectors, 1)

						return ""
					},
				},
				{
					name:   "get",
					method: http.MethodGet,
					path:   "/connectorsv2/connector-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "TestConnector", resp["Name"])

						return ""
					},
				},
				{
					name:   "update",
					method: http.MethodPatch,
					path:   "/connectorsv2/connector-v2-1",
					body:   map[string]any{"Name": "UpdatedConnector"},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "UpdatedConnector", resp["Name"])

						return ""
					},
				},
				{
					name:   "register",
					method: http.MethodPost,
					path:   "/connectorsv2/register",
					body:   map[string]any{"ConnectorId": "connector-v2-1"},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "REGISTERED", resp["ConnectorStatus"])

						return ""
					},
				},
				{
					name:   "delete",
					method: http.MethodDelete,
					path:   "/connectorsv2/connector-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return ""
					},
				},
				{
					name:   "get after delete returns 404",
					method: http.MethodGet,
					path:   "/connectorsv2/connector-v2-1",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)

						return ""
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

func TestTicketV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body  any
		check func(t *testing.T, code int, resp map[string]any)
		name  string
	}{
		{
			name: "CreateTicketV2 returns ARN",
			body: map[string]any{
				"TicketConfiguration": map[string]any{
					"TicketDestination": "jira-project",
				},
			},
			check: func(t *testing.T, code int, resp map[string]any) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				arn, _ := resp["TicketConfigurationArn"].(string)
				assert.NotEmpty(t, arn)
				assert.Contains(t, arn, "ticket-v2")
				assert.NotEmpty(t, resp["CreatedAt"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/ticketsv2", tc.body)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tc.check(t, rec.Code, resp)
		})
	}
}

func TestHandler_ConnectorV2_UpdateNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "update non-existent connector returns 404",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPatch, "/connectorsv2/nonexistent-id", map[string]any{
				"Name": "Updated",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_ConnectorV2_DeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "delete non-existent connector returns 404",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodDelete, "/connectorsv2/nonexistent-id", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_RegisterConnectorV2_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "register non-existent connector returns 404",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/connectorsv2/register", map[string]any{
				"ConnectorId": "nonexistent-connector",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
