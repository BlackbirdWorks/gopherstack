package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	securityhubtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
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

// TestTicketV2 drives CreateTicketV2 through the real aws-sdk-go-v2 client.
// Before the fix, the handler read a fabricated "TicketConfiguration"/"Tags"
// shape that doesn't exist on the real CreateTicketV2Input (only ConnectorId,
// FindingMetadataUid, ClientToken and Mode do -- securityhub@v1.75.4
// api_op_CreateTicketV2.go:31-49) and returned a fabricated
// "TicketConfigurationArn" the real CreateTicketV2Output doesn't have either
// (only TicketId, TicketSrcUrl -- api_op_CreateTicketV2.go:53-63). A real SDK
// client round-tripped through that handler would deserialize a nil TicketId.
func TestTicketV2(t *testing.T) {
	t.Parallel()

	t.Run("create ticket with connector and finding", func(t *testing.T) {
		t.Parallel()

		backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

		conn, err := client.CreateConnectorV2(t.Context(), &securityhubsdk.CreateConnectorV2Input{
			Name: aws.String("ticket-test-connector"),
			Provider: &securityhubtypes.ProviderConfigurationMemberJiraCloud{
				Value: securityhubtypes.JiraCloudProviderConfiguration{ProjectKey: aws.String("SEC")},
			},
		})
		require.NoError(t, err)

		out, err := client.CreateTicketV2(t.Context(), &securityhubsdk.CreateTicketV2Input{
			ConnectorId:        conn.ConnectorId,
			FindingMetadataUid: aws.String("finding-metadata-uid-1"),
		})
		require.NoError(t, err)
		require.NotNil(t, out.TicketId, "CreateTicketV2Output.TicketId is required on the real wire")
		assert.NotEmpty(t, *out.TicketId)
	})

	// The real SDK client validates ConnectorId/FindingMetadataUid client-side
	// (validators.go's addOpCreateTicketV2ValidationMiddleware) and refuses to
	// even send a request missing either -- so these two cases drive the raw
	// HTTP handler directly to exercise the server-side ValidationException
	// path a non-SDK caller (or a future SDK without that middleware) would
	// still hit.
	t.Run("missing connectorid rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/ticketsv2", map[string]any{
			"FindingMetadataUid": "finding-metadata-uid-1",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "ValidationException", rec.Header().Get("X-Amzn-Errortype"))
	})

	t.Run("missing findingmetadatauid rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := doRequest(t, h, http.MethodPost, "/connectorsv2", map[string]any{
			"Name":     "ticket-test-connector",
			"Provider": map[string]any{"Type": "JIRA"},
		})
		require.Equal(t, http.StatusOK, createRec.Code)

		var created map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
		connectorID, _ := created["ConnectorId"].(string)
		require.NotEmpty(t, connectorID)

		rec := doRequest(t, h, http.MethodPost, "/ticketsv2", map[string]any{
			"ConnectorId": connectorID,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "ValidationException", rec.Header().Get("X-Amzn-Errortype"))
	})

	t.Run("unknown connector rejected", func(t *testing.T) {
		t.Parallel()

		backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

		_, err := client.CreateTicketV2(t.Context(), &securityhubsdk.CreateTicketV2Input{
			ConnectorId:        aws.String("nonexistent-connector"),
			FindingMetadataUid: aws.String("finding-metadata-uid-1"),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	})
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

// TestConnectors covers the CSPM Connector family (POST/GET/PATCH/DELETE
// /connectors[...]) -- third-party CLOUD PROVIDER connectors (currently
// Azure only), distinct from Connectors V2's ticketing-system connectors
// exercised by TestConnectorsV2 above. Unlike Connectors V2 (which has
// RegisterConnectorV2 to complete an out-of-band OAuth handshake), the real
// CreateConnector surface has no companion "complete authorization"
// operation at all, so this backend never advances a connector's health
// ConnectorStatus away from UNKNOWN -- see connectors.go's CreateConnector
// doc comment.
func TestConnectors(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
		name   string
		method string
		path   string
	}

	azureProvider := map[string]any{
		"Azure": map[string]any{
			"AWSConfigConnectorArn": "arn:aws:cloudformation:us-east-1:000000000000:stack/config-connector/abc",
			"AzureRegions":          []any{"eastus"},
			"ScopeConfiguration": map[string]any{
				"ScopeType":   "SUBSCRIPTION",
				"ScopeValues": []any{"sub-1"},
			},
		},
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Delete Connector",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/connectors",
					body: map[string]any{
						"Name":        "TestConnector",
						"Description": "A test connector",
						"Provider":    azureProvider,
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotEmpty(t, resp["ConnectorId"])
						assert.Contains(t, resp["ConnectorArn"], "connector/")
						assert.Equal(t, "PENDING_ENABLEMENT", resp["EnablementStatus"])
						assert.Equal(t, "UNKNOWN", resp["ConnectorStatus"])
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/connectors",
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						connectors, _ := resp["Connectors"].([]any)
						require.Len(t, connectors, 1)
						first, _ := connectors[0].(map[string]any)
						summary, _ := first["ProviderSummary"].(map[string]any)
						assert.Equal(t, "AZURE", summary["ProviderName"])
						assert.Equal(t, "UNKNOWN", summary["ConnectorStatus"])
					},
				},
				{
					name:   "get",
					method: http.MethodGet,
					path:   "/connectors/connector-1",
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "TestConnector", resp["Name"])
						assert.Equal(t, "PENDING_ENABLEMENT", resp["EnablementStatus"])
						health, _ := resp["Health"].(map[string]any)
						require.NotNil(t, health)
						assert.Equal(t, "UNKNOWN", health["ConnectorStatus"])
						detail, _ := resp["ProviderDetail"].(map[string]any)
						azure, _ := detail["Azure"].(map[string]any)
						assert.Equal(t, "eastus", azure["AzureRegions"].([]any)[0])
					},
				},
				{
					name:   "update",
					method: http.MethodPatch,
					path:   "/connectors/connector-1",
					body: map[string]any{
						"Description": "updated description",
						"Provider": map[string]any{
							"Azure": map[string]any{
								"AzureRegions": []any{"westus"},
								"ScopeConfiguration": map[string]any{
									"ScopeType":   "SUBSCRIPTION",
									"ScopeValues": []any{"sub-1"},
								},
							},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "PENDING_UPDATE", resp["EnablementStatus"])
					},
				},
				{
					name:   "get after update preserves AWSConfigConnectorArn and applies new region",
					method: http.MethodGet,
					path:   "/connectors/connector-1",
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "updated description", resp["Description"])
						detail, _ := resp["ProviderDetail"].(map[string]any)
						azure, _ := detail["Azure"].(map[string]any)
						assert.Equal(t, "westus", azure["AzureRegions"].([]any)[0])
						assert.Equal(t,
							"arn:aws:cloudformation:us-east-1:000000000000:stack/config-connector/abc",
							azure["AWSConfigConnectorArn"],
						)
					},
				},
				{
					name:   "delete",
					method: http.MethodDelete,
					path:   "/connectors/connector-1",
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "PENDING_DELETION", resp["EnablementStatus"])
					},
				},
				{
					name:   "get after delete returns 404",
					method: http.MethodGet,
					path:   "/connectors/connector-1",
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
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

// TestHandler_Connector_NotFoundAndValidation covers the CSPM Connector
// family's error paths: missing-resource 404s across Get/Update/Delete, and
// CreateConnector's required-field 400s (Name/Provider are both "This member
// is required" on the real CreateConnectorInput).
func TestHandler_Connector_NotFoundAndValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "get non-existent connector returns 404",
			method:   http.MethodGet,
			path:     "/connectors/nonexistent-id",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "update non-existent connector returns 404",
			method:   http.MethodPatch,
			path:     "/connectors/nonexistent-id",
			body:     map[string]any{"Description": "x"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete non-existent connector returns 404",
			method:   http.MethodDelete,
			path:     "/connectors/nonexistent-id",
			wantCode: http.StatusNotFound,
		},
		{
			name:   "create without Name returns 400",
			method: http.MethodPost,
			path:   "/connectors",
			body: map[string]any{
				"Provider": map[string]any{"Azure": map[string]any{}},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create without Provider returns 400",
			method:   http.MethodPost,
			path:     "/connectors",
			body:     map[string]any{"Name": "NoProvider"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
