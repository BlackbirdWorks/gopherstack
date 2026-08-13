package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateApplication_EmptyName(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateApplication("", nil, nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrInvalidParameter)
}

func TestCreateApplication_Duplicate(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateApplication("my-app", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateApplication("my-app", nil, nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrApplicationAlreadyExists)
}

func TestApplications_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		appName     string
		appConfigs  []map[string]string
		wantInList  bool
		wantGetOK   bool
		wantDeleted bool
	}{
		{
			name:       "create_and_list",
			appName:    "my-app",
			wantInList: true,
			wantGetOK:  true,
		},
		{
			name:        "create_and_delete",
			appName:     "del-app",
			wantInList:  true,
			wantDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

			app, err := b.CreateApplication(tt.appName, nil, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, app.ID)
			assert.Equal(t, tt.appName, app.Name)

			apps := b.ListApplications()
			if tt.wantInList {
				require.NotEmpty(t, apps)
				found := false
				for _, a := range apps {
					if a.Name == tt.appName {
						found = true
					}
				}
				assert.True(t, found, "app should appear in list")
			}

			if tt.wantGetOK {
				got, getErr := b.GetApplication(app.ID)
				require.NoError(t, getErr)
				assert.Equal(t, tt.appName, got.Name)
			}

			if tt.wantDeleted {
				err = b.DeleteApplication(app.ID)
				require.NoError(t, err)

				apps = b.ListApplications()
				for _, a := range apps {
					assert.NotEqual(t, app.ID, a.ID, "deleted app should not appear in list")
				}
			}
		})
	}
}

func TestApplications_HTTPHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		wantFields []string
	}{
		{
			name:       "get_returns_real_app_data",
			appName:    "real-app",
			wantFields: []string{"Id", "Name", "Arn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create app via HTTP.
			cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application",
				map[string]any{"Name": tt.appName, "AppConfigs": []any{}, "DataSources": []any{}})
			cr.Body.Close()
			require.Equal(t, http.StatusOK, cr.StatusCode)

			// List apps via HTTP.
			lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/list-applications", nil)
			defer lr.Body.Close()
			require.Equal(t, http.StatusOK, lr.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(lr.Body).Decode(&out))

			apps, ok := out["ApplicationSummaries"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, apps)

			first := apps[0].(map[string]any)
			for _, field := range tt.wantFields {
				assert.Contains(t, first, field, "expected field %q in application list", field)
			}
			assert.Equal(t, tt.appName, first["Name"])
		})
	}
}

func TestApplications_GetAndUpdateWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application",
		map[string]any{"Name": "wire-app"})
	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	cr.Body.Close()
	require.Equal(t, http.StatusOK, cr.StatusCode)

	appID := cOut["id"].(string)
	assert.NotEmpty(t, cOut["createdAt"])
	// CreateApplicationOutput has no Status field on the real API.
	assert.NotContains(t, cOut, "Status")
	assert.NotContains(t, cOut, "status")

	// GetApplication must include Status, Endpoint, CreatedAt, LastUpdatedAt.
	gr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/application/"+appID, nil)
	defer gr.Body.Close()
	require.Equal(t, http.StatusOK, gr.StatusCode)

	var gOut map[string]any
	require.NoError(t, json.NewDecoder(gr.Body).Decode(&gOut))
	assert.Equal(t, "ACTIVE", gOut["Status"])
	assert.NotEmpty(t, gOut["Endpoint"])
	assert.NotEmpty(t, gOut["CreatedAt"])
	assert.NotEmpty(t, gOut["LastUpdatedAt"])

	// UpdateApplication must not carry a Status field on the real API.
	ur := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/application/"+appID,
		map[string]any{"AppConfigs": []any{}, "DataSources": []any{}})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	var uOut map[string]any
	require.NoError(t, json.NewDecoder(ur.Body).Decode(&uOut))
	assert.NotContains(t, uOut, "Status")
	assert.NotEmpty(t, uOut["CreatedAt"])
	assert.NotEmpty(t, uOut["LastUpdatedAt"])
}

func TestOpenSearchHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		appName      string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			appName:      "my-app",
			wantCode:     http.StatusOK,
			wantContains: []string{"my-app", "\"id\"", "\"arn\""},
		},
		{
			name:     "no_name",
			appName:  "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "duplicate",
			appName:  "dup-app",
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.name == "duplicate" {
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application",
					map[string]any{"Name": "dup-app"})
				r.Body.Close()
			}

			body := map[string]any{"Name": tt.appName}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application", body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestOpenSearchHandler_CreateApplication_WithConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := map[string]any{
		"Name": "configured-app",
		"AppConfigs": []map[string]string{
			{"Key": "opensearchDashboards.enabled", "Value": "true"},
		},
		"DataSources": []map[string]string{
			{"DataSourceArn": "arn:aws:opensearch:us-east-1:123456789012:domain/test"},
		},
	}

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application", body)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(bodyBytes), "configured-app")
	assert.Contains(t, string(bodyBytes), "opensearchDashboards.enabled")
}

// createTestApplication creates an application via HTTP and returns its ID.
func createTestApplication(t *testing.T, h *opensearch.Handler, name string) string {
	t.Helper()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application", map[string]any{"Name": name})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	id, ok := out["id"].(string)
	require.True(t, ok)

	return id
}

// domainARN creates a domain via HTTP and returns its real ARN, so data
// source attachment/migration tests reference a genuinely tracked resource
// rather than a hand-built string.
func domainARN(t *testing.T, h *opensearch.Handler, domainName string) string {
	t.Helper()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": domainName})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	status, ok := out["DomainStatus"].(map[string]any)
	require.True(t, ok)

	arn, ok := status["ARN"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

// TestDataSourceAttachments_HTTPHandler covers the AttachDataSource/
// DetachDataSource/DescribeDataSourceAttachment/ListDataSourceAttachments
// family end to end through the real router path, including that
// attachments reference genuinely tracked domain state (see domainARN
// above) rather than an invented parallel resource.
func TestDataSourceAttachments_HTTPHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *opensearch.Handler)
		name string
	}{
		{
			name: "attach_describe_list_detach_lifecycle",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-app")
				arn := domainARN(t, h, "ds-domain")

				ar := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{"dataSourceArn": arn})
				defer ar.Body.Close()
				require.Equal(t, http.StatusOK, ar.StatusCode)

				var attOut map[string]any
				require.NoError(t, json.NewDecoder(ar.Body).Decode(&attOut))
				assert.Equal(t, appID, attOut["id"])
				assert.Equal(t, arn, attOut["dataSourceArn"])
				assert.Equal(t, arn, attOut["arn"])
				assert.NotEmpty(t, attOut["attachmentId"])
				// No transient window is configured, and the referenced domain is
				// created (not still processing), so the attachment settles to
				// ATTACHED immediately rather than staying PENDING.
				assert.Equal(t, "ATTACHED", attOut["status"])

				// Idempotent re-attach: same attachmentId, no duplicate created.
				ar2 := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{"dataSourceArn": arn})
				defer ar2.Body.Close()
				var attOut2 map[string]any
				require.NoError(t, json.NewDecoder(ar2.Body).Decode(&attOut2))
				assert.Equal(t, attOut["attachmentId"], attOut2["attachmentId"])

				lr := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/listDataSourceAttachments", nil)
				defer lr.Body.Close()
				require.Equal(t, http.StatusOK, lr.StatusCode)

				var listOut map[string]any
				require.NoError(t, json.NewDecoder(lr.Body).Decode(&listOut))
				attachments, ok := listOut["attachments"].([]any)
				require.True(t, ok)
				require.Len(t, attachments, 1)
				summary := attachments[0].(map[string]any)
				assert.Equal(t, arn, summary["dataSourceArn"])
				// ListDataSourceAttachmentsOutput's element is
				// DataSourceAttachmentSummary, which has no "arn"/"id" field
				// (unlike the single-attachment ops above).
				assert.NotContains(t, summary, "arn")
				assert.NotContains(t, summary, "id")

				dr := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/describeDataSourceAttachment",
					map[string]any{"dataSourceArn": arn})
				defer dr.Body.Close()
				require.Equal(t, http.StatusOK, dr.StatusCode)

				var descOut map[string]any
				require.NoError(t, json.NewDecoder(dr.Body).Decode(&descOut))
				assert.Equal(t, "ATTACHED", descOut["status"])

				detR := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/detachDataSource",
					map[string]any{"dataSourceArn": arn})
				defer detR.Body.Close()
				assert.Equal(t, http.StatusOK, detR.StatusCode)

				dr2 := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/describeDataSourceAttachment",
					map[string]any{"dataSourceArn": arn})
				defer dr2.Body.Close()
				assert.Equal(t, http.StatusConflict, dr2.StatusCode)
			},
		},
		{
			name: "attach_unknown_application_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				arn := domainARN(t, h, "ds-domain-2")
				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/no-such-app/attachDataSource",
					map[string]any{"dataSourceArn": arn})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			name: "attach_unknown_arn_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-app-2")
				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{"dataSourceArn": "arn:aws:es:us-east-1:123456789012:domain/no-such"})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			name: "detach_without_attach_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-app-3")
				arn := domainARN(t, h, "ds-domain-3")
				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/detachDataSource",
					map[string]any{"dataSourceArn": arn})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			name: "deleting_application_cascades_attachment",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-app-4")
				arn := domainARN(t, h, "ds-domain-4")

				ar := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{"dataSourceArn": arn})
				ar.Body.Close()
				require.Equal(t, http.StatusOK, ar.StatusCode)

				delR := doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/application/"+appID, nil)
				delR.Body.Close()
				require.Equal(t, http.StatusOK, delR.StatusCode)

				// Re-creating the same attachment via a fresh application must not
				// find a stale attachment left over from the deleted one.
				appID2 := createTestApplication(t, h, "ds-app-4b")
				dr := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID2+"/describeDataSourceAttachment",
					map[string]any{"dataSourceArn": arn})
				defer dr.Body.Close()
				assert.Equal(t, http.StatusConflict, dr.StatusCode)
			},
		},
		{
			// types.WorkspaceConfigurationInput.Name/.WorkspaceType are both
			// "This member is required" once WorkspaceConfiguration is
			// supplied at all.
			name: "attach_workspace_configuration_missing_name_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-ws-app-1")
				arn := domainARN(t, h, "ds-ws-domain-1")
				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{
						"dataSourceArn":          arn,
						"workspaceConfiguration": map[string]any{"workspaceType": "SEARCH"},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			// WorkspaceType's doc text is an exhaustive, closed enum
			// (OBSERVABILITY, SECURITY_ANALYTICS, SEARCH).
			name: "attach_workspace_configuration_invalid_type_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-ws-app-2")
				arn := domainARN(t, h, "ds-ws-domain-2")
				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{
						"dataSourceArn":          arn,
						"workspaceConfiguration": map[string]any{"name": "ws", "workspaceType": "BOGUS"},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			// "Mutually exclusive with workspaceId" per
			// types.AttachDataSourceInput.WorkspaceConfiguration's doc text.
			name: "attach_workspace_configuration_and_workspace_id_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-ws-app-3")
				arn := domainARN(t, h, "ds-ws-domain-3")
				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{
						"dataSourceArn":          arn,
						"workspaceConfiguration": map[string]any{"name": "ws", "workspaceType": "SEARCH"},
						"workspaceId":            "workspace-1",
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			name: "attach_unknown_workspace_id_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-ws-app-4")
				arn := domainARN(t, h, "ds-ws-domain-4")
				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{"dataSourceArn": arn, "workspaceId": "no-such-workspace"})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			// A workspace created (via WorkspaceConfiguration on one
			// application) can be referenced by WorkspaceId afterward -- but
			// only from the same application it was created against;
			// AttachDataSourceOutput never echoes the generated WorkspaceId
			// (see the Workspace doc comment in models.go), so this uses
			// createWorkspaceLocked's documented deterministic
			// "workspace-<N>" ID scheme, exercised for the first time on a
			// fresh handler/backend here.
			name: "attach_workspace_id_scoped_to_creating_application",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "ds-ws-app-5")
				arn := domainARN(t, h, "ds-ws-domain-5")
				createResp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{
						"dataSourceArn":          arn,
						"workspaceConfiguration": map[string]any{"name": "ws-5", "workspaceType": "OBSERVABILITY"},
					})
				createResp.Body.Close()
				require.Equal(t, http.StatusOK, createResp.StatusCode)

				arn2 := domainARN(t, h, "ds-ws-domain-5b")
				sameAppResp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/attachDataSource",
					map[string]any{"dataSourceArn": arn2, "workspaceId": "workspace-1"})
				sameAppResp.Body.Close()
				assert.Equal(t, http.StatusOK, sameAppResp.StatusCode)

				otherAppID := createTestApplication(t, h, "ds-ws-app-5-other")
				arn3 := domainARN(t, h, "ds-ws-domain-5c")
				otherAppResp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+otherAppID+"/attachDataSource",
					map[string]any{"dataSourceArn": arn3, "workspaceId": "workspace-1"})
				otherAppResp.Body.Close()
				assert.Equal(t, http.StatusConflict, otherAppResp.StatusCode)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.run(t, h)
		})
	}
}

// TestCapabilities_HTTPHandler covers RegisterCapability/GetCapability/
// DeregisterCapability end to end through the real router path.
func TestCapabilities_HTTPHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *opensearch.Handler)
		name string
	}{
		{
			name: "register_get_deregister_lifecycle",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "cap-app")

				rr := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/capability/register",
					map[string]any{
						"capabilityName":   "ai-capability",
						"capabilityConfig": map[string]any{"aiConfig": map[string]any{}},
					})
				defer rr.Body.Close()
				require.Equal(t, http.StatusOK, rr.StatusCode)

				var regOut map[string]any
				require.NoError(t, json.NewDecoder(rr.Body).Decode(&regOut))
				assert.Equal(t, appID, regOut["applicationId"])
				assert.Equal(t, "ai-capability", regOut["capabilityName"])
				assert.Equal(t, "active", regOut["status"])
				cfg, ok := regOut["capabilityConfig"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, cfg, "aiConfig")

				gr := doRequest(t, h, http.MethodGet,
					"/2021-01-01/opensearch/application/"+appID+"/capability/ai-capability", nil)
				defer gr.Body.Close()
				require.Equal(t, http.StatusOK, gr.StatusCode)

				var getOut map[string]any
				require.NoError(t, json.NewDecoder(gr.Body).Decode(&getOut))
				assert.Equal(t, "active", getOut["status"])
				assert.Equal(t, []any{}, getOut["failures"])

				dr := doRequest(t, h, http.MethodDelete,
					"/2021-01-01/opensearch/application/"+appID+"/capability/deregister/ai-capability", nil)
				defer dr.Body.Close()
				require.Equal(t, http.StatusOK, dr.StatusCode)

				var deregOut map[string]any
				require.NoError(t, json.NewDecoder(dr.Body).Decode(&deregOut))
				// DeregisterCapabilityOutput always reports "deleting" per its doc,
				// regardless of this emulator's actual (synchronous) removal.
				assert.Equal(t, "deleting", deregOut["status"])

				gr2 := doRequest(t, h, http.MethodGet,
					"/2021-01-01/opensearch/application/"+appID+"/capability/ai-capability", nil)
				defer gr2.Body.Close()
				assert.Equal(t, http.StatusConflict, gr2.StatusCode)
			},
		},
		{
			name: "register_invalid_name_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "cap-app-2")
				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/"+appID+"/capability/register",
					map[string]any{"capabilityName": "x"})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			name: "register_unknown_application_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				resp := doRequest(t, h, http.MethodPost,
					"/2021-01-01/opensearch/application/no-such-app/capability/register",
					map[string]any{"capabilityName": "ai-capability"})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			name: "get_unknown_capability_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "cap-app-3")
				resp := doRequest(t, h, http.MethodGet,
					"/2021-01-01/opensearch/application/"+appID+"/capability/ai-capability", nil)
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			name: "deregister_unknown_capability_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "cap-app-4")
				resp := doRequest(t, h, http.MethodDelete,
					"/2021-01-01/opensearch/application/"+appID+"/capability/deregister/ai-capability", nil)
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.run(t, h)
		})
	}
}

// TestMigrations_HTTPHandler covers StartMigration/GetMigration/
// ListMigrations end to end through the real router path.
func TestMigrations_HTTPHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *opensearch.Handler)
		name string
	}{
		{
			name: "start_get_list_lifecycle",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app")
				arn := domainARN(t, h, "mig-domain")

				sr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source":    map[string]any{"datasourceArn": arn},
							"workspace": map[string]any{"createWorkspace": true, "name": "mig-workspace"},
						},
					})
				defer sr.Body.Close()
				require.Equal(t, http.StatusOK, sr.StatusCode)

				var startOut map[string]any
				require.NoError(t, json.NewDecoder(sr.Body).Decode(&startOut))
				migrationID, ok := startOut["migrationId"].(string)
				require.True(t, ok)
				require.NotEmpty(t, migrationID)
				// No transient window is configured, so the migration settles to
				// its terminal state immediately (see resolveMigrationStatus).
				assert.Equal(t, "SUCCEEDED", startOut["status"])

				gr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/app-migrations/"+migrationID, nil)
				defer gr.Body.Close()
				require.Equal(t, http.StatusOK, gr.StatusCode)

				var getOut map[string]any
				require.NoError(t, json.NewDecoder(gr.Body).Decode(&getOut))
				assert.Equal(t, appID, getOut["applicationId"])
				assert.Equal(t, "SUCCEEDED", getOut["status"])
				// No saved-object store exists to migrate real objects, so counts
				// are honestly 0 rather than a fabricated non-zero result.
				assert.Zero(t, getOut["exportedCount"])
				assert.Zero(t, getOut["importedCount"])
				source, ok := getOut["source"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, arn, source["datasourceArn"])

				lr := doRequest(t, h, http.MethodGet,
					"/2021-01-01/opensearch/app-migrations?applicationId="+appID, nil)
				defer lr.Body.Close()
				require.Equal(t, http.StatusOK, lr.StatusCode)

				var listOut map[string]any
				require.NoError(t, json.NewDecoder(lr.Body).Decode(&listOut))
				migrations, ok := listOut["migrations"].([]any)
				require.True(t, ok)
				require.Len(t, migrations, 1)

				filteredR := doRequest(t, h, http.MethodGet,
					"/2021-01-01/opensearch/app-migrations?applicationId="+appID+"&status=PENDING", nil)
				defer filteredR.Body.Close()
				var filteredOut map[string]any
				require.NoError(t, json.NewDecoder(filteredR.Body).Decode(&filteredOut))
				assert.Empty(t, filteredOut["migrations"])
			},
		},
		{
			name: "start_missing_source_arn_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app-2")
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{"applicationId": appID})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			name: "start_unknown_application_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				arn := domainARN(t, h, "mig-domain-2")
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": "no-such-app",
						"migrationOptions": map[string]any{
							"source": map[string]any{"datasourceArn": arn},
						},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			name: "start_unknown_source_arn_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app-3")
				bogusArn := "arn:aws:es:us-east-1:123456789012:domain/no-such"
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source": map[string]any{"datasourceArn": bogusArn},
						},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			name: "get_unknown_migration_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/app-migrations/no-such-migration", nil)
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			name: "list_unknown_application_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				resp := doRequest(t, h, http.MethodGet,
					"/2021-01-01/opensearch/app-migrations?applicationId=no-such-app", nil)
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			// MigrationOptions.Workspace is "This member is required" --
			// previously unenforced by this backend at all.
			name: "start_missing_workspace_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app-4")
				arn := domainARN(t, h, "mig-domain-4")
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source": map[string]any{"datasourceArn": arn},
						},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			// types.MigrationWorkspace.Name is "Required when
			// createWorkspace is true".
			name: "start_create_workspace_missing_name_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app-5")
				arn := domainARN(t, h, "mig-domain-5")
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source":    map[string]any{"datasourceArn": arn},
							"workspace": map[string]any{"createWorkspace": true},
						},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			// "Specify either this parameter [workspaceId] or
			// createWorkspace" per types.MigrationWorkspace's doc text.
			name: "start_workspace_id_and_create_workspace_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app-6")
				arn := domainARN(t, h, "mig-domain-6")
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source": map[string]any{"datasourceArn": arn},
							"workspace": map[string]any{
								"createWorkspace": true, "name": "ws", "workspaceId": "workspace-1",
							},
						},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			name: "start_unknown_workspace_id_returns_409",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app-7")
				arn := domainARN(t, h, "mig-domain-7")
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source":    map[string]any{"datasourceArn": arn},
							"workspace": map[string]any{"workspaceId": "no-such-workspace"},
						},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
			},
		},
		{
			// ConflictResolution's doc text is an exhaustive, closed enum
			// (CREATE_NEW_COPIES, overwrite).
			name: "start_invalid_conflict_resolution_returns_400",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app-8")
				arn := domainARN(t, h, "mig-domain-8")
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source":             map[string]any{"datasourceArn": arn},
							"workspace":          map[string]any{"createWorkspace": true, "name": "ws"},
							"conflictResolution": "BOGUS",
						},
					})
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			},
		},
		{
			// A workspace created via StartMigration's CreateWorkspace can
			// be referenced by WorkspaceId in a later migration -- neither
			// StartMigrationOutput/GetMigrationOutput ever echoes the
			// generated WorkspaceId (see the Workspace doc comment in
			// models.go), so this uses createWorkspaceLocked's documented
			// deterministic "workspace-<N>" ID scheme on a fresh
			// handler/backend, matching the analogous AttachDataSource
			// coverage above.
			name: "start_reuses_workspace_created_by_earlier_migration",
			run: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()

				appID := createTestApplication(t, h, "mig-app-9")
				arn := domainARN(t, h, "mig-domain-9")
				firstResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source":    map[string]any{"datasourceArn": arn},
							"workspace": map[string]any{"createWorkspace": true, "name": "ws-9"},
						},
					})
				firstResp.Body.Close()
				require.Equal(t, http.StatusOK, firstResp.StatusCode)

				secondResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/app-migrations",
					map[string]any{
						"applicationId": appID,
						"migrationOptions": map[string]any{
							"source":    map[string]any{"datasourceArn": arn},
							"workspace": map[string]any{"workspaceId": "workspace-1"},
						},
					})
				secondResp.Body.Close()
				assert.Equal(t, http.StatusOK, secondResp.StatusCode)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.run(t, h)
		})
	}
}
