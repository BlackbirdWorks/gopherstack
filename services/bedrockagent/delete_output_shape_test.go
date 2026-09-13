package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"sort"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// TestDeleteOps_ExactWireKeySet sweeps every Delete* op Handler exposes and
// asserts the raw response body's top-level key set is EXACTLY the SDK
// deserializer's declared member set (bedrockagent@v1.58.4 deserializers.go,
// awsRestjson1_deserializeOpDocumentDelete*Output). A typed client silently
// drops unknown keys, so only a raw-body comparison catches a fabricated
// key -- see gopherstack-okok.
func TestDeleteOps_ExactWireKeySet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T) (h *bedrockagent.Handler, e *echo.Echo, method, path string, body any)
		name      string
		wantKeys  []string
		wantCode  int
		emptyBody bool
	}{
		{
			name: "DeleteAgent",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				agentID := createTestAgentForDeleteSweep(t, h, e, "doa-agent")

				return h, e, http.MethodDelete, "/agents/" + agentID, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"agentId", "agentStatus"},
		},
		{
			name: "DeleteAgentAlias",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				agentID := createTestAgentForDeleteSweep(t, h, e, "doaa-agent")

				aliasID, _ := createTestAgentAliasForDeleteSweep(t, h, e, agentID, "doaa-alias")

				return h, e, http.MethodDelete, "/agents/" + agentID + "/agentaliases/" + aliasID, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"agentId", "agentAliasId", "agentAliasStatus"},
		},
		{
			name: "DeleteAgentVersion",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				agentID := createTestAgentForDeleteSweep(t, h, e, "doav-agent")
				_, version := createTestAgentAliasForDeleteSweep(t, h, e, agentID, "doav-alias")

				return h, e, http.MethodDelete,
					"/agents/" + agentID + "/agentversions/" + version + "?skipResourceInUseCheck=true", nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"agentId", "agentVersion", "agentStatus"},
		},
		{
			name: "DeleteAgentActionGroup",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				agentID := createTestAgentForDeleteSweep(t, h, e, "doag-agent")

				agRec := doRequest(t, h, e, http.MethodPut,
					"/agents/"+agentID+"/agentversions/DRAFT/actiongroups",
					map[string]any{"actionGroupName": "doag-group"})
				require.Equal(t, http.StatusOK, agRec.Code, agRec.Body.String())

				var agOut map[string]map[string]any
				require.NoError(t, json.Unmarshal(agRec.Body.Bytes(), &agOut))
				groupID, _ := agOut["agentActionGroup"]["actionGroupId"].(string)
				require.NotEmpty(t, groupID)

				return h, e, http.MethodDelete,
					"/agents/" + agentID + "/agentversions/DRAFT/actiongroups/" + groupID, nil
			},
			wantCode:  http.StatusNoContent,
			emptyBody: true,
		},
		{
			name: "DeleteFlow",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				flowID := createTestFlowForDeleteSweep(t, h, e, "dof-flow")

				return h, e, http.MethodDelete, "/flows/" + flowID, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id"},
		},
		{
			name: "DeleteFlowVersion",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				flowID := createTestFlowForDeleteSweep(t, h, e, "dofv-flow")

				verRec := doRequest(t, h, e, http.MethodPost, "/flows/"+flowID+"/versions", nil)
				require.Equal(t, http.StatusCreated, verRec.Code, verRec.Body.String())

				var verBody map[string]any
				require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verBody))
				version, _ := verBody["version"].(string)
				require.NotEmpty(t, version)

				return h, e, http.MethodDelete, "/flows/" + flowID + "/versions/" + version, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id", "version"},
		},
		{
			name: "DeleteFlowAlias",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				flowID := createTestFlowForDeleteSweep(t, h, e, "dofa-flow")

				aliasRec := doRequest(t, h, e, http.MethodPost, "/flows/"+flowID+"/aliases",
					map[string]any{"name": "dofa-alias"})
				require.Equal(t, http.StatusCreated, aliasRec.Code, aliasRec.Body.String())

				var aliasBody map[string]any
				require.NoError(t, json.Unmarshal(aliasRec.Body.Bytes(), &aliasBody))
				aliasID, _ := aliasBody["id"].(string)
				require.NotEmpty(t, aliasID)

				return h, e, http.MethodDelete, "/flows/" + flowID + "/aliases/" + aliasID, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id", "flowId"},
		},
		{
			name: "DeleteKnowledgeBase",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				kbID := createTestKBForDeleteSweep(t, h, e, "dokb-kb")

				return h, e, http.MethodDelete, "/knowledgebases/" + kbID, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"knowledgeBaseId", "status"},
		},
		{
			name: "DeleteDataSource",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				kbID := createTestKBForDeleteSweep(t, h, e, "dods-kb")
				dsID := createTestDSForDeleteSweep(t, h, e, kbID, "dods-ds")

				return h, e, http.MethodDelete, "/knowledgebases/" + kbID + "/datasources/" + dsID, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"dataSourceId", "knowledgeBaseId", "status"},
		},
		{
			name: "DeleteKnowledgeBaseDocuments",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				kbID := createTestKBForDeleteSweep(t, h, e, "dokd-kb")
				dsID := createTestDSForDeleteSweep(t, h, e, kbID, "dokd-ds")

				ingestRec := doRequest(t, h, e, http.MethodPut,
					"/knowledgebases/"+kbID+"/datasources/"+dsID+"/documents",
					map[string]any{"documents": []map[string]any{
						{"content": map[string]any{
							"dataSourceType": "CUSTOM",
							"custom": map[string]any{
								"customDocumentIdentifier": map[string]any{"id": "dokd-doc"},
								"sourceType":               "IN_LINE",
							},
						}},
					}})
				require.Equal(t, http.StatusAccepted, ingestRec.Code, ingestRec.Body.String())

				delBody := map[string]any{"documentIdentifiers": []map[string]any{
					{"dataSourceType": "CUSTOM", "custom": map[string]any{"id": "dokd-doc"}},
				}}

				return h, e, http.MethodPost, "/knowledgebases/" + kbID + "/datasources/" + dsID +
					"/documents/deleteDocuments", delBody
			},
			wantCode: http.StatusAccepted,
			wantKeys: []string{"documentDetails"},
		},
		{
			name: "DeletePrompt",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				promptID := createTestPromptForDeleteSweep(t, h, e, "dop-prompt")

				return h, e, http.MethodDelete, "/prompts/" + promptID, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id"},
		},
		{
			// DeletePromptVersion is an internal-only route (see handler.go's
			// GetSupportedOperations comment): real bedrock-agent deletes a
			// specific prompt version via DeletePrompt's promptVersion query
			// parameter, not this path. Its output still mirrors
			// DeletePromptOutput's real member set (id, version), so it's
			// swept here too.
			name: "DeletePromptVersion",
			setup: func(t *testing.T) (*bedrockagent.Handler, *echo.Echo, string, string, any) {
				t.Helper()

				h, e := setupHandler(t)
				promptID := createTestPromptForDeleteSweep(t, h, e, "dopv-prompt")

				verRec := doRequest(t, h, e, http.MethodPost, "/prompts/"+promptID+"/versions", nil)
				require.Equal(t, http.StatusCreated, verRec.Code, verRec.Body.String())

				var verBody map[string]any
				require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verBody))
				version, _ := verBody["version"].(string)
				require.NotEmpty(t, version)

				return h, e, http.MethodDelete, "/prompts/" + promptID + "/versions/" + version, nil
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id", "version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, method, path, body := tt.setup(t)

			rec := doRequest(t, h, e, method, path, body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.emptyBody {
				assert.Empty(t, rec.Body.Bytes(), "output has no members: body must be empty")

				return
			}

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			gotKeys := make([]string, 0, len(out))
			for k := range out {
				gotKeys = append(gotKeys, k)
			}

			sort.Strings(gotKeys)
			wantKeys := append([]string{}, tt.wantKeys...)
			sort.Strings(wantKeys)

			assert.Equal(t, wantKeys, gotKeys, "raw response body key set must equal the SDK's declared member set")
		})
	}
}

func createTestAgentForDeleteSweep(t *testing.T, h *bedrockagent.Handler, e *echo.Echo, name string) string {
	t.Helper()

	rec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
		"agentName":            name,
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/BedrockRole",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	agentID, _ := out["agent"]["agentId"].(string)
	require.NotEmpty(t, agentID)

	return agentID
}

// createTestAgentAliasForDeleteSweep creates an alias, which snapshots DRAFT
// into a real numbered agent version (real bedrock-agent has no
// CreateAgentVersion wire op -- see the dispatchAgentVersions comment in
// handler.go). Returns the alias id and the version the alias routes to.
func createTestAgentAliasForDeleteSweep(
	t *testing.T, h *bedrockagent.Handler, e *echo.Echo, agentID, aliasName string,
) (string, string) {
	t.Helper()

	rec := doRequest(t, h, e, http.MethodPut, "/agents/"+agentID+"/agentaliases", map[string]any{
		"agentAliasName": aliasName,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	aliasID, _ := out["agentAlias"]["agentAliasId"].(string)
	require.NotEmpty(t, aliasID)

	routing, _ := out["agentAlias"]["routingConfiguration"].([]any)
	require.NotEmpty(t, routing)
	firstRoute, _ := routing[0].(map[string]any)
	version, _ := firstRoute["agentVersion"].(string)
	require.NotEmpty(t, version)

	return aliasID, version
}

func createTestFlowForDeleteSweep(t *testing.T, h *bedrockagent.Handler, e *echo.Echo, name string) string {
	t.Helper()

	rec := doRequest(t, h, e, http.MethodPost, "/flows", map[string]any{
		"name":             name,
		"executionRoleArn": "arn:aws:iam::123456789012:role/FlowRole",
		"definition":       map[string]any{"nodes": []any{}, "connections": []any{}},
	})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	flowID, _ := out["id"].(string)
	require.NotEmpty(t, flowID)

	return flowID
}

func createTestKBForDeleteSweep(t *testing.T, h *bedrockagent.Handler, e *echo.Echo, name string) string {
	t.Helper()

	rec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", map[string]any{
		"name":                       name,
		"roleArn":                    "arn:aws:iam::123456789012:role/KBRole",
		"knowledgeBaseConfiguration": map[string]any{"type": "VECTOR"},
		"storageConfiguration":       map[string]any{"type": "OPENSEARCH_SERVERLESS"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	kbID, _ := out["knowledgeBase"]["knowledgeBaseId"].(string)
	require.NotEmpty(t, kbID)

	return kbID
}

func createTestDSForDeleteSweep(t *testing.T, h *bedrockagent.Handler, e *echo.Echo, kbID, name string) string {
	t.Helper()

	rec := doRequest(t, h, e, http.MethodPut, "/knowledgebases/"+kbID+"/datasources", map[string]any{
		"name":                    name,
		"dataSourceConfiguration": map[string]any{"type": "CUSTOM"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	dsID, _ := out["dataSource"]["dataSourceId"].(string)
	require.NotEmpty(t, dsID)

	return dsID
}

func createTestPromptForDeleteSweep(t *testing.T, h *bedrockagent.Handler, e *echo.Echo, name string) string {
	t.Helper()

	rec := doRequest(t, h, e, http.MethodPost, "/prompts", map[string]any{
		"name":           name,
		"defaultVariant": "v1",
		"variants": []any{
			map[string]any{"name": "v1", "templateType": "TEXT"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	promptID, _ := out["id"].(string)
	require.NotEmpty(t, promptID)

	return promptID
}
