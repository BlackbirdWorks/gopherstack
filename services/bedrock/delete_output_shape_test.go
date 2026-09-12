package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestDeleteOps_ExactWireKeySet sweeps every Delete* op AgentsHandler exposes
// and asserts the raw response body's top-level key set is EXACTLY the SDK
// deserializer's declared member set (bedrockagent@v1.58.4 deserializers.go,
// awsRestjson1_deserializeOpDocumentDelete*Output). A typed client silently
// drops unknown keys, so only a raw-body comparison catches a fabricated or
// misnamed key -- see gopherstack-okok and the DeleteAgentVersion "version"
// vs "agentVersion" bug this test caught (fixed in handler_agents.go).
func TestDeleteOps_ExactWireKeySet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T) (h *bedrock.AgentsHandler, method, path string)
		name     string
		wantKeys []string
		wantCode int
	}{
		{
			name: "DeletePrompt",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, _ := newTestAgentsHandler(t)
				rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "dop-prompt"})
				require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				promptID, _ := out["id"].(string)
				require.NotEmpty(t, promptID)

				return h, http.MethodDelete, "/prompts/" + promptID
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id"},
		},
		{
			name: "DeletePromptVersion",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, _ := newTestAgentsHandler(t)
				rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "dopv-prompt"})
				require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

				var created map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				promptID, _ := created["id"].(string)
				require.NotEmpty(t, promptID)

				verRec := doAgentRequest(t, h, http.MethodPost, fmt.Sprintf("/prompts/%s/versions", promptID), nil)
				require.Equal(t, http.StatusCreated, verRec.Code, verRec.Body.String())

				var verBody map[string]any
				require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verBody))
				version, _ := verBody["version"].(string)
				require.NotEmpty(t, version)

				return h, http.MethodDelete, fmt.Sprintf("/prompts/%s/versions/%s", promptID, version)
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id", "version"},
		},
		{
			name: "DeleteFlow",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, _ := newTestAgentsHandler(t)
				rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "dof-flow"})
				require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				flowID, _ := out["id"].(string)
				require.NotEmpty(t, flowID)

				return h, http.MethodDelete, "/flows/" + flowID
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id"},
		},
		{
			name: "DeleteFlowVersion",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, _ := newTestAgentsHandler(t)
				rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "dofv-flow"})
				require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

				var created map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				flowID, _ := created["id"].(string)
				require.NotEmpty(t, flowID)

				verRec := doAgentRequest(t, h, http.MethodPost, fmt.Sprintf("/flows/%s/versions", flowID), nil)
				require.Equal(t, http.StatusCreated, verRec.Code, verRec.Body.String())

				var verBody map[string]any
				require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verBody))
				version, _ := verBody["version"].(string)
				require.NotEmpty(t, version)

				return h, http.MethodDelete, fmt.Sprintf("/flows/%s/versions/%s", flowID, version)
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"id", "version"},
		},
		{
			name: "DeleteFlowAlias",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, _ := newTestAgentsHandler(t)
				rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "dofa-flow"})
				require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

				var created map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				flowID, _ := created["id"].(string)
				require.NotEmpty(t, flowID)

				aliasRec := doAgentRequest(
					t, h, http.MethodPost, fmt.Sprintf("/flows/%s/aliases", flowID),
					map[string]any{"name": "dofa-alias"},
				)
				require.Equal(t, http.StatusCreated, aliasRec.Code, aliasRec.Body.String())

				var aliasBody map[string]any
				require.NoError(t, json.Unmarshal(aliasRec.Body.Bytes(), &aliasBody))
				aliasID, _ := aliasBody["id"].(string)
				require.NotEmpty(t, aliasID)

				return h, http.MethodDelete, fmt.Sprintf("/flows/%s/aliases/%s", flowID, aliasID)
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"flowId", "id"},
		},
		{
			name: "DeleteAgent",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, b := newTestAgentsHandler(t)
				ag, err := b.CreateAgent("doa-agent", "", "", "", nil)
				require.NoError(t, err)

				return h, http.MethodDelete, "/agents/" + ag.AgentID
			},
			wantCode: http.StatusAccepted,
			wantKeys: []string{"agentId", "agentStatus"},
		},
		{
			name: "DeleteAgentAlias",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, b := newTestAgentsHandler(t)
				ag, err := b.CreateAgent("doaa-agent", "", "", "", nil)
				require.NoError(t, err)

				aliasRec := doAgentRequest(t, h, http.MethodPut, "/agents/"+ag.AgentID+"/aliases", map[string]any{
					"agentAliasName": "doaa-alias",
				})
				require.Equal(t, http.StatusAccepted, aliasRec.Code, aliasRec.Body.String())

				var aliasOut map[string]any
				require.NoError(t, json.Unmarshal(aliasRec.Body.Bytes(), &aliasOut))
				alias, _ := aliasOut["agentAlias"].(map[string]any)
				aliasID, _ := alias["agentAliasId"].(string)
				require.NotEmpty(t, aliasID)

				return h, http.MethodDelete, "/agents/" + ag.AgentID + "/aliases/" + aliasID
			},
			wantCode: http.StatusAccepted,
			wantKeys: []string{"agentId", "agentAliasId", "agentAliasStatus"},
		},
		{
			// Regression coverage for the bug this sweep found: the handler
			// emitted the version under "version" instead of the real
			// DeleteAgentVersionOutput wire key "agentVersion" (confirmed
			// against bedrockagent@v1.58.4 deserializers.go's
			// awsRestjson1_deserializeOpDocumentDeleteAgentVersionOutput,
			// case "agentVersion").
			name: "DeleteAgentVersion",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, b := newTestAgentsHandler(t)
				ag, err := b.CreateAgent("doav-agent", "", "", "", nil)
				require.NoError(t, err)

				av, err := b.CreateAgentVersion(ag.AgentID)
				require.NoError(t, err)

				return h, http.MethodDelete, "/agents/" + ag.AgentID + "/agentversions/" + av.AgentVersion
			},
			wantCode: http.StatusAccepted,
			wantKeys: []string{"agentId", "agentVersion", "agentStatus"},
		},
		{
			name: "DeleteKnowledgeBase",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, b := newTestAgentsHandler(t)
				kb, err := b.CreateKnowledgeBase("dokb-kb", "", "arn:aws:iam::000000000000:role/kb-role", nil, nil, nil)
				require.NoError(t, err)

				return h, http.MethodDelete, "/knowledgebases/" + kb.KnowledgeBaseID
			},
			wantCode: http.StatusAccepted,
			wantKeys: []string{"knowledgeBaseId", "status"},
		},
		{
			name: "DeleteDataSource",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, _ := newTestAgentsHandler(t)
				kbID, dsID := createKBAndDS(t, h)

				return h, http.MethodDelete, fmt.Sprintf("/knowledgebases/%s/datasources/%s", kbID, dsID)
			},
			wantCode: http.StatusOK,
			wantKeys: []string{"dataSourceId", "knowledgeBaseId", "status"},
		},
		{
			// DeleteAgentActionGroupOutput (bedrockagent@v1.58.4
			// api_op_DeleteAgentActionGroup.go) declares no members at all.
			name: "DeleteAgentActionGroup",
			setup: func(t *testing.T) (*bedrock.AgentsHandler, string, string) {
				t.Helper()

				h, b := newTestAgentsHandler(t)
				ag, err := b.CreateAgent("doag-agent", "", "", "", nil)
				require.NoError(t, err)

				agRec := doAgentRequest(t, h, http.MethodPost, "/agents/"+ag.AgentID+"/action-groups", map[string]any{
					"actionGroupName": "doag-group",
				})
				require.Equal(t, http.StatusOK, agRec.Code, agRec.Body.String())

				var agOut map[string]any
				require.NoError(t, json.Unmarshal(agRec.Body.Bytes(), &agOut))
				group, _ := agOut["agentActionGroup"].(map[string]any)
				groupID, _ := group["actionGroupId"].(string)
				require.NotEmpty(t, groupID)

				return h, http.MethodDelete, fmt.Sprintf("/agents/%s/action-groups/DRAFT/%s", ag.AgentID, groupID)
			},
			wantCode: http.StatusOK,
			wantKeys: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, method, path := tt.setup(t)

			rec := doAgentRequest(t, h, method, path, nil)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

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
