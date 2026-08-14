package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBedrockAgentLists_OmitGetOnlyFields asserts the raw decoded response
// body -- not an SDK-typed client, which silently discards unmodeled keys --
// for two List ops that were reusing a Get-shaped struct and leaking members
// the real AWS SDK Summary type does not declare (bedrockagent@v1.58.4,
// types/types.go): AgentKnowledgeBaseSummary has no agentId/agentVersion/
// createdAt, and FlowVersionSummary has no name/description.
func TestBedrockAgentLists_OmitGetOnlyFields(t *testing.T) {
	t.Parallel()

	t.Run("list agent knowledge bases", func(t *testing.T) {
		t.Parallel()

		h, e := setupHandler(t)

		agentRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
			"agentName":            "wire-kb-agent",
			"foundationModel":      "anthropic.claude-v2",
			"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		})
		require.Equal(t, http.StatusOK, agentRec.Code, agentRec.Body.String())

		var agentResp map[string]map[string]any
		require.NoError(t, json.Unmarshal(agentRec.Body.Bytes(), &agentResp))
		agentID, _ := agentResp["agent"]["agentId"].(string)
		require.NotEmpty(t, agentID)

		kbRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", map[string]any{
			"name":                       "wire-kb",
			"roleArn":                    "arn:aws:iam::123456789012:role/KBRole",
			"knowledgeBaseConfiguration": map[string]any{"type": "VECTOR"},
			"storageConfiguration":       map[string]any{"type": "OPENSEARCH_SERVERLESS"},
		})
		require.Equal(t, http.StatusOK, kbRec.Code, kbRec.Body.String())

		var kbResp map[string]map[string]any
		require.NoError(t, json.Unmarshal(kbRec.Body.Bytes(), &kbResp))
		kbID, _ := kbResp["knowledgeBase"]["knowledgeBaseId"].(string)
		require.NotEmpty(t, kbID)

		assocRec := doRequest(t, h, e, http.MethodPut,
			"/agents/"+agentID+"/agentversions/DRAFT/knowledgebases", map[string]any{
				"knowledgeBaseId": kbID,
				"description":     "wire test association",
			})
		require.Equal(t, http.StatusOK, assocRec.Code, assocRec.Body.String())

		listRec := doRequest(t, h, e, http.MethodGet,
			"/agents/"+agentID+"/agentversions/DRAFT/knowledgebases", nil)
		require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

		var raw map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &raw))

		summaries, ok := raw["agentKnowledgeBaseSummaries"].([]any)
		require.True(t, ok)
		require.Len(t, summaries, 1)

		member, ok := summaries[0].(map[string]any)
		require.True(t, ok)

		require.ElementsMatch(t,
			[]string{"updatedAt", "knowledgeBaseId", "knowledgeBaseState", "description"},
			keysOf(member),
		)
	})

	t.Run("list flow versions", func(t *testing.T) {
		t.Parallel()

		h, e := setupHandler(t)

		createRec := doRequest(t, h, e, http.MethodPost, "/flows", map[string]any{
			"name":             "wire-flow-version",
			"executionRoleArn": "arn:aws:iam::123456789012:role/FlowRole",
			"description":      "should not leak into the list summary",
			"definition": map[string]any{
				"nodes":       []any{},
				"connections": []any{},
			},
		})
		require.Equal(t, http.StatusCreated, createRec.Code, createRec.Body.String())

		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		flowID, _ := createResp["id"].(string)
		require.NotEmpty(t, flowID)

		versionRec := doRequest(t, h, e, http.MethodPost, "/flows/"+flowID+"/versions",
			map[string]any{"description": "version-only description, must not leak"})
		require.Equal(t, http.StatusCreated, versionRec.Code, versionRec.Body.String())

		listRec := doRequest(t, h, e, http.MethodGet, "/flows/"+flowID+"/versions", nil)
		require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

		var raw map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &raw))

		summaries, ok := raw["flowVersionSummaries"].([]any)
		require.True(t, ok)
		require.Len(t, summaries, 1)

		member, ok := summaries[0].(map[string]any)
		require.True(t, ok)

		require.ElementsMatch(t,
			[]string{"createdAt", "arn", "id", "status", "version"},
			keysOf(member),
		)
	})
}

func keysOf(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
