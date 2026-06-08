package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
)

func TestAgentsHandler_AgentExtendedConfiguration(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	rec := doAgentRequest(t, h, http.MethodPost, "/agents", map[string]any{
		"agentName":          "configured-agent",
		"foundationModel":    "amazon.titan-text-express-v1",
		"agentCollaboration": "SUPERVISOR",
		"description":        "coordinates specialists",
		"guardrailConfiguration": map[string]any{
			"guardrailIdentifier": "guardrail-1",
			"guardrailVersion":    "1",
		},
		"memoryConfiguration": map[string]any{
			"enabledMemoryTypes": []string{"SESSION_SUMMARY"},
			"storageDays":        float64(7),
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	agent := body["agent"].(map[string]any)
	assert.Equal(t, "SUPERVISOR", agent["agentCollaboration"])
	assert.Equal(t, "guardrail-1", agent["guardrailConfiguration"].(map[string]any)["guardrailIdentifier"])
	assert.InDelta(t, 7, agent["memoryConfiguration"].(map[string]any)["storageDays"], 0)
}

func TestAgentsHandler_ActionGroupSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		field     string
		schema    map[string]any
		wantValue string
	}{
		{
			name:      "openapi schema",
			field:     "apiSchema",
			schema:    map[string]any{"payload": "openapi: 3.0.0"},
			wantValue: "openapi: 3.0.0",
		},
		{
			name:      "function schema",
			field:     "functionSchema",
			schema:    map[string]any{"functions": "search"},
			wantValue: "search",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			agent, err := b.CreateAgent("schema-"+tt.name, "model", "", "", nil)
			require.NoError(t, err)
			body := map[string]any{"actionGroupName": "tools", tt.field: tt.schema}
			rec := doAgentRequest(t, h, http.MethodPost, "/agents/"+agent.AgentID+"/action-groups", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			schema := out["agentActionGroup"].(map[string]any)[tt.field].(map[string]any)
			assert.Contains(t, fmt.Sprint(schema), tt.wantValue)
		})
	}
}

func TestAgentsHandler_AliasHistoryEvents(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("history-agent", "model", "", "", nil)
	require.NoError(t, err)
	path := "/agents/" + agent.AgentID + "/aliases"

	create := doAgentRequest(t, h, http.MethodPost, path, map[string]any{
		"agentAliasName":       "live",
		"routingConfiguration": []map[string]string{{"agentVersion": "1"}},
	})
	require.Equal(t, http.StatusAccepted, create.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(create.Body.Bytes(), &created))
	aliasID := created["agentAlias"].(map[string]any)["agentAliasId"].(string)
	update := doAgentRequest(t, h, http.MethodPut, path+"/"+aliasID, map[string]any{
		"routingConfiguration": []map[string]string{{"agentVersion": "2"}},
	})
	require.Equal(t, http.StatusOK, update.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(update.Body.Bytes(), &updated))
	alias := updated["agentAlias"].(map[string]any)
	events := alias["agentAliasHistoryEvents"].([]any)
	require.Len(t, events, 2)
	assert.NotEmpty(t, events[0].(map[string]any)["endDate"])
	assert.Equal(t, "2", alias["routingConfiguration"].([]any)[0].(map[string]any)["agentVersion"])
}

func TestAgentsHandler_DataSourceIngestionConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sourceType       string
		chunkingStrategy string
		parsingStrategy  string
	}{
		{sourceType: "S3", chunkingStrategy: "FIXED_SIZE", parsingStrategy: "BEDROCK_FOUNDATION_MODEL"},
		{sourceType: "WEB", chunkingStrategy: "HIERARCHICAL", parsingStrategy: "BEDROCK_DATA_AUTOMATION"},
		{sourceType: "CONFLUENCE", chunkingStrategy: "SEMANTIC", parsingStrategy: "BEDROCK_FOUNDATION_MODEL"},
		{sourceType: "SALESFORCE", chunkingStrategy: "FIXED_SIZE", parsingStrategy: "BEDROCK_DATA_AUTOMATION"},
		{sourceType: "SHAREPOINT", chunkingStrategy: "HIERARCHICAL", parsingStrategy: "BEDROCK_FOUNDATION_MODEL"},
	}

	for _, tt := range tests {
		t.Run(tt.sourceType, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			kb, err := b.CreateKnowledgeBase("kb-"+tt.sourceType, "", "", nil, nil, nil)
			require.NoError(t, err)
			rec := doAgentRequest(
				t, h, http.MethodPost,
				fmt.Sprintf("/knowledgebases/%s/datasources", kb.KnowledgeBaseID),
				map[string]any{
					"name":                    "source",
					"dataSourceConfiguration": map[string]any{"type": tt.sourceType},
					"vectorIngestionConfiguration": map[string]any{
						"chunkingConfiguration": map[string]any{"chunkingStrategy": tt.chunkingStrategy},
						"parsingConfiguration":  map[string]any{"parsingStrategy": tt.parsingStrategy},
					},
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			ds := body["dataSource"].(map[string]any)
			assert.Equal(t, tt.sourceType, ds["dataSourceConfiguration"].(map[string]any)["type"])
			vector := ds["vectorIngestionConfiguration"].(map[string]any)
			assert.Equal(t, tt.chunkingStrategy, vector["chunkingConfiguration"].(map[string]any)["chunkingStrategy"])
			assert.Equal(t, tt.parsingStrategy, vector["parsingConfiguration"].(map[string]any)["parsingStrategy"])
		})
	}
}

func TestAgentsHandler_AWSCanonicalRoutes(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	agentRec := doAgentRequest(t, h, http.MethodPut, "/agents/", map[string]any{
		"agentName":       "sdk-agent",
		"foundationModel": "model",
	})
	require.Equal(t, http.StatusAccepted, agentRec.Code)

	var agentBody map[string]any
	require.NoError(t, json.Unmarshal(agentRec.Body.Bytes(), &agentBody))
	agentID := agentBody["agent"].(map[string]any)["agentId"].(string)

	tests := []struct {
		body   map[string]any
		name   string
		method string
		path   string
		code   int
	}{
		{
			name:   "prepare draft",
			method: http.MethodPost,
			path:   fmt.Sprintf("/agents/%s/agentversions/DRAFT/", agentID),
			code:   http.StatusAccepted,
		},
		{
			name:   "create alias",
			method: http.MethodPut,
			path:   fmt.Sprintf("/agents/%s/agentaliases/", agentID),
			body:   map[string]any{"agentAliasName": "live"},
			code:   http.StatusAccepted,
		},
		{
			name:   "create action group",
			method: http.MethodPut,
			path:   fmt.Sprintf("/agents/%s/agentversions/DRAFT/actiongroups/", agentID),
			body:   map[string]any{"actionGroupName": "tools"},
			code:   http.StatusOK,
		},
		{
			name:   "create flow",
			method: http.MethodPut,
			path:   "/flows/",
			body:   map[string]any{"name": "sdk-flow"},
			code:   http.StatusCreated,
		},
		{
			name:   "create prompt",
			method: http.MethodPut,
			path:   "/prompts/",
			body:   map[string]any{"name": "sdk-prompt"},
			code:   http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doAgentRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.code, rec.Code, rec.Body.String())
		})
	}

	kbRec := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/", map[string]any{"name": "sdk-kb"})
	require.Equal(t, http.StatusAccepted, kbRec.Code)
	var kbBody map[string]any
	require.NoError(t, json.Unmarshal(kbRec.Body.Bytes(), &kbBody))
	kbID := kbBody["knowledgeBase"].(map[string]any)["knowledgeBaseId"].(string)
	dsRec := doAgentRequest(t, h, http.MethodPut,
		fmt.Sprintf("/knowledgebases/%s/datasources/", kbID), map[string]any{"name": "sdk-source"})
	assert.Equal(t, http.StatusOK, dsRec.Code)
}

func TestAgentsHandler_KnowledgeBaseAssociationUpdateAndDocumentGet(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("kb-update-agent", "model", "", "", nil)
	require.NoError(t, err)
	kbID, dsID := createKBAndDS(t, h)
	assocPath := fmt.Sprintf("/agents/%s/agentversions/DRAFT/knowledgebases", agent.AgentID)

	rec := doAgentRequest(t, h, http.MethodPut, assocPath, map[string]any{"knowledgeBaseId": kbID})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doAgentRequest(t, h, http.MethodPut, assocPath+"/"+kbID, map[string]any{
		"description":        "updated association",
		"knowledgeBaseState": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var assoc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assoc))
	assert.Equal(t, "DISABLED", assoc["agentKnowledgeBase"].(map[string]any)["knowledgeBaseState"])

	docPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID)
	rec = doAgentRequest(t, h, http.MethodPost, docPath, map[string]any{"documentIds": []string{"one", "two"}})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doAgentRequest(t, h, http.MethodPost, docPath+"/getDocuments", map[string]any{"documentIds": []string{"two"}})
	require.Equal(t, http.StatusOK, rec.Code)
	var docs map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &docs))
	assert.Len(t, docs["documentDetails"], 1)
}

func TestAgentsHandler_SDKCompleteness(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	sdkcheck.CheckCompleteness(t, &bedrockagentsdk.Client{}, h.GetSupportedOperations(), []string{})
}
