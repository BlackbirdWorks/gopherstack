package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentsHandler_Reset(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)

	// Create a variety of resources.
	ag, err := b.CreateAgent("reset-agent", "", "", "", nil)
	require.NoError(t, err)

	kb, err := b.CreateKnowledgeBase("reset-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(kb.KnowledgeBaseID, "reset-ds", "", nil)
	require.NoError(t, err)

	_, err = b.CreateAgentAlias(ag.AgentID, "reset-alias", "DRAFT")
	require.NoError(t, err)

	// Verify resources exist.
	rec := doAgentRequest(t, h, http.MethodGet, "/agents", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Len(t, listOut["agentSummaries"].([]any), 1)

	// Reset.
	h.Reset()

	// All agents should be gone.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/agents", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2["agentSummaries"].([]any))

	// All KBs should be gone.
	rec3 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var listKBOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listKBOut))
	assert.Empty(t, listKBOut["knowledgeBaseSummaries"].([]any))

	// IDs reset - first agent after reset should get sequential id again.
	ag2, err := b.CreateAgent("post-reset-agent", "", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "agent-00000001", ag2.AgentID)
}

func TestAgentsHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	rec := doAgentRequest(t, h, http.MethodGet, "/unknown-path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_SupportedOperationsCount(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	ops := h.GetSupportedOperations()
	// Should be at least 75 operations (33 original + 43 new)
	assert.GreaterOrEqual(t, len(ops), 75)
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

func TestAgentsHandler_SDKCompleteness(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	sdkcheck.CheckCompleteness(t, &bedrockagentsdk.Client{}, h.GetSupportedOperations(), []string{})
}

// createTestKnowledgeBase creates a knowledge base directly on the backend
// and returns its ARN, for use as a real resource-policy target.
func createTestKnowledgeBase(t *testing.T, b *bedrock.InMemoryBackend, name string) string {
	t.Helper()

	kb, err := b.CreateKnowledgeBase(name, "", "", nil, nil, nil)
	require.NoError(t, err)

	return kb.KnowledgeBaseArn
}

// TestAgentsHandler_ResourcePolicy covers bedrock-agent's PutResourcePolicy,
// GetResourcePolicy, and DeleteResourcePolicy -- a knowledge-base-only
// resource family distinct from core bedrock's own ResourcePolicy ops (see
// resource_policy.go's package doc comment). Each case builds its own
// isolated handler/backend so subtests share no state and can run fully in
// parallel.
func TestAgentsHandler_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "put get delete lifecycle on a real knowledge base",
			run: func(t *testing.T) {
				t.Helper()

				h, b := newTestAgentsHandler(t)
				kbArn := createTestKnowledgeBase(t, b, "policy-kb")

				putRec := doAgentRequest(t, h, http.MethodPut, "/resourcepolicy/"+url.PathEscape(kbArn), map[string]any{
					"policy": `{"Version":"2012-10-17","Statement":[]}`,
				})
				require.Equal(t, http.StatusOK, putRec.Code)

				var putOut map[string]any
				require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
				assert.Equal(t, kbArn, putOut["resourceArn"])
				firstRevision, ok := putOut["revisionId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, firstRevision)

				getRec := doAgentRequest(t, h, http.MethodGet, "/resourcepolicy/"+url.PathEscape(kbArn), nil)
				require.Equal(t, http.StatusOK, getRec.Code)

				var getOut map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
				policy, ok := getOut["policy"].(string)
				require.True(t, ok)
				assert.Contains(t, policy, "Statement")
				assert.Equal(t, firstRevision, getOut["revisionId"])

				delRec := doAgentRequest(t, h, http.MethodDelete, "/resourcepolicy/"+url.PathEscape(kbArn), nil)
				require.Equal(t, http.StatusOK, delRec.Code)

				getAfterDelete := doAgentRequest(t, h, http.MethodGet, "/resourcepolicy/"+url.PathEscape(kbArn), nil)
				assert.Equal(t, http.StatusNotFound, getAfterDelete.Code)
			},
		},
		{
			name: "put on an ARN that is not a knowledge base is a validation error",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newTestAgentsHandler(t)
				rec := doAgentRequest(
					t, h, http.MethodPut,
					"/resourcepolicy/"+url.PathEscape("arn:aws:bedrock:us-east-1:000000000000:guardrail/gr-1"),
					map[string]any{"policy": `{}`},
				)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "put on a nonexistent knowledge base is not found",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newTestAgentsHandler(t)
				rec := doAgentRequest(
					t,
					h,
					http.MethodPut,
					"/resourcepolicy/"+url.PathEscape(
						"arn:aws:bedrock:us-east-1:000000000000:knowledge-base/kb-missing",
					),
					map[string]any{"policy": `{}`},
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "put with a stale expectedRevisionId is a conflict",
			run: func(t *testing.T) {
				t.Helper()

				h, b := newTestAgentsHandler(t)
				kbArn := createTestKnowledgeBase(t, b, "policy-kb-conflict")

				first := doAgentRequest(t, h, http.MethodPut, "/resourcepolicy/"+url.PathEscape(kbArn), map[string]any{
					"policy": `{}`,
				})
				require.Equal(t, http.StatusOK, first.Code)

				stale := doAgentRequest(t, h, http.MethodPut, "/resourcepolicy/"+url.PathEscape(kbArn), map[string]any{
					"policy":             `{}`,
					"expectedRevisionId": "not-the-current-revision",
				})
				assert.Equal(t, http.StatusConflict, stale.Code)
			},
		},
		{
			name: "delete with a matching expectedRevisionId returns the deleted revision",
			run: func(t *testing.T) {
				t.Helper()

				h, b := newTestAgentsHandler(t)
				kbArn := createTestKnowledgeBase(t, b, "policy-kb-delete-rev")

				putRec := doAgentRequest(t, h, http.MethodPut, "/resourcepolicy/"+url.PathEscape(kbArn), map[string]any{
					"policy": `{}`,
				})
				require.Equal(t, http.StatusOK, putRec.Code)

				var putOut map[string]any
				require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
				revision, ok := putOut["revisionId"].(string)
				require.True(t, ok)

				delRec := doAgentRequest(
					t, h, http.MethodDelete,
					"/resourcepolicy/"+url.PathEscape(kbArn)+"?expectedRevisionId="+revision,
					nil,
				)
				require.Equal(t, http.StatusOK, delRec.Code)

				var delOut map[string]any
				require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delOut))
				assert.Equal(t, revision, delOut["revisionId"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
