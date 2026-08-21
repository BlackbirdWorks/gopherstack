package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestDeleteDataSource_NoInventedStatusKey_RealClient covers gopherstack-y1zn.
// handleDeleteDataSource emitted "dataSourceStatus"; DeleteDataSourceOutput
// (bedrockagent@v1.58.4 deserializers.go's
// awsRestjson1_deserializeOpDocumentDeleteDataSourceOutput) declares exactly
// dataSourceId/knowledgeBaseId/status. A typed client silently ignores the
// unknown key and never sees Status at all, so the proof is the raw body.
func TestDeleteDataSource_NoInventedStatusKey_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	rec := doAgentRequest(
		t, h, http.MethodDelete,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s", kbID, dsID),
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"dataSourceStatus"`,
		"DeleteDataSourceOutput has no dataSourceStatus member")
	assert.Contains(t, body, `"status"`,
		"DeleteDataSourceOutput's real member is status")

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "DELETING", out["status"])
}

// TestIngestKBDocuments_DocumentDetailsKey_RealClient covers
// gopherstack-y1zn. handleIngestKBDocuments emitted "documents";
// IngestKnowledgeBaseDocumentsOutput (bedrockagent@v1.58.4 deserializers.go's
// awsRestjson1_deserializeOpDocumentIngestKnowledgeBaseDocumentsOutput)
// declares only documentDetails, exactly like its List/Get siblings in the
// same file already emit correctly.
func TestIngestKBDocuments_DocumentDetailsKey_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	rec := doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID),
		map[string]any{"documentIds": []string{"doc-1"}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"documents"`,
		"IngestKnowledgeBaseDocumentsOutput has no documents member")
	assert.Contains(t, body, `"documentDetails"`,
		"IngestKnowledgeBaseDocumentsOutput's real member is documentDetails")
}

// TestARPBuildWorkflowResultAssets_BuildWorkflowAssetsKey_RealClient covers
// gopherstack-y1zn. handleGetARPBuildWorkflowResultAssets emitted
// "resultAssets": []; GetAutomatedReasoningPolicyBuildWorkflowResultAssetsOutput
// (bedrock@v1.66.4 deserializers.go's
// awsRestjson1_deserializeOpDocumentGetAutomatedReasoningPolicyBuildWorkflowResultAssetsOutput)
// has no resultAssets member at all -- its real member, buildWorkflowAssets, is
// a union object (types.AutomatedReasoningPolicyBuildResultAssets), not a
// list, so a real client's decode of BuildWorkflowAssets stays nil either way
// (a wrong key is silently ignored same as a right key with no case matched);
// only the raw body can show the key was ever wrong. This also proves the
// value shape: the fixed handler omits the key entirely rather than emitting
// an empty array, which a real client's union deserializer rejects outright
// (see the sibling test in wire_output_required_r80d_test.go, which drove
// this fix and errors on decode if buildWorkflowAssets is ever an array).
func TestARPBuildWorkflowResultAssets_BuildWorkflowAssetsKey_RealClient(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	policy, err := b.CreateAutomatedReasoningPolicy("y1zn-arp-policy", "", nil)
	require.NoError(t, err)

	wf, err := b.StartAutomatedReasoningPolicyBuildWorkflow(
		policy.PolicyArn, "INGEST_CONTENT", []byte(`{"policyDefinition":{"version":"1"}}`),
	)
	require.NoError(t, err)

	rec := doRequest(
		t, h, http.MethodGet,
		fmt.Sprintf(
			"/automated-reasoning-policies/%s/build-workflows/%s/result-assets?assetType=POLICY_DEFINITION",
			policy.PolicyArn, wf.BuildWorkflowID,
		),
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"resultAssets"`,
		"GetAutomatedReasoningPolicyBuildWorkflowResultAssetsOutput has no resultAssets member")
}
