package bedrock_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

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
