package bedrock_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func TestDeleteAutomatedReasoningPolicy_PrunesVersionCounterAndAnnotations(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend(testAccountID, testRegion)

	policy, err := b.CreateAutomatedReasoningPolicy("policy1", "", nil)
	require.NoError(t, err)

	_, err = b.CreateAutomatedReasoningPolicyVersion(policy.PolicyArn, "hash1", nil)
	require.NoError(t, err)

	wf, err := b.StartAutomatedReasoningPolicyBuildWorkflow(
		policy.PolicyArn,
		"INGEST_CONTENT",
		json.RawMessage(`{}`),
	)
	require.NoError(t, err)

	_, err = b.UpdateAutomatedReasoningPolicyAnnotations(
		policy.PolicyArn,
		wf.BuildWorkflowID,
		[]any{},
		"hash",
	)
	require.NoError(t, err)

	require.True(t, b.ARPAnnotationStateExistsForTest(policy.PolicyArn, wf.BuildWorkflowID),
		"test setup must actually mint annotation state before deletion")
	assert.Positive(
		t,
		b.ARPVersionCountForTest(
			policy.PolicyArn,
		),
		"test setup must actually bump the version counter",
	)

	require.NoError(t, b.DeleteAutomatedReasoningPolicy(policy.PolicyArn, true))

	assert.Zero(
		t,
		b.ARPVersionCountForTest(policy.PolicyArn),
		"arpVersionCountByPolicy must be pruned on policy delete",
	)
	assert.False(t, b.ARPAnnotationStateExistsForTest(policy.PolicyArn, wf.BuildWorkflowID),
		"annotation state must be pruned when the owning policy is deleted")
}

func TestDeleteAutomatedReasoningPolicyBuildWorkflow_PrunesAnnotations(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend(testAccountID, testRegion)

	policy, err := b.CreateAutomatedReasoningPolicy("policy1", "", nil)
	require.NoError(t, err)

	wf, err := b.StartAutomatedReasoningPolicyBuildWorkflow(
		policy.PolicyArn,
		"INGEST_CONTENT",
		json.RawMessage(`{}`),
	)
	require.NoError(t, err)

	_, err = b.UpdateAutomatedReasoningPolicyAnnotations(
		policy.PolicyArn,
		wf.BuildWorkflowID,
		[]any{},
		"hash",
	)
	require.NoError(t, err)

	require.True(t, b.ARPAnnotationStateExistsForTest(policy.PolicyArn, wf.BuildWorkflowID),
		"test setup must actually mint annotation state before deletion")

	require.NoError(
		t,
		b.DeleteAutomatedReasoningPolicyBuildWorkflow(policy.PolicyArn, wf.BuildWorkflowID),
	)

	assert.False(t, b.ARPAnnotationStateExistsForTest(policy.PolicyArn, wf.BuildWorkflowID),
		"annotation state must be pruned when its owning build workflow is deleted")
}
