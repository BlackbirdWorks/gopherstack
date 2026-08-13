package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestARPStartBuildWorkflow_TypedClient drives
// StartAutomatedReasoningPolicyBuildWorkflow through the real aws-sdk-go-v2
// client (gopherstack-4sov). Before the fix this failed two independent ways:
// the real path is .../build-workflows/{buildWorkflowType}/start
// (bedrock@v1.66.4 serializers.go:8008), which gopherstack's routing never
// matched (a real client always got 404, never reaching the handler at all),
// and even routed correctly the required buildWorkflowType/sourceContent were
// never read. Get's buildWorkflowType now round-trips what Start captured.
func TestARPStartBuildWorkflow_TypedClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	policy, err := client.CreateAutomatedReasoningPolicy(
		t.Context(), &bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("typed-start-wf-policy")},
	)
	require.NoError(t, err)

	out, err := client.StartAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.StartAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:         policy.PolicyArn,
			BuildWorkflowType: types.AutomatedReasoningPolicyBuildWorkflowTypeIngestContent,
			SourceContent: &types.AutomatedReasoningPolicyBuildWorkflowSource{
				PolicyDefinition: &types.AutomatedReasoningPolicyDefinition{Version: aws.String("1")},
			},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.BuildWorkflowId))

	got, err := client.GetAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: out.BuildWorkflowId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.AutomatedReasoningPolicyBuildWorkflowTypeIngestContent, got.BuildWorkflowType)
}

// TestARPUpdatePolicy_TypedClient drives UpdateAutomatedReasoningPolicy
// through the real client (gopherstack-4sov). The old handler both dropped
// the required policyDefinition on the way in and returned a fabricated
// "status" key instead of the real definitionHash/name/policyArn/updatedAt on
// the way out -- out.DefinitionHash would have decoded to empty under the
// unfixed handler regardless of what was sent.
func TestARPUpdatePolicy_TypedClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	policy, err := client.CreateAutomatedReasoningPolicy(
		t.Context(), &bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("typed-update-policy")},
	)
	require.NoError(t, err)

	out, err := client.UpdateAutomatedReasoningPolicy(
		t.Context(),
		&bedrocksdk.UpdateAutomatedReasoningPolicyInput{
			PolicyArn:        policy.PolicyArn,
			PolicyDefinition: &types.AutomatedReasoningPolicyDefinition{Version: aws.String("2")},
			Description:      aws.String("typed update"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(policy.PolicyArn), aws.ToString(out.PolicyArn))
	assert.NotEmpty(t, aws.ToString(out.DefinitionHash))
	assert.NotEqual(t, aws.ToString(policy.DefinitionHash), aws.ToString(out.DefinitionHash))
}

// TestARPUpdateAnnotations_TypedClient drives Get/UpdateAutomatedReasoningPolicyAnnotations
// through the real client (gopherstack-4sov). The old handler read neither
// required request field (annotations, lastUpdatedAnnotationSetHash) and
// answered with an empty 200 body, so every required response field
// (annotationSetHash, buildWorkflowId, policyArn, updatedAt) would have
// decoded to its zero value, and a follow-up Get would never see the
// annotations that were supposedly just stored.
func TestARPUpdateAnnotations_TypedClient(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	policy, err := client.CreateAutomatedReasoningPolicy(
		t.Context(), &bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("typed-annotations-policy")},
	)
	require.NoError(t, err)

	wf, err := client.StartAutomatedReasoningPolicyBuildWorkflow(
		t.Context(),
		&bedrocksdk.StartAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:         policy.PolicyArn,
			BuildWorkflowType: types.AutomatedReasoningPolicyBuildWorkflowTypeIngestContent,
			SourceContent:     &types.AutomatedReasoningPolicyBuildWorkflowSource{},
		},
	)
	require.NoError(t, err)

	preAnn, err := client.GetAutomatedReasoningPolicyAnnotations(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyAnnotationsInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: wf.BuildWorkflowId,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(preAnn.AnnotationSetHash))

	updated, err := client.UpdateAutomatedReasoningPolicyAnnotations(
		t.Context(),
		&bedrocksdk.UpdateAutomatedReasoningPolicyAnnotationsInput{
			PolicyArn:                    policy.PolicyArn,
			BuildWorkflowId:              wf.BuildWorkflowId,
			LastUpdatedAnnotationSetHash: preAnn.AnnotationSetHash,
			Annotations: []types.AutomatedReasoningPolicyAnnotation{
				&types.AutomatedReasoningPolicyAnnotationMemberAddRule{
					Value: types.AutomatedReasoningPolicyAddRuleAnnotation{Expression: aws.String("x > 0")},
				},
			},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(updated.AnnotationSetHash))
	assert.Equal(t, aws.ToString(policy.PolicyArn), aws.ToString(updated.PolicyArn))
	assert.Equal(t, aws.ToString(wf.BuildWorkflowId), aws.ToString(updated.BuildWorkflowId))

	postAnn, err := client.GetAutomatedReasoningPolicyAnnotations(
		t.Context(),
		&bedrocksdk.GetAutomatedReasoningPolicyAnnotationsInput{
			PolicyArn:       policy.PolicyArn,
			BuildWorkflowId: wf.BuildWorkflowId,
		},
	)
	require.NoError(t, err)
	require.Len(t, postAnn.Annotations, 1)

	addRule, ok := postAnn.Annotations[0].(*types.AutomatedReasoningPolicyAnnotationMemberAddRule)
	require.True(t, ok)
	assert.Equal(t, "x > 0", aws.ToString(addRule.Value.Expression))
}
