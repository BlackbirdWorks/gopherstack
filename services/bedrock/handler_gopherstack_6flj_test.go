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

// TestListOps_WrapperKeyRegressions drives bedrock List ops through the real
// aws-sdk-go-v2 client and asserts the decoded collection is non-empty with
// correct contents (gopherstack-6flj). Before the fix, ListCustomModelDeployments
// emitted "deploymentSummaries" instead of the real modelDeploymentSummaries
// (bedrock@v1.66.4 deserializers.go,
// awsRestjson1_deserializeOpDocumentListCustomModelDeploymentsOutput),
// ListAutomatedReasoningPolicies emitted "automatedReasoningPolicies" instead of
// automatedReasoningPolicySummaries, and ListAutomatedReasoningPolicyBuildWorkflows
// emitted "buildWorkflows" instead of
// automatedReasoningPolicyBuildWorkflowSummaries -- in every case a real typed
// client silently decoded an empty slice, 200 OK, err == nil. Also fixed:
// ListCustomModelDeployments' per-item summary used modelDeploymentName/
// creationTime/lastModifiedTime, but CustomModelDeploymentSummary's own wire
// shape is customModelDeploymentName/createdAt/lastUpdatedAt (a sibling of the
// singular GetCustomModelDeployment shape, which genuinely does use
// modelDeploymentName/createdAt/lastUpdatedAt -- confirmed independently
// against awsRestjson1_deserializeOpDocumentGetCustomModelDeploymentOutput
// before assuming the two shapes matched).
func TestListOps_WrapperKeyRegressions(t *testing.T) {
	t.Parallel()

	t.Run("custom model deployments", func(t *testing.T) {
		t.Parallel()

		client := newTestBedrockClient(
			t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
		)

		created, err := client.CreateCustomModelDeployment(t.Context(), &bedrocksdk.CreateCustomModelDeploymentInput{
			ModelArn:            aws.String("arn:aws:bedrock:us-east-1::custom-model/src"),
			ModelDeploymentName: aws.String("wrapper-key-deploy"),
		})
		require.NoError(t, err)

		out, err := client.ListCustomModelDeployments(t.Context(), &bedrocksdk.ListCustomModelDeploymentsInput{})
		require.NoError(t, err)
		require.Len(t, out.ModelDeploymentSummaries, 1)

		got := out.ModelDeploymentSummaries[0]
		assert.Equal(t, aws.ToString(created.CustomModelDeploymentArn), aws.ToString(got.CustomModelDeploymentArn))
		assert.Equal(t, "wrapper-key-deploy", aws.ToString(got.CustomModelDeploymentName))
		assert.NotNil(t, got.CreatedAt)
		assert.NotNil(t, got.LastUpdatedAt)
	})

	t.Run("automated reasoning policies", func(t *testing.T) {
		t.Parallel()

		client := newTestBedrockClient(
			t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
		)

		created, err := client.CreateAutomatedReasoningPolicy(
			t.Context(), &bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("wrapper-key-policy")},
		)
		require.NoError(t, err)

		out, err := client.ListAutomatedReasoningPolicies(
			t.Context(), &bedrocksdk.ListAutomatedReasoningPoliciesInput{},
		)
		require.NoError(t, err)
		require.Len(t, out.AutomatedReasoningPolicySummaries, 1)
		assert.Equal(
			t,
			aws.ToString(created.PolicyArn),
			aws.ToString(out.AutomatedReasoningPolicySummaries[0].PolicyArn),
		)
	})

	t.Run("automated reasoning policy build workflows", func(t *testing.T) {
		t.Parallel()

		client := newTestBedrockClient(
			t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
		)

		policy, err := client.CreateAutomatedReasoningPolicy(
			t.Context(), &bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("wrapper-key-wf-policy")},
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

		out, err := client.ListAutomatedReasoningPolicyBuildWorkflows(
			t.Context(),
			&bedrocksdk.ListAutomatedReasoningPolicyBuildWorkflowsInput{PolicyArn: policy.PolicyArn},
		)
		require.NoError(t, err)
		require.Len(t, out.AutomatedReasoningPolicyBuildWorkflowSummaries, 1)
		assert.Equal(
			t,
			aws.ToString(wf.BuildWorkflowId),
			aws.ToString(out.AutomatedReasoningPolicyBuildWorkflowSummaries[0].BuildWorkflowId),
		)
	})
}
