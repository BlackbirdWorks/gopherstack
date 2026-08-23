package codepipeline_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListDeployActionExecutionTargets_ActionExecutionIDOptionalPipelineName
// covers gopherstack-2wvq. ListDeployActionExecutionTargetsInput marks only
// ActionExecutionId required (codepipeline@v1.49.4
// api_op_ListDeployActionExecutionTargets.go); PipelineName is not required.
// The handler previously demanded PipelineName and silently discarded
// ActionExecutionId (`_ = executionID`), rejecting a real AWS-accepted
// request identifying the execution by ID alone.
func TestListDeployActionExecutionTargets_ActionExecutionIDOptionalPipelineName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCodePipelineClient(t, h)

	_, err := client.CreatePipeline(t.Context(), &cpsdk.CreatePipelineInput{
		Pipeline: &types.PipelineDeclaration{
			Name:    aws.String("2wvq-deploy-pipeline"),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/pipeline-role"),
			ArtifactStore: &types.ArtifactStore{
				Type:     types.ArtifactStoreTypeS3,
				Location: aws.String("my-artifact-bucket"),
			},
			Stages: []types.StageDeclaration{
				{
					Name: aws.String("Source"),
					Actions: []types.ActionDeclaration{
						{
							Name: aws.String("SourceAction"),
							ActionTypeId: &types.ActionTypeId{
								Category: types.ActionCategorySource,
								Owner:    types.ActionOwnerThirdParty,
								Provider: aws.String("GitHub"),
								Version:  aws.String("1"),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.StartPipelineExecution(t.Context(), &cpsdk.StartPipelineExecutionInput{
		Name: aws.String("2wvq-deploy-pipeline"),
	})
	require.NoError(t, err)

	listOut, err := client.ListActionExecutions(t.Context(), &cpsdk.ListActionExecutionsInput{
		PipelineName: aws.String("2wvq-deploy-pipeline"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.ActionExecutionDetails)

	actionExecutionID := aws.ToString(listOut.ActionExecutionDetails[0].ActionExecutionId)
	require.NotEmpty(t, actionExecutionID)

	t.Run("action execution id alone resolves", func(t *testing.T) {
		t.Parallel()

		out, targetsErr := client.ListDeployActionExecutionTargets(
			t.Context(),
			&cpsdk.ListDeployActionExecutionTargetsInput{
				ActionExecutionId: aws.String(actionExecutionID),
			},
		)
		require.NoError(t, targetsErr)
		assert.Empty(t, out.Targets)
	})

	t.Run("pipeline name plus action execution id still works", func(t *testing.T) {
		t.Parallel()

		out, targetsErr := client.ListDeployActionExecutionTargets(
			t.Context(),
			&cpsdk.ListDeployActionExecutionTargetsInput{
				PipelineName:      aws.String("2wvq-deploy-pipeline"),
				ActionExecutionId: aws.String(actionExecutionID),
			},
		)
		require.NoError(t, targetsErr)
		assert.Empty(t, out.Targets)
	})

	t.Run("unknown action execution id without pipeline name errors", func(t *testing.T) {
		t.Parallel()

		_, targetsErr := client.ListDeployActionExecutionTargets(
			t.Context(),
			&cpsdk.ListDeployActionExecutionTargetsInput{
				ActionExecutionId: aws.String("no-such-execution"),
			},
		)
		require.Error(t, targetsErr)
	})
}
