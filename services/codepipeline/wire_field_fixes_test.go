package codepipeline_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListPipelineExecutions_SucceededInStageFilter_RealClient covers a
// dropped-filter bug: ListPipelineExecutionsInput.Filter
// (types.PipelineExecutionFilter, codepipeline@v1.49.4 types/types.go:1661)
// was accepted nowhere by gopherstack, so a real client's
// Filter.SucceededInStage.StageName request silently returned every
// execution instead of only the ones where that stage succeeded.
func TestListPipelineExecutions_SucceededInStageFilter_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCodePipelineClient(t, h)

	const pipelineName = "succeeded-in-stage-pipeline"

	_, err := h.Backend.CreatePipeline(t.Context(), approvalPipeline(pipelineName), nil)
	require.NoError(t, err)

	// Execution 1: gate at Approve, then reject -- Approve (and therefore
	// Deploy) never succeeds in this execution; Source does.
	startRec := doRequest(t, h, "StartPipelineExecution", map[string]any{"name": pipelineName})
	exec1ID, _ := decodeBody(t, startRec.Body.Bytes())["pipelineExecutionId"].(string)
	require.NotEmpty(t, exec1ID)

	stateRec := doRequest(t, h, "GetPipelineState", map[string]any{"name": pipelineName})
	token1 := approvalToken(t, decodeBody(t, stateRec.Body.Bytes()), "Approve", "ApprovalAction")
	require.NotEmpty(t, token1)

	rejectRec := doRequest(t, h, "PutApprovalResult", map[string]any{
		"pipelineName": pipelineName, "stageName": "Approve", "actionName": "ApprovalAction",
		"token":  token1,
		"result": map[string]any{"status": "Rejected", "summary": "no"},
	})
	require.Equal(t, 200, rejectRec.Code, rejectRec.Body.String())

	// Execution 2: gate at Approve, then approve -- Source, Approve, AND
	// Deploy all succeed in this execution.
	startRec2 := doRequest(t, h, "StartPipelineExecution", map[string]any{"name": pipelineName})
	exec2ID, _ := decodeBody(t, startRec2.Body.Bytes())["pipelineExecutionId"].(string)
	require.NotEmpty(t, exec2ID)
	require.NotEqual(t, exec1ID, exec2ID)

	stateRec2 := doRequest(t, h, "GetPipelineState", map[string]any{"name": pipelineName})
	token2 := approvalToken(t, decodeBody(t, stateRec2.Body.Bytes()), "Approve", "ApprovalAction")
	require.NotEmpty(t, token2)

	approveRec := doRequest(t, h, "PutApprovalResult", map[string]any{
		"pipelineName": pipelineName, "stageName": "Approve", "actionName": "ApprovalAction",
		"token":  token2,
		"result": map[string]any{"status": "Approved", "summary": "lgtm"},
	})
	require.Equal(t, 200, approveRec.Code, approveRec.Body.String())

	t.Run("unfiltered lists both executions", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListPipelineExecutions(t.Context(), &cpsdk.ListPipelineExecutionsInput{
			PipelineName: aws.String(pipelineName),
		})
		require.NoError(t, listErr)
		assert.Len(t, out.PipelineExecutionSummaries, 2)
	})

	t.Run("SucceededInStage Deploy returns only the execution where Deploy succeeded", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListPipelineExecutions(t.Context(), &cpsdk.ListPipelineExecutionsInput{
			PipelineName: aws.String(pipelineName),
			Filter: &types.PipelineExecutionFilter{
				SucceededInStage: &types.SucceededInStageFilter{StageName: aws.String("Deploy")},
			},
		})
		require.NoError(t, listErr)
		require.Len(t, out.PipelineExecutionSummaries, 1)
		assert.Equal(t, exec2ID, aws.ToString(out.PipelineExecutionSummaries[0].PipelineExecutionId))
	})

	t.Run("SucceededInStage Source returns both executions", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListPipelineExecutions(t.Context(), &cpsdk.ListPipelineExecutionsInput{
			PipelineName: aws.String(pipelineName),
			Filter: &types.PipelineExecutionFilter{
				SucceededInStage: &types.SucceededInStageFilter{StageName: aws.String("Source")},
			},
		})
		require.NoError(t, listErr)
		assert.Len(t, out.PipelineExecutionSummaries, 2)
	})
}

// TestListActionExecutions_LatestInPipelineExecutionFilter_RealClient covers
// a dropped-filter bug: ActionExecutionFilter.LatestInPipelineExecution
// (types.LatestInPipelineExecutionFilter, types/types.go:1409) was accepted
// nowhere by gopherstack, so a real client narrowing to a single execution
// via this member (rather than the flat PipelineExecutionId member) got
// every action execution for the whole pipeline back, unfiltered.
func TestListActionExecutions_LatestInPipelineExecutionFilter_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCodePipelineClient(t, h)

	const pipelineName = "latest-in-exec-pipeline"

	_, err := client.CreatePipeline(t.Context(), &cpsdk.CreatePipelineInput{
		Pipeline: &types.PipelineDeclaration{
			Name:    aws.String(pipelineName),
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

	exec1, err := client.StartPipelineExecution(t.Context(), &cpsdk.StartPipelineExecutionInput{
		Name: aws.String(pipelineName),
	})
	require.NoError(t, err)

	exec2, err := client.StartPipelineExecution(t.Context(), &cpsdk.StartPipelineExecutionInput{
		Name: aws.String(pipelineName),
	})
	require.NoError(t, err)
	require.NotEqual(t, aws.ToString(exec1.PipelineExecutionId), aws.ToString(exec2.PipelineExecutionId))

	t.Run("unfiltered lists actions from both executions", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListActionExecutions(t.Context(), &cpsdk.ListActionExecutionsInput{
			PipelineName: aws.String(pipelineName),
		})
		require.NoError(t, listErr)
		assert.Len(t, out.ActionExecutionDetails, 2)
	})

	t.Run("LatestInPipelineExecution narrows to the named execution", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListActionExecutions(t.Context(), &cpsdk.ListActionExecutionsInput{
			PipelineName: aws.String(pipelineName),
			Filter: &types.ActionExecutionFilter{
				LatestInPipelineExecution: &types.LatestInPipelineExecutionFilter{
					PipelineExecutionId: exec2.PipelineExecutionId,
					StartTimeRange:      types.StartTimeRangeLatest,
				},
			},
		})
		require.NoError(t, listErr)
		require.Len(t, out.ActionExecutionDetails, 1)
		assert.Equal(
			t,
			aws.ToString(exec2.PipelineExecutionId),
			aws.ToString(out.ActionExecutionDetails[0].PipelineExecutionId),
		)
	})
}
