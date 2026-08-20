package codepipeline_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// newTestPipelineForState creates a minimal pipeline via the real SDK client
// and returns its name, ready for GetPipelineState/DisableStageTransition
// round trips.
func newTestPipelineForState(t *testing.T, client *cpsdk.Client, name string) {
	t.Helper()

	_, err := client.CreatePipeline(t.Context(), &cpsdk.CreatePipelineInput{
		Pipeline: &types.PipelineDeclaration{
			Name:    aws.String(name),
			RoleArn: aws.String("arn:aws:iam::123456789012:role/pipeline-role"),
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
								Owner:    types.ActionOwnerAws,
								Provider: aws.String("S3"),
								Version:  aws.String("1"),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

// TestGetPipelineState_InboundTransitionState proves the real SDK client can
// read back a disabled stage transition's Enabled flag and DisabledReason.
// Real types.TransitionState (awsAwsjson11_deserializeDocumentTransitionState)
// has no "disabled"/"reason" members -- only "enabled" (bool, inverse
// semantics) and "disabledReason". Before the fix, gopherstack emitted
// "disabled"/"reason", so a real client's TransitionState.DisabledReason
// stayed nil (the "reason" key was never recognized).
func TestGetPipelineState_InboundTransitionState(t *testing.T) {
	t.Parallel()

	h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestCodePipelineClient(t, h)

	newTestPipelineForState(t, client, "transition-pipeline")

	_, err := client.DisableStageTransition(t.Context(), &cpsdk.DisableStageTransitionInput{
		PipelineName:   aws.String("transition-pipeline"),
		StageName:      aws.String("Source"),
		TransitionType: types.StageTransitionTypeInbound,
		Reason:         aws.String("paused for review"),
	})
	require.NoError(t, err)

	out, err := client.GetPipelineState(t.Context(), &cpsdk.GetPipelineStateInput{
		Name: aws.String("transition-pipeline"),
	})
	require.NoError(t, err)
	require.Len(t, out.StageStates, 1)

	inbound := out.StageStates[0].InboundTransitionState
	require.NotNil(t, inbound)
	assert.False(t, inbound.Enabled)
	assert.Equal(t, "paused for review", aws.ToString(inbound.DisabledReason))
}

// TestGetPipelineState_CreatedUpdated proves GetPipelineState's top-level
// Created/Updated (real, always-populated members of
// awsAwsjson11_deserializeOpDocumentGetPipelineStateOutput) are wire-visible
// through the real SDK client; previously they were never emitted at all.
func TestGetPipelineState_CreatedUpdated(t *testing.T) {
	t.Parallel()

	h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestCodePipelineClient(t, h)

	newTestPipelineForState(t, client, "created-updated-pipeline")

	out, err := client.GetPipelineState(t.Context(), &cpsdk.GetPipelineStateInput{
		Name: aws.String("created-updated-pipeline"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.Created)
	require.NotNil(t, out.Updated)
	assert.False(t, out.Created.IsZero())
	assert.False(t, out.Updated.IsZero())
}

// TestGetPipelineState_OmitsOutboundTransitionState proves the wire response
// carries no "outboundTransitionState" key. Real types.StageState
// (awsAwsjson11_deserializeDocumentStageState) has no such member at all --
// there is no field on the real SDK type to bind it to, so unlike a
// wrong-key bug this cannot be proven via a real-client round trip (the
// client would just silently discard the extra key either way); a raw body
// check is the only way to prove the fabricated field is actually gone.
func TestGetPipelineState_OmitsOutboundTransitionState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(t.Context(), samplePipeline("outbound-pl"), nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "DisableStageTransition", map[string]any{
		"pipelineName":   "outbound-pl",
		"stageName":      "Source",
		"transitionType": "Outbound",
		"reason":         "blocking downstream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetPipelineState", map[string]any{"name": "outbound-pl"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	stageStates := out["stageStates"].([]any)
	require.Len(t, stageStates, 1)
	stage := stageStates[0].(map[string]any)
	assert.NotContains(t, stage, "outboundTransitionState")
}
