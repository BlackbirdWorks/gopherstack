package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline"
	cptypes "github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func minimalPipeline(name string) *cptypes.PipelineDeclaration {
	return &cptypes.PipelineDeclaration{
		Name:    aws.String(name),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/pipeline-role"),
		ArtifactStore: &cptypes.ArtifactStore{
			Type:     cptypes.ArtifactStoreTypeS3,
			Location: aws.String("my-artifact-bucket"),
		},
		Stages: []cptypes.StageDeclaration{
			{
				Name: aws.String("Source"),
				Actions: []cptypes.ActionDeclaration{
					{
						Name: aws.String("SourceAction"),
						ActionTypeId: &cptypes.ActionTypeId{
							Category: cptypes.ActionCategorySource,
							Owner:    cptypes.ActionOwnerThirdParty,
							Provider: aws.String("GitHub"),
							Version:  aws.String("1"),
						},
					},
				},
			},
		},
	}
}

func TestIntegration_CodePipeline_PipelineLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodePipelineClient(t)
	ctx := t.Context()

	pipelineName := fmt.Sprintf("test-pipeline-%s", uuid.NewString()[:8])

	// CreatePipeline
	createOut, err := client.CreatePipeline(ctx, &codepipeline.CreatePipelineInput{
		Pipeline: minimalPipeline(pipelineName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Pipeline)
	assert.Equal(t, pipelineName, aws.ToString(createOut.Pipeline.Name))

	t.Cleanup(func() {
		_, _ = client.DeletePipeline(ctx, &codepipeline.DeletePipelineInput{
			Name: aws.String(pipelineName),
		})
	})

	// GetPipeline
	getOut, err := client.GetPipeline(ctx, &codepipeline.GetPipelineInput{
		Name: aws.String(pipelineName),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Pipeline)
	assert.Equal(t, pipelineName, aws.ToString(getOut.Pipeline.Name))

	// ListPipelines
	listOut, err := client.ListPipelines(ctx, &codepipeline.ListPipelinesInput{})
	require.NoError(t, err)

	found := false

	for _, p := range listOut.Pipelines {
		if aws.ToString(p.Name) == pipelineName {
			found = true

			break
		}
	}

	assert.True(t, found, "created pipeline should appear in ListPipelines")

	// DeletePipeline
	_, err = client.DeletePipeline(ctx, &codepipeline.DeletePipelineInput{
		Name: aws.String(pipelineName),
	})
	require.NoError(t, err)

	// Verify deleted
	listOut2, err := client.ListPipelines(ctx, &codepipeline.ListPipelinesInput{})
	require.NoError(t, err)

	for _, p := range listOut2.Pipelines {
		assert.NotEqual(t, pipelineName, aws.ToString(p.Name), "deleted pipeline should not appear in list")
	}
}

func TestIntegration_CodePipeline_GetPipelineNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodePipelineClient(t)
	ctx := t.Context()

	_, err := client.GetPipeline(ctx, &codepipeline.GetPipelineInput{
		Name: aws.String("does-not-exist"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PipelineNotFoundException")
}
