package codepipeline //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stageNameSource is the shared "Source" literal used across the isolation tests.
const stageNameSource = "Source"

func cpCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

func samplePipelineDecl(name string) PipelineDeclaration {
	return PipelineDeclaration{
		Name:    name,
		RoleArn: "arn:aws:iam::000000000000:role/pipeline",
		Stages: []Stage{
			{
				Name: stageNameSource,
				Actions: []Action{
					{
						Name: "SourceAction",
						ActionTypeID: ActionTypeID{
							Category: stageNameSource, Owner: "AWS", Provider: "S3", Version: "1",
						},
					},
				},
			},
		},
	}
}

// TestCodePipelineRegionIsolation proves that same-named pipelines created in two
// different regions are fully isolated: each region sees only its own pipelines,
// ARNs embed the correct region, tags resolve only within the owning region, and
// deleting in one region leaves the other untouched.
func TestCodePipelineRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := cpCtxRegion("us-east-1")
	ctxWest := cpCtxRegion("us-west-2")

	// 1. Create a pipeline with the SAME name in both regions.
	eastDecl := samplePipelineDecl("shared-pipeline")
	eastDecl.PipelineType = PipelineTypeV1

	eastP, err := backend.CreatePipeline(ctxEast, eastDecl, map[string]string{"env": "east"})
	require.NoError(t, err)
	assert.Contains(t, eastP.Metadata.PipelineArn, "us-east-1")

	westDecl := samplePipelineDecl("shared-pipeline")
	westDecl.PipelineType = PipelineTypeV2

	westP, err := backend.CreatePipeline(ctxWest, westDecl, map[string]string{"env": "west"})
	require.NoError(t, err)
	assert.Contains(t, westP.Metadata.PipelineArn, "us-west-2")

	// ARNs must differ (region-qualified) even though names match.
	assert.NotEqual(t, eastP.Metadata.PipelineArn, westP.Metadata.PipelineArn)

	// 2. Each region reads back its own pipeline type.
	eastGet, err := backend.GetPipeline(ctxEast, "shared-pipeline")
	require.NoError(t, err)
	assert.Equal(t, PipelineTypeV1, eastGet.Declaration.PipelineType)

	westGet, err := backend.GetPipeline(ctxWest, "shared-pipeline")
	require.NoError(t, err)
	assert.Equal(t, PipelineTypeV2, westGet.Declaration.PipelineType)

	// 3. Listing returns exactly one pipeline per region.
	eastList := backend.ListPipelines(ctxEast)
	require.Len(t, eastList, 1)
	assert.Contains(t, eastList[0].PipelineArn, "us-east-1")

	westList := backend.ListPipelines(ctxWest)
	require.Len(t, westList, 1)
	assert.Contains(t, westList[0].PipelineArn, "us-west-2")

	// 4. Tags resolve only within the owning region. The east ARN must not be
	//    tag-resolvable from the west region.
	eastTags, err := backend.ListTagsForResource(ctxEast, eastP.Metadata.PipelineArn)
	require.NoError(t, err)
	require.Len(t, eastTags, 1)
	assert.Equal(t, "east", eastTags[0].Value)

	_, err = backend.ListTagsForResource(ctxWest, eastP.Metadata.PipelineArn)
	require.Error(t, err, "east ARN must not be tag-resolvable from the west region")

	// 5. Deleting the pipeline in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeletePipeline(ctxEast, "shared-pipeline"))

	_, err = backend.GetPipeline(ctxEast, "shared-pipeline")
	require.Error(t, err)

	westStill, err := backend.GetPipeline(ctxWest, "shared-pipeline")
	require.NoError(t, err)
	assert.Equal(t, PipelineTypeV2, westStill.Declaration.PipelineType)
}

// TestCodePipelineExecutionAndWebhookRegionIsolation proves that executions,
// custom action types, jobs, and webhooks are scoped to the request region.
func TestCodePipelineExecutionAndWebhookRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := cpCtxRegion("us-east-1")
	ctxWest := cpCtxRegion("us-west-2")

	// Create a same-named pipeline in both regions.
	_, err := backend.CreatePipeline(ctxEast, samplePipelineDecl("p"), nil)
	require.NoError(t, err)
	_, err = backend.CreatePipeline(ctxWest, samplePipelineDecl("p"), nil)
	require.NoError(t, err)

	// Start an execution only in us-east-1.
	exec, err := backend.StartPipelineExecution(ctxEast, "p")
	require.NoError(t, err)
	require.NotEmpty(t, exec.PipelineExecutionID)

	eastExecs, err := backend.ListPipelineExecutions(ctxEast, "p")
	require.NoError(t, err)
	require.Len(t, eastExecs, 1)

	// us-west-2 sees no executions for the same-named pipeline.
	westExecs, err := backend.ListPipelineExecutions(ctxWest, "p")
	require.NoError(t, err)
	assert.Empty(t, westExecs)

	// Action executions recorded by the east run are not visible from the west.
	eastActions, err := backend.ListActionExecutions(ctxEast, "p", "")
	require.NoError(t, err)
	require.Len(t, eastActions, 1)

	westActions, err := backend.ListActionExecutions(ctxWest, "p", "")
	require.NoError(t, err)
	assert.Empty(t, westActions)

	// Webhooks with the same name are isolated; ARNs are region-qualified.
	eastWH, err := backend.PutWebhook(ctxEast, &Webhook{Name: "wh", TargetPipeline: "p", TargetAction: stageNameSource})
	require.NoError(t, err)
	assert.Contains(t, eastWH.ARN, "us-east-1")

	westWH, err := backend.PutWebhook(ctxWest, &Webhook{Name: "wh", TargetPipeline: "p", TargetAction: stageNameSource})
	require.NoError(t, err)
	assert.Contains(t, westWH.ARN, "us-west-2")
	assert.NotEqual(t, eastWH.ARN, westWH.ARN)

	require.Len(t, backend.ListWebhooks(ctxEast), 1)
	require.Len(t, backend.ListWebhooks(ctxWest), 1)

	// Custom action types are region-scoped: deleting the east copy leaves west.
	const (
		catCategory = "Build"
		catProvider = keyOwnerCustom
		catVersion  = "1"
	)

	_, err = backend.CreateCustomActionType(
		ctxEast, &CustomActionType{Category: catCategory, Provider: catProvider, Version: catVersion},
	)
	require.NoError(t, err)
	_, err = backend.CreateCustomActionType(
		ctxWest, &CustomActionType{Category: catCategory, Provider: catProvider, Version: catVersion},
	)
	require.NoError(t, err)

	require.NoError(t, backend.DeleteCustomActionType(ctxEast, catCategory, catProvider, catVersion))

	_, err = backend.GetActionType(ctxEast, catCategory, keyOwnerCustom, catProvider, catVersion)
	require.Error(t, err)

	_, err = backend.GetActionType(ctxWest, catCategory, keyOwnerCustom, catProvider, catVersion)
	require.NoError(t, err, "west custom action type must survive deletion in east")
}

// TestCodePipelineDefaultRegionFallback verifies that a context without a region
// falls back to the backend's configured default region.
func TestCodePipelineDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	_, err := backend.CreatePipeline(context.Background(), samplePipelineDecl("def-pipeline"), nil)
	require.NoError(t, err)

	// Reading via the explicit default region sees it.
	got, err := backend.GetPipeline(cpCtxRegion("eu-central-1"), "def-pipeline")
	require.NoError(t, err)
	assert.Contains(t, got.Metadata.PipelineArn, "eu-central-1")

	// A different region sees nothing.
	_, err = backend.GetPipeline(cpCtxRegion("ap-south-1"), "def-pipeline")
	require.Error(t, err)
}
