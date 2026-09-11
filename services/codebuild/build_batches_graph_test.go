package codebuild_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

const buildGraphSpec = `version: 0.2
batch:
  build-graph:
    - identifier: a
    - identifier: b
    - identifier: c
      depend-on:
        - a
        - b
`

const buildListTwoNodesSpec = `version: 0.2
batch:
  build-list:
    - identifier: one
    - identifier: two
`

const noBatchSpec = `version: 0.2
phases:
  build:
    commands:
      - echo hi
`

// createGraphTestProject creates a project through the real SDK client with
// the given buildspec, returning its name for convenience chaining.
func createGraphTestProject(t *testing.T, client *codebuildsdk.Client, name, buildspec string) {
	t.Helper()

	_, err := client.CreateProject(t.Context(), &codebuildsdk.CreateProjectInput{
		Name:        aws.String(name),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source:      &types.ProjectSource{Type: types.SourceTypeNoSource, Buildspec: aws.String(buildspec)},
		Artifacts:   &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)
}

// runJanitorUntilBatchTerminal repeatedly ticks a Janitor over backend until
// the named batch reaches a terminal BuildBatchStatus, then returns the
// final BatchGetBuildBatches result. Builds in this emulator only advance on
// a Janitor tick (see janitor.go), so driving it directly -- rather than
// sleeping for a real timer -- is how a test observes batch completion.
func runJanitorUntilBatchTerminal(
	t *testing.T, client *codebuildsdk.Client, backend *codebuild.InMemoryBackend, batchID string,
) *types.BuildBatch {
	t.Helper()

	janitor := codebuild.NewJanitor(backend, time.Hour, 24*time.Hour)

	var final *types.BuildBatch

	require.Eventually(t, func() bool {
		janitor.SweepOnce(t.Context())

		out, err := client.BatchGetBuildBatches(t.Context(), &codebuildsdk.BatchGetBuildBatchesInput{
			Ids: []string{batchID},
		})
		require.NoError(t, err)
		require.Len(t, out.BuildBatches, 1)

		bb := &out.BuildBatches[0]
		if bb.BuildBatchStatus == types.StatusTypeInProgress {
			return false
		}

		final = bb

		return true
	}, 5*time.Second, time.Millisecond, "batch must reach a terminal status")

	return final
}

func awsErrorCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "expected a real typed API error from the SDK deserializer")

	return apiErr.ErrorCode()
}

// TestStartBuildBatch_BuildGraph drives a 3-node build-graph (c depends on a
// and b) through the real typed CodeBuild client end to end: group shape,
// real child builds, ListBuildsForProject visibility, and the batch reaching
// SUCCEEDED once every group has.
func TestStartBuildBatch_BuildGraph(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	createGraphTestProject(t, client, "graph-proj", buildGraphSpec)

	startOut, err := client.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
		ProjectName: aws.String("graph-proj"),
	})
	require.NoError(t, err)
	require.NotNil(t, startOut.BuildBatch)
	batchID := aws.ToString(startOut.BuildBatch.Id)
	require.NotEmpty(t, batchID)

	final := runJanitorUntilBatchTerminal(t, client, backend, batchID)
	assert.Equal(t, types.StatusTypeSucceeded, final.BuildBatchStatus)
	require.Len(t, final.BuildGroups, 3)

	groups := make(map[string]types.BuildGroup, 3)
	for _, g := range final.BuildGroups {
		groups[aws.ToString(g.Identifier)] = g
	}

	require.Contains(t, groups, "a")
	require.Contains(t, groups, "b")
	require.Contains(t, groups, "c")
	assert.Empty(t, groups["a"].DependsOn)
	assert.Empty(t, groups["b"].DependsOn)
	assert.ElementsMatch(t, []string{"a", "b"}, groups["c"].DependsOn)

	for id, g := range groups {
		require.NotNil(t, g.CurrentBuildSummary, "group %s must have a real child build", id)
		assert.Equal(t, types.StatusTypeSucceeded, g.CurrentBuildSummary.BuildStatus)
		assert.NotEmpty(t, aws.ToString(g.CurrentBuildSummary.Arn))
	}

	childArn := aws.ToString(groups["c"].CurrentBuildSummary.Arn)
	getOut, err := client.BatchGetBuilds(t.Context(), &codebuildsdk.BatchGetBuildsInput{Ids: []string{childArn}})
	require.NoError(t, err)
	require.Len(t, getOut.Builds, 1)

	child := getOut.Builds[0]
	assert.Equal(t, aws.ToString(startOut.BuildBatch.Arn), aws.ToString(child.BuildBatchArn))
	require.NotNil(t, child.Environment)
	assert.Equal(t, "aws/codebuild/standard:7.0", aws.ToString(child.Environment.Image))

	listOut, err := client.ListBuildsForProject(
		t.Context(), &codebuildsdk.ListBuildsForProjectInput{ProjectName: aws.String("graph-proj")},
	)
	require.NoError(t, err)
	assert.Len(t, listOut.Ids, 3, "ListBuildsForProject must include every batch child")
}

// TestStartBuildBatch_EnvironmentOverride verifies an environment override on
// StartBuildBatch reaches the batch's children but never the project itself.
func TestStartBuildBatch_EnvironmentOverride(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	createGraphTestProject(t, client, "override-proj", testSingleNodeBatchSpec)

	startOut, err := client.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
		ProjectName:             aws.String("override-proj"),
		EnvironmentTypeOverride: types.EnvironmentTypeLinuxGpuContainer,
		ComputeTypeOverride:     types.ComputeTypeBuildGeneral1Large,
		ImageOverride:           aws.String("aws/codebuild/standard:6.0"),
	})
	require.NoError(t, err)
	require.NotNil(t, startOut.BuildBatch.Environment)
	assert.Equal(t, types.EnvironmentTypeLinuxGpuContainer, startOut.BuildBatch.Environment.Type)
	assert.Equal(t, "aws/codebuild/standard:6.0", aws.ToString(startOut.BuildBatch.Environment.Image))

	batchID := aws.ToString(startOut.BuildBatch.Id)
	final := runJanitorUntilBatchTerminal(t, client, backend, batchID)
	require.Len(t, final.BuildGroups, 1)

	summary := final.BuildGroups[0].CurrentBuildSummary
	require.NotNil(t, summary)

	getOut, err := client.BatchGetBuilds(
		t.Context(), &codebuildsdk.BatchGetBuildsInput{Ids: []string{aws.ToString(summary.Arn)}},
	)
	require.NoError(t, err)
	require.Len(t, getOut.Builds, 1)
	require.NotNil(t, getOut.Builds[0].Environment)
	assert.Equal(t, types.EnvironmentTypeLinuxGpuContainer, getOut.Builds[0].Environment.Type,
		"the override must reach the batch's child build")
	assert.Equal(t, "aws/codebuild/standard:6.0", aws.ToString(getOut.Builds[0].Environment.Image))

	projOut, err := client.BatchGetProjects(
		t.Context(), &codebuildsdk.BatchGetProjectsInput{Names: []string{"override-proj"}},
	)
	require.NoError(t, err)
	require.Len(t, projOut.Projects, 1)
	assert.Equal(t, types.EnvironmentTypeLinuxContainer, projOut.Projects[0].Environment.Type,
		"the project's own environment must not be mutated by a StartBuildBatch override")
	assert.Equal(t, "aws/codebuild/standard:7.0", aws.ToString(projOut.Projects[0].Environment.Image))
}

// TestStartBuildBatch_MaximumBuildsAllowedExceeded verifies a batch
// definition with more nodes than BuildBatchConfig.Restrictions.
// MaximumBuildsAllowed fails to start, matching real AWS.
func TestStartBuildBatch_MaximumBuildsAllowedExceeded(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	_, err := client.CreateProject(t.Context(), &codebuildsdk.CreateProjectInput{
		Name:        aws.String("maxbuilds-proj"),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source:      &types.ProjectSource{Type: types.SourceTypeNoSource, Buildspec: aws.String(buildListTwoNodesSpec)},
		Artifacts:   &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
		BuildBatchConfig: &types.ProjectBuildBatchConfig{
			Restrictions: &types.BatchRestrictions{MaximumBuildsAllowed: aws.Int32(1)},
		},
	})
	require.NoError(t, err)

	_, err = client.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
		ProjectName: aws.String("maxbuilds-proj"),
	})
	require.Error(t, err)
	assert.Equal(t, "InvalidInputException", awsErrorCode(t, err))
}

// TestStartBuildBatch_NoBatchSection verifies StartBuildBatch fails with the
// real error when the project's buildspec has no `batch:` section.
func TestStartBuildBatch_NoBatchSection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		buildspec string
	}{
		{name: "buildspec with no batch section", buildspec: noBatchSpec},
		{name: "empty buildspec", buildspec: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

			createGraphTestProject(t, client, "no-batch-proj", tt.buildspec)

			_, err := client.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
				ProjectName: aws.String("no-batch-proj"),
			})
			require.Error(t, err)
			assert.Equal(t, "InvalidInputException", awsErrorCode(t, err))
		})
	}
}

// TestStopBuildBatch_StopsInProgressChildren verifies StopBuildBatch stops
// every in-progress child build and the batch itself reads STOPPED.
func TestStopBuildBatch_StopsInProgressChildren(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	createGraphTestProject(t, client, "stop-graph-proj", buildGraphSpec)

	startOut, err := client.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{
		ProjectName: aws.String("stop-graph-proj"),
	})
	require.NoError(t, err)
	batchID := aws.ToString(startOut.BuildBatch.Id)

	// a and b start immediately (no dependencies); c is still blocked on them.
	require.Len(t, startOut.BuildBatch.BuildGroups, 3)

	stopOut, err := client.StopBuildBatch(t.Context(), &codebuildsdk.StopBuildBatchInput{Id: aws.String(batchID)})
	require.NoError(t, err)
	assert.Equal(t, types.StatusTypeStopped, stopOut.BuildBatch.BuildBatchStatus)

	for _, g := range stopOut.BuildBatch.BuildGroups {
		if g.CurrentBuildSummary == nil {
			continue
		}

		assert.Equal(t, types.StatusTypeStopped, g.CurrentBuildSummary.BuildStatus,
			"group %s's child must be stopped", aws.ToString(g.Identifier))
	}
}
