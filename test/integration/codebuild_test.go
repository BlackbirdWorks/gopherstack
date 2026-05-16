package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	codebuildtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CodeBuild_ProjectLifecycle exercises the core CodeBuild
// workflow end-to-end via the AWS SDK v2: create a project, list/get it,
// start a build, batch-get the build, then delete the project. This is the
// primary integration coverage for the AWS CodeBuild handler and protects
// against regressions in JSON-RPC dispatch and per-op shape compatibility.
func TestIntegration_CodeBuild_ProjectLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeBuildClient(t)
	ctx := t.Context()

	const (
		projectName = "it-codebuild-project"
		roleArn     = "arn:aws:iam::000000000000:role/CodeBuildServiceRole"
		image       = "aws/codebuild/standard:7.0"
		sourceLoc   = "https://github.com/example/repo.git"
	)

	// CreateProject (omit tags: SDK serialises tags as an array of {key,value}
	// which the backend currently stores as a map; covered separately).
	createOut, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:        aws.String(projectName),
		Description: aws.String("integration test project"),
		ServiceRole: aws.String(roleArn),
		Source: &codebuildtypes.ProjectSource{
			Type:     codebuildtypes.SourceTypeGithub,
			Location: aws.String(sourceLoc),
		},
		Artifacts: &codebuildtypes.ProjectArtifacts{
			Type: codebuildtypes.ArtifactsTypeNoArtifacts,
		},
		Environment: &codebuildtypes.ProjectEnvironment{
			Type:        codebuildtypes.EnvironmentTypeLinuxContainer,
			Image:       aws.String(image),
			ComputeType: codebuildtypes.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Project)
	assert.Equal(t, projectName, aws.ToString(createOut.Project.Name))

	t.Cleanup(func() {
		_, _ = client.DeleteProject(ctx, &codebuildsdk.DeleteProjectInput{Name: aws.String(projectName)})
	})

	// ListProjects should include the new project.
	listOut, err := client.ListProjects(ctx, &codebuildsdk.ListProjectsInput{})
	require.NoError(t, err)
	assert.Contains(t, listOut.Projects, projectName)

	// BatchGetProjects returns full details.
	batchOut, err := client.BatchGetProjects(ctx, &codebuildsdk.BatchGetProjectsInput{
		Names: []string{projectName},
	})
	require.NoError(t, err)
	require.Len(t, batchOut.Projects, 1)
	assert.Equal(t, projectName, aws.ToString(batchOut.Projects[0].Name))
	assert.Equal(t, roleArn, aws.ToString(batchOut.Projects[0].ServiceRole))
	assert.Empty(t, batchOut.ProjectsNotFound)

	// StartBuild and verify via BatchGetBuilds.
	startOut, err := client.StartBuild(ctx, &codebuildsdk.StartBuildInput{
		ProjectName: aws.String(projectName),
	})
	require.NoError(t, err)
	require.NotNil(t, startOut.Build)
	buildID := aws.ToString(startOut.Build.Id)
	require.NotEmpty(t, buildID)

	buildsOut, err := client.BatchGetBuilds(ctx, &codebuildsdk.BatchGetBuildsInput{
		Ids: []string{buildID},
	})
	require.NoError(t, err)
	require.Len(t, buildsOut.Builds, 1)
	assert.Equal(t, buildID, aws.ToString(buildsOut.Builds[0].Id))
	assert.Equal(t, projectName, aws.ToString(buildsOut.Builds[0].ProjectName))
}
