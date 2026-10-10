package codebuild_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func realismProjectInput(name string) *codebuildsdk.CreateProjectInput {
	return &codebuildsdk.CreateProjectInput{
		Name:        aws.String(name),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/r"),
		Source:      &types.ProjectSource{Type: types.SourceTypeNoSource, Buildspec: aws.String("version: 0.2")},
		Artifacts:   &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
	}
}

func TestSDKCreateProjectValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *codebuildsdk.CreateProjectInput)
		name   string
	}{
		{name: "space_in_name", mutate: func(in *codebuildsdk.CreateProjectInput) { in.Name = aws.String("bad name") }},
		{name: "one_char_name", mutate: func(in *codebuildsdk.CreateProjectInput) { in.Name = aws.String("a") }},
		{name: "bad_compute", mutate: func(in *codebuildsdk.CreateProjectInput) {
			in.Environment.ComputeType = types.ComputeType("BOGUS")
		}},
		{name: "bad_env_type", mutate: func(in *codebuildsdk.CreateProjectInput) {
			in.Environment.Type = types.EnvironmentType("BOGUS")
		}},
		{
			name:   "timeout_too_big",
			mutate: func(in *codebuildsdk.CreateProjectInput) { in.TimeoutInMinutes = aws.Int32(5000) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodeBuildClient(t, newTestHandler(t))
			in := realismProjectInput("proj")
			tt.mutate(in)

			_, err := client.CreateProject(t.Context(), in)

			var invalid *types.InvalidInputException
			require.ErrorAs(t, err, &invalid)
			assert.False(t, strings.HasPrefix(aws.ToString(invalid.Message), "InvalidInputException"))
		})
	}
}

func TestSDKNotFoundCarriesMessage(t *testing.T) {
	t.Parallel()

	client := newTestCodeBuildClient(t, newTestHandler(t))

	_, err := client.StartBuild(t.Context(), &codebuildsdk.StartBuildInput{ProjectName: aws.String("missing")})

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.Contains(t, aws.ToString(nf.Message), "Project cannot be found")

	_, err = client.CreateProject(t.Context(), realismProjectInput("dup"))
	require.NoError(t, err)

	_, err = client.CreateProject(t.Context(), realismProjectInput("dup"))

	var exists *types.ResourceAlreadyExistsException
	require.ErrorAs(t, err, &exists)
	assert.Contains(t, aws.ToString(exists.Message), "Project already exists")
}

func TestSDKBuildLifecycle(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))

	_, err := client.CreateProject(t.Context(), realismProjectInput("life"))
	require.NoError(t, err)

	started, err := client.StartBuild(t.Context(), &codebuildsdk.StartBuildInput{ProjectName: aws.String("life")})
	require.NoError(t, err)

	_, uuidPart, ok := strings.Cut(aws.ToString(started.Build.Id), ":")
	require.True(t, ok)
	_, err = uuid.Parse(uuidPart)
	require.NoError(t, err)

	codebuild.NewJanitor(backend, time.Hour, 24*time.Hour).SweepOnce(t.Context())

	got, err := client.BatchGetBuilds(
		t.Context(),
		&codebuildsdk.BatchGetBuildsInput{Ids: []string{aws.ToString(started.Build.Id)}},
	)
	require.NoError(t, err)
	require.Len(t, got.Builds, 1)

	build := got.Builds[0]
	assert.Equal(t, types.StatusTypeSucceeded, build.BuildStatus)

	phases := make([]types.BuildPhaseType, 0, len(build.Phases))
	for _, p := range build.Phases {
		phases = append(phases, p.PhaseType)
	}

	assert.Equal(t, []types.BuildPhaseType{
		types.BuildPhaseTypeSubmitted, types.BuildPhaseTypeQueued, types.BuildPhaseTypeProvisioning,
		types.BuildPhaseTypeDownloadSource, types.BuildPhaseTypeInstall, types.BuildPhaseTypePreBuild,
		types.BuildPhaseTypeBuild, types.BuildPhaseTypePostBuild, types.BuildPhaseTypeUploadArtifacts,
		types.BuildPhaseTypeFinalizing, types.BuildPhaseTypeCompleted,
	}, phases)

	stopped, err := client.StopBuild(t.Context(), &codebuildsdk.StopBuildInput{Id: started.Build.Id})
	require.NoError(t, err)
	assert.Equal(t, types.StatusTypeSucceeded, stopped.Build.BuildStatus)

	var apiErr smithy.APIError
	_, err = client.StartBuild(t.Context(), &codebuildsdk.StartBuildInput{
		ProjectName: aws.String("life"), TimeoutInMinutesOverride: aws.Int32(99999),
	})
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())
}
