package codebuild_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// TestListProjects_RealClient_BoundaryWalk confirms, through the real
// aws-sdk-go-v2 client, that paginateIDs (which delegates to
// pkgs/page.New, an offset token always clamped to the collection length)
// walks a full ListProjects collection without dropping or duplicating
// entries.
func TestListProjects_RealClient_BoundaryWalk(t *testing.T) {
	t.Parallel()

	h := codebuild.NewHandler(codebuild.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCodeBuildClient(t, h)

	const n = 7

	names := make([]string, n)
	for i := range n {
		name := fmt.Sprintf("proj-%03d", i)
		names[i] = name

		_, err := client.CreateProject(t.Context(), &codebuildsdk.CreateProjectInput{
			Name:        aws.String(name),
			ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
			Source:      &types.ProjectSource{Type: types.SourceTypeNoSource},
			Artifacts:   &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
			Environment: &types.ProjectEnvironment{
				Type:        types.EnvironmentTypeLinuxContainer,
				Image:       aws.String("aws/codebuild/standard:7.0"),
				ComputeType: types.ComputeTypeBuildGeneral1Small,
			},
		})
		require.NoError(t, err)
	}

	var got []string

	var token *string
	for range n + 1 {
		out, err := client.ListProjects(t.Context(), &codebuildsdk.ListProjectsInput{NextToken: token})
		require.NoError(t, err)

		got = append(got, out.Projects...)

		token = out.NextToken
		if aws.ToString(token) == "" {
			break
		}
	}

	assert.ElementsMatch(t, names, got, "boundary walk must reproduce the collection exactly, no drops or dupes")
}
