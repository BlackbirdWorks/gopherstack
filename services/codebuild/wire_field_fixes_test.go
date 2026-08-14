package codebuild_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// TestStartBuild_InheritsProjectCacheVpcConfigFileSystemLocations_RealClient
// covers a layer-3 bug (gopherstack-g8k9): a project's Cache, VpcConfig, and
// FileSystemLocations are real, tracked configuration -- BatchGetProjects
// already emits all three correctly (the second-op signal: Project's own
// describe path was already correct) -- but StartBuild never copied them
// onto the Build it created, so a real client's Build.Cache/VpcConfig/
// FileSystemLocations were always nil regardless of what the project was
// configured with, even though every build genuinely runs with that
// project's cache/VPC/file-system configuration. Real Build carries all
// three (codebuild@v1.72.4 deserializers.go's
// awsAwsjson11_deserializeDocumentBuild "cache"/"vpcConfig"/
// "fileSystemLocations" cases), the same way Artifacts and EncryptionKey
// were already correctly copied from the project at build time.
func TestStartBuild_InheritsProjectCacheVpcConfigFileSystemLocations_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:        aws.String("cache-vpc-project"),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source:      &types.ProjectSource{Type: types.SourceTypeNoSource},
		Artifacts:   &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
		Cache: &types.ProjectCache{
			Type:     types.CacheTypeS3,
			Location: aws.String("my-cache-bucket/prefix"),
		},
		VpcConfig: &types.VpcConfig{
			VpcId:            aws.String("vpc-0123456789abcdef0"),
			Subnets:          []string{"subnet-abc123"},
			SecurityGroupIds: []string{"sg-abc123"},
		},
		FileSystemLocations: []types.ProjectFileSystemLocation{
			{
				Type:       types.FileSystemTypeEfs,
				Location:   aws.String("fs-abc123.efs.us-east-1.amazonaws.com:/"),
				MountPoint: aws.String("/mnt/efs"),
			},
		},
	})
	require.NoError(t, err)

	started, err := client.StartBuild(ctx, &codebuildsdk.StartBuildInput{
		ProjectName: aws.String("cache-vpc-project"),
	})
	require.NoError(t, err)

	build := started.Build
	require.NotNil(t, build.Cache,
		"Build.Cache must round-trip from the project; pre-fix it was always nil")
	assert.Equal(t, types.CacheTypeS3, build.Cache.Type)
	assert.Equal(t, "my-cache-bucket/prefix", aws.ToString(build.Cache.Location))

	require.NotNil(t, build.VpcConfig,
		"Build.VpcConfig must round-trip from the project; pre-fix it was always nil")
	assert.Equal(t, "vpc-0123456789abcdef0", aws.ToString(build.VpcConfig.VpcId))
	assert.Equal(t, []string{"subnet-abc123"}, build.VpcConfig.Subnets)

	require.Len(t, build.FileSystemLocations, 1,
		"Build.FileSystemLocations must round-trip from the project; pre-fix it was always empty")
	assert.Equal(t, "/mnt/efs", aws.ToString(build.FileSystemLocations[0].MountPoint))
}
