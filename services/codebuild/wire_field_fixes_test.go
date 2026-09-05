package codebuild_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	smithy "github.com/aws/smithy-go"
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

// TestCreateProject_BadgeEnabled_RealClient covers gopherstack-6flj-codebuild-1:
// CreateProjectInput/UpdateProjectInput.BadgeEnabled (codebuild@v1.72.4
// api_op_CreateProject.go/api_op_UpdateProject.go) is a real request field
// (serializers.go's "badgeEnabled" key on both ops), but gopherstack's
// projectConfigFields wire struct had no such field at all, so a real
// client's BadgeEnabled was silently dropped before ever reaching the
// backend -- Project.Badge was always nil regardless of what the client
// requested (deserializers.go's "badge" case on Project, which types.Project
// declares as *types.ProjectBadge{BadgeEnabled, BadgeRequestUrl}).
func TestCreateProject_BadgeEnabled_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:        aws.String("badge-project"),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source:      &types.ProjectSource{Type: types.SourceTypeNoSource},
		Artifacts:   &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
		BadgeEnabled: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Project.Badge,
		"Project.Badge must round-trip from CreateProject's badgeEnabled; pre-fix it was always nil")
	assert.True(t, created.Project.Badge.BadgeEnabled)
	assert.NotEmpty(t, aws.ToString(created.Project.Badge.BadgeRequestUrl))

	got, err := client.BatchGetProjects(ctx, &codebuildsdk.BatchGetProjectsInput{
		Names: []string{"badge-project"},
	})
	require.NoError(t, err)
	require.Len(t, got.Projects, 1)
	require.NotNil(t, got.Projects[0].Badge, "BatchGetProjects must also see the badge")
	assert.True(t, got.Projects[0].Badge.BadgeEnabled)

	updated, err := client.UpdateProject(ctx, &codebuildsdk.UpdateProjectInput{
		Name:         aws.String("badge-project"),
		BadgeEnabled: aws.Bool(false),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Project.Badge)
	assert.False(t, updated.Project.Badge.BadgeEnabled,
		"UpdateProject's badgeEnabled must also round-trip; pre-fix it was silently dropped")
}

// TestCreateProject_SourceBuildStatusAndGitSubmodulesConfig_RealClient covers
// gopherstack-6flj-codebuild-2: real types.ProjectSource
// (codebuild@v1.72.4/types/types.go) has BuildStatusConfig/GitSubmodulesConfig
// members (serializers.go's awsAwsjson11_serializeDocumentProjectSource
// "buildStatusConfig"/"gitSubmodulesConfig" cases; deserializers.go's mirror
// cases in awsAwsjson11_deserializeDocumentProjectSource), but gopherstack's
// ProjectSource model had neither field at all -- both were silently dropped
// on Create/UpdateProject regardless of what a real client set.
func TestCreateProject_SourceBuildStatusAndGitSubmodulesConfig_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:        aws.String("source-config-project"),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source: &types.ProjectSource{
			Type:     types.SourceTypeGithub,
			Location: aws.String("https://github.com/example/repo"),
			BuildStatusConfig: &types.BuildStatusConfig{
				Context:   aws.String("my-context"),
				TargetUrl: aws.String("https://example.com/status"),
			},
			GitSubmodulesConfig: &types.GitSubmodulesConfig{
				FetchSubmodules: aws.Bool(true),
			},
		},
		Artifacts: &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)

	got, err := client.BatchGetProjects(ctx, &codebuildsdk.BatchGetProjectsInput{
		Names: []string{"source-config-project"},
	})
	require.NoError(t, err)
	require.Len(t, got.Projects, 1)

	src := got.Projects[0].Source
	require.NotNil(t, src.BuildStatusConfig,
		"Source.BuildStatusConfig must round-trip; pre-fix it was always nil")
	assert.Equal(t, "my-context", aws.ToString(src.BuildStatusConfig.Context))
	assert.Equal(t, "https://example.com/status", aws.ToString(src.BuildStatusConfig.TargetUrl))

	require.NotNil(t, src.GitSubmodulesConfig,
		"Source.GitSubmodulesConfig must round-trip; pre-fix it was always nil")
	assert.True(t, aws.ToBool(src.GitSubmodulesConfig.FetchSubmodules))

	build, err := client.StartBuild(ctx, &codebuildsdk.StartBuildInput{
		ProjectName: aws.String("source-config-project"),
	})
	require.NoError(t, err)
	require.NotNil(t, build.Build.Source.BuildStatusConfig,
		"Build.Source must inherit the project's BuildStatusConfig, same as Cache/VpcConfig")
	assert.Equal(t, "my-context", aws.ToString(build.Build.Source.BuildStatusConfig.Context))
}

// TestCreateProject_EnvironmentFleetComputeConfigDockerServerHostKernel_RealClient
// covers gopherstack-6flj-codebuild-3: real types.ProjectEnvironment has
// ComputeConfiguration/DockerServer/Fleet/HostKernel members (confirmed via
// serializers.go's awsAwsjson11_serializeDocumentProjectEnvironment
// "computeConfiguration"/"dockerServer"/"fleet"/"hostKernel" cases), but
// gopherstack's ProjectEnvironment model had none of the four -- most
// notably Fleet (the field that ties a project to a reserved-capacity
// compute fleet, the exact feature this service's Fleet API already models)
// was silently dropped on every Create/UpdateProject call.
func TestCreateProject_EnvironmentFleetComputeConfigDockerServerHostKernel_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:        aws.String("env-config-project"),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source:      &types.ProjectSource{Type: types.SourceTypeNoSource},
		Artifacts:   &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeAttributeBasedCompute,
			HostKernel:  types.HostKernelLinuxKernel6,
			ComputeConfiguration: &types.ComputeConfiguration{
				VCpu:   aws.Int64(4),
				Memory: aws.Int64(8192),
			},
			DockerServer: &types.DockerServer{
				ComputeType:      types.ComputeTypeBuildGeneral1Small,
				SecurityGroupIds: []string{"sg-docker"},
			},
			Fleet: &types.ProjectFleet{
				FleetArn: aws.String("arn:aws:codebuild:us-east-1:123456789012:fleet/my-fleet"),
			},
		},
	})
	require.NoError(t, err)

	got, err := client.BatchGetProjects(ctx, &codebuildsdk.BatchGetProjectsInput{
		Names: []string{"env-config-project"},
	})
	require.NoError(t, err)
	require.Len(t, got.Projects, 1)

	env := got.Projects[0].Environment
	assert.Equal(t, types.HostKernelLinuxKernel6, env.HostKernel,
		"Environment.HostKernel must round-trip; pre-fix the field did not exist")

	require.NotNil(t, env.ComputeConfiguration,
		"Environment.ComputeConfiguration must round-trip; pre-fix it was always nil")
	assert.Equal(t, int64(4), aws.ToInt64(env.ComputeConfiguration.VCpu))

	require.NotNil(t, env.DockerServer,
		"Environment.DockerServer must round-trip; pre-fix it was always nil")
	assert.Equal(t, []string{"sg-docker"}, env.DockerServer.SecurityGroupIds)

	require.NotNil(t, env.Fleet,
		"Environment.Fleet must round-trip; pre-fix it was always nil, silently discarding "+
			"which reserved-capacity fleet the project runs on")
	assert.Equal(t, "arn:aws:codebuild:us-east-1:123456789012:fleet/my-fleet", aws.ToString(env.Fleet.FleetArn))
}

// TestStartBuild_SourceVersionOverride_RealClient covers gopherstack-6flj-codebuild-4:
// real types.Build (codebuild@v1.72.4/types/types.go) has a SourceVersion
// field distinct from both Source.Location and ResolvedSourceVersion.
// Pre-fix, StartBuildInput.SourceVersion was misapplied onto
// Build.Source.Location (corrupting the source's own URL/checkout location
// with a commit SHA/branch name) instead of surfacing on the real
// Build.SourceVersion field, which didn't even exist on gopherstack's Build
// model. This also exercises StartBuildInput.ArtifactsOverride, which was
// already parsed off the wire into startBuildInput but never forwarded to
// the backend at all (accepted, then silently dropped on the floor).
func TestStartBuild_SourceVersionOverride_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:        aws.String("source-version-project"),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source: &types.ProjectSource{
			Type:     types.SourceTypeGithub,
			Location: aws.String("https://github.com/example/repo"),
		},
		Artifacts: &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)

	started, err := client.StartBuild(ctx, &codebuildsdk.StartBuildInput{
		ProjectName:   aws.String("source-version-project"),
		SourceVersion: aws.String("refs/heads/feature-branch"),
		ArtifactsOverride: &types.ProjectArtifacts{
			Type:     types.ArtifactsTypeS3,
			Location: aws.String("my-override-bucket"),
		},
	})
	require.NoError(t, err)

	build := started.Build
	assert.Equal(t, "refs/heads/feature-branch", aws.ToString(build.SourceVersion),
		"Build.SourceVersion must carry the requested override")
	assert.Equal(t, "https://github.com/example/repo", aws.ToString(build.Source.Location),
		"Build.Source.Location must not be corrupted by sourceVersion; pre-fix it held the version string")
	require.NotNil(t, build.Artifacts)
	assert.Equal(t, "my-override-bucket", aws.ToString(build.Artifacts.Location),
		"Build.Artifacts must reflect artifactsOverride; pre-fix this field was parsed then silently dropped")

	got, err := client.BatchGetBuilds(ctx, &codebuildsdk.BatchGetBuildsInput{Ids: []string{aws.ToString(build.Id)}})
	require.NoError(t, err)
	require.Len(t, got.Builds, 1)
	assert.Equal(t, "refs/heads/feature-branch", aws.ToString(got.Builds[0].SourceVersion))
}

// TestStartBuild_InheritsProjectSourceVersion_RealClient covers a bug where
// StartBuild always wrote cfg.SourceVersion verbatim onto the new Build,
// even when the request omitted it -- StartBuildInput.SourceVersion's own
// doc ("If sourceVersion is specified at the project level, then this
// sourceVersion (at the build level) takes precedence") establishes the
// project-level value as the fallback when the build-level one is absent,
// the same override-with-fallback pattern every other StartBuild override
// field already follows. Pre-fix, omitting sourceVersion on StartBuild
// produced a Build.SourceVersion/ResolvedSourceVersion of "" even though the
// project had a real, configured SourceVersion.
func TestStartBuild_InheritsProjectSourceVersion_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:        aws.String("inherited-source-version-project"),
		ServiceRole: aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source: &types.ProjectSource{
			Type:     types.SourceTypeGithub,
			Location: aws.String("https://github.com/example/repo"),
		},
		SourceVersion: aws.String("refs/heads/main"),
		Artifacts:     &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)

	started, err := client.StartBuild(ctx, &codebuildsdk.StartBuildInput{
		ProjectName: aws.String("inherited-source-version-project"),
	})
	require.NoError(t, err)
	assert.Equal(t, "refs/heads/main", aws.ToString(started.Build.SourceVersion),
		"Build.SourceVersion must inherit the project's sourceVersion when the request omits an override")
	assert.Equal(t, "refs/heads/main", aws.ToString(started.Build.ResolvedSourceVersion))
}

// TestRetryBuild_AutoRetryConfigChain_RealClient covers gopherstack-6flj-codebuild-5:
// real types.Build has an AutoRetryConfig field
// (codebuild@v1.72.4/types/types.go's AutoRetryConfig{AutoRetryLimit,
// AutoRetryNumber, NextAutoRetry, PreviousAutoRetry}), entirely absent from
// gopherstack's Build model -- a real client using RetryBuild to detect its
// own retry chain (a documented real-world use of this field) always saw a
// nil AutoRetryConfig, on both the original and the retried build.
func TestRetryBuild_AutoRetryConfigChain_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:           aws.String("retry-project"),
		ServiceRole:    aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source:         &types.ProjectSource{Type: types.SourceTypeNoSource},
		Artifacts:      &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		AutoRetryLimit: aws.Int32(2),
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)

	started, err := client.StartBuild(ctx, &codebuildsdk.StartBuildInput{ProjectName: aws.String("retry-project")})
	require.NoError(t, err)
	require.NotNil(t, started.Build.AutoRetryConfig,
		"Build.AutoRetryConfig must round-trip from the project's autoRetryLimit; pre-fix the field did not exist")
	assert.Equal(t, int32(2), aws.ToInt32(started.Build.AutoRetryConfig.AutoRetryLimit))
	assert.Equal(t, int32(0), aws.ToInt32(started.Build.AutoRetryConfig.AutoRetryNumber))

	retried, err := client.RetryBuild(ctx, &codebuildsdk.RetryBuildInput{Id: started.Build.Id})
	require.NoError(t, err)
	require.NotNil(t, retried.Build.AutoRetryConfig)
	assert.Equal(t, int32(1), aws.ToInt32(retried.Build.AutoRetryConfig.AutoRetryNumber),
		"the retried build's AutoRetryNumber must increment")
	assert.Equal(t, aws.ToString(started.Build.Arn), aws.ToString(retried.Build.AutoRetryConfig.PreviousAutoRetry))

	original, err := client.BatchGetBuilds(
		ctx,
		&codebuildsdk.BatchGetBuildsInput{Ids: []string{aws.ToString(started.Build.Id)}},
	)
	require.NoError(t, err)
	require.Len(t, original.Builds, 1)
	require.NotNil(t, original.Builds[0].AutoRetryConfig)
	assert.Equal(t, aws.ToString(retried.Build.Arn), aws.ToString(original.Builds[0].AutoRetryConfig.NextAutoRetry),
		"the original build's NextAutoRetry must point at the retry, matching real AWS's chain")
}

// TestStartBuild_AutoRetryLimitOverrideZero_RealClient covers a bug where an
// explicit AutoRetryLimitOverride of 0 was indistinguishable from an absent
// override: StartBuildInput.AutoRetryLimitOverride is *int32 (codebuild@
// v1.72.4 api_op_StartBuild.go), and the backend's own override-with-nil
// pattern requires a nil check, not a `> 0` guard, to apply an explicit
// zero. Pre-fix, an explicit 0 fell through the `> 0` guard and silently
// inherited the project's non-zero AutoRetryLimit instead of disabling
// automatic retries for that build.
func TestStartBuild_AutoRetryLimitOverrideZero_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:           aws.String("auto-retry-override-project"),
		ServiceRole:    aws.String("arn:aws:iam::123456789012:role/service-role"),
		Source:         &types.ProjectSource{Type: types.SourceTypeNoSource},
		Artifacts:      &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		AutoRetryLimit: aws.Int32(3),
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)

	started, err := client.StartBuild(ctx, &codebuildsdk.StartBuildInput{
		ProjectName:            aws.String("auto-retry-override-project"),
		AutoRetryLimitOverride: aws.Int32(0),
	})
	require.NoError(t, err)
	require.NotNil(t, started.Build.AutoRetryConfig)
	assert.Equal(t, int32(0), aws.ToInt32(started.Build.AutoRetryConfig.AutoRetryLimit),
		"an explicit AutoRetryLimitOverride of 0 must disable retries, not inherit the project's non-zero limit")
}

// TestStartSandbox_InheritsProjectConfiguration_RealClient covers
// gopherstack-6flj-codebuild-6: real types.Sandbox carries the same
// project-derived configuration fields as types.Build (environment/source/
// vpcConfig/serviceRole/encryptionKey/timeouts -- confirmed via
// codebuild@v1.72.4/deserializers.go's awsAwsjson11_deserializeDocumentSandbox),
// but gopherstack's Sandbox model only ever carried
// id/arn/projectName/status/startTime/endTime -- StartSandbox never copied
// any of the project's real configuration onto the sandbox it created, so a
// real client's Sandbox.Environment/Source/ServiceRole/etc. were always nil
// regardless of the project's actual settings.
func TestStartSandbox_InheritsProjectConfiguration_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:          aws.String("sandbox-project"),
		ServiceRole:   aws.String("arn:aws:iam::123456789012:role/service-role"),
		EncryptionKey: aws.String("arn:aws:kms:us-east-1:123456789012:key/my-key"),
		SourceVersion: aws.String("main"),
		Source: &types.ProjectSource{
			Type:     types.SourceTypeGithub,
			Location: aws.String("https://github.com/example/repo"),
		},
		Artifacts:        &types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts},
		TimeoutInMinutes: aws.Int32(45),
		Environment: &types.ProjectEnvironment{
			Type:        types.EnvironmentTypeLinuxContainer,
			Image:       aws.String("aws/codebuild/standard:7.0"),
			ComputeType: types.ComputeTypeBuildGeneral1Small,
		},
	})
	require.NoError(t, err)

	started, err := client.StartSandbox(
		ctx,
		&codebuildsdk.StartSandboxInput{ProjectName: aws.String("sandbox-project")},
	)
	require.NoError(t, err)

	sb := started.Sandbox
	require.NotNil(t, sb.Environment, "Sandbox.Environment must inherit from the project; pre-fix it was always nil")
	assert.Equal(t, "aws/codebuild/standard:7.0", aws.ToString(sb.Environment.Image))
	require.NotNil(t, sb.Source, "Sandbox.Source must inherit from the project; pre-fix it was always nil")
	assert.Equal(t, "https://github.com/example/repo", aws.ToString(sb.Source.Location))
	assert.Equal(t, "arn:aws:iam::123456789012:role/service-role", aws.ToString(sb.ServiceRole))
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/my-key", aws.ToString(sb.EncryptionKey))
	assert.Equal(t, int32(45), aws.ToInt32(sb.TimeoutInMinutes))

	got, err := client.BatchGetSandboxes(ctx, &codebuildsdk.BatchGetSandboxesInput{Ids: []string{aws.ToString(sb.Id)}})
	require.NoError(t, err)
	require.Len(t, got.Sandboxes, 1)
	require.NotNil(t, got.Sandboxes[0].Environment)
	assert.Equal(t, "aws/codebuild/standard:7.0", aws.ToString(got.Sandboxes[0].Environment.Image))
}

// TestStartCommandExecution_ExitCodeAndStandardErrContent_RealClient covers
// gopherstack-6flj-codebuild-7: real types.CommandExecution.ExitCode is a
// *string (codebuild@v1.72.4/deserializers.go's
// awsAwsjson11_deserializeDocumentCommandExecution "exitCode" case: "expected
// NonEmptyString to be of type string"), but gopherstack modeled it as
// int32 -- a real client's JSON decoder rejects a numeric exitCode outright.
// Also covers the wire key for stderr content: real AWS uses
// "standardErrContent", not "standardErrorContent" (the key gopherstack
// previously emitted), so a real client's StandardErrContent was always nil.
func TestStartCommandExecution_ExitCodeAndStandardErrContent_RealClient(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateProject(ctx, &codebuildsdk.CreateProjectInput{
		Name:        aws.String("sandbox-cmd-project"),
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

	started, err := client.StartSandbox(
		ctx,
		&codebuildsdk.StartSandboxInput{ProjectName: aws.String("sandbox-cmd-project")},
	)
	require.NoError(t, err)

	// The real SDK client itself proves ExitCode decodes without error: a
	// pre-fix int32 field would still marshal (Go->JSON) as a bare number,
	// which the real client's own strict type-switch decoder rejects with
	// "expected NonEmptyString to be of type string" -- if that were still
	// happening, this call would return a deserialization error, not a
	// clean response.
	exec, err := client.StartCommandExecution(ctx, &codebuildsdk.StartCommandExecutionInput{
		SandboxId: started.Sandbox.Id,
		Command:   aws.String("echo hi"),
		Type:      types.CommandTypeShell,
	})
	require.NoError(t, err)
	assert.Equal(t, "0", aws.ToString(exec.CommandExecution.ExitCode),
		"ExitCode must decode as a string via the real client")
}

// TestDeleteReportGroup_RejectsWhenReportsExistAndDeleteReportsFalse covers
// gopherstack-6flj wrapper-key sweep (workspaces/codebuild/elasticbeanstalk
// pass): real DeleteReportGroupInput.DeleteReports
// (codebuild@v1.72.4/api_op_DeleteReportGroup.go: "If false, you must delete
// any reports in the report group... If you call DeleteReportGroup for a
// report group that contains one or more reports, an exception is thrown")
// was parsed off the wire and never passed to the backend at all --
// InMemoryBackend.DeleteReportGroup always succeeded regardless of the
// group's contents or the caller's DeleteReports value.
func TestDeleteReportGroup_RejectsWhenReportsExistAndDeleteReportsFalse(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateReportGroup(ctx, &codebuildsdk.CreateReportGroupInput{
		Name: aws.String("rg-with-reports"),
		Type: types.ReportTypeTest,
		ExportConfig: &types.ReportExportConfig{
			ExportConfigType: types.ReportExportConfigTypeNoExport,
		},
	})
	require.NoError(t, err)
	rgArn := aws.ToString(created.ReportGroup.Arn)

	backend.AddReportInternal(&codebuild.Report{
		Arn:            rgArn + ":report-1",
		ReportGroupArn: rgArn,
		Type:           "TEST",
		Status:         "SUCCEEDED",
	})

	_, err = client.DeleteReportGroup(ctx, &codebuildsdk.DeleteReportGroupInput{Arn: aws.String(rgArn)})
	require.Error(
		t, err,
		"must reject deleting a report group that still has reports when DeleteReports is unset/false",
	)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())

	got, _ := client.BatchGetReportGroups(
		ctx, &codebuildsdk.BatchGetReportGroupsInput{ReportGroupArns: []string{rgArn}},
	)
	require.Len(t, got.ReportGroups, 1, "report group must still exist after the rejected delete")
}

// TestDeleteReportGroup_CascadeDeletesReportsWhenDeleteReportsTrue covers the
// other half of gopherstack-6flj-codebuild-8 (same DeleteReports field):
// setting DeleteReports=true must delete the group's reports along with the
// group itself, per the real op's documented behavior.
func TestDeleteReportGroup_CascadeDeletesReportsWhenDeleteReportsTrue(t *testing.T) {
	t.Parallel()

	backend := codebuild.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodeBuildClient(t, codebuild.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateReportGroup(ctx, &codebuildsdk.CreateReportGroupInput{
		Name: aws.String("rg-cascade-delete"),
		Type: types.ReportTypeTest,
		ExportConfig: &types.ReportExportConfig{
			ExportConfigType: types.ReportExportConfigTypeNoExport,
		},
	})
	require.NoError(t, err)
	rgArn := aws.ToString(created.ReportGroup.Arn)
	reportArn := rgArn + ":report-1"

	backend.AddReportInternal(&codebuild.Report{
		Arn:            reportArn,
		ReportGroupArn: rgArn,
		Type:           "TEST",
		Status:         "SUCCEEDED",
	})

	_, err = client.DeleteReportGroup(ctx, &codebuildsdk.DeleteReportGroupInput{
		Arn:           aws.String(rgArn),
		DeleteReports: true,
	})
	require.NoError(t, err)

	got, err := client.BatchGetReportGroups(
		ctx,
		&codebuildsdk.BatchGetReportGroupsInput{ReportGroupArns: []string{rgArn}},
	)
	require.NoError(t, err)
	require.Empty(t, got.ReportGroups)
	require.Len(t, got.ReportGroupsNotFound, 1)

	reportsOut, err := client.BatchGetReports(ctx, &codebuildsdk.BatchGetReportsInput{ReportArns: []string{reportArn}})
	require.NoError(t, err)
	assert.Empty(t, reportsOut.Reports, "the group's report must be cascade-deleted, not left orphaned")
	assert.Len(t, reportsOut.ReportsNotFound, 1)
}
