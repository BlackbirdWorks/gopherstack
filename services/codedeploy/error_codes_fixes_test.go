package codedeploy_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// These tests drive the real aws-sdk-go-v2 CodeDeploy client against
// backend failures whose emitted error code named no type in the real SDK
// (found by cmd/errcodeaudit). Each asserts the specific typed exception
// the operation's own deserializeOpError<Op> switch models, via errors.As,
// not merely that an error occurred.

func TestCreateDeploymentConfig_RealClient_InvalidComputePlatform(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateDeploymentConfig(t.Context(), &codedeploysdk.CreateDeploymentConfigInput{
		DeploymentConfigName: aws.String("ecf-bad-platform"),
		ComputePlatform:      "NotAPlatform",
	})
	require.Error(t, err)

	var target *types.InvalidComputePlatformException

	require.ErrorAs(t, err, &target)
}

func TestBatchGetApplicationRevisions_RealClient_TooMany(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("ecf-batch-app"),
	})
	require.NoError(t, err)

	revisions := make([]types.RevisionLocation, 0, 26)
	for range 26 {
		revisions = append(revisions, types.RevisionLocation{
			RevisionType: types.RevisionLocationTypeS3,
			S3Location:   &types.S3Location{Bucket: aws.String("b"), Key: aws.String("k")},
		})
	}

	_, err = client.BatchGetApplicationRevisions(t.Context(), &codedeploysdk.BatchGetApplicationRevisionsInput{
		ApplicationName: aws.String("ecf-batch-app"),
		Revisions:       revisions,
	})
	require.Error(t, err)

	var target *types.BatchLimitExceededException

	require.ErrorAs(t, err, &target)
}

func TestTagResource_RealClient_ReservedPrefix(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("ecf-tag-app"),
	})
	require.NoError(t, err)

	appARN := backend.ApplicationARN("ecf-tag-app")

	_, err = client.TagResource(t.Context(), &codedeploysdk.TagResourceInput{
		ResourceArn: aws.String(appARN),
		Tags:        []types.Tag{{Key: aws.String("aws:reserved"), Value: aws.String("x")}},
	})
	require.Error(t, err)

	var target *types.InvalidTagsToAddException

	require.ErrorAs(t, err, &target)
}

func TestRegisterOnPremisesInstance_RealClient_InvalidName(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.RegisterOnPremisesInstance(t.Context(), &codedeploysdk.RegisterOnPremisesInstanceInput{
		InstanceName: aws.String("bad name with spaces!!"),
		IamUserArn:   aws.String("arn:aws:iam::000000000000:user/test"),
	})
	require.Error(t, err)

	var target *types.InvalidInstanceNameException

	require.ErrorAs(t, err, &target)
}

func TestCreateApplication_RealClient_NameRequired(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String(""),
	})
	require.Error(t, err)

	var target *types.ApplicationNameRequiredException

	require.ErrorAs(t, err, &target)
}

func TestCreateDeploymentGroup_RealClient_GroupNameRequired(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("ecf-dg-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("ecf-dg-app"),
		DeploymentGroupName: aws.String(""),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/test"),
	})
	require.Error(t, err)

	var target *types.DeploymentGroupNameRequiredException

	require.ErrorAs(t, err, &target)
}

// TestGetDeploymentInstance_RealClient_InstanceIDRequired exercises a
// deprecated but still-implemented op (GetDeploymentTarget is its modern
// replacement); InstanceIdRequiredException is only modeled here and by
// BatchGetDeploymentInstances.
func TestGetDeploymentInstance_RealClient_InstanceIDRequired(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	//nolint:staticcheck // deprecated op under test still needs error-path coverage
	_, err := client.GetDeploymentInstance(t.Context(), &codedeploysdk.GetDeploymentInstanceInput{
		DeploymentId: aws.String("d-EXAMPLE"),
		InstanceId:   aws.String(""),
	})
	require.Error(t, err)

	var target *types.InstanceIdRequiredException

	require.ErrorAs(t, err, &target)
}

func TestTagResource_RealClient_ResourceArnRequired(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.TagResource(t.Context(), &codedeploysdk.TagResourceInput{
		ResourceArn: aws.String(""),
		Tags:        []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.Error(t, err)

	var target *types.ResourceArnRequiredException

	require.ErrorAs(t, err, &target)
}

func TestDeleteGitHubAccountToken_RealClient_TokenNameRequired(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.DeleteGitHubAccountToken(t.Context(), &codedeploysdk.DeleteGitHubAccountTokenInput{
		TokenName: aws.String(""),
	})
	require.Error(t, err)

	var target *types.GitHubAccountTokenNameRequiredException

	require.ErrorAs(t, err, &target)
}
