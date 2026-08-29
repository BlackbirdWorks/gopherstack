package appconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartDeploymentViaSDKClient_TagsKmsKeyIdentifierLatestDeploymentNumber
// drives StartDeployment through a real aws-sdk-go-v2 client (bd
// gopherstack-6flj/21my wrapper-key/silent-drop sweep). Real
// StartDeploymentInput (appconfig@v1.48.4 api_op_StartDeployment.go) has
// three real members this handler's request struct did not bind at all:
// Tags (inline tags applied to the deployment's own ARN, same pattern as
// the six other Create* ops fixed under bd gopherstack-lcan -- StartDeployment
// was not among those six despite also accepting inline Tags),
// KmsKeyIdentifier (an explicit per-deployment override of the profile's
// stored KmsKeyIdentifier -- previously only the profile's own value was
// ever used, so a caller-supplied override was silently discarded), and
// LatestDeploymentNumber (an optimistic-concurrency check identical in
// shape to CreateHostedConfigurationVersion's already-fixed
// Latest-Version-Number header -- a stale value must return
// ConflictException instead of silently racing another writer).
func TestStartDeploymentViaSDKClient_TagsKmsKeyIdentifierLatestDeploymentNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	appOut, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{
		Name: aws.String("wire-fix-dep-app"),
	})
	require.NoError(t, err)

	envOut, err := client.CreateEnvironment(t.Context(), &appconfigsdk.CreateEnvironmentInput{
		ApplicationId: appOut.Id,
		Name:          aws.String("wire-fix-dep-env"),
	})
	require.NoError(t, err)

	profOut, err := client.CreateConfigurationProfile(t.Context(), &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId:    appOut.Id,
		Name:             aws.String("wire-fix-dep-profile"),
		LocationUri:      aws.String("hosted"),
		KmsKeyIdentifier: aws.String("alias/profile-default-key"),
	})
	require.NoError(t, err)

	_, err = client.CreateHostedConfigurationVersion(t.Context(), &appconfigsdk.CreateHostedConfigurationVersionInput{
		ApplicationId:          appOut.Id,
		ConfigurationProfileId: profOut.Id,
		Content:                []byte("enabled"),
		ContentType:            aws.String("text/plain"),
	})
	require.NoError(t, err)

	stratOut, err := client.CreateDeploymentStrategy(t.Context(), &appconfigsdk.CreateDeploymentStrategyInput{
		Name:                        aws.String("wire-fix-dep-strategy"),
		DeploymentDurationInMinutes: aws.Int32(0),
		GrowthFactor:                aws.Float32(100),
		ReplicateTo:                 types.ReplicateToNone,
	})
	require.NoError(t, err)

	// A stale LatestDeploymentNumber (this environment has no deployments
	// yet, so the real current value is 0) must be rejected with a
	// conflict rather than silently accepted.
	_, err = client.StartDeployment(t.Context(), &appconfigsdk.StartDeploymentInput{
		ApplicationId:          appOut.Id,
		EnvironmentId:          envOut.Id,
		ConfigurationProfileId: profOut.Id,
		DeploymentStrategyId:   stratOut.Id,
		ConfigurationVersion:   aws.String("1"),
		LatestDeploymentNumber: aws.Int32(5),
	})
	require.Error(t, err, "a stale LatestDeploymentNumber must be rejected")

	startOut, err := client.StartDeployment(t.Context(), &appconfigsdk.StartDeploymentInput{
		ApplicationId:          appOut.Id,
		EnvironmentId:          envOut.Id,
		ConfigurationProfileId: profOut.Id,
		DeploymentStrategyId:   stratOut.Id,
		ConfigurationVersion:   aws.String("1"),
		LatestDeploymentNumber: aws.Int32(0),
		KmsKeyIdentifier:       aws.String("alias/deployment-override-key"),
		Tags: map[string]string{
			"team": "platform",
		},
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), startOut.DeploymentNumber)
	assert.Equal(t, "alias/deployment-override-key", aws.ToString(startOut.KmsKeyIdentifier),
		"StartDeploymentInput.KmsKeyIdentifier must override the profile's stored default")

	getOut, err := client.GetDeployment(t.Context(), &appconfigsdk.GetDeploymentInput{
		ApplicationId:    appOut.Id,
		EnvironmentId:    envOut.Id,
		DeploymentNumber: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, "alias/deployment-override-key", aws.ToString(getOut.KmsKeyIdentifier),
		"the override must persist and round-trip through GetDeployment")

	tagsOut, err := client.ListTagsForResource(t.Context(), &appconfigsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(
			"arn:aws:appconfig:us-east-1:123456789012:application/" + aws.ToString(appOut.Id) +
				"/environment/" + aws.ToString(envOut.Id) + "/deployment/1",
		),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "platform"}, tagsOut.Tags,
		"StartDeploymentInput.Tags must be applied to the deployment's own ARN, same as the six other Create* ops")
}
