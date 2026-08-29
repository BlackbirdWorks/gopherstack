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

// TestListExtensionAssociations_ExtensionVersionNumberFilter_RealClient drives
// ListExtensionAssociations through the real client. The real
// ListExtensionAssociationsInput.ExtensionVersionNumber filters by wire key
// "extension_version_number" (appconfig@v1.48.4 serializers.go:3282) --
// gopherstack never read it, so a real client's version-scoped request
// always returned every association on the extension regardless of version.
func TestListExtensionAssociations_ExtensionVersionNumberFilter_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	appOut, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{
		Name: aws.String("extassoc-filter-app"),
	})
	require.NoError(t, err)

	ext, err := client.CreateExtension(t.Context(), &appconfigsdk.CreateExtensionInput{
		Name: aws.String("extassoc-filter-ext"),
		Actions: map[string][]types.Action{
			"ON_DEPLOYMENT_START": {
				{Name: aws.String("act1"), Uri: aws.String("arn:aws:sns:us-east-1:123456789012:topic")},
			},
		},
	})
	require.NoError(t, err)

	assoc, err := client.CreateExtensionAssociation(t.Context(), &appconfigsdk.CreateExtensionAssociationInput{
		ExtensionIdentifier: ext.Id,
		ResourceIdentifier:  appOut.Id,
	})
	require.NoError(t, err)
	require.Equal(t, ext.VersionNumber, assoc.ExtensionVersionNumber)

	matched, err := client.ListExtensionAssociations(t.Context(), &appconfigsdk.ListExtensionAssociationsInput{
		ExtensionVersionNumber: aws.Int32(assoc.ExtensionVersionNumber),
	})
	require.NoError(t, err)
	require.Len(t, matched.Items, 1, "matching extension_version_number must return the association")

	excluded, err := client.ListExtensionAssociations(t.Context(), &appconfigsdk.ListExtensionAssociationsInput{
		ExtensionVersionNumber: aws.Int32(assoc.ExtensionVersionNumber + 1),
	})
	require.NoError(t, err)
	assert.Empty(t, excluded.Items,
		"extension_version_number filter must exclude an association on a different version")
}

// TestListConfigurationProfiles_TypeFilter_RealClient drives
// ListConfigurationProfiles through the real client. The real
// ListConfigurationProfilesInput.Type filters by wire key "type"
// (appconfig@v1.48.4 serializers.go:2700) -- gopherstack never read it, so a
// real client's type-scoped request always returned every profile on the
// application regardless of type.
func TestListConfigurationProfiles_TypeFilter_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	appOut, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{
		Name: aws.String("profile-type-filter-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateConfigurationProfile(t.Context(), &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: appOut.Id, Name: aws.String("freeform-profile"),
		LocationUri: aws.String("hosted"), Type: aws.String("AWS.Freeform"),
	})
	require.NoError(t, err)
	_, err = client.CreateConfigurationProfile(t.Context(), &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: appOut.Id, Name: aws.String("flag-profile"),
		LocationUri: aws.String("hosted"), Type: aws.String("AWS.AppConfig.FeatureFlags"),
	})
	require.NoError(t, err)

	out, err := client.ListConfigurationProfiles(t.Context(), &appconfigsdk.ListConfigurationProfilesInput{
		ApplicationId: appOut.Id,
		Type:          aws.String("AWS.Freeform"),
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1, "type filter must exclude the AWS.AppConfig.FeatureFlags profile")
	assert.Equal(t, "freeform-profile", aws.ToString(out.Items[0].Name))
}

// TestGetConfiguration_ClientConfigurationVersionUnchanged_RealClient drives
// GetConfiguration through the real client. Real GetConfigurationInput binds
// ClientConfigurationVersion as "client_configuration_version"
// (appconfig@v1.48.4 api_op_GetConfiguration.go:89); when it matches the
// currently deployed version, AWS returns 204 with empty Content instead of
// resending the same data (api_op_GetConfiguration.go:101-104) --
// gopherstack always resent the full content regardless.
func TestGetConfiguration_ClientConfigurationVersionUnchanged_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	appOut, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{
		Name: aws.String("getconfig-cv-app"),
	})
	require.NoError(t, err)
	envOut, err := client.CreateEnvironment(t.Context(), &appconfigsdk.CreateEnvironmentInput{
		ApplicationId: appOut.Id, Name: aws.String("getconfig-cv-env"),
	})
	require.NoError(t, err)
	profOut, err := client.CreateConfigurationProfile(t.Context(), &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: appOut.Id, Name: aws.String("getconfig-cv-profile"), LocationUri: aws.String("hosted"),
	})
	require.NoError(t, err)
	_, err = client.CreateHostedConfigurationVersion(t.Context(), &appconfigsdk.CreateHostedConfigurationVersionInput{
		ApplicationId: appOut.Id, ConfigurationProfileId: profOut.Id,
		Content: []byte("enabled"), ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)
	stratOut, err := client.CreateDeploymentStrategy(t.Context(), &appconfigsdk.CreateDeploymentStrategyInput{
		Name: aws.String("getconfig-cv-strategy"), DeploymentDurationInMinutes: aws.Int32(0),
		GrowthFactor: aws.Float32(100), ReplicateTo: types.ReplicateToNone,
	})
	require.NoError(t, err)
	_, err = client.StartDeployment(t.Context(), &appconfigsdk.StartDeploymentInput{
		ApplicationId: appOut.Id, EnvironmentId: envOut.Id, ConfigurationProfileId: profOut.Id,
		DeploymentStrategyId: stratOut.Id, ConfigurationVersion: aws.String("1"),
	})
	require.NoError(t, err)

	// deliberately testing the deprecated op's own wire behavior.
	//nolint:staticcheck // deliberately testing the deprecated op's own wire behavior
	first, err := client.GetConfiguration(t.Context(), &appconfigsdk.GetConfigurationInput{
		Application: appOut.Id, Environment: envOut.Id, Configuration: profOut.Id,
		ClientId: aws.String("test-client"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, first.Content)
	firstVersion := aws.ToString(first.ConfigurationVersion)
	require.NotEmpty(t, firstVersion)

	//nolint:staticcheck // deliberately testing the deprecated op's own wire behavior
	unchanged, err := client.GetConfiguration(t.Context(), &appconfigsdk.GetConfigurationInput{
		Application: appOut.Id, Environment: envOut.Id, Configuration: profOut.Id,
		ClientId:                   aws.String("test-client"),
		ClientConfigurationVersion: aws.String(firstVersion),
	})
	require.NoError(t, err)
	assert.Empty(t, unchanged.Content,
		"a matching client_configuration_version must return empty Content, not resend the same data")
}
