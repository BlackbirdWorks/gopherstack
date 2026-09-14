package appconfig_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	appconfigtypes "github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func newRealClientHandler() *appconfig.Handler {
	return appconfig.NewHandler(appconfig.NewInMemoryBackend("123456789012", "us-east-1"))
}

// applicationARN builds an application's ARN the same way store.go's
// appconfigARN does -- CreateApplicationOutput carries no Arn field of its
// own (confirmed via api_op_CreateApplication.go), so a real client
// wanting to Tag/UntagResource an application must construct it.
func applicationARN(appID string) string {
	return "arn:aws:appconfig:us-east-1:123456789012:application/" + appID
}

// TestRealClient_DeploymentConfigAndExperiments drives every
// gopherstack-n3zi uncovered appconfig op through the real aws-sdk-go-v2
// client (newTestAppConfigClient, shared with handler_error_type_test.go).
func TestRealClient_DeploymentConfigAndExperiments(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testDeploymentStrategiesRealClient, "deployment_strategies"},
		{testHostedConfigurationVersionsRealClient, "hosted_configuration_versions"},
		{testDeleteConfigurationProfileRealClient, "delete_configuration_profile"},
		{testApplicationEnvironmentUpdatesRealClient, "application_environment_updates"},
		{testValidateConfigurationRealClient, "validate_configuration"},
		{testTagsRealClient, "tags"},
		{testExtensionsRealClient, "extensions"},
		{testExperimentDefinitionsRealClient, "experiment_definitions"},
		{testExperimentRunsRealClient, "experiment_runs"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testDeploymentStrategiesRealClient covers GetDeploymentStrategy,
// ListDeploymentStrategies, UpdateDeploymentStrategy,
// DeleteDeploymentStrategy.
func testDeploymentStrategiesRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateDeploymentStrategy(ctx, &appconfigsdk.CreateDeploymentStrategyInput{
		Name:                        aws.String("my-strategy"),
		DeploymentDurationInMinutes: aws.Int32(20),
		GrowthFactor:                aws.Float32(10),
		GrowthType:                  appconfigtypes.GrowthTypeLinear,
	})
	require.NoError(t, err)
	strategyID := aws.ToString(createOut.Id)

	getOut, err := client.GetDeploymentStrategy(ctx, &appconfigsdk.GetDeploymentStrategyInput{
		DeploymentStrategyId: aws.String(strategyID),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-strategy", aws.ToString(getOut.Name))

	listOut, err := client.ListDeploymentStrategies(ctx, &appconfigsdk.ListDeploymentStrategiesInput{})
	require.NoError(t, err)
	found := false
	for _, s := range listOut.Items {
		if aws.ToString(s.Id) == strategyID {
			found = true
		}
	}
	assert.True(t, found, "expected created strategy in ListDeploymentStrategies")

	updOut, err := client.UpdateDeploymentStrategy(ctx, &appconfigsdk.UpdateDeploymentStrategyInput{
		DeploymentStrategyId: aws.String(strategyID),
		Description:          aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(updOut.Description))

	_, err = client.DeleteDeploymentStrategy(ctx, &appconfigsdk.DeleteDeploymentStrategyInput{
		DeploymentStrategyId: aws.String(strategyID),
	})
	require.NoError(t, err)

	_, err = client.GetDeploymentStrategy(ctx, &appconfigsdk.GetDeploymentStrategyInput{
		DeploymentStrategyId: aws.String(strategyID),
	})
	require.Error(t, err)
}

// testHostedConfigurationVersionsRealClient covers
// GetHostedConfigurationVersion, ListHostedConfigurationVersions,
// DeleteHostedConfigurationVersion (CreateHostedConfigurationVersion is
// already typed-covered elsewhere, used here only to seed state).
func testHostedConfigurationVersionsRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	appOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
		Name: aws.String("hcv-app"),
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.Id)

	profOut, err := client.CreateConfigurationProfile(ctx, &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: aws.String(appID),
		Name:          aws.String("hcv-profile"),
		LocationUri:   aws.String("hosted"),
	})
	require.NoError(t, err)
	profileID := aws.ToString(profOut.Id)

	createOut, err := client.CreateHostedConfigurationVersion(ctx, &appconfigsdk.CreateHostedConfigurationVersionInput{
		ApplicationId:          aws.String(appID),
		ConfigurationProfileId: aws.String(profileID),
		Content:                []byte(`{"key":"value"}`),
		ContentType:            aws.String("application/json"),
	})
	require.NoError(t, err)
	versionNumber := createOut.VersionNumber

	getOut, err := client.GetHostedConfigurationVersion(ctx, &appconfigsdk.GetHostedConfigurationVersionInput{
		ApplicationId:          aws.String(appID),
		ConfigurationProfileId: aws.String(profileID),
		VersionNumber:          aws.Int32(versionNumber),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"key":"value"}`, string(getOut.Content))
	assert.Equal(t, versionNumber, getOut.VersionNumber)

	listOut, err := client.ListHostedConfigurationVersions(
		ctx, &appconfigsdk.ListHostedConfigurationVersionsInput{
			ApplicationId:          aws.String(appID),
			ConfigurationProfileId: aws.String(profileID),
		},
	)
	require.NoError(t, err)
	require.Len(t, listOut.Items, 1)
	assert.Equal(t, versionNumber, listOut.Items[0].VersionNumber)

	_, err = client.DeleteHostedConfigurationVersion(ctx, &appconfigsdk.DeleteHostedConfigurationVersionInput{
		ApplicationId:          aws.String(appID),
		ConfigurationProfileId: aws.String(profileID),
		VersionNumber:          aws.Int32(versionNumber),
	})
	require.NoError(t, err)

	listOut2, err := client.ListHostedConfigurationVersions(
		ctx, &appconfigsdk.ListHostedConfigurationVersionsInput{
			ApplicationId:          aws.String(appID),
			ConfigurationProfileId: aws.String(profileID),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, listOut2.Items)
}

// testDeleteConfigurationProfileRealClient covers DeleteConfigurationProfile.
func testDeleteConfigurationProfileRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	appOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
		Name: aws.String("del-profile-app"),
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.Id)

	profOut, err := client.CreateConfigurationProfile(ctx, &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: aws.String(appID),
		Name:          aws.String("del-profile"),
		LocationUri:   aws.String("hosted"),
	})
	require.NoError(t, err)
	profileID := aws.ToString(profOut.Id)

	_, err = client.DeleteConfigurationProfile(ctx, &appconfigsdk.DeleteConfigurationProfileInput{
		ApplicationId:          aws.String(appID),
		ConfigurationProfileId: aws.String(profileID),
	})
	require.NoError(t, err)

	_, err = client.GetConfigurationProfile(ctx, &appconfigsdk.GetConfigurationProfileInput{
		ApplicationId:          aws.String(appID),
		ConfigurationProfileId: aws.String(profileID),
	})
	require.Error(t, err)
}

// testApplicationEnvironmentUpdatesRealClient covers UpdateApplication and
// UpdateEnvironment.
func testApplicationEnvironmentUpdatesRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	appOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
		Name: aws.String("upd-app"),
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.Id)

	updAppOut, err := client.UpdateApplication(ctx, &appconfigsdk.UpdateApplicationInput{
		ApplicationId: aws.String(appID),
		Name:          aws.String("upd-app-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "upd-app-renamed", aws.ToString(updAppOut.Name))

	envOut, err := client.CreateEnvironment(ctx, &appconfigsdk.CreateEnvironmentInput{
		ApplicationId: aws.String(appID),
		Name:          aws.String("upd-env"),
	})
	require.NoError(t, err)
	envID := aws.ToString(envOut.Id)

	updEnvOut, err := client.UpdateEnvironment(ctx, &appconfigsdk.UpdateEnvironmentInput{
		ApplicationId: aws.String(appID),
		EnvironmentId: aws.String(envID),
		Name:          aws.String("upd-env-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "upd-env-renamed", aws.ToString(updEnvOut.Name))
}

// testValidateConfigurationRealClient covers ValidateConfiguration.
func testValidateConfigurationRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	appOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
		Name: aws.String("validate-app"),
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.Id)

	profOut, err := client.CreateConfigurationProfile(ctx, &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: aws.String(appID),
		Name:          aws.String("validate-profile"),
		LocationUri:   aws.String("hosted"),
	})
	require.NoError(t, err)
	profileID := aws.ToString(profOut.Id)

	createOut, err := client.CreateHostedConfigurationVersion(ctx, &appconfigsdk.CreateHostedConfigurationVersionInput{
		ApplicationId:          aws.String(appID),
		ConfigurationProfileId: aws.String(profileID),
		Content:                []byte(`{"key":"value"}`),
		ContentType:            aws.String("application/json"),
	})
	require.NoError(t, err)

	_, err = client.ValidateConfiguration(ctx, &appconfigsdk.ValidateConfigurationInput{
		ApplicationId:          aws.String(appID),
		ConfigurationProfileId: aws.String(profileID),
		ConfigurationVersion:   aws.String(strconv.Itoa(int(createOut.VersionNumber))),
	})
	require.NoError(t, err)
}

// testTagsRealClient covers TagResource and UntagResource
// (ListTagsForResource is already typed-covered elsewhere).
func testTagsRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	appOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
		Name: aws.String("tag-app"),
	})
	require.NoError(t, err)
	appARN := applicationARN(aws.ToString(appOut.Id))

	_, err = client.TagResource(ctx, &appconfigsdk.TagResourceInput{
		ResourceArn: aws.String(appARN),
		Tags: map[string]string{
			"env":  "prod",
			"team": "platform",
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &appconfigsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(appARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	_, err = client.UntagResource(ctx, &appconfigsdk.UntagResourceInput{
		ResourceArn: aws.String(appARN),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &appconfigsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(appARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "prod", listOut2.Tags["env"])
}

// testExtensionsRealClient covers ListExtensions, UpdateExtension,
// DeleteExtension, GetExtensionAssociation, UpdateExtensionAssociation,
// DeleteExtensionAssociation.
func testExtensionsRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	appOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
		Name: aws.String("ext-app"),
	})
	require.NoError(t, err)
	appARN := applicationARN(aws.ToString(appOut.Id))

	extOut, err := client.CreateExtension(ctx, &appconfigsdk.CreateExtensionInput{
		Name: aws.String("my-extension"),
		Actions: map[string][]appconfigtypes.Action{
			string(appconfigtypes.ActionPointOnDeploymentComplete): {
				{Name: aws.String("notify"), Uri: aws.String("arn:aws:sns:us-east-1:123456789012:topic")},
			},
		},
	})
	require.NoError(t, err)
	extensionID := aws.ToString(extOut.Id)

	listOut, err := client.ListExtensions(ctx, &appconfigsdk.ListExtensionsInput{})
	require.NoError(t, err)
	found := false
	for _, e := range listOut.Items {
		if aws.ToString(e.Id) == extensionID {
			found = true
		}
	}
	assert.True(t, found, "expected created extension in ListExtensions")

	updExtOut, err := client.UpdateExtension(ctx, &appconfigsdk.UpdateExtensionInput{
		ExtensionIdentifier: aws.String(extensionID),
		Description:         aws.String("updated description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", aws.ToString(updExtOut.Description))

	assocOut, err := client.CreateExtensionAssociation(ctx, &appconfigsdk.CreateExtensionAssociationInput{
		ExtensionIdentifier: aws.String(extensionID),
		ResourceIdentifier:  aws.String(appARN),
	})
	require.NoError(t, err)
	assocID := aws.ToString(assocOut.Id)

	getAssocOut, err := client.GetExtensionAssociation(ctx, &appconfigsdk.GetExtensionAssociationInput{
		ExtensionAssociationId: aws.String(assocID),
	})
	require.NoError(t, err)
	assert.Equal(t, appARN, aws.ToString(getAssocOut.ResourceArn))

	updAssocOut, err := client.UpdateExtensionAssociation(ctx, &appconfigsdk.UpdateExtensionAssociationInput{
		ExtensionAssociationId: aws.String(assocID),
		Parameters:             map[string]string{"key": "value"},
	})
	require.NoError(t, err)
	assert.Equal(t, "value", updAssocOut.Parameters["key"])

	_, err = client.DeleteExtensionAssociation(ctx, &appconfigsdk.DeleteExtensionAssociationInput{
		ExtensionAssociationId: aws.String(assocID),
	})
	require.NoError(t, err)

	_, err = client.GetExtensionAssociation(ctx, &appconfigsdk.GetExtensionAssociationInput{
		ExtensionAssociationId: aws.String(assocID),
	})
	require.Error(t, err)

	// DeleteExtension without a version number deletes only the latest
	// version (extensions.go's own doc comment: "matching real AWS
	// AppConfig ... not every version"); UpdateExtension above created
	// version 2, so both versions must be deleted before GetExtension
	// (which resolves to the latest remaining version) 404s.
	_, err = client.DeleteExtension(ctx, &appconfigsdk.DeleteExtensionInput{
		ExtensionIdentifier: aws.String(extensionID),
		VersionNumber:       aws.Int32(2),
	})
	require.NoError(t, err)

	_, err = client.DeleteExtension(ctx, &appconfigsdk.DeleteExtensionInput{
		ExtensionIdentifier: aws.String(extensionID),
		VersionNumber:       aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.GetExtension(ctx, &appconfigsdk.GetExtensionInput{
		ExtensionIdentifier: aws.String(extensionID),
	})
	require.Error(t, err)
}

// experimentDefinitionSetup creates an application, environment, and
// feature-flags-typed configuration profile, returning their identifiers
// for use by the experiment definition/run subtests.
func experimentDefinitionSetup(t *testing.T, client *appconfigsdk.Client) (string, string, string) {
	t.Helper()

	ctx := t.Context()

	appOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
		Name: aws.String("exp-app"),
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.Id)

	envOut, err := client.CreateEnvironment(ctx, &appconfigsdk.CreateEnvironmentInput{
		ApplicationId: aws.String(appID),
		Name:          aws.String("exp-env"),
	})
	require.NoError(t, err)
	envID := aws.ToString(envOut.Id)

	profOut, err := client.CreateConfigurationProfile(ctx, &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: aws.String(appID),
		Name:          aws.String("exp-profile"),
		LocationUri:   aws.String("hosted"),
	})
	require.NoError(t, err)
	profileID := aws.ToString(profOut.Id)

	return appID, envID, profileID
}

// testExperimentDefinitionsRealClient covers GetExperimentDefinition,
// DeleteExperimentDefinition, UpdateExperimentDefinition
// (CreateExperimentDefinition is already typed-covered elsewhere, used here
// only to seed state).
func testExperimentDefinitionsRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	appID, envID, profileID := experimentDefinitionSetup(t, client)

	defOut, err := client.CreateExperimentDefinition(ctx, &appconfigsdk.CreateExperimentDefinitionInput{
		ApplicationIdentifier:          aws.String(appID),
		EnvironmentIdentifier:          aws.String(envID),
		ConfigurationProfileIdentifier: aws.String(profileID),
		Name:                           aws.String("my-experiment"),
		FlagKey:                        aws.String("my-flag"),
		AudienceRule:                   aws.String("true"),
		Control: &appconfigtypes.TreatmentInput{
			FlagValue: &appconfigtypes.FlagValue{Enabled: false},
		},
		Treatments: []appconfigtypes.TreatmentInput{
			{FlagValue: &appconfigtypes.FlagValue{Enabled: true}, Weight: 100},
		},
	})
	require.NoError(t, err)
	defID := aws.ToString(defOut.Id)

	getOut, err := client.GetExperimentDefinition(ctx, &appconfigsdk.GetExperimentDefinitionInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-experiment", aws.ToString(getOut.Name))

	newHypothesis := "new hypothesis"

	updOut, err := client.UpdateExperimentDefinition(ctx, &appconfigsdk.UpdateExperimentDefinitionInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
		Hypothesis:                     aws.String(newHypothesis),
	})
	require.NoError(t, err)
	assert.Equal(t, newHypothesis, aws.ToString(updOut.Hypothesis))

	// DeleteType defaults to ARCHIVE (a soft delete that leaves the record
	// readable with Status=ARCHIVED, per DeleteExperimentDefinition's own
	// doc comment); DESTROY is required for a real removal.
	_, err = client.DeleteExperimentDefinition(ctx, &appconfigsdk.DeleteExperimentDefinitionInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
		DeleteType:                     appconfigtypes.DeleteTypeDestroy,
	})
	require.NoError(t, err)

	_, err = client.GetExperimentDefinition(ctx, &appconfigsdk.GetExperimentDefinitionInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
	})
	require.Error(t, err)
}

// testExperimentRunsRealClient covers StartExperimentRun, GetExperimentRun,
// ListExperimentRuns, UpdateExperimentRun, StopExperimentRun,
// ListExperimentRunEvents.
func testExperimentRunsRealClient(t *testing.T) {
	t.Helper()

	h := newRealClientHandler()
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	appID, envID, profileID := experimentDefinitionSetup(t, client)

	defOut, err := client.CreateExperimentDefinition(ctx, &appconfigsdk.CreateExperimentDefinitionInput{
		ApplicationIdentifier:          aws.String(appID),
		EnvironmentIdentifier:          aws.String(envID),
		ConfigurationProfileIdentifier: aws.String(profileID),
		Name:                           aws.String("run-experiment"),
		FlagKey:                        aws.String("my-flag"),
		AudienceRule:                   aws.String("true"),
		Control: &appconfigtypes.TreatmentInput{
			FlagValue: &appconfigtypes.FlagValue{Enabled: false},
		},
		Treatments: []appconfigtypes.TreatmentInput{
			{FlagValue: &appconfigtypes.FlagValue{Enabled: true}, Weight: 100},
		},
	})
	require.NoError(t, err)
	defID := aws.ToString(defOut.Id)

	startOut, err := client.StartExperimentRun(ctx, &appconfigsdk.StartExperimentRunInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
		Description:                    aws.String("first run"),
	})
	require.NoError(t, err)
	runNumber := startOut.Run
	assert.Equal(t, appconfigtypes.ExperimentRunStatusRunning, startOut.Status)

	getOut, err := client.GetExperimentRun(ctx, &appconfigsdk.GetExperimentRunInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
		Run:                            aws.Int32(runNumber),
	})
	require.NoError(t, err)
	assert.Equal(t, "first run", aws.ToString(getOut.Description))

	listOut, err := client.ListExperimentRuns(ctx, &appconfigsdk.ListExperimentRunsInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Items, 1)
	assert.Equal(t, runNumber, listOut.Items[0].Run)

	newDescription := "updated run description"

	updOut, err := client.UpdateExperimentRun(ctx, &appconfigsdk.UpdateExperimentRunInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
		Run:                            aws.Int32(runNumber),
		Description:                    aws.String(newDescription),
	})
	require.NoError(t, err)
	assert.Equal(t, newDescription, aws.ToString(updOut.Description))

	eventsOut, err := client.ListExperimentRunEvents(ctx, &appconfigsdk.ListExperimentRunEventsInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
		Run:                            aws.Int32(runNumber),
	})
	require.NoError(t, err)
	assert.NotNil(t, eventsOut.Items)

	stopOut, err := client.StopExperimentRun(ctx, &appconfigsdk.StopExperimentRunInput{
		ApplicationIdentifier:          aws.String(appID),
		ExperimentDefinitionIdentifier: aws.String(defID),
		Run:                            aws.Int32(runNumber),
		Result: &appconfigtypes.ExperimentRunResult{
			ExecutiveSummary: aws.String("looks good"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, appconfigtypes.ExperimentRunStatusDone, stopOut.Status)
}
