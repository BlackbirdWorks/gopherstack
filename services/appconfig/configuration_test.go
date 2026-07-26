package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// seedDeployableConfig creates an application, environment, hosted
// configuration profile, hosted configuration version, and zero-duration
// deployment strategy -- everything StartDeployment needs -- returning the
// IDs and the raw content that was uploaded.
func seedDeployableConfig(
	t *testing.T, b *appconfig.InMemoryBackend, content []byte,
) (string, string, string, string) {
	t.Helper()

	app, err := b.CreateApplication("cfg-app", "", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "cfg-env", "", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(app.ID, "cfg-profile", "", "hosted", "AWS.Freeform", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(app.ID, profile.ID, "application/json", "", "", content, nil)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("cfg-strat", "", 0, 0, 100, "LINEAR", "NONE", nil)
	require.NoError(t, err)

	return app.ID, env.ID, profile.ID, strategy.ID
}

// TestBackend_GetConfiguration_EmptyBeforeAnyDeployment verifies the real
// AWS AppConfig contract that GetConfiguration ("Retrieves the latest
// DEPLOYED configuration") returns empty content -- not the highest
// hosted-version content -- until a deployment has actually completed, even
// when a hosted configuration version already exists.
func TestBackend_GetConfiguration_EmptyBeforeAnyDeployment(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID, _ := seedDeployableConfig(t, b, []byte(`{"k":"v"}`))

	got, err := b.GetConfiguration(appID, envID, profileID)
	require.NoError(t, err)
	assert.Empty(t, got.Content, "must not surface a never-deployed hosted version")
}

// TestBackend_GetConfiguration_ReturnsDeployedVersion verifies that once a
// deployment COMPLETEs, GetConfiguration returns that deployment's content.
func TestBackend_GetConfiguration_ReturnsDeployedVersion(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	content := []byte(`{"feature":"on"}`)
	appID, envID, profileID, strategyID := seedDeployableConfig(t, b, content)

	_, err := b.StartDeployment(appID, envID, profileID, strategyID, "1", "")
	require.NoError(t, err)

	got, err := b.GetConfiguration(appID, envID, profileID)
	require.NoError(t, err)
	assert.Equal(t, content, got.Content)
	assert.Equal(t, int32(1), got.VersionNumber)
}

// TestBackend_CurrentDeployedConfiguration_MatchesDeployedVersion verifies
// the public CurrentDeployedConfiguration accessor (exposed for a future
// appconfig -> appconfigdata bridge, see bd gopherstack-uiyi) returns the
// same content GetConfiguration does, resolved by name.
func TestBackend_CurrentDeployedConfiguration_MatchesDeployedVersion(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	content := []byte(`{"feature":"on"}`)
	appID, envID, profileID, strategyID := seedDeployableConfig(t, b, content)

	_, err := b.StartDeployment(appID, envID, profileID, strategyID, "1", "")
	require.NoError(t, err)

	gotContent, gotContentType, _, err := b.CurrentDeployedConfiguration("cfg-app", "cfg-env", "cfg-profile")
	require.NoError(t, err)
	assert.Equal(t, content, gotContent)
	assert.Equal(t, "application/json", gotContentType)
}

// TestBackend_CurrentDeployedConfiguration_NotFound verifies the accessor
// surfaces the same not-found errors GetConfiguration does for an unknown
// application/environment/profile.
func TestBackend_CurrentDeployedConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	_, _, _, err := b.CurrentDeployedConfiguration("nonexistent", "env", "profile")
	require.Error(t, err)
}

// TestBackend_GetConfiguration_NotFound verifies each resolution stage
// (application, environment, configuration profile) surfaces a not-found
// error for an unknown identifier.
func TestBackend_GetConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		application   string
		environment   string
		configuration string
	}{
		{name: "unknown application", application: "nonexistent", environment: "e", configuration: "c"},
		{name: "unknown environment", application: "", environment: "nonexistent", configuration: "c"},
		{name: "unknown profile", application: "", environment: "", configuration: "nonexistent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

			app, err := b.CreateApplication("nf-app-"+tt.name, "", nil)
			require.NoError(t, err)

			env, err := b.CreateEnvironment(app.ID, "nf-env", "", nil, nil)
			require.NoError(t, err)

			application := tt.application
			if application == "" {
				application = app.ID
			}

			environment := tt.environment
			if environment == "" {
				environment = env.ID
			}

			_, err = b.GetConfiguration(application, environment, tt.configuration)
			require.Error(t, err)
		})
	}
}

// TestBackend_DeployedConfig_CascadeDeleteOnEnvironment verifies that
// deleting an environment removes its deployedConfigs tracking entry, so a
// re-created environment with the same ID never inherits stale deployed
// content.
func TestBackend_DeployedConfig_CascadeDeleteOnEnvironment(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID, strategyID := seedDeployableConfig(t, b, []byte(`{}`))

	_, err := b.StartDeployment(appID, envID, profileID, strategyID, "1", "")
	require.NoError(t, err)
	require.Equal(t, 1, b.DeployedConfigCount())

	require.NoError(t, b.DeleteEnvironment(appID, envID))
	assert.Equal(t, 0, b.DeployedConfigCount(), "deployedConfigs must not leak after DeleteEnvironment")
}

// TestBackend_DeployedConfig_CascadeDeleteOnApplication verifies that
// deleting an application removes every deployedConfigs entry for it.
func TestBackend_DeployedConfig_CascadeDeleteOnApplication(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID, strategyID := seedDeployableConfig(t, b, []byte(`{}`))

	_, err := b.StartDeployment(appID, envID, profileID, strategyID, "1", "")
	require.NoError(t, err)
	require.Equal(t, 1, b.DeployedConfigCount())

	require.NoError(t, b.DeleteApplication(appID))
	assert.Equal(t, 0, b.DeployedConfigCount(), "deployedConfigs must not leak after DeleteApplication")
}

// TestBackend_DeployedConfig_CascadeDeleteOnProfile verifies that deleting
// a configuration profile removes its deployedConfigs entry.
func TestBackend_DeployedConfig_CascadeDeleteOnProfile(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID, strategyID := seedDeployableConfig(t, b, []byte(`{}`))

	_, err := b.StartDeployment(appID, envID, profileID, strategyID, "1", "")
	require.NoError(t, err)
	require.Equal(t, 1, b.DeployedConfigCount())

	require.NoError(t, b.DeleteConfigurationProfile(appID, profileID))
	assert.Equal(t, 0, b.DeployedConfigCount(), "deployedConfigs must not leak after DeleteConfigurationProfile")
}

// TestBackend_ExtensionAssociation_CascadeDeleteOnApplication verifies that
// deleting an application removes extension associations targeting the
// application, its environments, and its configuration profiles -- not
// just the resources themselves.
func TestBackend_ExtensionAssociation_CascadeDeleteOnApplication(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	app, err := b.CreateApplication("cascade-assoc-app", "", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "cascade-assoc-env", "", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "cascade-assoc-profile", "", "hosted", "AWS.Freeform", "", nil,
		nil,
	)
	require.NoError(t, err)

	ext, err := b.CreateExtension("cascade-assoc-ext", "", nil, nil, nil)
	require.NoError(t, err)

	appArn := "arn:aws:appconfig:us-east-1:123456789012:application/" + app.ID
	envArn := appArn + "/environment/" + env.ID
	profileArn := appArn + "/configurationprofile/" + profile.ID

	for _, resourceArn := range []string{appArn, envArn, profileArn} {
		_, assocErr := b.CreateExtensionAssociation(ext.ID, resourceArn, nil, nil, nil)
		require.NoError(t, assocErr)
	}

	require.Equal(t, 3, b.ExtensionAssociationCount())

	require.NoError(t, b.DeleteApplication(app.ID))
	assert.Equal(t, 0, b.ExtensionAssociationCount(),
		"associations targeting the app/env/profile must not survive DeleteApplication")
}
