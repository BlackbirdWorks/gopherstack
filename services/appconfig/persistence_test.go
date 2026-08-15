package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateApplication("seed-app", "", nil)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	items, _ := b.ListApplications("", 0)
	assert.Empty(t, items)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches appconfigSnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 on-disk shape (AppConfig had no persistence at all
// before this conversion, so there is no legacy shape to actually collide
// with -- any input lacking "version" hits this same path).
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateApplication("seed-app", "", nil)
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"applications":{}}`))
	require.NoError(t, err)

	items, _ := b.ListApplications("", 0)
	assert.Empty(t, items)
}

// seedState is every resource created by seedFullState, kept together so the
// post-restore assertions in TestInMemoryBackend_SnapshotRestore_FullState
// can refer back to the original IDs.
type seedState struct {
	app        *appconfig.Application
	env        *appconfig.Environment
	profile    *appconfig.ConfigurationProfile
	hcv        *appconfig.HostedConfigurationVersion
	labeledHCV *appconfig.HostedConfigurationVersion
	strategy   *appconfig.DeploymentStrategy
	deployment *appconfig.Deployment
	ext        *appconfig.Extension
	assoc      *appconfig.ExtensionAssociation
	// expProfile is a second, feature-flag-typed ConfigurationProfile (the
	// seeded profile above is AWS.Freeform, which CreateExperimentDefinition
	// rejects) -- see experiment_definitions.go's featureFlagsProfileType
	// check.
	expProfile *appconfig.ConfigurationProfile
	expDef     *appconfig.ExperimentDefinition
	expRun     *appconfig.ExperimentRun
}

// seedFullState populates one instance of every store.Table-backed resource
// family the Phase 3.3 conversion touched, plus the tagsByArn/versionCounters
// /deploymentCounters raw maps and accountSettings, so
// TestInMemoryBackend_SnapshotRestore_FullState can exercise a Snapshot ->
// Restore round trip across all of them.
func seedFullState(t *testing.T, b *appconfig.InMemoryBackend) seedState {
	t.Helper()

	app, err := b.CreateApplication("app-1", "an application", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "env-1", "an environment", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "profile-1", "a profile", "hosted", "AWS.Freeform", "", "", nil,
		nil,
	)
	require.NoError(t, err)

	hcv, err := b.CreateHostedConfigurationVersion(
		app.ID, profile.ID, "application/json", "v1", "", []byte(`{"flag":true}`), nil,
	)
	require.NoError(t, err)

	// A second, labeled version exercises the byLabel index round trip and
	// proves the versionCounters map survives (this must land as version 2,
	// not collide with a reset counter).
	labeledHCV, err := b.CreateHostedConfigurationVersion(
		app.ID, profile.ID, "application/json", "v2", "release-1", []byte(`{"flag":false}`), nil,
	)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("strategy-1", "a strategy", 10, 0, 100, "LINEAR", "NONE", nil)
	require.NoError(t, err)

	deployment, err := b.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "1", "a deployment")
	require.NoError(t, err)

	ext, err := b.CreateExtension("ext-1", "an extension", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.CreateExtensionAssociation(ext.ID, app.ID, nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.TagResource(assoc.Arn, map[string]string{"team": "core"}))

	enabled := true
	_, err = b.UpdateAccountSettings(&appconfig.DeletionProtectionSettings{Enabled: &enabled}, nil)
	require.NoError(t, err)

	expProfile, err := b.CreateConfigurationProfile(
		app.ID, "flag-profile-1", "a feature flag profile", "hosted", "AWS.AppConfig.FeatureFlags", "", "", nil,
		nil,
	)
	require.NoError(t, err)

	expDef, err := b.CreateExperimentDefinition(
		app.ID, "exp-def-1", env.ID, expProfile.ID, "flag1", "true", "everyone", "", "",
		&appconfig.Treatment{FlagValue: &appconfig.FlagValue{Enabled: false}, Weight: 50},
		[]appconfig.Treatment{{FlagValue: &appconfig.FlagValue{Enabled: true}, Weight: 50}},
		nil,
	)
	require.NoError(t, err)

	exposure := float32(10)
	expRun, err := b.StartExperimentRun(app.ID, expDef.ID, "a run", &exposure, nil, nil)
	require.NoError(t, err)

	return seedState{
		app:        app,
		env:        env,
		profile:    profile,
		hcv:        hcv,
		labeledHCV: labeledHCV,
		strategy:   strategy,
		deployment: deployment,
		ext:        ext,
		assoc:      assoc,
		expProfile: expProfile,
		expDef:     expDef,
		expRun:     expRun,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot ->
// Restore round trip across every store.Table-backed resource family the
// Phase 3.3 conversion touched -- applications, environments (byApp/
// byAppName composite indexes), configProfiles (byApp/byAppName), hosted
// config versions (composite key, byProfile/byApp/byLabel indexes),
// deployment strategies, deployments (composite key, byEnv/byApp indexes),
// extensions, and extension associations -- plus the raw-left tags,
// versionCounters, deploymentCounters, and accountSettings state.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := appconfig.NewInMemoryBackend("111122223333", "us-west-2")
	seed := seedFullState(t, original)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assertApplicationFamilyRestored(t, fresh, seed)
	assertHostedConfigVersionsRestored(t, fresh, seed)
	assertDeploymentFamilyRestored(t, fresh, seed)
	assertExtensionFamilyRestored(t, fresh, seed)
	assertExperimentFamilyRestored(t, fresh, seed)
	assertRawStateRestored(t, fresh, seed)
	// Run last: deletes the seeded application, so nothing else may depend
	// on app/env/profile/hcv/deployment state after this point.
	assertApplicationCascadeDeleteRestored(t, fresh, seed)
}

// assertApplicationFamilyRestored checks applications, environments, and
// configProfiles -- including that the byAppName indexes were rebuilt (a
// second Create with the same name is rejected) and byApp indexes still
// answer List correctly.
func assertApplicationFamilyRestored(t *testing.T, fresh *appconfig.InMemoryBackend, seed seedState) {
	t.Helper()

	gotApp, err := fresh.GetApplication(seed.app.ID)
	require.NoError(t, err)
	assert.Equal(t, seed.app.Name, gotApp.Name)

	// Name-uniqueness index was rebuilt, not left stale.
	_, err = fresh.CreateApplication(seed.app.Name, "duplicate", nil)
	require.Error(t, err)

	gotEnv, err := fresh.GetEnvironment(seed.app.ID, seed.env.ID)
	require.NoError(t, err)
	assert.Equal(t, seed.env.Name, gotEnv.Name)

	envItems, _, err := fresh.ListEnvironments(seed.app.ID, "", 0)
	require.NoError(t, err)
	assert.Len(t, envItems, 1)

	_, err = fresh.CreateEnvironment(seed.app.ID, seed.env.Name, "duplicate", nil, nil)
	require.Error(t, err)

	gotProfile, err := fresh.GetConfigurationProfile(seed.app.ID, seed.profile.ID)
	require.NoError(t, err)
	assert.Equal(t, seed.profile.Name, gotProfile.Name)

	profileItems, _, err := fresh.ListConfigurationProfiles(seed.app.ID, "", 0)
	require.NoError(t, err)
	assert.Len(t, profileItems, 2, "seedFullState creates the freeform profile plus a feature-flag profile")
}

// assertApplicationCascadeDeleteRestored checks that DeleteApplication's
// cascade delete (environmentsByApp/configProfilesByApp/
// hostedConfigVersionsByApp/deploymentsByApp indexes) still works
// post-restore -- proving those indexes were rebuilt, not left stale. It
// deletes the seeded application, so it must run after every other
// assertion that depends on app/env/profile/hcv/deployment state.
func assertApplicationCascadeDeleteRestored(t *testing.T, fresh *appconfig.InMemoryBackend, seed seedState) {
	t.Helper()

	require.NoError(t, fresh.DeleteApplication(seed.app.ID))
	_, err := fresh.GetEnvironment(seed.app.ID, seed.env.ID)
	require.Error(t, err)
	_, err = fresh.GetConfigurationProfile(seed.app.ID, seed.profile.ID)
	require.Error(t, err)
	_, err = fresh.GetHostedConfigurationVersion(seed.app.ID, seed.profile.ID, seed.hcv.VersionNumber)
	require.Error(t, err)
	_, err = fresh.GetDeployment(seed.app.ID, seed.env.ID, seed.deployment.DeploymentNumber)
	require.Error(t, err)
	_, err = fresh.GetExperimentDefinition(seed.app.ID, seed.expDef.ID)
	require.Error(t, err, "DeleteApplication must cascade-delete experiment definitions too")
}

// assertHostedConfigVersionsRestored checks the composite-keyed
// hostedConfigVersions table plus its byProfile and byLabel indexes,
// including that versionCounters survived (a version created post-restore
// must not collide with the two seeded versions).
func assertHostedConfigVersionsRestored(t *testing.T, fresh *appconfig.InMemoryBackend, seed seedState) {
	t.Helper()

	gotHCV, err := fresh.GetHostedConfigurationVersion(seed.app.ID, seed.profile.ID, seed.hcv.VersionNumber)
	require.NoError(t, err)
	assert.Equal(t, seed.hcv.Description, gotHCV.Description)

	versionItems, _, err := fresh.ListHostedConfigurationVersions(seed.app.ID, seed.profile.ID, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, versionItems, 2)

	// byLabel index was rebuilt: the same label is still rejected as a
	// duplicate on the restored profile.
	_, err = fresh.CreateHostedConfigurationVersion(
		seed.app.ID, seed.profile.ID, "application/json", "dup", seed.labeledHCV.VersionLabel, []byte(`{}`), nil,
	)
	require.Error(t, err)

	// versionCounters survived: the next created version must be 3, not a
	// reset-to-1 collision with the seeded versions.
	newHCV, err := fresh.CreateHostedConfigurationVersion(
		seed.app.ID, seed.profile.ID, "application/json", "v3", "", []byte(`{}`), nil,
	)
	require.NoError(t, err)
	assert.Equal(t, seed.labeledHCV.VersionNumber+1, newHCV.VersionNumber)
}

// assertDeploymentFamilyRestored checks deployment strategies and the
// composite-keyed deployments table plus its byEnv index, including that
// deploymentCounters survived.
func assertDeploymentFamilyRestored(t *testing.T, fresh *appconfig.InMemoryBackend, seed seedState) {
	t.Helper()

	gotStrategy, err := fresh.GetDeploymentStrategy(seed.strategy.ID)
	require.NoError(t, err)
	assert.Equal(t, seed.strategy.Name, gotStrategy.Name)

	_, err = fresh.CreateDeploymentStrategy(seed.strategy.Name, "duplicate", 10, 0, 100, "LINEAR", "NONE", nil)
	require.Error(t, err)

	gotDeployment, err := fresh.GetDeployment(seed.app.ID, seed.env.ID, seed.deployment.DeploymentNumber)
	require.NoError(t, err)
	assert.Equal(t, seed.deployment.ConfigurationVersion, gotDeployment.ConfigurationVersion)

	deploymentItems, _, err := fresh.ListDeployments(seed.app.ID, seed.env.ID, "", 0)
	require.NoError(t, err)
	assert.Len(t, deploymentItems, 1)

	// deploymentCounters survived: the next deployment must be number 2.
	newDeployment, err := fresh.StartDeployment(
		seed.app.ID, seed.env.ID, seed.profile.ID, seed.strategy.ID, "1", "second deployment",
	)
	require.NoError(t, err)
	assert.Equal(t, seed.deployment.DeploymentNumber+1, newDeployment.DeploymentNumber)
}

// assertExtensionFamilyRestored checks extensions and extension
// associations, including that the extensionsByName index was rebuilt.
func assertExtensionFamilyRestored(t *testing.T, fresh *appconfig.InMemoryBackend, seed seedState) {
	t.Helper()

	gotExt, err := fresh.GetExtension(seed.ext.ID, 0)
	require.NoError(t, err)
	assert.Equal(t, seed.ext.Name, gotExt.Name)

	_, err = fresh.CreateExtension(seed.ext.Name, "duplicate", nil, nil, nil)
	require.Error(t, err)

	gotAssoc, err := fresh.GetExtensionAssociation(seed.assoc.ID)
	require.NoError(t, err)
	assert.Equal(t, seed.assoc.ResourceArn, gotAssoc.ResourceArn)

	assocItems, _ := fresh.ListExtensionAssociations("", "", "", 0)
	assert.Len(t, assocItems, 1)
}

// assertExperimentFamilyRestored checks the experiment definition and run
// families, including that experimentDefinitionsByAppName was rebuilt (a
// second Create with the same name is rejected), experimentRunCounters
// survived (a run started post-restore does not collide with the seeded
// run number), and the run's recorded event history (a plain map, not
// store.Table-backed) round-tripped.
func assertExperimentFamilyRestored(t *testing.T, fresh *appconfig.InMemoryBackend, seed seedState) {
	t.Helper()

	gotDef, err := fresh.GetExperimentDefinition(seed.app.ID, seed.expDef.ID)
	require.NoError(t, err)
	assert.Equal(t, seed.expDef.Name, gotDef.Name)
	assert.Equal(t, "ACTIVE", gotDef.Status, "the seeded run is still RUNNING, so the definition is ACTIVE")

	_, err = fresh.CreateExperimentDefinition(
		seed.app.ID, seed.expDef.Name, seed.env.ID, seed.expProfile.ID, "flag1", "true", "", "", "",
		&appconfig.Treatment{FlagValue: &appconfig.FlagValue{Enabled: false}, Weight: 100},
		[]appconfig.Treatment{{FlagValue: &appconfig.FlagValue{Enabled: true}, Weight: 100}},
		nil,
	)
	require.Error(t, err, "byAppName index must have been rebuilt")

	gotRun, err := fresh.GetExperimentRun(seed.app.ID, seed.expDef.ID, seed.expRun.Run)
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", gotRun.Status)

	events, _, err := fresh.ListExperimentRunEvents(seed.app.ID, seed.expDef.ID, seed.expRun.Run, "", 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "RUN_STARTED", events[0].EventType)

	// experimentRunCounters survived: since the seeded run is still
	// RUNNING, starting a second run is rejected (StartExperimentRun
	// allows only one RUNNING run per definition) -- stop it first, then
	// verify the next run number is 2, not a reset-to-1 collision.
	_, err = fresh.StopExperimentRun(seed.app.ID, seed.expDef.ID, seed.expRun.Run, nil)
	require.NoError(t, err)

	newRun, err := fresh.StartExperimentRun(seed.app.ID, seed.expDef.ID, "second run", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, seed.expRun.Run+1, newRun.Run)
}

// assertRawStateRestored checks the plain tags map (populated via
// TagResource) and accountSettings.
func assertRawStateRestored(t *testing.T, fresh *appconfig.InMemoryBackend, seed seedState) {
	t.Helper()

	tags, err := fresh.ListTagsForResource(seed.assoc.Arn)
	require.NoError(t, err)
	assert.Equal(t, "core", tags["team"])

	settings, err := fresh.GetAccountSettings()
	require.NoError(t, err)
	require.NotNil(t, settings.DeletionProtection)
	require.NotNil(t, settings.DeletionProtection.Enabled)
	assert.True(t, *settings.DeletionProtection.Enabled)
}
