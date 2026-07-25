package appconfig_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestBackend_GetDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetDeployment("app-1", "env-1", 1)
	require.Error(t, err)
}

func TestBackend_StopDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.StopDeployment("app-1", "env-1", 1, false)
	require.Error(t, err)
}

// TestBackend_StartDeployment_ZeroDurationCompletesSynchronously verifies
// that a strategy with no duration and no bake time (e.g. real AWS's
// AppConfig.AllAtOnce) completes immediately, with no growth curve to poll
// for.
func TestBackend_StartDeployment_ZeroDurationCompletesSynchronously(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID, strategyID := seedDeployableConfig(t, b, []byte(`{}`))

	dep, err := b.StartDeployment(appID, envID, profileID, strategyID, "1", "")
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", dep.State)
	assert.InDelta(t, float32(100), dep.PercentageComplete, 0.001)
}

// TestBackend_StartDeployment_ProgressesThroughGrowthAndBake verifies the
// real DEPLOYING -> BAKING -> COMPLETE state machine: a non-zero-duration,
// non-zero-bake strategy must NOT complete synchronously, must eventually
// reach COMPLETE, and must record its event history most-recent-event-first
// (matching real AWS's EventLog ordering) including both a
// DEPLOYMENT_STARTED and DEPLOYMENT_COMPLETED event.
func TestBackend_StartDeployment_ProgressesThroughGrowthAndBake(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	app, err := b.CreateApplication("progress-app", "")
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "progress-env", "", nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "progress-profile", "", "hosted", "AWS.Freeform", "", nil,
	)
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(app.ID, profile.ID, "application/json", "", "", []byte(`{}`), nil)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("progress-strat", "", 10, 5, 10, "LINEAR", "NONE")
	require.NoError(t, err)

	dep, err := b.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "1", "")
	require.NoError(t, err)
	assert.Equal(t, "DEPLOYING", dep.State, "a non-zero-duration strategy must not complete synchronously")
	require.Len(t, dep.EventLog, 1)
	assert.Equal(t, "DEPLOYMENT_STARTED", dep.EventLog[0].EventType)

	deadline := time.Now().Add(2 * time.Second)

	var final *appconfig.Deployment

	for time.Now().Before(deadline) {
		final, err = b.GetDeployment(app.ID, env.ID, dep.DeploymentNumber)
		require.NoError(t, err)

		if final.State == "COMPLETE" {
			break
		}

		time.Sleep(time.Millisecond)
	}

	require.NotNil(t, final)
	assert.Equal(t, "COMPLETE", final.State)
	assert.InDelta(t, float32(100), final.PercentageComplete, 0.001)
	assert.Equal(
		t, "DEPLOYMENT_COMPLETED", final.EventLog[0].EventType,
		"EventLog must be ordered most-recent-first",
	)

	var sawStarted bool

	for _, e := range final.EventLog {
		if e.EventType == "DEPLOYMENT_STARTED" {
			sawStarted = true
		}
	}

	assert.True(t, sawStarted, "the original DEPLOYMENT_STARTED event must be preserved in history")
}

// TestBackend_StartDeployment_UnknownHostedVersion_NotFound verifies the
// PARITY.md gap fix: StartDeployment for an AppConfig-hosted profile
// ("hosted" LocationUri) must validate ConfigurationVersion against an
// actual HostedConfigurationVersion, returning ResourceNotFoundException
// for an unknown version rather than silently accepting it.
func TestBackend_StartDeployment_UnknownHostedVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	app, err := b.CreateApplication("unknown-ver-app", "")
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "unknown-ver-env", "", nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "unknown-ver-profile", "", "hosted", "AWS.Freeform", "", nil,
	)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("unknown-ver-strat", "", 0, 0, 100, "LINEAR", "NONE")
	require.NoError(t, err)

	// No HostedConfigurationVersion was ever created for this profile.
	_, err = b.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "1", "")
	require.Error(t, err)
}

// TestBackend_StartDeployment_NonHostedProfile_SkipsVersionValidation
// verifies that a profile whose LocationUri is not "hosted" (SSM Parameter
// Store, S3, ...) is not validated against this backend's hosted-version
// store, since it has no way to check the real external source.
func TestBackend_StartDeployment_NonHostedProfile_SkipsVersionValidation(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	app, err := b.CreateApplication("ssm-app", "")
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "ssm-env", "", nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "ssm-profile", "", "ssm-parameter://my-param", "AWS.Freeform", "", nil,
	)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("ssm-strat", "", 0, 0, 100, "LINEAR", "NONE")
	require.NoError(t, err)

	_, err = b.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "1", "")
	require.NoError(t, err, "non-hosted profiles must not be validated against hostedConfigVersions")
}

// TestBackend_StopDeployment_AllowRevert_RevertsToPreviousVersion verifies
// that stopping a COMPLETE deployment with allowRevert=true moves it to
// REVERTED and restores the environment's deployed configuration to the
// previous COMPLETE deployment's version, matching real
// StopDeploymentInput.AllowRevert semantics.
func TestBackend_StopDeployment_AllowRevert_RevertsToPreviousVersion(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	app, err := b.CreateApplication("revert-app", "")
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "revert-env", "", nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "revert-profile", "", "hosted", "AWS.Freeform", "", nil,
	)
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(app.ID, profile.ID, "application/json", "", "", []byte(`{"v":1}`), nil)
	require.NoError(t, err)
	_, err = b.CreateHostedConfigurationVersion(app.ID, profile.ID, "application/json", "", "", []byte(`{"v":2}`), nil)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("revert-strat", "", 0, 0, 100, "LINEAR", "NONE")
	require.NoError(t, err)

	_, err = b.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "1", "")
	require.NoError(t, err)

	dep2, err := b.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "2", "")
	require.NoError(t, err)
	require.Equal(t, "COMPLETE", dep2.State)

	got, err := b.GetConfiguration(app.ID, env.ID, profile.ID)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"v":2}`), got.Content)

	require.NoError(t, b.StopDeployment(app.ID, env.ID, dep2.DeploymentNumber, true))

	reverted, err := b.GetDeployment(app.ID, env.ID, dep2.DeploymentNumber)
	require.NoError(t, err)
	assert.Equal(t, "REVERTED", reverted.State)

	got, err = b.GetConfiguration(app.ID, env.ID, profile.ID)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"v":1}`), got.Content, "must revert to the previous COMPLETE deployment's version")
}

// TestBackend_StopDeployment_CompleteWithoutAllowRevert_Rejected verifies
// that a COMPLETE deployment can only be stopped via AllowRevert -- calling
// StopDeployment on it without AllowRevert must fail rather than silently
// rolling back an already-finished deployment.
func TestBackend_StopDeployment_CompleteWithoutAllowRevert_Rejected(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID, strategyID := seedDeployableConfig(t, b, []byte(`{}`))

	dep, err := b.StartDeployment(appID, envID, profileID, strategyID, "1", "")
	require.NoError(t, err)
	require.Equal(t, "COMPLETE", dep.State)

	err = b.StopDeployment(appID, envID, dep.DeploymentNumber, false)
	require.Error(t, err)
}
