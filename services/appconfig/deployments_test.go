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

	app, err := b.CreateApplication("progress-app", "", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "progress-env", "", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "progress-profile", "", "hosted", "AWS.Freeform", "", nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(app.ID, profile.ID, "application/json", "", "", []byte(`{}`), nil)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("progress-strat", "", 10, 5, 10, "LINEAR", "NONE", nil)
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

	app, err := b.CreateApplication("unknown-ver-app", "", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "unknown-ver-env", "", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "unknown-ver-profile", "", "hosted", "AWS.Freeform", "", nil,
		nil,
	)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("unknown-ver-strat", "", 0, 0, 100, "LINEAR", "NONE", nil)
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

	app, err := b.CreateApplication("ssm-app", "", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "ssm-env", "", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "ssm-profile", "", "ssm-parameter://my-param", "AWS.Freeform", "", nil,
		nil,
	)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("ssm-strat", "", 0, 0, 100, "LINEAR", "NONE", nil)
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

	app, err := b.CreateApplication("revert-app", "", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "revert-env", "", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "revert-profile", "", "hosted", "AWS.Freeform", "", nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(app.ID, profile.ID, "application/json", "", "", []byte(`{"v":1}`), nil)
	require.NoError(t, err)
	_, err = b.CreateHostedConfigurationVersion(app.ID, profile.ID, "application/json", "", "", []byte(`{"v":2}`), nil)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("revert-strat", "", 0, 0, 100, "LINEAR", "NONE", nil)
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

// TestBackend_ExperimentRun_StartValidation covers StartExperimentRun's
// state-machine preconditions: exposure defaults/bounds, at most one
// RUNNING run per definition, and rejecting a run against an ARCHIVED
// definition.
func TestBackend_ExperimentRun_StartValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		exposure   *float32
		setup      func(t *testing.T, b *appconfig.InMemoryBackend, appID, defID string)
		name       string
		wantErr    bool
		wantExpose float32
	}{
		{
			name:       "nil exposure defaults to zero",
			exposure:   nil,
			wantExpose: 0,
		},
		{
			name:       "explicit exposure honored",
			exposure:   new(float32(42.5)),
			wantExpose: 42.5,
		},
		{
			name:     "exposure above 100 rejected",
			exposure: new(float32(150)),
			wantErr:  true,
		},
		{
			name:     "negative exposure rejected",
			exposure: new(float32(-1)),
			wantErr:  true,
		},
		{
			name: "second concurrent run rejected",
			setup: func(t *testing.T, b *appconfig.InMemoryBackend, appID, defID string) {
				t.Helper()

				_, err := b.StartExperimentRun(appID, defID, "", nil, nil, nil)
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			name: "archived definition rejected",
			setup: func(t *testing.T, b *appconfig.InMemoryBackend, appID, defID string) {
				t.Helper()

				require.NoError(t, b.DeleteExperimentDefinition(appID, defID, "ARCHIVE"))
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
			appID, envID, profileID := seedExperimentApp(t, b)

			def, err := b.CreateExperimentDefinition(
				appID, "run-def", envID, profileID, "flag1", "true", "", "", "",
				experimentTreatment(false, 100), []appconfig.Treatment{*experimentTreatment(true, 100)},
				nil,
			)
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(t, b, appID, def.ID)
			}

			run, err := b.StartExperimentRun(appID, def.ID, "", tc.exposure, nil, nil)

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "RUNNING", run.Status)
			assert.InDelta(t, tc.wantExpose, run.ExposurePercentage, 0.001)
			assert.NotNil(t, run.ExperimentDefinitionSnapshot)
			assert.Equal(t, def.Name, run.ExperimentDefinitionSnapshot.Name)

			gotDef, err := b.GetExperimentDefinition(appID, def.ID)
			require.NoError(t, err)
			assert.Equal(t, "ACTIVE", gotDef.Status, "starting a run must move the definition to ACTIVE")
		})
	}
}

// TestBackend_ExperimentRun_StopAndEvents verifies StopExperimentRun's
// state transition (RUNNING -> DONE, definition reverts to IDLE) and that
// ListExperimentRunEvents returns exactly the events this backend actually
// recorded (RUN_STARTED then RUN_STOPPED, most-recent-first) rather than a
// fabricated timeline.
func TestBackend_ExperimentRun_StopAndEvents(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID := seedExperimentApp(t, b)

	def, err := b.CreateExperimentDefinition(
		appID, "stop-def", envID, profileID, "flag1", "true", "", "", "",
		experimentTreatment(false, 100), []appconfig.Treatment{*experimentTreatment(true, 100)},
		nil,
	)
	require.NoError(t, err)

	run, err := b.StartExperimentRun(appID, def.ID, "", new(float32(20)), nil, nil)
	require.NoError(t, err)

	// Stopping an already-DONE run must fail.
	_, err = b.StopExperimentRun(appID, def.ID, run.Run, nil)
	require.NoError(t, err)

	_, err = b.StopExperimentRun(appID, def.ID, run.Run, nil)
	require.Error(t, err, "cannot stop a run that is already DONE")

	stopped, err := b.GetExperimentRun(appID, def.ID, run.Run)
	require.NoError(t, err)
	assert.Equal(t, "DONE", stopped.Status)
	assert.False(t, stopped.EndedAt.IsZero())

	gotDef, err := b.GetExperimentDefinition(appID, def.ID)
	require.NoError(t, err)
	assert.Equal(t, "IDLE", gotDef.Status, "stopping the only running run must revert the definition to IDLE")

	events, _, err := b.ListExperimentRunEvents(appID, def.ID, run.Run, "", 0)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, "RUN_STOPPED", events[0].EventType, "most-recent-first ordering")
	assert.Equal(t, "RUN_STARTED", events[1].EventType)
}

// TestBackend_ExperimentRun_Update verifies UpdateExperimentRun's
// RUNNING-only precondition, that ExposurePercentage can only increase, and
// that a genuine exposure/override change is recorded as an event.
func TestBackend_ExperimentRun_Update(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID := seedExperimentApp(t, b)

	def, err := b.CreateExperimentDefinition(
		appID, "update-def", envID, profileID, "flag1", "true", "", "", "",
		experimentTreatment(false, 100), []appconfig.Treatment{*experimentTreatment(true, 100)},
		nil,
	)
	require.NoError(t, err)

	run, err := b.StartExperimentRun(appID, def.ID, "", new(float32(10)), nil, nil)
	require.NoError(t, err)

	// Decreasing exposure must be rejected.
	_, err = b.UpdateExperimentRun(appID, def.ID, run.Run, nil, new(float32(5)), nil)
	require.Error(t, err)

	updated, err := b.UpdateExperimentRun(appID, def.ID, run.Run, nil, new(float32(50)), nil)
	require.NoError(t, err)
	assert.InDelta(t, float32(50), updated.ExposurePercentage, 0.001)

	overrides := &appconfig.TreatmentOverrides{Inline: map[string]string{"user-1": "Treatment1"}}
	updated, err = b.UpdateExperimentRun(appID, def.ID, run.Run, nil, nil, overrides)
	require.NoError(t, err)
	assert.Equal(t, "Treatment1", updated.TreatmentOverrides.Inline["user-1"])

	events, _, err := b.ListExperimentRunEvents(appID, def.ID, run.Run, "", 0)
	require.NoError(t, err)
	require.Len(t, events, 3, "RUN_STARTED, EXPOSURE_UPDATED, OVERRIDES_UPDATED")
	assert.Equal(t, "OVERRIDES_UPDATED", events[0].EventType)
	assert.Equal(t, "EXPOSURE_UPDATED", events[1].EventType)

	_, err = b.StopExperimentRun(appID, def.ID, run.Run, nil)
	require.NoError(t, err)

	_, err = b.UpdateExperimentRun(appID, def.ID, run.Run, nil, new(float32(60)), nil)
	require.Error(t, err, "cannot update a run that is not RUNNING")
}

// TestBackend_ExperimentRun_ListFilteredByStatus verifies ListExperimentRuns'
// status filter across multiple runs of the same definition.
func TestBackend_ExperimentRun_ListFilteredByStatus(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID := seedExperimentApp(t, b)

	def, err := b.CreateExperimentDefinition(
		appID, "list-def", envID, profileID, "flag1", "true", "", "", "",
		experimentTreatment(false, 100), []appconfig.Treatment{*experimentTreatment(true, 100)},
		nil,
	)
	require.NoError(t, err)

	run1, err := b.StartExperimentRun(appID, def.ID, "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.StopExperimentRun(appID, def.ID, run1.Run, nil)
	require.NoError(t, err)

	run2, err := b.StartExperimentRun(appID, def.ID, "", nil, nil, nil)
	require.NoError(t, err)

	all, _, err := b.ListExperimentRuns(appID, def.ID, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, all, 2)

	running, _, err := b.ListExperimentRuns(appID, def.ID, "RUNNING", "", 0)
	require.NoError(t, err)
	require.Len(t, running, 1)
	assert.Equal(t, run2.Run, running[0].Run)

	done, _, err := b.ListExperimentRuns(appID, def.ID, "DONE", "", 0)
	require.NoError(t, err)
	require.Len(t, done, 1)
	assert.Equal(t, run1.Run, done[0].Run)
}
