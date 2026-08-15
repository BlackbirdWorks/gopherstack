package appconfig_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// TestDeployedConfigurationPublisher_SatisfiedByRealAppConfigDataBackend
// locks the cross-service contract the appconfig -> appconfigdata bridge (bd
// gopherstack-uiyi) depends on: appconfig's DeployedConfigurationPublisher
// interface is structurally satisfied by appconfigdata's own
// InMemoryBackend.PublishConfiguration, with no adapter needed -- the same
// no-adapter pairing as cloudwatch's FirehosePutter/firehose.InMemoryBackend.
func TestDeployedConfigurationPublisher_SatisfiedByRealAppConfigDataBackend(t *testing.T) {
	t.Parallel()

	var _ appconfig.DeployedConfigurationPublisher = (*appconfigdata.InMemoryBackend)(nil)
}

// bridgeFixture wires a real AppConfig backend to a real AppConfigData
// backend -- the same SetDeployedConfigurationPublisher call cli.go's
// wireAppConfigDeployments makes -- around one hosted configuration profile
// ready to deploy.
type bridgeFixture struct {
	ac        *appconfig.InMemoryBackend
	acd       *appconfigdata.InMemoryBackend
	appID     string
	envID     string
	profileID string
}

func newBridgeFixture(t *testing.T) *bridgeFixture {
	t.Helper()

	ac := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	acd := appconfigdata.NewInMemoryBackend()
	ac.SetDeployedConfigurationPublisher(acd)

	app, err := ac.CreateApplication("bridge-app", "", nil)
	require.NoError(t, err)

	env, err := ac.CreateEnvironment(app.ID, "bridge-env", "", nil, nil)
	require.NoError(t, err)

	profile, err := ac.CreateConfigurationProfile(
		app.ID, "bridge-profile", "", "hosted", "AWS.Freeform", "", "", nil, nil,
	)
	require.NoError(t, err)

	return &bridgeFixture{ac: ac, acd: acd, appID: app.ID, envID: env.ID, profileID: profile.ID}
}

// deployHostedContent creates a new hosted configuration version and starts
// a deployment of it under a fresh deployment strategy.
func (f *bridgeFixture) deployHostedContent(
	t *testing.T, content []byte, strategyName string, duration, bake int32, growthFactor float32,
) *appconfig.Deployment {
	t.Helper()

	hcv, err := f.ac.CreateHostedConfigurationVersion(f.appID, f.profileID, "application/json", "", "", content, nil)
	require.NoError(t, err)

	strategy, err := f.ac.CreateDeploymentStrategy(
		strategyName, "", duration, bake, growthFactor, "LINEAR", "NONE", nil,
	)
	require.NoError(t, err)

	dep, err := f.ac.StartDeployment(
		f.appID, f.envID, f.profileID, strategy.ID,
		strconv.FormatInt(int64(hcv.VersionNumber), 10), "",
	)
	require.NoError(t, err)

	return dep
}

// pollLatestConfiguration starts a real AppConfigData configuration session
// and takes one GetLatestConfiguration poll against it.
func (f *bridgeFixture) pollLatestConfiguration(t *testing.T) ([]byte, string, string) {
	t.Helper()

	token, err := f.acd.StartSession(f.appID, f.envID, f.profileID, 0)
	require.NoError(t, err)

	content, contentType, _, _, versionLabel, err := f.acd.GetLatestConfiguration(token)
	require.NoError(t, err)

	return content, contentType, versionLabel
}

// deploymentIDFor returns the DeploymentID AppConfigData recorded for the
// fixture's profile, or "" if the bridge has never published to it.
func (f *bridgeFixture) deploymentIDFor() string {
	for _, p := range f.acd.ListProfiles() {
		if p.ApplicationIdentifier == f.appID && p.EnvironmentIdentifier == f.envID &&
			p.ConfigurationProfileIdentifier == f.profileID {
			return p.DeploymentID
		}
	}

	return ""
}

// TestAppConfigDeploymentBridge_EndToEnd proves the full bridge path bd
// gopherstack-uiyi asked for: a real StartDeployment against the AppConfig
// control plane becomes observable through a real StartConfigurationSession
// + GetLatestConfiguration poll against AppConfigData, with DeploymentId
// populated.
func TestAppConfigDeploymentBridge_EndToEnd(t *testing.T) {
	t.Parallel()

	f := newBridgeFixture(t)
	content := []byte(`{"featureFlag":true}`)

	// Zero-duration, zero-bake strategy completes synchronously (real AWS
	// AppConfig.AllAtOnce shape) -- no need to poll for COMPLETE first.
	dep := f.deployHostedContent(t, content, "all-at-once", 0, 0, 100)
	require.Equal(t, "COMPLETE", dep.State)

	gotContent, gotContentType, gotVersionLabel := f.pollLatestConfiguration(t)
	assert.Equal(t, content, gotContent)
	assert.Equal(t, "application/json", gotContentType)
	assert.NotEmpty(t, gotVersionLabel)
	assert.Equal(t, strconv.FormatInt(int64(dep.DeploymentNumber), 10), f.deploymentIDFor())
}

// TestAppConfigDeploymentBridge_StateTransitions covers the bridge across
// AppConfig's real deployment-state machine: a growth/bake deployment that
// completes asynchronously, a deployment stopped before it ever completes
// (must never reach AppConfigData), and an AllowRevert stop that republishes
// the prior COMPLETE version.
func TestAppConfigDeploymentBridge_StateTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *bridgeFixture)
		name string
	}{
		{
			name: "growth_and_bake_eventually_publishes",
			run: func(t *testing.T, f *bridgeFixture) {
				t.Helper()

				content := []byte(`{"v":1}`)
				dep := f.deployHostedContent(t, content, "growth-strat", 10, 5, 100)
				require.Equal(t, "DEPLOYING", dep.State, "a non-zero-duration strategy must not complete synchronously")

				require.Eventually(t, func() bool {
					d, err := f.ac.GetDeployment(f.appID, f.envID, dep.DeploymentNumber)

					return err == nil && d.State == "COMPLETE"
				}, 2*time.Second, 10*time.Millisecond, "deployment should reach COMPLETE")

				wantID := strconv.FormatInt(int64(dep.DeploymentNumber), 10)
				require.Eventually(t, func() bool {
					return f.deploymentIDFor() == wantID
				}, 2*time.Second, 10*time.Millisecond, "bridge should publish once the deployment completes")

				gotContent, _, _ := f.pollLatestConfiguration(t)
				assert.Equal(t, content, gotContent)
			},
		},
		{
			name: "stopped_deployment_never_publishes",
			run: func(t *testing.T, f *bridgeFixture) {
				t.Helper()

				dep := f.deployHostedContent(t, []byte(`{"v":1}`), "stoppable-strat", 100, 50, 1)

				_, err := f.ac.StopDeployment(f.appID, f.envID, dep.DeploymentNumber, false)
				require.NoError(t, err)

				final, err := f.ac.GetDeployment(f.appID, f.envID, dep.DeploymentNumber)
				require.NoError(t, err)
				require.Equal(t, "ROLLED_BACK", final.State)

				assert.Empty(t, f.deploymentIDFor(), "a rolled-back deployment must never reach AppConfigData")
			},
		},
		{
			name: "allow_revert_republishes_previous_version",
			run: func(t *testing.T, f *bridgeFixture) {
				t.Helper()

				first := []byte(`{"v":1}`)
				dep1 := f.deployHostedContent(t, first, "revert-strat-1", 0, 0, 100)
				require.Equal(t, "COMPLETE", dep1.State)

				second := []byte(`{"v":2}`)
				dep2 := f.deployHostedContent(t, second, "revert-strat-2", 0, 0, 100)
				require.Equal(t, "COMPLETE", dep2.State)

				_, err := f.ac.StopDeployment(f.appID, f.envID, dep2.DeploymentNumber, true)
				require.NoError(t, err)

				gotContent, _, _ := f.pollLatestConfiguration(t)
				assert.Equal(t, first, gotContent, "revert should republish the prior deployment's content")
				assert.Equal(t, strconv.FormatInt(int64(dep1.DeploymentNumber), 10), f.deploymentIDFor())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newBridgeFixture(t))
		})
	}
}
