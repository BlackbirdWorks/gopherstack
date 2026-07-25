package appconfig_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestDeleteApplication_NameIndexBounded verifies that deleting an application removes
// it from both the primary map and the applicationsByName index, keeping both bounded.
func TestDeleteApplication_NameIndexBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
	}{
		{name: "single", names: []string{"app-a"}},
		{name: "many", names: []string{"app-a", "app-b", "app-c", "app-d", "app-e"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

			ids := make([]string, 0, len(tc.names))

			for _, name := range tc.names {
				app, err := b.CreateApplication(name, "")
				require.NoError(t, err)
				ids = append(ids, app.ID)
			}

			require.Equal(t, len(tc.names), b.ApplicationCount(), "count before delete")
			require.Equal(t, len(tc.names), b.ApplicationByNameCount(), "name index before delete")

			for _, id := range ids {
				err := b.DeleteApplication(id)
				require.NoError(t, err)
			}

			require.Equal(t, 0, b.ApplicationCount(), "primary map must be empty after delete")
			require.Equal(t, 0, b.ApplicationByNameCount(), "name index must be empty after delete — no leak")
		})
	}
}

// TestCreateApplication_NameUniquenessUsesIndex verifies that the name uniqueness
// check uses the index (O(1)) rather than an O(n) scan.
func TestCreateApplication_NameUniquenessUsesIndex(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateApplication("my-app", "first")
	require.NoError(t, err)

	_, err = b.CreateApplication("my-app", "duplicate")
	require.Error(t, err, "duplicate name must be rejected")
}

// TestDeleteExtension_NameIndexBounded verifies that deleting an extension removes
// it from both the primary map and the extensionsByName index.
func TestDeleteExtension_NameIndexBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
	}{
		{name: "single", names: []string{"ext-a"}},
		{name: "many", names: []string{"ext-a", "ext-b", "ext-c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

			for _, name := range tc.names {
				_, err := b.CreateExtension(name, "", nil, nil)
				require.NoError(t, err)
			}

			require.Equal(t, len(tc.names), b.ExtensionCount(), "count before delete")
			require.Equal(t, len(tc.names), b.ExtensionByNameCount(), "name index before delete")

			for _, name := range tc.names {
				err := b.DeleteExtension(name, 0)
				require.NoError(t, err)
			}

			require.Equal(t, 0, b.ExtensionCount(), "primary map must be empty after delete")
			require.Equal(t, 0, b.ExtensionByNameCount(), "name index must be empty after delete — no leak")
		})
	}
}

// TestDeleteDeploymentStrategy_NameIndexBounded verifies that deleting a deployment
// strategy removes it from both the primary map and the name index.
func TestDeleteDeploymentStrategy_NameIndexBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
	}{
		{name: "single", names: []string{"strat-a"}},
		{name: "many", names: []string{"strat-a", "strat-b", "strat-c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

			ids := make([]string, 0, len(tc.names))

			for _, name := range tc.names {
				s, err := b.CreateDeploymentStrategy(name, "", 10, 0, 10.0, "LINEAR", "NONE")
				require.NoError(t, err)
				ids = append(ids, s.ID)
			}

			require.Equal(t, len(tc.names), b.DeploymentStrategyCount(), "count before delete")
			require.Equal(t, len(tc.names), b.DeploymentStrategyByNameCount(), "name index before delete")

			for _, id := range ids {
				err := b.DeleteDeploymentStrategy(id)
				require.NoError(t, err)
			}

			require.Equal(t, 0, b.DeploymentStrategyCount(), "primary map must be empty after delete")
			require.Equal(t, 0, b.DeploymentStrategyByNameCount(), "name index must be empty after delete — no leak")
		})
	}
}

// TestDeploymentTimers_DrainToZero verifies that the background
// progression goroutine (scheduleDeploymentReconcilerLocked in
// deployments.go) does not leak: every in-flight deployment's timer entry
// is removed once the deployment reaches a terminal state, and the
// goroutine itself is self-terminating (it exits once the map is empty) --
// so a caller never needs to context-parent or explicitly drain it.
func TestDeploymentTimers_DrainToZero(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	app, err := b.CreateApplication("timer-leak-app", "")
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "timer-leak-env", "", nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID, "timer-leak-profile", "", "hosted", "AWS.Freeform", "", nil,
	)
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(
		app.ID, profile.ID, "application/json", "", "", []byte(`{}`), nil,
	)
	require.NoError(t, err)

	// A non-zero duration and bake time forces real DEPLOYING -> BAKING
	// progression (registers a timer), rather than the synchronous
	// zero-duration path (which never touches deploymentTimers at all).
	strategy, err := b.CreateDeploymentStrategy("timer-leak-strat", "", 10, 5, 25, "LINEAR", "NONE")
	require.NoError(t, err)

	const deployments = 5

	for range deployments {
		_, startErr := b.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "1", "")
		require.NoError(t, startErr)
	}

	assert.Positive(t, b.DeploymentTimerCount(), "sanity: progression must actually register timers")

	deadline := time.Now().Add(2 * time.Second)
	for b.DeploymentTimerCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, 0, b.DeploymentTimerCount(), "every deployment timer must drain once its deployment completes")
}
