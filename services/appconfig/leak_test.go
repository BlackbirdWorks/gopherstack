package appconfig_test

import (
	"testing"

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
				app, err := b.CreateApplication(name, "", nil)
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

	_, err := b.CreateApplication("my-app", "first", nil)
	require.NoError(t, err)

	_, err = b.CreateApplication("my-app", "duplicate", nil)
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
				_, err := b.CreateExtension(name, "", nil, nil, nil)
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
				s, err := b.CreateDeploymentStrategy(name, "", 10, 0, 10.0, "LINEAR", "NONE", nil)
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

// TestDeploymentTimers_DrainToZero lives in whitebox_test.go: it needs direct
// access to the unexported deploymentTimers map.
