package kinesisanalyticsv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// TestDeleteApplication_ReleasesVersionsAndOperations verifies that deleting an
// application also removes its version history and operations entries, preventing
// the versions and operations maps from growing unbounded across the backend lifetime.
func TestDeleteApplication_ReleasesVersionsAndOperations(t *testing.T) {
	t.Parallel()

	const region = "us-east-1"

	tests := []struct {
		name     string
		appNames []string
	}{
		{name: "single app", appNames: []string{"app-0"}},
		{name: "many apps", appNames: []string{"app-0", "app-1", "app-2", "app-3", "app-4"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := kinesisanalyticsv2.NewInMemoryBackend("123456789012", region)
			ctx := context.Background()

			for _, name := range tc.appNames {
				_, err := b.CreateApplication(ctx, name, "SQL-1_0", "arn:aws:iam::123456789012:role/r", "", "", nil)
				require.NoError(t, err)
			}

			require.Equal(t, len(tc.appNames), kinesisanalyticsv2.ApplicationCount(b),
				"all apps must be present before delete")
			require.Equal(t, len(tc.appNames), kinesisanalyticsv2.VersionsMapKeyCount(b, region),
				"versions map must have an entry per app before delete")

			for _, name := range tc.appNames {
				require.NoError(t, b.DeleteApplication(ctx, name, nil))
			}

			require.Equal(t, 0, kinesisanalyticsv2.ApplicationCount(b),
				"applications must be empty after all deletes")
			require.Equal(t, 0, kinesisanalyticsv2.VersionsMapKeyCount(b, region),
				"versions map must be empty after all deletes — no leak")
			require.Equal(t, 0, kinesisanalyticsv2.OperationsMapKeyCount(b, region),
				"operations map must be empty after all deletes — no leak")
		})
	}
}
