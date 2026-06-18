package kinesisanalytics_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

// TestDeleteApplication_ReleasesAppsMap verifies that deleting an application removes
// it from the apps and appsByARN maps after the async transition delay, so the maps
// stay bounded as applications are created and deleted over the backend lifetime.
func TestDeleteApplication_ReleasesAppsMap(t *testing.T) {
	t.Parallel()

	const region = "us-east-1"

	tests := []struct {
		name     string
		appNames []string
	}{
		{name: "single app", appNames: []string{"app-0"}},
		{name: "many apps", appNames: []string{"app-0", "app-1", "app-2"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := kinesisanalytics.NewInMemoryBackend(region, "123456789012")
			ctx := context.WithValue(context.Background(), kinesisanalytics.RegionContextKeyForTest{}, region)

			for _, name := range tc.appNames {
				app, err := kinesisanalytics.CreateApp(b, region, "123456789012", name, "desc", "SELECT 1;", nil)
				require.NoError(t, err)
				require.NotNil(t, app)
			}

			require.Equal(t, len(tc.appNames), kinesisanalytics.ApplicationCount(b),
				"all apps must be present before delete")

			for _, name := range tc.appNames {
				app, err := b.DescribeApplication(ctx, name)
				require.NoError(t, err)
				require.NoError(t, b.DeleteApplication(ctx, name, app.CreateTimestamp))
			}

			// The deletion is async (goroutine fires after transitionDelay=50ms).
			// Wait long enough for all goroutines to complete.
			require.Eventually(t,
				func() bool { return kinesisanalytics.ApplicationCount(b) == 0 },
				500*time.Millisecond, 10*time.Millisecond,
				"apps map must be empty after delete goroutines fire",
			)
			require.Equal(t, 0, kinesisanalytics.GetCancelFuncsLen(b),
				"cancelFuncs must be empty after all goroutines complete")
		})
	}
}
