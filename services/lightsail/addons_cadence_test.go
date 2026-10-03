package lightsail_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

// TestEnableAddOn_AutoSnapshotCadence proves AutoSnapshot accumulates daily
// entries capped at AWS's documented 7, with disable stopping the cadence.
func TestEnableAddOn_AutoSnapshotCadence(t *testing.T) {
	t.Parallel()

	// docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-configuring-automatic-snapshots.html:
	// "The latest seven daily automatic snapshots are stored before the oldest one is replaced."
	const retentionCount = 7

	synctest.Test(t, func(t *testing.T) {
		b := lightsail.NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
		defer b.Close()

		_, err := b.CreateInstances(lightsail.CreateInstancesRequest{
			Names: []string{"host-cadence"}, AvailabilityZone: "us-east-1a",
			BlueprintID: "amazon_linux_2023", BundleID: "nano_3_0",
		})
		require.NoError(t, err)

		_, err = b.EnableAddOn("host-cadence", lightsail.AddOnRequest{
			Type: lightsail.AddOnTypeAutoSnapshot, AutoSnapshotTimeOfDay: "06:00",
		})
		require.NoError(t, err)

		seeded, _, err := b.GetAutoSnapshots("host-cadence")
		require.NoError(t, err)
		require.Len(t, seeded, 1, "enabling seeds exactly one entry immediately")
		oldestDate := seeded[0].Date

		time.Sleep(24*time.Hour + time.Second)
		synctest.Wait()

		secondTick, _, err := b.GetAutoSnapshots("host-cadence")
		require.NoError(t, err)
		require.Len(t, secondTick, 2, "cadence must add a second entry a day later")
		secondOldestDate := secondTick[1].Date

		// Fast-forward past N+2 cycles total: the cap must evict both dates above.
		for range retentionCount {
			time.Sleep(24*time.Hour + time.Second)
			synctest.Wait()
		}

		capped, _, err := b.GetAutoSnapshots("host-cadence")
		require.NoError(t, err)
		require.Len(t, capped, retentionCount, "must stay capped at the documented retention count")

		for _, s := range capped {
			require.NotEqual(t, oldestDate, s.Date, "oldest entry must be evicted")
			require.NotEqual(t, secondOldestDate, s.Date, "second-oldest entry must be evicted")
		}

		_, err = b.DisableAddOn("host-cadence", lightsail.AddOnTypeAutoSnapshot)
		require.NoError(t, err)

		time.Sleep(24*time.Hour + time.Second)
		synctest.Wait()

		afterDisable, _, err := b.GetAutoSnapshots("host-cadence")
		require.NoError(t, err)
		require.Len(t, afterDisable, retentionCount, "disabling AutoSnapshot must stop the cadence")
	})
}
