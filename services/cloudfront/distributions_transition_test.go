package cloudfront_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

const (
	distTransitionWait = 2 * time.Second
	distTransitionTick = 10 * time.Millisecond
)

// waitForDistributionStatus polls GetDistribution until it reports want.
func waitForDistributionStatus(t *testing.T, b *cloudfront.InMemoryBackend, distID, want string) {
	t.Helper()

	require.Eventually(t, func() bool {
		d, err := b.GetDistribution(distID)

		return err == nil && d.Status == want
	}, distTransitionWait, distTransitionTick, "distribution never reached status %s", want)
}

// TestDistributionStatusTransition covers UpdateDistribution's async
// InProgress -> Deployed transition (distributions.go's
// scheduleDistributionDeployed), both on the live backend and across a
// Snapshot/Restore round trip.
func TestDistributionStatusTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *cloudfront.InMemoryBackend, distID string)
		name string
	}{
		{
			name: "reaches deployed",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend, distID string) {
				t.Helper()

				waitForDistributionStatus(t, b, distID, "Deployed")
			},
		},
		{
			name: "survives snapshot restore",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend, distID string) {
				t.Helper()

				data := b.Snapshot(t.Context())
				require.NotEmpty(t, data)

				restored := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
				require.NoError(t, restored.Restore(t.Context(), data))

				d, err := restored.GetDistribution(distID)
				require.NoError(t, err)
				require.Equal(t, "InProgress", d.Status, "restore should preserve the in-flight InProgress status")

				waitForDistributionStatus(t, restored, distID, "Deployed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

			callerRef := "ref-transition-" + tt.name
			d, err := b.CreateDistribution(callerRef, "orig", true, minimalDistConfig(callerRef, "orig", true))
			require.NoError(t, err)
			require.Equal(t, "Deployed", d.Status)

			upd, err := b.UpdateDistribution(d.ID, "updated", true, minimalDistConfig(callerRef, "updated", true))
			require.NoError(t, err)
			require.Equal(t, "InProgress", upd.Status,
				"UpdateDistribution should return the real intermediate InProgress status")

			tt.run(t, b, d.ID)
		})
	}
}
