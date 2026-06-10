package pinpoint_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

const (
	leakRegion    = "us-east-1"
	leakAccountID = "123456789012"
)

// TestDeleteApp_ReleasesPerAppState verifies that deleting a Pinpoint app drops
// every piece of per-application state so the backing maps return to baseline.
func TestDeleteApp_ReleasesPerAppState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nEvents  int
		campaign bool
		segment  bool
		journey  bool
	}{
		{name: "events only", nEvents: 5},
		{name: "events + campaign", nEvents: 3, campaign: true},
		{name: "full graph", nEvents: 4, campaign: true, segment: true, journey: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := pinpoint.NewInMemoryBackend(leakRegion, leakAccountID)
			baseline := pinpoint.TotalPerAppEntries(b)

			app, err := b.CreateApp(leakRegion, leakAccountID, "leak-app", nil)
			require.NoError(t, err)

			if tc.campaign {
				require.NoError(t, pinpoint.CreateCampaignForTest(b, leakRegion, leakAccountID, app.ID))
			}
			if tc.segment {
				require.NoError(t, pinpoint.CreateSegmentForTest(b, leakRegion, leakAccountID, app.ID))
			}
			if tc.journey {
				require.NoError(t, pinpoint.CreateJourneyForTest(b, leakRegion, leakAccountID, app.ID))
			}
			if tc.nEvents > 0 {
				require.NoError(t, pinpoint.PutEventsForTest(b, app.ID, tc.nEvents))
			}

			require.Greater(t, pinpoint.TotalPerAppEntries(b), baseline,
				"per-app state should have accumulated before delete")

			_, err = b.DeleteApp(app.ID)
			require.NoError(t, err)

			require.Equal(t, baseline, pinpoint.TotalPerAppEntries(b),
				"all per-app state must be released when the app is deleted")
		})
	}
}

// TestPutEvents_CapsAppEvents verifies that ingested events are bounded per app
// so the appEvents slice cannot grow without limit.
func TestPutEvents_CapsAppEvents(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend(leakRegion, leakAccountID)
	app, err := b.CreateApp(leakRegion, leakAccountID, "cap-app", nil)
	require.NoError(t, err)

	const batches = 3
	for range batches {
		require.NoError(t, pinpoint.PutEventsForTest(b, app.ID, pinpoint.MaxAppEvents))
	}

	require.LessOrEqual(t, pinpoint.AppEventCount(b, app.ID), pinpoint.MaxAppEvents,
		"retained events must not exceed the cap")
}
