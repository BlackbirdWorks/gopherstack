package emrserverless

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplicationTokens_TTLBoundsMapGrowth locks in the fix for
// b.applicationTokens growing unbounded: entries were never deleted, not even
// when the application they pointed at was deleted, so a long-running backend
// fed unique ClientTokens leaked memory forever. CreateApplication now sweeps
// entries past clientTokenTTL on every write.
func TestApplicationTokens_TTLBoundsMapGrowth(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend("123456789012", "us-east-1")

		first, err := b.CreateApplication("app-one", "SPARK", "emr-6.9.0", "X86_64", nil,
			CreateApplicationOptions{ClientToken: "tok-1"})
		require.NoError(t, err)
		assert.Len(t, b.applicationTokens, 1, "one entry recorded after the first CreateApplication call")

		// Still inside the window: same token replays the original application.
		time.Sleep(clientTokenTTL - time.Second)

		replay, err := b.CreateApplication("app-one", "SPARK", "emr-6.9.0", "X86_64", nil,
			CreateApplicationOptions{ClientToken: "tok-1"})
		require.NoError(t, err)
		assert.Equal(t, first.ApplicationID, replay.ApplicationID,
			"a replay inside the window must return the original resource, not create a new one")
		assert.Len(t, b.applicationTokens, 1, "replay must not grow the map")

		// Cross the window and write a new token: the sweep must reclaim tok-1's
		// now-expired entry, leaving only the new one behind.
		time.Sleep(2 * time.Second)

		second, err := b.CreateApplication("app-two", "SPARK", "emr-6.9.0", "X86_64", nil,
			CreateApplicationOptions{ClientToken: "tok-2"})
		require.NoError(t, err)
		assert.NotEqual(t, first.ApplicationID, second.ApplicationID)
		assert.Len(t, b.applicationTokens, 1, "expired tok-1 entry must be swept, not accumulate")

		_, hasExpired := b.applicationTokens["tok-1"]
		assert.False(t, hasExpired, "expired entry must be deleted from the map")
	})
}

// TestDeleteApplication_PurgesTokenState locks in DeleteApplication's cascade
// cleanup of applicationTokens and the flat clientTokenCreatedAt timestamp map for
// all three token domains: previously only sessionTokens/jobRunTokens' per-app
// submaps were dropped, leaving applicationTokens and every clientTokenCreatedAt
// entry as a permanent orphan once the application was gone.
func TestDeleteApplication_PurgesTokenState(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")

	app, err := b.CreateApplication("app", "SPARK", "emr-6.9.0", "X86_64", nil,
		CreateApplicationOptions{ClientToken: "app-tok"})
	require.NoError(t, err)

	require.NoError(t, b.StartApplication(app.ApplicationID))

	_, err = b.StartSession(app.ApplicationID, "sess-tok", "arn:aws:iam::123456789012:role/x", "sess", 15, nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.StopApplication(app.ApplicationID))
	require.NoError(t, b.DeleteApplication(app.ApplicationID))

	assert.NotContains(t, b.applicationTokens, "app-tok")
	assert.NotContains(t, b.sessionTokens, app.ApplicationID)
	assert.NotContains(t, b.clientTokenCreatedAt, clientTokenKey("application", "", "app-tok"))
	assert.NotContains(t, b.clientTokenCreatedAt, clientTokenKey("session", app.ApplicationID, "sess-tok"))
}
