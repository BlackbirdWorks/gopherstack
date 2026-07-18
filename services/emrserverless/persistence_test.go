package emrserverless_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// buildFullBackend seeds a backend with one of everything: an application, a
// job run under it, an interactive session (with tags and configuration
// overrides) under it, plus resource tags on all three, so a Snapshot/Restore
// round trip exercises every store.Table the backend registers (applications,
// jobRuns, sessions) and the one raw-left map (sessionTokens).
func buildFullBackend(t *testing.T) (
	*emrserverless.InMemoryBackend, string, string, string, string,
) {
	t.Helper()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")

	app, err := b.CreateApplication("full-state-app", "SPARK", "emr-6.6.0", "", map[string]string{"app": "yes"})
	require.NoError(t, err)
	require.NoError(t, b.StartApplication(app.ApplicationID))

	jr, err := b.StartJobRun(
		app.ApplicationID, "arn:aws:iam::000000000000:role/exec", "full-state-run", "",
		map[string]string{"run": "yes"},
	)
	require.NoError(t, err)

	token := "client-token-1"
	session, err := b.StartSession(
		app.ApplicationID, token, "arn:aws:iam::000000000000:role/exec", "full-state-session",
		15, map[string]any{"k": "v"}, map[string]string{"session": "yes"},
	)
	require.NoError(t, err)

	return b, app.ApplicationID, jr.JobRunID, session.SessionID, token
}

func Test_Persistence_RoundTrip_FullState(t *testing.T) {
	t.Parallel()

	b, appID, jobRunID, sessionID, clientToken := buildFullBackend(t)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	assert.Equal(t, 1, emrserverless.ApplicationCount(restored))
	assert.Equal(t, 1, emrserverless.JobRunCount(restored))

	gotApp, err := restored.GetApplication(appID)
	require.NoError(t, err)
	assert.Equal(t, "full-state-app", gotApp.Name)
	assert.Equal(t, emrserverless.ApplicationStateStarted, gotApp.State)
	assert.Equal(t, "yes", gotApp.Tags["app"])

	gotJobRun, err := restored.GetJobRun(appID, jobRunID)
	require.NoError(t, err)
	assert.Equal(t, "full-state-run", gotJobRun.Name)
	assert.Equal(t, "yes", gotJobRun.Tags["run"])

	gotSession, err := restored.GetSession(appID, sessionID)
	require.NoError(t, err)
	assert.Equal(t, "full-state-session", gotSession.Name)
	assert.Equal(t, "yes", gotSession.Tags["session"])
	assert.Equal(t, "v", gotSession.ConfigurationOverrides["k"])

	// Resource tags reachable by ARN (applicationsByARN / jobRunsByARN /
	// sessionsByARN indexes) survive the round trip.
	appTags, err := restored.ListTagsForResource(gotApp.Arn)
	require.NoError(t, err)
	assert.Equal(t, "yes", appTags["app"])

	jobRunTags, err := restored.ListTagsForResource(gotJobRun.Arn)
	require.NoError(t, err)
	assert.Equal(t, "yes", jobRunTags["run"])

	sessionTags, err := restored.ListTagsForResource(gotSession.Arn)
	require.NoError(t, err)
	assert.Equal(t, "yes", sessionTags["session"])

	// sessionTokens (the one raw-left map) survives the round trip: starting
	// a session with the same clientToken against the restored backend must
	// return the already-restored session rather than minting a new one.
	again, err := restored.StartSession(
		appID, clientToken, "arn:aws:iam::000000000000:role/exec", "full-state-session",
		15, map[string]any{"k": "v"}, map[string]string{"session": "yes"},
	)
	require.NoError(t, err)
	assert.Equal(t, sessionID, again.SessionID)

	sessions, _, err := restored.ListSessions(appID, "", 0, time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Len(t, sessions, 1)
}

func Test_Persistence_RoundTrip_Empty(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := emrserverless.NewInMemoryBackend("111111111111", "us-west-2")
	require.NoError(t, restored.Restore(t.Context(), snap))

	assert.Equal(t, 0, emrserverless.ApplicationCount(restored))
	assert.Equal(t, 0, emrserverless.JobRunCount(restored))

	appARNs, jobRunARNs := emrserverless.ARNIndexSizes(restored)
	assert.Equal(t, 0, appARNs)
	assert.Equal(t, 0, jobRunARNs)
}

// Test_Persistence_VersionMismatch covers the version guard in Restore: a
// snapshot whose "version" field does not match the current build's
// emrserverlessSnapshotVersion must be discarded wholesale (registry reset,
// raw maps reset) rather than partially decoded, and Restore must still
// report success (nil error) since this is a recoverable condition, not
// corruption.
func Test_Persistence_VersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutateSnap func(t *testing.T, raw map[string]any)
		name       string
	}{
		{
			name: "future version",
			mutateSnap: func(t *testing.T, raw map[string]any) {
				t.Helper()
				raw["version"] = 999
			},
		},
		{
			name: "absent version (pre-Phase-3.3 format)",
			mutateSnap: func(t *testing.T, raw map[string]any) {
				t.Helper()
				delete(raw, "version")
			},
		},
		{
			name: "zero version",
			mutateSnap: func(t *testing.T, raw map[string]any) {
				t.Helper()
				raw["version"] = 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _, _, _ := buildFullBackend(t)
			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(snap, &raw))
			tt.mutateSnap(t, raw)

			mutated, err := json.Marshal(raw)
			require.NoError(t, err)

			// Restore into a backend that already holds unrelated state, to
			// prove the mismatch path resets it rather than merging.
			target := emrserverless.NewInMemoryBackend("222222222222", "eu-west-1")
			_, createErr := target.CreateApplication("pre-existing", "SPARK", "emr-6.6.0", "", nil)
			require.NoError(t, createErr)
			require.Equal(t, 1, emrserverless.ApplicationCount(target))

			require.NoError(t, target.Restore(t.Context(), mutated))

			assert.Equal(t, 0, emrserverless.ApplicationCount(target))
			assert.Equal(t, 0, emrserverless.JobRunCount(target))

			appARNs, jobRunARNs := emrserverless.ARNIndexSizes(target)
			assert.Equal(t, 0, appARNs)
			assert.Equal(t, 0, jobRunARNs)
		})
	}
}

func Test_Persistence_Restore_MalformedJSON(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("{not valid json"))
	require.Error(t, err)
}

// Test_Persistence_EnsureMaps_NilJobRunSubMap verifies a snapshot/restore
// round trip survives when the job runs map has entries (ensureNonNilTags
// runs and handles any nil sub-maps).
func Test_Persistence_EnsureMaps_NilJobRunSubMap(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("ensure-maps-app", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)

	// Start a job run so the job runs map has an entry.
	_, err = b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "run1", "", nil)
	require.NoError(t, err)

	// Snapshot and restore — ensureMaps will run and handle any nil sub-maps.
	snap := b.Snapshot(t.Context())
	b2 := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 1, emrserverless.ApplicationCount(b2))
	assert.Equal(t, 1, emrserverless.JobRunCount(b2))
}

// Test_Persistence_Restore_InvalidJSON covers Restore with completely
// non-JSON input.
func Test_Persistence_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// Test_Persistence_RoundTrip_AppAndJobRun covers a Snapshot/Restore round
// trip of one application (with tags) and one job run.
func Test_Persistence_RoundTrip_AppAndJobRun(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("persist-app", "SPARK", "emr-6.6.0", "", map[string]string{"k": "v"})
	require.NoError(t, err)

	_, err = b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "persist-run", "", nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, emrserverless.ApplicationCount(b2))
	assert.Equal(t, 1, emrserverless.JobRunCount(b2))

	got, err := b2.GetApplication(app.ApplicationID)
	require.NoError(t, err)
	assert.Equal(t, app.Name, got.Name)
	assert.Equal(t, "v", got.Tags["k"])
}

// Test_Persistence_Empty covers a Snapshot/Restore round trip of a
// completely empty backend.
func Test_Persistence_Empty(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 0, emrserverless.ApplicationCount(b2))
	assert.Equal(t, 0, emrserverless.JobRunCount(b2))
}

// Test_Persistence_HandlerSnapshotRestore covers Handler.Snapshot/Restore
// delegation to the backend.
func Test_Persistence_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	h := emrserverless.NewHandler(emrserverless.NewInMemoryBackend("000000000000", "us-east-1"))
	createApp(t, h, "snap-app")

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := emrserverless.NewHandler(emrserverless.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	rec := doRequest(t, h2, http.MethodGet, "/applications", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	apps := out["applications"].([]any)
	assert.Len(t, apps, 1)
}
