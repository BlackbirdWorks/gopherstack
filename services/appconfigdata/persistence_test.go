package appconfigdata_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", `{"flag":true}`, "application/json"))

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListProfiles())
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches appconfigdataSnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 on-disk shape (AppConfigData had no persistence at all
// before this conversion, so there is no legacy shape to actually collide
// with -- any input lacking "version" hits this same path).
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", `{"flag":true}`, "application/json"))

	err := b.Restore(t.Context(), []byte(`{"profiles":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListProfiles())
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot ->
// Restore round trip across every store.Table-backed resource family the
// Phase 3.3 conversion touched: profiles (composite app|env|profile key),
// sessions (keyed by their own Token field), and graceTokens (keyed by the
// hidden Token field added for the rotated-token cache entry).
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := appconfigdata.NewInMemoryBackend()
	require.NoError(t, original.SetConfiguration(
		"app-1", "env-1", "profile-1", `{"flag":true}`, "application/json",
	))

	initialToken, err := original.StartSession("app-1", "env-1", "profile-1", 0)
	require.NoError(t, err)

	// Poll once: rotates initialToken into a grace-cache entry and creates a
	// new, live session under the returned nextToken.
	content, contentType, nextToken, hash, versionLabel, err := original.GetLatestConfiguration(initialToken)
	require.NoError(t, err)
	require.NotEmpty(t, nextToken)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := appconfigdata.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assertProfileRestored(t, fresh)
	assertLiveSessionRestored(t, fresh, nextToken)
	assertGraceTokenRestored(t, fresh, initialToken, content, contentType, nextToken, hash, versionLabel)
}

// assertProfileRestored checks the profiles table (composite app|env|profile
// key, derived from real ConfigurationProfile fields -- no DTO needed).
func assertProfileRestored(t *testing.T, fresh *appconfigdata.InMemoryBackend) {
	t.Helper()

	profiles := fresh.ListProfiles()
	require.Len(t, profiles, 1)
	assert.Equal(t, "app-1", profiles[0].ApplicationIdentifier)
	assert.Equal(t, "env-1", profiles[0].EnvironmentIdentifier)
	assert.Equal(t, "profile-1", profiles[0].ConfigurationProfileIdentifier)
	assert.JSONEq(t, `{"flag":true}`, profiles[0].Content)
}

// assertLiveSessionRestored checks the sessions table survived under the
// rotated (post-poll) token, including PollCount and PreviousContentHash --
// state that only exists after GetLatestConfiguration's rotation logic ran.
func assertLiveSessionRestored(t *testing.T, fresh *appconfigdata.InMemoryBackend, nextToken string) {
	t.Helper()

	sess := fresh.LookupSession(nextToken)
	require.NotNil(t, sess)
	assert.Equal(t, "app-1", sess.ApplicationIdentifier)
	assert.Equal(t, 1, sess.PollCount)
	assert.NotEmpty(t, sess.PreviousContentHash)

	stats := fresh.GetStats()
	assert.Equal(t, 1, stats.SessionCount)
	assert.Equal(t, 1, stats.ProfileCount)
}

// assertGraceTokenRestored checks the graceTokens table survived under its
// hidden Token identity field: polling again with the now-rotated-away
// initialToken must still hit the idempotent-retry fast path and return the
// exact response cached before the snapshot, not a fresh ErrSessionNotFound.
func assertGraceTokenRestored(
	t *testing.T,
	fresh *appconfigdata.InMemoryBackend,
	initialToken string,
	wantContent []byte,
	wantContentType, wantNextToken, wantHash, wantVersionLabel string,
) {
	t.Helper()

	content, contentType, nextToken, hash, versionLabel, err := fresh.GetLatestConfiguration(initialToken)
	require.NoError(t, err)
	assert.Equal(t, wantContent, content)
	assert.Equal(t, wantContentType, contentType)
	assert.Equal(t, wantNextToken, nextToken)
	assert.Equal(t, wantHash, hash)
	assert.Equal(t, wantVersionLabel, versionLabel)
}
