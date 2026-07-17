package amplify_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_AccessLogsAndArtifacts(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("ArtifactApp", "", "", "", nil)
	require.NoError(t, err)

	// GenerateAccessLogs for nonexistent app
	_, err = b.GenerateAccessLogs("nonexistent", "", "", "")
	require.Error(t, err)

	// GenerateAccessLogs success
	url, err := b.GenerateAccessLogs(app.AppID, "", "", "")
	require.NoError(t, err)
	assert.NotEmpty(t, url)

	// GetArtifactURL for nonexistent artifact
	_, _, err = b.GetArtifactURL("nonexistent-artifact")
	require.Error(t, err)

	// ListArtifacts
	artifacts, _, err := b.ListArtifacts(app.AppID, "main", "job1", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, artifacts)
}
