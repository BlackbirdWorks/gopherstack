package emrserverless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// TestListJobRunAttempts_NoRunsForApp verifies ListJobRunAttempts returns
// ErrNotFound when the application has no job runs at all.
func TestListJobRunAttempts_NoRunsForApp(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("no-runs-attempts-app", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)

	_, _, err = b.ListJobRunAttempts(app.ApplicationID, "nonexistent-run", "", 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, emrserverless.ErrNotFound)
}
