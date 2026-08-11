package codedeploy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func applicationRevisionCount(b *InMemoryBackend) int {
	b.mu.RLock("test.applicationRevisionCount")
	defer b.mu.RUnlock()

	return b.applicationRevisions.Len()
}

// TestInMemoryBackend_Persistence_ApplicationRevisionsRoundTrip verifies that
// application revisions survive a Snapshot/Restore round trip.
func TestInMemoryBackend_Persistence_ApplicationRevisionsRoundTrip(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	_, err := b.CreateApplication("app-1", "Server", nil)
	require.NoError(t, err)
	_, err = b.CreateDeploymentGroup("app-1", "dg-1", DeploymentGroupInput{
		ServiceRoleArn: "arn:role",
	}, nil)
	require.NoError(t, err)

	revision := RevisionLocation{
		RevisionType: "S3",
		S3Location:   &RevisionS3Location{Bucket: "b", Key: "k"},
	}
	_, err = b.CreateDeployment("app-1", "dg-1", DeploymentOptions{
		Creator:  "user",
		Revision: &revision,
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	require.Equal(t, 1, applicationRevisionCount(fresh))

	rev, err := fresh.GetApplicationRevision("app-1", revision)
	require.NoError(t, err)
	assert.NotZero(t, rev.RegisterTime)
	require.NotNil(t, rev.FirstUsedTime)
	require.NotNil(t, rev.LastUsedTime)
	assert.Contains(t, rev.DeploymentGroups, "dg-1")
}

// TestStore_Reset verifies Reset clears every backend map, including the
// unexported applicationRevisions map that has no other exported accessor.
func TestStore_Reset(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))

	_, _ = h.Backend.CreateApplication("app-1", "Server", nil)
	_, _ = h.Backend.CreateApplication("app-2", "Lambda", nil)
	require.NoError(t, h.Backend.RegisterApplicationRevision("app-1", RevisionLocation{
		RevisionType: "S3",
		S3Location:   &RevisionS3Location{Bucket: "b", Key: "k"},
	}, ""))

	require.Equal(t, 2, h.Backend.ApplicationCount())
	require.Equal(t, 1, applicationRevisionCount(h.Backend))

	h.Reset()

	assert.Equal(t, 0, h.Backend.ApplicationCount())
	assert.Equal(t, 0, h.Backend.DeploymentCount())
	assert.Equal(t, 0, applicationRevisionCount(h.Backend))
	// 9 CodeDeployDefault.* configs are re-seeded on every Reset/NewInMemoryBackend.
	assert.Equal(t, 9, h.Backend.DeploymentConfigCount())
}
