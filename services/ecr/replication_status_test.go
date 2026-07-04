package ecr_test

// replication_status_test.go — verifies that DescribeImageReplicationStatus is
// backed by real per-destination state derived from the registry replication
// configuration: repositoryFilters gate destinations, same-region+account
// destinations are excluded, and the status settles from IN_PROGRESS to COMPLETE
// rather than being hardcoded to COMPLETE.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func putReplication(t *testing.T, b *ecr.InMemoryBackend, rules []ecr.ReplicationRule) {
	t.Helper()

	_, err := b.PutReplicationConfiguration(context.Background(), &ecr.ReplicationConfig{Rules: rules})
	require.NoError(t, err)
}

func TestReplication_DestinationsDerivedFromConfig(t *testing.T) {
	t.Parallel()

	// Backend region is us-east-1, account 123456789012.
	tests := []struct {
		name        string
		rules       []ecr.ReplicationRule
		repo        string
		wantRegions []string
	}{
		{
			name: "no filters replicates to all cross-region destinations",
			rules: []ecr.ReplicationRule{{
				Destinations: []ecr.ReplicationDestination{
					{Region: "us-west-2", RegistryID: "123456789012"},
					{Region: "eu-west-1", RegistryID: "210987654321"},
				},
			}},
			repo:        "app",
			wantRegions: []string{"eu-west-1", "us-west-2"},
		},
		{
			name: "same region and account destination is excluded",
			rules: []ecr.ReplicationRule{{
				Destinations: []ecr.ReplicationDestination{
					{Region: "us-east-1", RegistryID: "123456789012"}, // == source, skipped
					{Region: "us-east-1", RegistryID: "999988887777"}, // cross-account, kept
				},
			}},
			repo:        "app",
			wantRegions: []string{"us-east-1"},
		},
		{
			name: "repositoryFilters gate which repos replicate",
			rules: []ecr.ReplicationRule{{
				Destinations:      []ecr.ReplicationDestination{{Region: "us-west-2", RegistryID: "123456789012"}},
				RepositoryFilters: []ecr.RepositoryFilter{{Filter: "prod", FilterType: "PREFIX"}},
			}},
			repo:        "dev-app",
			wantRegions: nil, // dev-app does not match "prod" prefix
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal(tt.repo)
			seedImage(b, tt.repo, "sha256:img", "v1", time.Now())
			putReplication(t, b, tt.rules)

			out, err := b.DescribeImageReplicationStatus(context.Background(), tt.repo,
				ecr.ImageIdentifier{ImageDigest: "sha256:img"})
			require.NoError(t, err)

			got := make([]string, 0, len(out.ReplicationStatuses))
			for _, s := range out.ReplicationStatuses {
				got = append(got, s.Region)
			}
			assert.ElementsMatch(t, tt.wantRegions, got)
		})
	}
}

func TestReplication_StatusSettlesFromInProgressToComplete(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("app")
	b.SetReplicationSettleDelayForTest(time.Hour)
	seedImage(b, "app", "sha256:img", "v1", time.Now())
	putReplication(t, b, []ecr.ReplicationRule{{
		Destinations: []ecr.ReplicationDestination{{Region: "us-west-2", RegistryID: "123456789012"}},
	}})

	out, err := b.DescribeImageReplicationStatus(context.Background(), "app",
		ecr.ImageIdentifier{ImageDigest: "sha256:img"})
	require.NoError(t, err)
	require.Len(t, out.ReplicationStatuses, 1)
	assert.Equal(t, "IN_PROGRESS", out.ReplicationStatuses[0].Status,
		"freshly pushed image within settle window is IN_PROGRESS")

	// Age the image past the settle delay -> replication reports COMPLETE.
	b.AgeImageForTest("app", "sha256:img", 2*time.Hour)

	out, err = b.DescribeImageReplicationStatus(context.Background(), "app",
		ecr.ImageIdentifier{ImageDigest: "sha256:img"})
	require.NoError(t, err)
	require.Len(t, out.ReplicationStatuses, 1)
	assert.Equal(t, "COMPLETE", out.ReplicationStatuses[0].Status)
	assert.Equal(t, "123456789012", out.ReplicationStatuses[0].RegistryID)
}

func TestReplication_NoConfig_EmptyStatuses(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("app")
	seedImage(b, "app", "sha256:img", "v1", time.Now())

	out, err := b.DescribeImageReplicationStatus(context.Background(), "app",
		ecr.ImageIdentifier{ImageDigest: "sha256:img"})
	require.NoError(t, err)
	assert.Empty(t, out.ReplicationStatuses)
}
