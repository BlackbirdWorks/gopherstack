package ecr_test

// replication_filter_type_test.go -- ecr's internal RepositoryFilter struct
// (models.go:274) is shared across replication, scanning, and signing
// configs, but those map to two DISTINCT real AWS types with DISTINCT
// FilterType enums: types.RepositoryFilter (replication rules,
// RepositoryFilterType) supports only "PREFIX_MATCH"
// (aws-sdk-go-v2/service/ecr@v1.60.4 types/enums.go:385); types.
// ScanningRepositoryFilter (scanning rules, ScanningRepositoryFilterType)
// supports only "WILDCARD" (enums.go:441). repoMatchesFilters
// (repositories.go:161-170) switched on "WILDCARD" and "PREFIX" -- "PREFIX"
// is not a real AWS enum value for either type, so a replication rule built
// with the real, only-valid "PREFIX_MATCH" fell through both cases and
// NEVER matched any repository, silently disabling prefix-filtered
// replication for every real client.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestReplication_RepositoryFilters_PrefixMatch_HonoursRealEnumValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		repo        string
		wantRegions []string
	}{
		{name: "matching prefix replicates", repo: "prod-app", wantRegions: []string{"us-west-2"}},
		{name: "non-matching prefix does not replicate", repo: "dev-app", wantRegions: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal(tt.repo)
			seedImage(b, tt.repo, "sha256:img", "v1", time.Now())

			putReplication(t, b, []ecr.ReplicationRule{{
				Destinations: []ecr.ReplicationDestination{{Region: "us-west-2", RegistryID: "123456789012"}},
				RepositoryFilters: []ecr.RepositoryFilter{
					{Filter: "prod", FilterType: "PREFIX_MATCH"},
				},
			}})

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
