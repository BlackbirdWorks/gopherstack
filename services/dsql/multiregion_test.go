package dsql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	peerRegion    = "us-east-2"
	witnessRegion = "us-west-2"
)

func waitClusterStatus(t *testing.T, client *dsqlsdk.Client, id *string, want types.ClusterStatus) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := client.GetCluster(t.Context(), &dsqlsdk.GetClusterInput{Identifier: id})

		return err == nil && out.Status == want
	}, waitTimeout, pollInterval)
}

func TestMultiRegionPeeringLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	east := newRegionClient(t, h, testRegion)
	other := newRegionClient(t, h, peerRegion)
	ctx := t.Context()

	a, err := east.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{
		MultiRegionProperties: &types.MultiRegionProperties{WitnessRegion: aws.String(witnessRegion)},
	})
	require.NoError(t, err)

	b, err := other.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{
		MultiRegionProperties: &types.MultiRegionProperties{
			WitnessRegion: aws.String(witnessRegion),
			Clusters:      []string{aws.ToString(a.Arn)},
		},
	})
	require.NoError(t, err)

	waitClusterStatus(t, east, a.Identifier, types.ClusterStatusPendingSetup)
	waitClusterStatus(t, other, b.Identifier, types.ClusterStatusPendingSetup)

	_, err = east.UpdateCluster(ctx, &dsqlsdk.UpdateClusterInput{
		Identifier: a.Identifier,
		MultiRegionProperties: &types.MultiRegionProperties{
			WitnessRegion: aws.String(witnessRegion),
			Clusters:      []string{aws.ToString(b.Arn)},
		},
	})
	require.NoError(t, err)

	waitClusterStatus(t, east, a.Identifier, types.ClusterStatusActive)
	waitClusterStatus(t, other, b.Identifier, types.ClusterStatusActive)

	got, err := east.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: a.Identifier})
	require.NoError(t, err)
	require.NotNil(t, got.MultiRegionProperties)
	assert.Equal(t, []string{aws.ToString(b.Arn)}, got.MultiRegionProperties.Clusters)

	_, err = other.DeleteCluster(ctx, &dsqlsdk.DeleteClusterInput{Identifier: b.Identifier})
	require.NoError(t, err)

	got, err = east.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: a.Identifier})
	require.NoError(t, err)
	assert.Empty(t, got.MultiRegionProperties.Clusters)
}

func TestMultiRegionPeerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		peer       func(t *testing.T, c *dsqlsdk.Client) string
		name       string
		witness    string
		wantCode   string
		sameRegion bool
	}{
		{
			name:     "malformed_arn",
			witness:  witnessRegion,
			peer:     func(*testing.T, *dsqlsdk.Client) string { return "not-an-arn" },
			wantCode: "ValidationException",
		},
		{
			name:    "unknown_cluster",
			witness: witnessRegion,
			peer: func(*testing.T, *dsqlsdk.Client) string {
				return "arn:aws:dsql:us-east-1:123456789012:cluster/doesnotexist"
			},
			wantCode: "ResourceNotFoundException",
		},
		{
			name:    "witness_mismatch",
			witness: "eu-west-1",
			peer: func(t *testing.T, c *dsqlsdk.Client) string {
				t.Helper()

				out, err := c.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{
					MultiRegionProperties: &types.MultiRegionProperties{WitnessRegion: aws.String(witnessRegion)},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
			wantCode: "ValidationException",
		},
		{
			name:       "same_region_peer",
			witness:    witnessRegion,
			sameRegion: true,
			peer: func(t *testing.T, c *dsqlsdk.Client) string {
				t.Helper()

				out, err := c.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{
					MultiRegionProperties: &types.MultiRegionProperties{WitnessRegion: aws.String(witnessRegion)},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			east := newRegionClient(t, h, testRegion)
			other := newRegionClient(t, h, peerRegion)

			peerSource := other
			if tt.sameRegion {
				peerSource = east
			}

			_, err := east.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{
				MultiRegionProperties: &types.MultiRegionProperties{
					WitnessRegion: aws.String(tt.witness),
					Clusters:      []string{tt.peer(t, peerSource)},
				},
			})
			require.Error(t, err)
			assertAPIErrorCode(t, err, tt.wantCode)
		})
	}
}
