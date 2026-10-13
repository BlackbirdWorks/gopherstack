package redshift_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func TestRealClient_ModifyClusterNewClusterIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		newID   string
		wantErr string
	}{
		{name: "renames", newID: "renamed"},
		{name: "invalid", newID: "Bad_ID", wantErr: "InvalidParameter"},
		{name: "taken", newID: "other", wantErr: "ClusterAlreadyExists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := redshift.NewInMemoryBackend("000000000000", rtTestRegion)
			client := newTestRedshiftClient(t, redshift.NewHandler(backend))

			for _, id := range []string{"orig", "other"} {
				_, err := backend.CreateCluster(
					id,
					"dc2.large",
					"dev",
					"admin",
					nil,
					"",
					redshift.CreateClusterOptions{},
				)
				require.NoError(t, err)
			}

			ul, err := backend.CreateUsageLimit("orig", "spectrum", "data-scanned", "log", 5, nil)
			require.NoError(t, err)
			_, err = backend.CreateClusterSnapshot("snap", "orig")
			require.NoError(t, err)

			out, err := client.ModifyCluster(t.Context(), &redshiftsdk.ModifyClusterInput{
				ClusterIdentifier:    aws.String("orig"),
				NewClusterIdentifier: aws.String(tt.newID),
			})

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.newID, aws.ToString(out.Cluster.ClusterIdentifier))
			assert.True(t, strings.HasPrefix(aws.ToString(out.Cluster.Endpoint.Address), tt.newID+"."))

			_, err = client.DescribeClusters(
				t.Context(),
				&redshiftsdk.DescribeClustersInput{ClusterIdentifier: aws.String("orig")},
			)
			require.Error(t, err)

			got, err := client.DescribeUsageLimits(t.Context(), &redshiftsdk.DescribeUsageLimitsInput{
				ClusterIdentifier: aws.String(tt.newID),
			})
			require.NoError(t, err)
			require.Len(t, got.UsageLimits, 1)
			assert.Equal(t, ul.UsageLimitID, aws.ToString(got.UsageLimits[0].UsageLimitId))

			snaps, err := client.DescribeClusterSnapshots(t.Context(), &redshiftsdk.DescribeClusterSnapshotsInput{})
			require.NoError(t, err)
			require.Len(t, snaps.Snapshots, 1)
			assert.Equal(t, "orig", aws.ToString(snaps.Snapshots[0].ClusterIdentifier))
		})
	}
}
