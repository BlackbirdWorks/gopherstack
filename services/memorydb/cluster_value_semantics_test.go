package memorydb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCluster_DefaultsAndPartialUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update    *memorydbsdk.UpdateClusterInput
		name      string
		wantRetn  int32
		wantShard int32
	}{
		{
			name:      "retention only",
			update:    &memorydbsdk.UpdateClusterInput{SnapshotRetentionLimit: aws.Int32(7)},
			wantRetn:  7,
			wantShard: 1,
		},
		{name: "empty update", update: &memorydbsdk.UpdateClusterInput{}, wantRetn: 0, wantShard: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestMemoryDBClient(t, newTestHandler(t))
			_, err := c.CreateCluster(t.Context(), &memorydbsdk.CreateClusterInput{
				ClusterName: aws.String("c1"), NodeType: aws.String("db.r6g.large"), ACLName: aws.String("open-access"),
			})
			require.NoError(t, err)

			tc.update.ClusterName = aws.String("c1")
			_, err = c.UpdateCluster(t.Context(), tc.update)
			require.NoError(t, err)

			got, err := c.DescribeClusters(
				t.Context(),
				&memorydbsdk.DescribeClustersInput{ClusterName: aws.String("c1")},
			)
			require.NoError(t, err)
			require.Len(t, got.Clusters, 1)

			cl := got.Clusters[0]
			assert.Equal(t, tc.wantRetn, aws.ToInt32(cl.SnapshotRetentionLimit))
			assert.Equal(t, tc.wantShard, aws.ToInt32(cl.NumberOfShards))
			assert.True(t, aws.ToBool(cl.TLSEnabled))
			assert.Equal(t, "open-access", aws.ToString(cl.ACLName))
			assert.Equal(t, int32(6379), cl.ClusterEndpoint.Port)
		})
	}
}
