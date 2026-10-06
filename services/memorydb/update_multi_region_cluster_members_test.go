package memorydb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateMultiRegionClusterAppliesShardConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy memorydbtypes.UpdateStrategy
		shards   int32
		wantErr  bool
	}{
		{name: "coordinated", shards: 4, strategy: memorydbtypes.UpdateStrategyCoordinated},
		{name: "uncoordinated", shards: 3, strategy: memorydbtypes.UpdateStrategyUncoordinated},
		{name: "too many shards", shards: 501, wantErr: true},
		{name: "bad strategy", shards: 2, strategy: memorydbtypes.UpdateStrategy("bogus"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMemoryDBClient(t, newTestHandler(t))
			ctx := t.Context()

			created, err := client.CreateMultiRegionCluster(ctx, &memorydbsdk.CreateMultiRegionClusterInput{
				MultiRegionClusterNameSuffix: aws.String("shard"),
				NodeType:                     aws.String("db.r6g.large"),
			})
			require.NoError(t, err)
			name := created.MultiRegionCluster.MultiRegionClusterName

			_, err = client.UpdateMultiRegionCluster(ctx, &memorydbsdk.UpdateMultiRegionClusterInput{
				MultiRegionClusterName: name,
				ShardConfiguration:     &memorydbtypes.ShardConfigurationRequest{ShardCount: tt.shards},
				UpdateStrategy:         tt.strategy,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			desc, err := client.DescribeMultiRegionClusters(ctx, &memorydbsdk.DescribeMultiRegionClustersInput{
				MultiRegionClusterName: name,
			})
			require.NoError(t, err)
			require.Len(t, desc.MultiRegionClusters, 1)
			assert.Equal(t, tt.shards, aws.ToInt32(desc.MultiRegionClusters[0].NumberOfShards))
		})
	}
}
