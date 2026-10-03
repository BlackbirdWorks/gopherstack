package docdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ClusterNetworkTypeAndServerlessScaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		netType string
		minCap  *float64
		maxCap  *float64
		name    string
		errCode string
	}{
		{name: "valid", netType: "DUAL", minCap: aws.Float64(0.5), maxCap: aws.Float64(8)},
		{name: "bad network type", netType: "IPV6", errCode: "InvalidParameterValue"},
		{name: "min above max", minCap: aws.Float64(9), maxCap: aws.Float64(2), errCode: "InvalidParameterValue"},
		{name: "not half step", minCap: aws.Float64(1.3), errCode: "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()
			in := &docdbsdk.CreateDBClusterInput{DBClusterIdentifier: aws.String("x-a"), Engine: aws.String("docdb")}
			if tt.netType != "" {
				in.NetworkType = aws.String(tt.netType)
			}
			if tt.minCap != nil || tt.maxCap != nil {
				in.ServerlessV2ScalingConfiguration = &types.ServerlessV2ScalingConfiguration{
					MinCapacity: tt.minCap, MaxCapacity: tt.maxCap,
				}
			}
			out, err := client.CreateDBCluster(ctx, in)
			if tt.errCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errCode)
				_, derr := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("x-a"),
				})
				require.Error(t, derr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.netType, aws.ToString(out.DBCluster.NetworkType))
			require.NotNil(t, out.DBCluster.ServerlessV2ScalingConfiguration)
			assert.InDelta(t, 0.5, aws.ToFloat64(out.DBCluster.ServerlessV2ScalingConfiguration.MinCapacity), 0)
			assert.InDelta(t, 8, aws.ToFloat64(out.DBCluster.ServerlessV2ScalingConfiguration.MaxCapacity), 0)

			mod, err := client.ModifyDBCluster(ctx, &docdbsdk.ModifyDBClusterInput{
				DBClusterIdentifier: aws.String("x-a"),
				NetworkType:         aws.String("IPV4"),
				ServerlessV2ScalingConfiguration: &types.ServerlessV2ScalingConfiguration{
					MaxCapacity: aws.Float64(16),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "IPV4", aws.ToString(mod.DBCluster.NetworkType))
			assert.InDelta(t, 0.5, aws.ToFloat64(mod.DBCluster.ServerlessV2ScalingConfiguration.MinCapacity), 0)
			assert.InDelta(t, 16, aws.ToFloat64(mod.DBCluster.ServerlessV2ScalingConfiguration.MaxCapacity), 0)

			_, err = client.ModifyDBCluster(ctx, &docdbsdk.ModifyDBClusterInput{
				DBClusterIdentifier: aws.String("x-a"),
				ServerlessV2ScalingConfiguration: &types.ServerlessV2ScalingConfiguration{
					MaxCapacity: aws.Float64(0),
				},
			})
			require.Error(t, err)

			_, err = client.CreateDBClusterSnapshot(ctx, &docdbsdk.CreateDBClusterSnapshotInput{
				DBClusterIdentifier: aws.String("x-a"), DBClusterSnapshotIdentifier: aws.String("x-snap"),
			})
			require.NoError(t, err)
			rs, err := client.RestoreDBClusterFromSnapshot(ctx, &docdbsdk.RestoreDBClusterFromSnapshotInput{
				DBClusterIdentifier: aws.String("x-b"), SnapshotIdentifier: aws.String("x-snap"),
				Engine: aws.String("docdb"), NetworkType: aws.String("DUAL"),
				ServerlessV2ScalingConfiguration: &types.ServerlessV2ScalingConfiguration{
					MinCapacity: aws.Float64(1), MaxCapacity: aws.Float64(2),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "DUAL", aws.ToString(rs.DBCluster.NetworkType))
			assert.InDelta(t, 2, aws.ToFloat64(rs.DBCluster.ServerlessV2ScalingConfiguration.MaxCapacity), 0)

			pit, err := client.RestoreDBClusterToPointInTime(ctx, &docdbsdk.RestoreDBClusterToPointInTimeInput{
				DBClusterIdentifier: aws.String("x-c"), SourceDBClusterIdentifier: aws.String("x-a"),
				UseLatestRestorableTime: aws.Bool(true), NetworkType: aws.String("DUAL"),
				ServerlessV2ScalingConfiguration: &types.ServerlessV2ScalingConfiguration{
					MinCapacity: aws.Float64(4), MaxCapacity: aws.Float64(5),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "DUAL", aws.ToString(pit.DBCluster.NetworkType))
			assert.InDelta(t, 4, aws.ToFloat64(pit.DBCluster.ServerlessV2ScalingConfiguration.MinCapacity), 0)

			desc, err := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("x-a"),
			})
			require.NoError(t, err)
			assert.Equal(t, "IPV4", aws.ToString(desc.DBClusters[0].NetworkType))
		})
	}
}

func TestRealClient_GlobalClusterTagList(t *testing.T) {
	t.Parallel()

	tests := []struct{ name string }{{name: "tags surface and clear on delete"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			gc, err := client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gtl"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)
			assert.Empty(t, gc.GlobalCluster.TagList)

			_, err = client.AddTagsToResource(ctx, &docdbsdk.AddTagsToResourceInput{
				ResourceName: gc.GlobalCluster.GlobalClusterArn,
				Tags:         []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
			})
			require.NoError(t, err)

			desc, err := client.DescribeGlobalClusters(ctx, &docdbsdk.DescribeGlobalClustersInput{
				GlobalClusterIdentifier: aws.String("gtl"),
			})
			require.NoError(t, err)
			require.Len(t, desc.GlobalClusters, 1)
			require.Len(t, desc.GlobalClusters[0].TagList, 1)
			assert.Equal(t, "env", aws.ToString(desc.GlobalClusters[0].TagList[0].Key))
			assert.Equal(t, "dev", aws.ToString(desc.GlobalClusters[0].TagList[0].Value))

			_, err = client.DeleteGlobalCluster(ctx, &docdbsdk.DeleteGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gtl"),
			})
			require.NoError(t, err)
			again, err := client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gtl"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)
			assert.Empty(t, again.GlobalCluster.TagList)
		})
	}
}
