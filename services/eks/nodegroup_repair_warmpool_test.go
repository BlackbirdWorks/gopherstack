package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodegroup_NodeRepairAndWarmPoolConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		repair      *ekstypes.NodeRepairConfig
		warm        *ekstypes.WarmPoolConfig
		name        string
		wantInvalid bool
	}{
		{
			name: "both_round_trip",
			repair: &ekstypes.NodeRepairConfig{
				Enabled:                        aws.Bool(true),
				MaxUnhealthyNodeThresholdCount: aws.Int32(3),
				NodeRepairConfigOverrides: []ekstypes.NodeRepairConfigOverrides{{
					MinRepairWaitTimeMins:   aws.Int32(10),
					NodeMonitoringCondition: aws.String("AcceleratedHardwareReady"),
					NodeUnhealthyReason:     aws.String("NvidiaXID13Error"),
					RepairAction:            ekstypes.RepairActionReboot,
				}},
			},
			warm: &ekstypes.WarmPoolConfig{
				Enabled:                  aws.Bool(true),
				MinSize:                  aws.Int32(2),
				MaxGroupPreparedCapacity: aws.Int32(8),
				PoolState:                ekstypes.WarmPoolStateRunning,
				ReuseOnScaleIn:           aws.Bool(true),
			},
		},
		{
			name: "repair_count_and_percentage_conflict",
			repair: &ekstypes.NodeRepairConfig{
				MaxParallelNodesRepairedCount:      aws.Int32(1),
				MaxParallelNodesRepairedPercentage: aws.Int32(10),
			},
			wantInvalid: true,
		},
		{
			name: "threshold_count_and_percentage_conflict",
			repair: &ekstypes.NodeRepairConfig{
				MaxUnhealthyNodeThresholdCount:      aws.Int32(1),
				MaxUnhealthyNodeThresholdPercentage: aws.Int32(10),
			},
			wantInvalid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newTestEKSHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:               aws.String("repair-cluster"),
				RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
			})
			require.NoError(t, err)

			in := &ekssdk.CreateNodegroupInput{
				ClusterName:      aws.String("repair-cluster"),
				NodegroupName:    aws.String("ng1"),
				NodeRole:         aws.String("arn:aws:iam::123456789012:role/ng-role"),
				Subnets:          []string{"subnet-abc123"},
				NodeRepairConfig: tt.repair,
				WarmPoolConfig:   tt.warm,
			}

			created, err := client.CreateNodegroup(ctx, in)
			if tt.wantInvalid {
				var ipe *ekstypes.InvalidParameterException
				require.ErrorAs(t, err, &ipe)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.repair, created.Nodegroup.NodeRepairConfig)
			assert.Equal(t, tt.warm, created.Nodegroup.WarmPoolConfig)

			_, err = client.UpdateNodegroupConfig(ctx, &ekssdk.UpdateNodegroupConfigInput{
				ClusterName:   aws.String("repair-cluster"),
				NodegroupName: aws.String("ng1"),
				WarmPoolConfig: &ekstypes.WarmPoolConfig{
					MinSize: aws.Int32(4),
				},
				NodeRepairConfig: &ekstypes.NodeRepairConfig{Enabled: aws.Bool(false)},
			})
			require.NoError(t, err)

			got, err := client.DescribeNodegroup(ctx, &ekssdk.DescribeNodegroupInput{
				ClusterName:   aws.String("repair-cluster"),
				NodegroupName: aws.String("ng1"),
			})
			require.NoError(t, err)

			assert.Equal(t, &ekstypes.NodeRepairConfig{Enabled: aws.Bool(false)}, got.Nodegroup.NodeRepairConfig)
			require.NotNil(t, got.Nodegroup.WarmPoolConfig)
			assert.Equal(t, int32(4), aws.ToInt32(got.Nodegroup.WarmPoolConfig.MinSize))
			assert.True(t, aws.ToBool(got.Nodegroup.WarmPoolConfig.Enabled), "Enabled preserved when omitted on update")
		})
	}
}
