package kafka_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestRealClient_ClusterLifecycleCreatingUpdatingActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  []string
		delay time.Duration
	}{
		{name: "instant", delay: 0, want: []string{"ACTIVE", "ACTIVE", "ACTIVE"}},
		{name: "delayed", delay: time.Minute, want: []string{"CREATING", "UPDATING", "ACTIVE"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend("123456789012", "us-east-1")
			now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			b.SetClock(func() time.Time { return now })
			b.SetLifecycleDelay(tt.delay)

			client := newTestKafkaClient(t, kafka.NewHandler(b))
			ctx := t.Context()

			created, err := client.CreateCluster(ctx, &kafkasdk.CreateClusterInput{
				ClusterName: aws.String("lc"), KafkaVersion: aws.String("3.5.1"), NumberOfBrokerNodes: aws.Int32(2),
				BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
					ClientSubnets: []string{"subnet-1", "subnet-2"}, InstanceType: aws.String("kafka.m5.large"),
				},
			})
			require.NoError(t, err)

			state := func() *types.ClusterInfo {
				out, descErr := client.DescribeCluster(
					ctx,
					&kafkasdk.DescribeClusterInput{ClusterArn: created.ClusterArn},
				)
				require.NoError(t, descErr)

				return out.ClusterInfo
			}

			if tt.delay > 0 {
				assert.Equal(t, tt.want[0], string(state().State))
				now = now.Add(2 * tt.delay)
			}

			info := state()
			assert.Equal(t, "ACTIVE", string(info.State))

			op, err := client.UpdateBrokerCount(ctx, &kafkasdk.UpdateBrokerCountInput{
				ClusterArn: created.ClusterArn, CurrentVersion: info.CurrentVersion,
				TargetNumberOfBrokerNodes: aws.Int32(4),
			})
			require.NoError(t, err)

			info = state()
			assert.Equal(t, tt.want[1], string(info.State))

			ops, err := client.ListClusterOperations(
				ctx,
				&kafkasdk.ListClusterOperationsInput{ClusterArn: created.ClusterArn},
			)
			require.NoError(t, err)
			require.Len(t, ops.ClusterOperationInfoList, 1)
			assert.Equal(
				t,
				aws.ToString(op.ClusterOperationArn),
				aws.ToString(ops.ClusterOperationInfoList[0].OperationArn),
			)

			wantOp := "UPDATE_COMPLETE"
			if tt.delay > 0 {
				wantOp = "UPDATE_IN_PROGRESS"
				assert.Equal(t, aws.ToString(op.ClusterOperationArn), aws.ToString(info.ActiveOperationArn))
				assert.Nil(t, ops.ClusterOperationInfoList[0].EndTime)
			}

			assert.Equal(t, wantOp, aws.ToString(ops.ClusterOperationInfoList[0].OperationState))

			now = now.Add(2 * tt.delay)

			assert.Equal(t, tt.want[2], string(state().State))
			assert.Nil(t, state().ActiveOperationArn)

			done, err := client.DescribeClusterOperation(ctx, &kafkasdk.DescribeClusterOperationInput{
				ClusterOperationArn: op.ClusterOperationArn,
			})
			require.NoError(t, err)
			assert.Equal(t, "UPDATE_COMPLETE", aws.ToString(done.ClusterOperationInfo.OperationState))
			assert.NotNil(t, done.ClusterOperationInfo.EndTime)
		})
	}
}
