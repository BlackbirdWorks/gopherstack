package kafka_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func describeClusterVersion(t *testing.T, client *kafkasdk.Client, arn string) *kafkasdk.DescribeClusterOutput {
	t.Helper()

	out, err := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{ClusterArn: aws.String(arn)})
	require.NoError(t, err)

	return out
}

func newMemberClient(t *testing.T) (*kafkasdk.Client, *kafka.InMemoryBackend) {
	t.Helper()

	b := kafka.NewInMemoryBackend("123456789012", "us-east-1")

	return newTestKafkaClient(t, kafka.NewHandler(b)), b
}

func TestDeleteClusterCurrentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		version func(current string) *string
		name    string
		wantErr bool
	}{
		{name: "stale_version_rejected", version: func(string) *string { return aws.String("STALE") }, wantErr: true},
		{name: "current_version_deletes", version: aws.String},
		{name: "omitted_version_deletes", version: func(string) *string { return nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newMemberClient(t)
			cl := b.AddClusterInternal("del-cluster", "3.5.1")
			desc := describeClusterVersion(t, client, cl.ClusterArn)

			_, err := client.DeleteCluster(t.Context(), &kafkasdk.DeleteClusterInput{
				ClusterArn:     aws.String(cl.ClusterArn),
				CurrentVersion: tt.version(aws.ToString(desc.ClusterInfo.CurrentVersion)),
			})

			_, descErr := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{
				ClusterArn: aws.String(cl.ClusterArn),
			})
			if tt.wantErr {
				require.Error(t, err)
				require.NoError(t, descErr)

				return
			}

			require.NoError(t, err)
			require.Error(t, descErr)
		})
	}
}

func TestDeleteReplicatorCurrentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		version func(current string) *string
		name    string
		wantErr bool
	}{
		{name: "stale_version_rejected", version: func(string) *string { return aws.String("STALE") }, wantErr: true},
		{name: "current_version_deletes", version: aws.String},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newMemberClient(t)
			replicator, _, _ := replicationInfoFixtureOn(t, b)

			_, err := client.DeleteReplicator(t.Context(), &kafkasdk.DeleteReplicatorInput{
				ReplicatorArn:  aws.String(replicator.ReplicatorArn),
				CurrentVersion: tt.version(replicator.CurrentVersion),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func replicationInfoFixtureOn(t *testing.T, b *kafka.InMemoryBackend) (*kafka.Replicator, string, string) {
	t.Helper()

	source := b.AddClusterInternal("upd-source", "3.5.1")
	target := b.AddClusterInternal("upd-target", "3.5.1")

	replicator, err := b.CreateReplicator(
		t.Context(), "upd-replicator", "", "arn:aws:iam::000000000000:role/r",
		nil,
		[]kafka.ReplicationInfoConfig{{
			SourceKafkaClusterArn: source.ClusterArn,
			TargetKafkaClusterArn: target.ClusterArn,
		}},
		nil,
		nil,
	)
	require.NoError(t, err)

	return replicator, source.ClusterArn, target.ClusterArn
}

func TestClusterPolicyVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		secondPutVer func(first string) *string
		name         string
		wantErr      bool
	}{
		{
			name:         "stale_version_rejected",
			secondPutVer: func(string) *string { return aws.String("STALE") },
			wantErr:      true,
		},
		{name: "current_version_accepted", secondPutVer: aws.String},
		{name: "omitted_version_accepted", secondPutVer: func(string) *string { return nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newMemberClient(t)
			cl := b.AddClusterInternal("policy-cluster", "3.5.1")

			first, err := client.PutClusterPolicy(t.Context(), &kafkasdk.PutClusterPolicyInput{
				ClusterArn: aws.String(cl.ClusterArn), Policy: aws.String(`{"v":1}`),
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(first.CurrentVersion))

			got, err := client.GetClusterPolicy(t.Context(), &kafkasdk.GetClusterPolicyInput{
				ClusterArn: aws.String(cl.ClusterArn),
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.CurrentVersion), aws.ToString(got.CurrentVersion))

			second, err := client.PutClusterPolicy(t.Context(), &kafkasdk.PutClusterPolicyInput{
				ClusterArn:     aws.String(cl.ClusterArn),
				Policy:         aws.String(`{"v":2}`),
				CurrentVersion: tt.secondPutVer(aws.ToString(first.CurrentVersion)),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEqual(t, aws.ToString(first.CurrentVersion), aws.ToString(second.CurrentVersion))
		})
	}
}

func TestUpdateClusterKafkaVersionConfiguration(t *testing.T) {
	t.Parallel()

	client, b := newMemberClient(t)
	cl := b.AddClusterInternal("kv-cluster", "3.5.1")
	desc := describeClusterVersion(t, client, cl.ClusterArn)

	out, err := client.UpdateClusterKafkaVersion(t.Context(), &kafkasdk.UpdateClusterKafkaVersionInput{
		ClusterArn:         aws.String(cl.ClusterArn),
		CurrentVersion:     desc.ClusterInfo.CurrentVersion,
		TargetKafkaVersion: aws.String("3.6.0"),
		ConfigurationInfo: &types.ConfigurationInfo{
			Arn:      aws.String("arn:aws:kafka:us-east-1:123456789012:configuration/c/1"),
			Revision: aws.Int64(2),
		},
	})
	require.NoError(t, err)

	after := describeClusterVersion(t, client, cl.ClusterArn)

	cur := after.ClusterInfo.CurrentBrokerSoftwareInfo
	require.NotNil(t, cur)
	assert.Equal(t, "3.6.0", aws.ToString(cur.KafkaVersion))
	require.NotNil(t, cur.ConfigurationArn)
	assert.Equal(t, "arn:aws:kafka:us-east-1:123456789012:configuration/c/1", aws.ToString(cur.ConfigurationArn))
	assert.EqualValues(t, 2, aws.ToInt64(cur.ConfigurationRevision))

	op, err := client.DescribeClusterOperation(t.Context(), &kafkasdk.DescribeClusterOperationInput{
		ClusterOperationArn: out.ClusterOperationArn,
	})
	require.NoError(t, err)
	info := op.ClusterOperationInfo
	require.NotNil(t, info.SourceClusterInfo)
	require.NotNil(t, info.TargetClusterInfo)
	assert.Equal(t, "3.5.1", aws.ToString(info.SourceClusterInfo.KafkaVersion))
	assert.Equal(t, "3.6.0", aws.ToString(info.TargetClusterInfo.KafkaVersion))
	require.NotNil(t, info.TargetClusterInfo.ConfigurationInfo)
	assert.EqualValues(t, 2, aws.ToInt64(info.TargetClusterInfo.ConfigurationInfo.Revision))
}

func TestUpdateOperationsRecordSourceAndTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run   func(t *testing.T, c *kafkasdk.Client, arn, version *string) *string
		check func(t *testing.T, src, tgt *types.MutableClusterInfo)
		name  string
	}{
		{
			name: "broker_type",
			run: func(t *testing.T, c *kafkasdk.Client, arn, version *string) *string {
				t.Helper()

				out, err := c.UpdateBrokerType(t.Context(), &kafkasdk.UpdateBrokerTypeInput{
					ClusterArn: arn, CurrentVersion: version, TargetInstanceType: aws.String("kafka.m5.xlarge"),
				})
				require.NoError(t, err)

				return out.ClusterOperationArn
			},
			check: func(t *testing.T, src, tgt *types.MutableClusterInfo) {
				t.Helper()
				require.NotNil(t, src)
				require.NotNil(t, tgt)
				assert.Equal(t, "kafka.m5.xlarge", aws.ToString(tgt.InstanceType))
			},
		},
		{
			name: "zookeeper_access",
			run: func(t *testing.T, c *kafkasdk.Client, arn, version *string) *string {
				t.Helper()

				out, err := c.UpdateConnectivity(t.Context(), &kafkasdk.UpdateConnectivityInput{
					ClusterArn: arn, CurrentVersion: version,
					ZookeeperAccess: &types.ZookeeperAccess{Enabled: aws.Bool(false)},
				})
				require.NoError(t, err)

				return out.ClusterOperationArn
			},
			check: func(t *testing.T, _, tgt *types.MutableClusterInfo) {
				t.Helper()
				require.NotNil(t, tgt)
				require.NotNil(t, tgt.ZookeeperAccess)
				assert.False(t, aws.ToBool(tgt.ZookeeperAccess.Enabled))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newMemberClient(t)
			cl := b.AddClusterInternal("op-cluster", "3.5.1")
			desc := describeClusterVersion(t, client, cl.ClusterArn)

			opArn := tt.run(t, client, aws.String(cl.ClusterArn), desc.ClusterInfo.CurrentVersion)
			op, err := client.DescribeClusterOperation(t.Context(), &kafkasdk.DescribeClusterOperationInput{
				ClusterOperationArn: opArn,
			})
			require.NoError(t, err)

			tt.check(t, op.ClusterOperationInfo.SourceClusterInfo, op.ClusterOperationInfo.TargetClusterInfo)
		})
	}
}

func TestUpdateReplicationInfoLogDelivery(t *testing.T) {
	t.Parallel()

	client, b := newMemberClient(t)
	replicator, sourceArn, targetArn := replicationInfoFixtureOn(t, b)

	_, err := client.UpdateReplicationInfo(t.Context(), &kafkasdk.UpdateReplicationInfoInput{
		ReplicatorArn:         aws.String(replicator.ReplicatorArn),
		CurrentVersion:        aws.String(replicator.CurrentVersion),
		SourceKafkaClusterArn: aws.String(sourceArn),
		TargetKafkaClusterArn: aws.String(targetArn),
		LogDelivery: &types.LogDelivery{ReplicatorLogDelivery: &types.ReplicatorLogDelivery{
			CloudWatchLogs: &types.ReplicatorCloudWatchLogs{
				Enabled: aws.Bool(true), LogGroup: aws.String("/aws/msk/new"),
			},
		}},
	})
	require.NoError(t, err)

	got, err := client.DescribeReplicator(t.Context(), &kafkasdk.DescribeReplicatorInput{
		ReplicatorArn: aws.String(replicator.ReplicatorArn),
	})
	require.NoError(t, err)
	require.NotNil(t, got.LogDelivery)
	require.NotNil(t, got.LogDelivery.ReplicatorLogDelivery.CloudWatchLogs)
	assert.Equal(t, "/aws/msk/new", aws.ToString(got.LogDelivery.ReplicatorLogDelivery.CloudWatchLogs.LogGroup))
}
