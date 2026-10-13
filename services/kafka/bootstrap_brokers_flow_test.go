package kafka_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestRealClient_GetBootstrapBrokersFollowsTopologyAndEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clientBroker types.ClientBroker
		wantPlain    bool
		wantTLS      bool
	}{
		{name: "default", wantTLS: true},
		{name: "tls", clientBroker: types.ClientBrokerTls, wantTLS: true},
		{name: "plaintext", clientBroker: types.ClientBrokerPlaintext, wantPlain: true},
		{name: "both", clientBroker: types.ClientBrokerTlsPlaintext, wantPlain: true, wantTLS: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKafkaClient(t, kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			in := &kafkasdk.CreateClusterInput{
				ClusterName:         aws.String("bs"),
				KafkaVersion:        aws.String("3.5.1"),
				NumberOfBrokerNodes: aws.Int32(2),
				BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
					ClientSubnets: []string{"subnet-1", "subnet-2"},
					InstanceType:  aws.String("kafka.m5.large"),
				},
			}
			if tt.clientBroker != "" {
				in.EncryptionInfo = &types.EncryptionInfo{
					EncryptionInTransit: &types.EncryptionInTransit{
						ClientBroker: tt.clientBroker,
						InCluster:    aws.Bool(true),
					},
				}
			}

			created, err := client.CreateCluster(ctx, in)
			require.NoError(t, err)

			check := func(brokers int) {
				out, getErr := client.GetBootstrapBrokers(
					ctx,
					&kafkasdk.GetBootstrapBrokersInput{ClusterArn: created.ClusterArn},
				)
				require.NoError(t, getErr)

				for _, c := range []struct {
					got  *string
					port string
					want bool
				}{
					{out.BootstrapBrokerString, ":9092", tt.wantPlain},
					{out.BootstrapBrokerStringTls, ":9094", tt.wantTLS},
				} {
					if !c.want {
						assert.Empty(t, aws.ToString(c.got))

						continue
					}

					hosts := strings.Split(aws.ToString(c.got), ",")
					require.Len(t, hosts, brokers)

					for i, h := range hosts {
						assert.True(t, strings.HasPrefix(h, "b-"+string(rune('1'+i))+".bs."), h)
						assert.True(t, strings.HasSuffix(h, ".kafka.us-east-1.amazonaws.com"+c.port), h)
					}
				}
			}

			check(2)

			desc, err := client.DescribeCluster(ctx, &kafkasdk.DescribeClusterInput{ClusterArn: created.ClusterArn})
			require.NoError(t, err)

			_, err = client.UpdateBrokerCount(ctx, &kafkasdk.UpdateBrokerCountInput{
				ClusterArn: created.ClusterArn, CurrentVersion: desc.ClusterInfo.CurrentVersion,
				TargetNumberOfBrokerNodes: aws.Int32(4),
			})
			require.NoError(t, err)

			check(4)
		})
	}
}

func TestRealClient_GetBootstrapBrokersServerless(t *testing.T) {
	t.Parallel()

	client := newTestKafkaClient(t, kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1")))
	ctx := t.Context()

	created, err := client.CreateClusterV2(ctx, &kafkasdk.CreateClusterV2Input{
		ClusterName: aws.String("sl"),
		Serverless: &types.ServerlessRequest{
			VpcConfigs: []types.VpcConfig{{SubnetIds: []string{"subnet-1"}, SecurityGroupIds: []string{"sg-1"}}},
			ClientAuthentication: &types.ServerlessClientAuthentication{
				Sasl: &types.ServerlessSasl{Iam: &types.Iam{Enabled: aws.Bool(true)}},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetBootstrapBrokers(ctx, &kafkasdk.GetBootstrapBrokersInput{ClusterArn: created.ClusterArn})
	require.NoError(t, err)

	assert.Empty(t, aws.ToString(out.BootstrapBrokerString))
	assert.Empty(t, aws.ToString(out.BootstrapBrokerStringTls))
	assert.True(t, strings.HasPrefix(aws.ToString(out.BootstrapBrokerStringSaslIam), "boot-"))
	assert.True(
		t,
		strings.HasSuffix(
			aws.ToString(out.BootstrapBrokerStringSaslIam),
			".kafka-serverless.us-east-1.amazonaws.com:9098",
		),
	)
}
