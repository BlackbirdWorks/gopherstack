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

func TestRealClient_BatchScramSecretEchoesClusterArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		associate bool
	}{
		{name: "associate", associate: true},
		{name: "disassociate", associate: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKafkaClient(t, kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			created, err := client.CreateCluster(ctx, &kafkasdk.CreateClusterInput{
				ClusterName: aws.String("sc"), KafkaVersion: aws.String("3.5.1"), NumberOfBrokerNodes: aws.Int32(2),
				BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
					ClientSubnets: []string{"subnet-1", "subnet-2"}, InstanceType: aws.String("kafka.m5.large"),
				},
			})
			require.NoError(t, err)

			secrets := []string{"arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_s-abc123"}

			var got *string

			if tt.associate {
				out, callErr := client.BatchAssociateScramSecret(ctx, &kafkasdk.BatchAssociateScramSecretInput{
					ClusterArn: created.ClusterArn, SecretArnList: secrets,
				})
				require.NoError(t, callErr)

				got = out.ClusterArn
			} else {
				out, callErr := client.BatchDisassociateScramSecret(ctx, &kafkasdk.BatchDisassociateScramSecretInput{
					ClusterArn: created.ClusterArn, SecretArnList: secrets,
				})
				require.NoError(t, callErr)

				got = out.ClusterArn
			}

			assert.Equal(t, aws.ToString(created.ClusterArn), aws.ToString(got))
		})
	}
}
