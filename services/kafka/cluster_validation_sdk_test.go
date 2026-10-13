package kafka_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_CreateClusterValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cluster  string
		version  string
		instance string
		wantCode string
	}{
		{name: "valid", cluster: "k-1", version: "3.5.1", instance: "kafka.m5.large"},
		{name: "kraft_version", cluster: "k-1", version: "3.8.0.kraft", instance: "express.m7g.large"},
		{
			name:     "name_space",
			cluster:  "bad name",
			version:  "3.5.1",
			instance: "kafka.m5.large",
			wantCode: "BadRequestException",
		},
		{
			name:     "name_too_long",
			cluster:  strings.Repeat("a", 65),
			version:  "3.5.1",
			instance: "kafka.m5.large",
			wantCode: "BadRequestException",
		},
		{
			name:     "version_garbage",
			cluster:  "k-1",
			version:  "latest",
			instance: "kafka.m5.large",
			wantCode: "BadRequestException",
		},
		{
			name:     "instance_prefix",
			cluster:  "k-1",
			version:  "3.5.1",
			instance: "m5.large",
			wantCode: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKafkaClient(t, newTestHandler(t))
			_, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
				ClusterName:         aws.String(tt.cluster),
				KafkaVersion:        aws.String(tt.version),
				NumberOfBrokerNodes: aws.Int32(3),
				BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
					ClientSubnets: []string{"subnet-1", "subnet-2", "subnet-3"},
					InstanceType:  aws.String(tt.instance),
				},
			})

			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func TestSDK_ErrorMessagesAreDescriptive(t *testing.T) {
	t.Parallel()

	client := newTestKafkaClient(t, newTestHandler(t))
	ctx := t.Context()

	create := func() error {
		_, err := client.CreateCluster(ctx, &kafkasdk.CreateClusterInput{
			ClusterName: aws.String("dup"), KafkaVersion: aws.String("3.5.1"), NumberOfBrokerNodes: aws.Int32(3),
			BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
				ClientSubnets: []string{"subnet-1", "subnet-2", "subnet-3"}, InstanceType: aws.String("kafka.m5.large"),
			},
		})

		return err
	}
	require.NoError(t, create())

	tests := []struct {
		err      error
		name     string
		wantCode string
	}{
		{name: "conflict", err: create(), wantCode: "ConflictException"},
		{
			name: "not_found", wantCode: "NotFoundException",
			err: func() error {
				_, err := client.DescribeCluster(ctx, &kafkasdk.DescribeClusterInput{
					ClusterArn: aws.String("arn:aws:kafka:us-east-1:123456789012:cluster/nope/abc"),
				})

				return err
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var apiErr smithy.APIError
			require.ErrorAs(t, tt.err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception")
			assert.NotEmpty(t, apiErr.ErrorMessage())
		})
	}
}

func TestSDK_ListNodesARNShape(t *testing.T) {
	t.Parallel()

	client := newTestKafkaClient(t, newTestHandler(t))
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &kafkasdk.CreateClusterInput{
		ClusterName: aws.String("nodes"), KafkaVersion: aws.String("3.5.1"), NumberOfBrokerNodes: aws.Int32(2),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"}, InstanceType: aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	nodes, err := client.ListNodes(ctx, &kafkasdk.ListNodesInput{ClusterArn: created.ClusterArn})
	require.NoError(t, err)
	require.Len(t, nodes.NodeInfoList, 2)

	clusterArn := aws.ToString(created.ClusterArn)
	prefix, suffix, _ := strings.Cut(clusterArn, ":cluster/")
	for i, n := range nodes.NodeInfoList {
		arn := aws.ToString(n.NodeARN)
		assert.Equal(t, 1, strings.Count(arn, "arn:"), arn)
		assert.True(t, strings.HasPrefix(arn, prefix+":broker/"+suffix+"/"), arn)
		assert.True(t, strings.HasSuffix(arn, "/"+string(rune('1'+i))), arn)
	}
}

func TestSDK_ListClustersMaxResultsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		max     int32
		wantErr bool
	}{
		{name: "in_range", max: 5},
		{name: "upper", max: 100},
		{name: "too_large", max: 101, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKafkaClient(t, newTestHandler(t))
			_, err := client.ListClusters(t.Context(), &kafkasdk.ListClustersInput{MaxResults: aws.Int32(tt.max)})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "BadRequestException", apiErr.ErrorCode())
		})
	}
}
