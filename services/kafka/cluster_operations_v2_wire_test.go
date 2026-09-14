package kafka_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestClusterOperationV2_WireShape proves DescribeClusterOperationV2 and
// ListClusterOperationsV2 emit the real ClusterOperationV2/
// ClusterOperationV2Summary shape, both through a real typed client and on
// the raw wire body: source/target cluster info nests under provisioned,
// never at the top level (gopherstack-mk3t item 2).
func TestClusterOperationV2_WireShape(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend(testAccountID, testRegion))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("v2-wire-shape-cluster"),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: aws.Int32(3),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)

	updated, err := client.UpdateBrokerCount(t.Context(), &kafkasdk.UpdateBrokerCountInput{
		ClusterArn:                created.ClusterArn,
		CurrentVersion:            described.ClusterInfo.CurrentVersion,
		TargetNumberOfBrokerNodes: aws.Int32(4),
	})
	require.NoError(t, err)

	clusterArn := aws.ToString(created.ClusterArn)
	opArn := aws.ToString(updated.ClusterOperationArn)

	tests := []struct {
		name    string
		rawPath string
	}{
		{name: "describe", rawPath: "/api/v2/operations/" + url.PathEscape(opArn)},
		{name: "list", rawPath: "/api/v2/clusters/" + url.PathEscape(clusterArn) + "/operations"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body, code := doKafkaRequestJSON(t, h, http.MethodGet, tc.rawPath, nil)
			require.Equal(t, http.StatusOK, code)

			var record map[string]any

			switch tc.name {
			case "describe":
				info, ok := body["clusterOperationInfo"].(map[string]any)
				require.True(t, ok)
				record = info

				provisioned, provOK := record["provisioned"].(map[string]any)
				require.True(t, provOK, "sourceClusterInfo/targetClusterInfo must nest under provisioned")
				assert.Contains(t, provisioned, "sourceClusterInfo")
				assert.Contains(t, provisioned, "targetClusterInfo")
				assert.NotContains(t, record, "serverless")

				opDesc, descErr := client.DescribeClusterOperationV2(
					t.Context(),
					&kafkasdk.DescribeClusterOperationV2Input{ClusterOperationArn: aws.String(opArn)},
				)
				require.NoError(t, descErr)
				require.NotNil(t, opDesc.ClusterOperationInfo)

				typed := opDesc.ClusterOperationInfo
				assert.Equal(t, types.ClusterTypeProvisioned, typed.ClusterType)
				require.NotNil(t, typed.Provisioned)
				assert.Equal(t, int32(3), aws.ToInt32(typed.Provisioned.SourceClusterInfo.NumberOfBrokerNodes))
				assert.Equal(t, int32(4), aws.ToInt32(typed.Provisioned.TargetClusterInfo.NumberOfBrokerNodes))
				assert.Nil(t, typed.Serverless)
			case "list":
				list, ok := body["clusterOperationInfoList"].([]any)
				require.True(t, ok)
				require.Len(t, list, 1)
				item, itemOK := list[0].(map[string]any)
				require.True(t, itemOK)
				record = item

				listed, listErr := client.ListClusterOperationsV2(
					t.Context(),
					&kafkasdk.ListClusterOperationsV2Input{ClusterArn: aws.String(clusterArn)},
				)
				require.NoError(t, listErr)
				require.Len(t, listed.ClusterOperationInfoList, 1)
				assert.Equal(t, types.ClusterTypeProvisioned, listed.ClusterOperationInfoList[0].ClusterType)
			}

			assert.Equal(t, "PROVISIONED", record["clusterType"])
			assert.NotContains(t, record, "sourceClusterInfo",
				"real ClusterOperationV2/Summary has no top-level sourceClusterInfo")
			assert.NotContains(t, record, "targetClusterInfo",
				"real ClusterOperationV2/Summary has no top-level targetClusterInfo")
		})
	}
}
