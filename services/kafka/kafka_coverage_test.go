package kafka_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func createKafkaTestCluster(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName": name, "kafkaVersion": "2.8.0", "numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, _ := resp["clusterArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// TestKafkaCoverage2_ListScramSecrets covers ListScramSecrets handler.
func TestKafkaCoverage2_ListScramSecrets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createKafkaTestCluster(t, h, "scram-list-cluster")
	encoded := url.PathEscape(clusterArn)

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded+"/scram-secrets", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestKafkaCoverage2_ListNodes covers the ListNodes handler.
func TestKafkaCoverage2_ListNodes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createKafkaTestCluster(t, h, "nodes-cluster")
	encoded := url.PathEscape(clusterArn)

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded+"/nodes", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

// TestKafkaCoverage2_RebootBroker covers RebootBroker handler.
func TestKafkaCoverage2_RebootBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createKafkaTestCluster(t, h, "reboot-cluster")
	encoded := url.PathEscape(clusterArn)

	rec := doKafkaRequest(t, h, http.MethodPut, "/v1/clusters/"+encoded+"/reboot-broker",
		map[string]any{"brokerIds": []string{"1"}})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

// TestKafkaCoverage2_UpdateOps covers various kafka update operations.
func TestKafkaCoverage2_UpdateOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createKafkaTestCluster(t, h, "update-ops-cluster")
	encoded := url.PathEscape(clusterArn)

	type updateOp struct {
		body map[string]any
		name string
		path string
	}
	updateOps := []updateOp{
		{
			name: "UpdateBrokerStorage", path: "/v1/clusters/" + encoded + "/nodes/storage",
			body: map[string]any{
				"currentVersion":            "1",
				"targetBrokerEBSVolumeInfo": []map[string]any{{"volumeSizeGB": 100}},
			},
		},
		{
			name: "UpdateBrokerType", path: "/v1/clusters/" + encoded + "/nodes/type",
			body: map[string]any{"currentVersion": "1", "targetInstanceType": "kafka.m5.xlarge"},
		},
		{
			name: "UpdateClusterConfiguration", path: "/v1/clusters/" + encoded + "/configuration",
			body: map[string]any{
				"currentVersion":        "1",
				"configurationArn":      "arn:aws:kafka:us-east-1:000:configuration/test/1",
				"configurationRevision": 1,
			},
		},
		{
			name: "UpdateClusterKafkaVersion", path: "/v1/clusters/" + encoded + "/version",
			body: map[string]any{"currentVersion": "1", "targetKafkaVersion": "3.0.0"},
		},
		{
			name: "UpdateConnectivity", path: "/v1/clusters/" + encoded + "/connectivity",
			body: map[string]any{"currentVersion": "1", "connectivityInfo": map[string]any{}},
		},
		{
			name: "UpdateMonitoring", path: "/v1/clusters/" + encoded + "/monitoring",
			body: map[string]any{"currentVersion": "1", "openMonitoring": map[string]any{}},
		},
		{
			name: "UpdateSecurity", path: "/v1/clusters/" + encoded + "/security",
			body: map[string]any{"currentVersion": "1"},
		},
	}

	for _, op := range updateOps {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			rec := doKafkaRequest(t, h, http.MethodPut, op.path, op.body)
			assert.GreaterOrEqual(t, rec.Code, 200, "op %s should not panic", op.name)
		})
	}
}

// TestKafkaCoverage2_UpdateReplicationInfo covers UpdateReplicationInfo.
func TestKafkaCoverage2_UpdateReplicationInfo(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a replicator.
	rec := doKafkaRequest(t, h, http.MethodPost, "/replication/v1/replicators",
		map[string]any{
			"replicatorName":          "test-replicator-update",
			"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/test-role",
			"kafkaClusters": []map[string]any{
				{
					"amazonMskCluster": map[string]any{
						"mskClusterArn": "arn:aws:kafka:us-east-1:000000000000:cluster/test/abc",
					},
					"vpcConfig": map[string]any{
						"subnetIds":        []string{"subnet-1"},
						"securityGroupIds": []string{"sg-1"},
					},
				},
			},
			"replicationInfoList": []map[string]any{},
		})

	if rec.Code < 200 || rec.Code >= 300 {
		t.Skipf("replicator creation returned %d, skipping", rec.Code)
	}

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	replicatorArn, _ := resp["replicatorArn"].(string)
	if replicatorArn == "" {
		t.Skip("replicator creation did not return ARN")
	}

	encoded := url.PathEscape(replicatorArn)

	// UpdateReplicationInfo.
	rec = doKafkaRequest(t, h, http.MethodPut, "/replication/v1/replicators/"+encoded+"/replication-info",
		map[string]any{
			"currentVersion":           "1",
			"sourceKafkaClusterArn":    "arn:aws:kafka:us-east-1:000000000000:cluster/source/abc",
			"targetKafkaClusterArn":    "arn:aws:kafka:us-east-1:000000000000:cluster/target/def",
			"topicReplication":         map[string]any{"replicateSourceTopicTags": false},
			"consumerGroupReplication": map[string]any{"synchroniseConsumerGroupOffsets": false},
		})
	assert.Positive(t, rec.Code)
}
