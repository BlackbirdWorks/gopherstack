package kafka_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestParseKafkaPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
	}{
		{
			name:   "list_clusters_v1",
			method: http.MethodGet,
			path:   "/v1/clusters",
			wantOp: "ListClusters",
		},
		{
			name:   "create_cluster_v1",
			method: http.MethodPost,
			path:   "/v1/clusters",
			wantOp: "CreateCluster",
		},
		{
			name:         "describe_cluster_v1",
			method:       http.MethodGet,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "DescribeCluster",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "delete_cluster_v1",
			method:       http.MethodDelete,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "DeleteCluster",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "bootstrap_brokers",
			method:       http.MethodGet,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/bootstrap-brokers",
			wantOp:       "GetBootstrapBrokers",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:   "list_clusters_v2",
			method: http.MethodGet,
			path:   "/api/v2/clusters",
			wantOp: "ListClustersV2",
		},
		{
			name:   "create_cluster_v2",
			method: http.MethodPost,
			path:   "/api/v2/clusters",
			wantOp: "CreateClusterV2",
		},
		{
			name:   "list_configurations",
			method: http.MethodGet,
			path:   "/v1/configurations",
			wantOp: "ListConfigurations",
		},
		{
			name:   "create_configuration",
			method: http.MethodPost,
			path:   "/v1/configurations",
			wantOp: "CreateConfiguration",
		},
		{
			name:         "describe_configuration",
			method:       http.MethodGet,
			path:         "/v1/configurations/arn:aws:kafka:us-east-1:000000000000:configuration/my-config/uuid-1",
			wantOp:       "DescribeConfiguration",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:configuration/my-config/uuid-1",
		},
		{
			name:         "list_tags",
			method:       http.MethodGet,
			path:         "/v1/tags/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "ListTagsForResource",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "tag_resource",
			method:       http.MethodPost,
			path:         "/v1/tags/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "TagResource",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "untag_resource",
			method:       http.MethodDelete,
			path:         "/v1/tags/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			wantOp:       "UntagResource",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:   "unknown_path",
			method: http.MethodGet,
			path:   "/unknown/path",
			wantOp: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, resource := kafka.ParseKafkaPathForTest(tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

func TestParseKafkaPathScramReplicatorTopicVpcOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
	}{
		{
			name:         "batch_associate_scram",
			method:       http.MethodPost,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/scram-secrets",
			wantOp:       "BatchAssociateScramSecret",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "batch_disassociate_scram",
			method:       http.MethodPatch,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/scram-secrets",
			wantOp:       "BatchDisassociateScramSecret",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "create_topic",
			method:       http.MethodPost,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/topics",
			wantOp:       "CreateTopic",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "delete_topic",
			method:       http.MethodDelete,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/topics/my-topic",
			wantOp:       "DeleteTopic",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1|my-topic",
		},
		{
			name:         "delete_cluster_policy",
			method:       http.MethodDelete,
			path:         "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1/policy",
			wantOp:       "DeleteClusterPolicy",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
		},
		{
			name:         "describe_cluster_operation",
			method:       http.MethodGet,
			path:         "/v1/operations/arn:aws:kafka:us-east-1:000000000000:cluster-operation/uuid-1",
			wantOp:       "DescribeClusterOperation",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:cluster-operation/uuid-1",
		},
		{
			name:   "create_replicator",
			method: http.MethodPost,
			path:   "/replication/v1/replicators",
			wantOp: "CreateReplicator",
		},
		{
			name:         "delete_replicator",
			method:       http.MethodDelete,
			path:         "/replication/v1/replicators/arn:aws:kafka:us-east-1:000000000000:replicator/test/uuid-1",
			wantOp:       "DeleteReplicator",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:replicator/test/uuid-1",
		},
		{
			name:   "create_vpc_connection",
			method: http.MethodPost,
			path:   "/v1/vpc-connection",
			wantOp: "CreateVpcConnection",
		},
		{
			name:         "delete_vpc_connection",
			method:       http.MethodDelete,
			path:         "/v1/vpc-connection/arn:aws:kafka:us-east-1:000000000000:vpc-connection/uuid-1",
			wantOp:       "DeleteVpcConnection",
			wantResource: "arn:aws:kafka:us-east-1:000000000000:vpc-connection/uuid-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, resource := kafka.ParseKafkaPathForTest(tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

// ----------------------------------------
// Additional tests to improve coverage
// ----------------------------------------

func TestUpdateOpsLiveUnderV1ClustersPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		suffix string
		method string
	}{
		{name: "broker_count", suffix: "/nodes/count", method: http.MethodPut,
			body: map[string]any{"targetNumberOfBrokerNodes": int32(6)}},
		{name: "broker_storage", suffix: "/nodes/storage", method: http.MethodPut,
			body: map[string]any{"targetBrokerEBSVolumeInfo": []map[string]any{{"volumeSizeGB": int32(200)}}}},
		{name: "broker_type", suffix: "/nodes/type", method: http.MethodPut,
			body: map[string]any{"targetInstanceType": "kafka.m5.xlarge"}},
		{name: "cluster_kafka_version", suffix: "/version", method: http.MethodPut,
			body: map[string]any{"targetKafkaVersion": "3.5.1"}},
		{name: "connectivity", suffix: "/connectivity", method: http.MethodPut, body: map[string]any{}},
		{name: "monitoring", suffix: "/monitoring", method: http.MethodPut, body: map[string]any{}},
		{name: "rebalancing", suffix: "/rebalancing", method: http.MethodPut, body: map[string]any{}},
		{name: "security", suffix: "/security", method: http.MethodPatch, body: map[string]any{}},
		{name: "storage", suffix: "/storage", method: http.MethodPut, body: map[string]any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clusterArn := createTestCluster(t, h, "route-fix-"+tt.name)
			encoded := url.PathEscape(clusterArn)

			body := map[string]any{"currentVersion": kafka.DefaultClusterVersion}
			maps.Copy(body, tt.body)

			rec := doKafkaRequest(t, h, tt.method, "/v1/clusters/"+encoded+tt.suffix, body)
			require.Equal(t, http.StatusOK, rec.Code, "method=%s suffix=%s body=%s",
				tt.method, tt.suffix, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["clusterOperationArn"])

			// The old (wrong) /api/v2 path must no longer resolve to the same op --
			// it should now 404 as an unrecognized route.
			v2Rec := doKafkaRequest(t, h, tt.method, "/api/v2/clusters/"+encoded+tt.suffix, body)
			assert.Equal(t, http.StatusNotFound, v2Rec.Code,
				"the real API has no Update* ops under /api/v2/clusters")
		})
	}
}

// TestRouteFix_RejectClientVpcConnection_RealPath verifies RejectClientVpcConnection
// is reachable at PUT /v1/clusters/{ClusterArn}/client-vpc-connection (singular,
// cluster-scoped) with the target vpcConnectionArn carried in the JSON body --
// not in the path, and not under a "/reject-client-vpc-connection/..." path.

func TestRejectClientVpcConnectionRealPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "route-fix-reject-vpc")

	createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/vpc-connection", map[string]any{
		"targetClusterArn": clusterArn,
		"vpcId":            "vpc-route-fix",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	vpcConnArn, _ := createResp["vpcConnectionArn"].(string)
	require.NotEmpty(t, vpcConnArn)

	rec := doKafkaRequest(t, h, http.MethodPut,
		"/v1/clusters/"+url.PathEscape(clusterArn)+"/client-vpc-connection",
		map[string]any{"vpcConnectionArn": vpcConnArn})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// A missing vpcConnectionArn in the body must be rejected, not silently no-op.
	badRec := doKafkaRequest(t, h, http.MethodPut,
		"/v1/clusters/"+url.PathEscape(clusterArn)+"/client-vpc-connection",
		map[string]any{})
	assert.Equal(t, http.StatusBadRequest, badRec.Code)
}

// TestRouteFix_ListVpcConnections_PluralPath verifies ListVpcConnections is
// reachable at GET /v1/vpc-connections (plural) -- a distinct root from the
// singular /v1/vpc-connection used by Create/Describe/Delete.

func TestListVpcConnectionsPluralPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "route-fix-list-vpc")

	createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/vpc-connection", map[string]any{
		"targetClusterArn": clusterArn,
		"vpcId":            "vpc-list-fix",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/vpc-connections", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	conns, _ := resp["vpcConnections"].([]any)
	assert.Len(t, conns, 1)

	// The singular root only accepts POST (CreateVpcConnection); GET there is
	// not a real MSK operation and must 404, not silently alias to the list.
	singularRec := doKafkaRequest(t, h, http.MethodGet, "/v1/vpc-connection", nil)
	assert.Equal(t, http.StatusNotFound, singularRec.Code)
}

// TestRouteFix_GetCompatibleKafkaVersions_TopLevelPath verifies
// GetCompatibleKafkaVersions is reachable at the top-level GET
// /v1/compatible-kafka-versions?clusterArn=... path (clusterArn is a query
// parameter, not a path segment nested under /v1/clusters/{arn}/...).

func TestGetCompatibleKafkaVersionsTopLevelPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "route-fix-compat-versions")

	rec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/compatible-kafka-versions?clusterArn="+url.QueryEscape(clusterArn), nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions, _ := resp["compatibleKafkaVersions"].([]any)
	assert.NotEmpty(t, versions)

	// The old (wrong) cluster-nested path must no longer resolve.
	oldRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+url.PathEscape(clusterArn)+"/compatible-kafka-versions", nil)
	assert.Equal(t, http.StatusNotFound, oldRec.Code)
}

// TestRouteFix_DescribeTopicPartitions_RealPath verifies DescribeTopicPartitions
// is reachable at GET /v1/clusters/{ClusterArn}/topics/{TopicName}/partitions
// (nested under the topic), not a sibling "/topic-partitions/{TopicName}" path.

func TestDescribeTopicPartitionsRealPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "route-fix-topic-partitions")
	encodedCluster := url.PathEscape(clusterArn)

	createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters/"+encodedCluster+"/topics",
		map[string]any{"topicName": "orders", "replicationFactor": 1, "partitionCount": 3})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encodedCluster+"/topics/orders/partitions", nil)
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

// TestRouteFix_UpdateReplicationInfo_StripsSuffix verifies UpdateReplicationInfo
// resolves the replicator ARN correctly from .../replicators/{ReplicatorArn}/replication-info
// (the "/replication-info" suffix must be stripped, not treated as part of the ARN).

func TestUpdateReplicationInfoStripsSuffix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	replicatorArn, currentVersion, sourceArn, targetArn := createTestReplicatorWithTopology(
		t, h, "route-fix-replicator",
	)

	rec := doKafkaRequest(t, h, http.MethodPut,
		"/replication/v1/replicators/"+url.PathEscape(replicatorArn)+"/replication-info",
		map[string]any{
			"currentVersion":        currentVersion,
			"sourceKafkaClusterArn": sourceArn,
			"targetKafkaClusterArn": targetArn,
		})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, replicatorArn, resp["replicatorArn"],
		"the resolved replicator ARN must not include the /replication-info suffix")
}
