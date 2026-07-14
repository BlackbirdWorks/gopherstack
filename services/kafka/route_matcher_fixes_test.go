package kafka_test

// route_matcher_fixes_test.go — regression coverage for MSK route-matcher bugs
// found during the parity audit: real aws-sdk-go-v2/service/kafka clients send
// several operations to paths gopherstack's RouteMatcher/parser did not
// recognize, making those ops 100% unreachable despite handlers existing for
// them. Each test below drives the exact method+path the real SDK serializer
// emits (verified against aws-sdk-go-v2/service/kafka@v1.49.0/serializers.go)
// through the full Handler()/RouteMatcher() stack, not by calling the handler
// method directly, so a regression here would be caught the same way a real
// client would hit it.

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

// rmCreateCluster creates a cluster via the real HTTP surface and returns its ARN.
func rmCreateCluster(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName":         name,
		"kafkaVersion":        "2.8.0",
		"numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create cluster: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, _ := resp["clusterArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// TestRouteFix_UpdateOps_LiveUnderV1ClustersPath verifies every cluster Update*
// operation is reachable at its real aws-sdk-go-v2 path (/v1/clusters/{arn}/...),
// not the /api/v2/clusters/{arn}/... path this handler previously (and
// incorrectly) required. UpdateSecurity additionally uses PATCH, not PUT.
func TestRouteFix_UpdateOps_LiveUnderV1ClustersPath(t *testing.T) {
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
			clusterArn := rmCreateCluster(t, h, "route-fix-"+tt.name)
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
func TestRouteFix_RejectClientVpcConnection_RealPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := rmCreateCluster(t, h, "route-fix-reject-vpc")

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
func TestRouteFix_ListVpcConnections_PluralPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := rmCreateCluster(t, h, "route-fix-list-vpc")

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
func TestRouteFix_GetCompatibleKafkaVersions_TopLevelPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := rmCreateCluster(t, h, "route-fix-compat-versions")

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
func TestRouteFix_DescribeTopicPartitions_RealPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := rmCreateCluster(t, h, "route-fix-topic-partitions")
	encodedCluster := url.PathEscape(clusterArn)

	createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters/"+encodedCluster+"/topics",
		map[string]any{"topicName": "orders", "replicationFactor": 1, "numPartitions": 3})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encodedCluster+"/topics/orders/partitions", nil)
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

// TestRouteFix_UpdateReplicationInfo_StripsSuffix verifies UpdateReplicationInfo
// resolves the replicator ARN correctly from .../replicators/{ReplicatorArn}/replication-info
// (the "/replication-info" suffix must be stripped, not treated as part of the ARN).
func TestRouteFix_UpdateReplicationInfo_StripsSuffix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doKafkaRequest(t, h, http.MethodPost, "/replication/v1/replicators", map[string]any{
		"replicatorName":          "route-fix-replicator",
		"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/r",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	replicatorArn, _ := createResp["replicatorArn"].(string)
	require.NotEmpty(t, replicatorArn)

	rec := doKafkaRequest(t, h, http.MethodPut,
		"/replication/v1/replicators/"+url.PathEscape(replicatorArn)+"/replication-info",
		map[string]any{"description": "updated"})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, replicatorArn, resp["replicatorArn"],
		"the resolved replicator ARN must not include the /replication-info suffix")
}
