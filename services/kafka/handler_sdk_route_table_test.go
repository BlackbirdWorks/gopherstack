package kafka_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Kafka (MSK)
// operation, extracted from kafka@v1.57.2 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchAssociateScramSecret", "POST", "/v1/clusters/PLACEHOLDER/scram-secrets"},
		{"BatchDisassociateScramSecret", "PATCH", "/v1/clusters/PLACEHOLDER/scram-secrets"},
		{"CreateChannel", "POST", "/v1/clusters/PLACEHOLDER/channels"},
		{"CreateCluster", "POST", "/v1/clusters"},
		{"CreateClusterV2", "POST", "/api/v2/clusters"},
		{"CreateConfiguration", "POST", "/v1/configurations"},
		{"CreateReplicator", "POST", "/replication/v1/replicators"},
		{"CreateTopic", "POST", "/v1/clusters/PLACEHOLDER/topics"},
		{"CreateVpcConnection", "POST", "/v1/vpc-connection"},
		{"DeleteChannel", "DELETE", "/v1/clusters/PLACEHOLDER/channels/PLACEHOLDER"},
		{"DeleteCluster", "DELETE", "/v1/clusters/PLACEHOLDER"},
		{"DeleteClusterPolicy", "DELETE", "/v1/clusters/PLACEHOLDER/policy"},
		{"DeleteConfiguration", "DELETE", "/v1/configurations/PLACEHOLDER"},
		{"DeleteReplicator", "DELETE", "/replication/v1/replicators/PLACEHOLDER"},
		{"DeleteTopic", "DELETE", "/v1/clusters/PLACEHOLDER/topics/PLACEHOLDER"},
		{"DeleteVpcConnection", "DELETE", "/v1/vpc-connection/PLACEHOLDER"},
		{"DescribeChannel", "GET", "/v1/clusters/PLACEHOLDER/channels/PLACEHOLDER"},
		{"DescribeCluster", "GET", "/v1/clusters/PLACEHOLDER"},
		{"DescribeClusterOperation", "GET", "/v1/operations/PLACEHOLDER"},
		{"DescribeClusterOperationV2", "GET", "/api/v2/operations/PLACEHOLDER"},
		{"DescribeClusterV2", "GET", "/api/v2/clusters/PLACEHOLDER"},
		{"DescribeConfiguration", "GET", "/v1/configurations/PLACEHOLDER"},
		{"DescribeConfigurationRevision", "GET", "/v1/configurations/PLACEHOLDER/revisions/PLACEHOLDER"},
		{"DescribeReplicator", "GET", "/replication/v1/replicators/PLACEHOLDER"},
		{"DescribeTopic", "GET", "/v1/clusters/PLACEHOLDER/topics/PLACEHOLDER"},
		{"DescribeTopicPartitions", "GET", "/v1/clusters/PLACEHOLDER/topics/PLACEHOLDER/partitions"},
		{"DescribeVpcConnection", "GET", "/v1/vpc-connection/PLACEHOLDER"},
		{"GetBootstrapBrokers", "GET", "/v1/clusters/PLACEHOLDER/bootstrap-brokers"},
		{"GetClusterPolicy", "GET", "/v1/clusters/PLACEHOLDER/policy"},
		{"GetCompatibleKafkaVersions", "GET", "/v1/compatible-kafka-versions"},
		{"ListChannels", "GET", "/v1/clusters/PLACEHOLDER/channels"},
		{"ListClientVpcConnections", "GET", "/v1/clusters/PLACEHOLDER/client-vpc-connections"},
		{"ListClusterOperations", "GET", "/v1/clusters/PLACEHOLDER/operations"},
		{"ListClusterOperationsV2", "GET", "/api/v2/clusters/PLACEHOLDER/operations"},
		{"ListClusters", "GET", "/v1/clusters"},
		{"ListClustersV2", "GET", "/api/v2/clusters"},
		{"ListConfigurationRevisions", "GET", "/v1/configurations/PLACEHOLDER/revisions"},
		{"ListConfigurations", "GET", "/v1/configurations"},
		{"ListKafkaVersions", "GET", "/v1/kafka-versions"},
		{"ListNodes", "GET", "/v1/clusters/PLACEHOLDER/nodes"},
		{"ListReplicators", "GET", "/replication/v1/replicators"},
		{"ListScramSecrets", "GET", "/v1/clusters/PLACEHOLDER/scram-secrets"},
		{"ListTagsForResource", "GET", "/v1/tags/PLACEHOLDER"},
		{"ListTopics", "GET", "/v1/clusters/PLACEHOLDER/topics"},
		{"ListVpcConnections", "GET", "/v1/vpc-connections"},
		{"PutClusterPolicy", "PUT", "/v1/clusters/PLACEHOLDER/policy"},
		{"RebootBroker", "PUT", "/v1/clusters/PLACEHOLDER/reboot-broker"},
		{"RejectClientVpcConnection", "PUT", "/v1/clusters/PLACEHOLDER/client-vpc-connection"},
		{"TagResource", "POST", "/v1/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/v1/tags/PLACEHOLDER"},
		{"UpdateBrokerCount", "PUT", "/v1/clusters/PLACEHOLDER/nodes/count"},
		{"UpdateBrokerStorage", "PUT", "/v1/clusters/PLACEHOLDER/nodes/storage"},
		{"UpdateBrokerType", "PUT", "/v1/clusters/PLACEHOLDER/nodes/type"},
		{"UpdateChannel", "PUT", "/v1/clusters/PLACEHOLDER/channels/PLACEHOLDER"},
		{"UpdateClusterConfiguration", "PUT", "/v1/clusters/PLACEHOLDER/configuration"},
		{"UpdateClusterKafkaVersion", "PUT", "/v1/clusters/PLACEHOLDER/version"},
		{"UpdateConfiguration", "PUT", "/v1/configurations/PLACEHOLDER"},
		{"UpdateConnectivity", "PUT", "/v1/clusters/PLACEHOLDER/connectivity"},
		{"UpdateMonitoring", "PUT", "/v1/clusters/PLACEHOLDER/monitoring"},
		{"UpdateRebalancing", "PUT", "/v1/clusters/PLACEHOLDER/rebalancing"},
		{"UpdateReplicationInfo", "PUT", "/replication/v1/replicators/PLACEHOLDER/replication-info"},
		{"UpdateSecurity", "PATCH", "/v1/clusters/PLACEHOLDER/security"},
		{"UpdateStorage", "PUT", "/v1/clusters/PLACEHOLDER/storage"},
		{"UpdateTopic", "PUT", "/v1/clusters/PLACEHOLDER/topics/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Kafka (MSK) op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 3: re-extracted all 64 kafka ops from the pinned SDK and confirmed the
// existing parseKafkaPath table already correct, including its singular
// (/v1/vpc-connection) vs. plural (/v1/vpc-connections) split and several
// suffix-discriminated same-prefix collisions (scram-secrets POST/PATCH/GET,
// nodes/{count,storage,type} vs. bare nodes).
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "unknown operation: " NotFoundException that
// dispatch's final default emits (handler.go:391) -- guarding against an
// operation name that resolves correctly but has no matching case in any
// dispatchXxx family (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation: ",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
