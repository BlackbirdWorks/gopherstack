package kafka_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func parseKafkaResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func createCoverageCluster(t *testing.T, h *kafka.Handler, _ *kafka.InMemoryBackend, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName":         name,
		"kafkaVersion":        "2.8.0",
		"numberOfBrokerNodes": 1,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-abc"},
			"storageInfo": map[string]any{
				"ebsStorageInfo": map[string]any{"volumeSize": 100},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseKafkaResp(t, rec)
	arn, _ := resp["clusterArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

func createCoverageReplicator(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/replication/v1/replicators", map[string]any{
		"replicatorName":          name,
		"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/my-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseKafkaResp(t, rec)
	arn, _ := resp["replicatorArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

func createCoverageConfig(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":             name,
		"kafkaVersions":    []string{"2.8.0"},
		"serverProperties": "auto.create.topics.enable=false",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseKafkaResp(t, rec)
	arn, _ := resp["arn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

func TestKafkaCoverage_DescribeReplicator(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := createCoverageReplicator(t, h, "desc-repl")

	rec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators/"+arn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseKafkaResp(t, rec)
	assert.Equal(t, arn, resp["replicatorArn"])
}

func TestKafkaCoverage_ListReplicators(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCoverageReplicator(t, h, "list-repl-1")
	createCoverageReplicator(t, h, "list-repl-2")

	rec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseKafkaResp(t, rec)
	repls, ok := resp["replicators"].([]any)
	assert.True(t, ok)
	assert.Len(t, repls, 2)
}

func TestKafkaCoverage_Topics(t *testing.T) {
	t.Parallel()

	h, be := newTestHandlerWithBackend(t)
	clusterArn := createCoverageCluster(t, h, be, "topic-cluster")

	// CreateTopic
	topic, err := be.CreateTopic(context.Background(), clusterArn, "my-topic", 1, 3, nil)
	require.NoError(t, err)
	assert.Equal(t, "my-topic", topic.TopicName)

	// DescribeTopic (via DescribeTopicPartitions)
	tp, err := be.DescribeTopicPartitions(context.Background(), clusterArn, "my-topic")
	require.NoError(t, err)
	assert.Equal(t, "my-topic", tp.TopicName)

	// ListTopics
	topics, err := be.ListTopics(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.NotEmpty(t, topics)

	// UpdateTopic
	updated, err := be.UpdateTopic(
		context.Background(),
		clusterArn,
		"my-topic",
		6,
		map[string]string{"retention.ms": "86400000"},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(6), updated.NumPartitions)

	// DeleteTopic
	err = be.DeleteTopic(context.Background(), clusterArn, "my-topic")
	require.NoError(t, err)

	// DescribeTopic after delete → not found
	_, err = be.DescribeTopicPartitions(context.Background(), clusterArn, "my-topic")
	assert.Error(t, err)
}

func TestKafkaCoverage_DescribeTopic(t *testing.T) {
	t.Parallel()

	_, be := newTestHandlerWithBackend(t)
	h, be2 := newTestHandlerWithBackend(t)
	_ = be
	clusterArn := createCoverageCluster(t, h, be2, "dt-cluster")

	_, err := be2.CreateTopic(context.Background(), clusterArn, "dt-topic", 1, 1, nil)
	require.NoError(t, err)

	// DescribeTopic
	topic, err := be2.DescribeTopic(context.Background(), clusterArn, "dt-topic")
	require.NoError(t, err)
	assert.Equal(t, "dt-topic", topic.TopicName)
}

func TestKafkaCoverage_VpcConnections(t *testing.T) {
	t.Parallel()

	h, be := newTestHandlerWithBackend(t)
	clusterArn := createCoverageCluster(t, h, be, "vpc-cluster")

	// CreateVpcConnection
	conn, err := be.CreateVpcConnection(context.Background(), clusterArn, "vpc-abc", "PLAINTEXT", nil)
	require.NoError(t, err)
	connArn := conn.VpcConnectionArn

	// DescribeVpcConnection
	c, err := be.DescribeVpcConnection(context.Background(), connArn)
	require.NoError(t, err)
	assert.Equal(t, connArn, c.VpcConnectionArn)

	// ListVpcConnections
	conns := be.ListVpcConnections(context.Background())
	assert.NotEmpty(t, conns)

	// ListClientVpcConnections
	clientConns, err := be.ListClientVpcConnections(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.NotEmpty(t, clientConns)

	// RejectClientVpcConnection
	err = be.RejectClientVpcConnection(context.Background(), connArn)
	require.NoError(t, err)
}

func TestKafkaCoverage_ClusterPolicy(t *testing.T) {
	t.Parallel()

	h, be := newTestHandlerWithBackend(t)
	clusterArn := createCoverageCluster(t, h, be, "policy-cluster")

	policy := `{"Version":"2012-10-17","Statement":[]}`

	err := be.PutClusterPolicy(context.Background(), clusterArn, policy)
	require.NoError(t, err)

	p, err := be.GetClusterPolicy(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.Equal(t, policy, p)
}

func TestKafkaCoverage_ClusterOperations(t *testing.T) {
	t.Parallel()

	h, be := newTestHandlerWithBackend(t)
	clusterArn := createCoverageCluster(t, h, be, "ops-cluster")

	_, err := be.UpdateBrokerCount(context.Background(), clusterArn, 2)
	require.NoError(t, err)

	// ListClusterOperations
	ops, err := be.ListClusterOperations(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.NotEmpty(t, ops)

	// ListClusterOperationsV2
	ops2, err := be.ListClusterOperationsV2(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.NotEmpty(t, ops2)

	if len(ops2) > 0 {
		op, opErr := be.DescribeClusterOperationV2(context.Background(), ops2[0].ClusterOperationArn)
		require.NoError(t, opErr)
		assert.Equal(t, ops2[0].ClusterOperationArn, op.ClusterOperationArn)
	}
}

func TestKafkaCoverage_ConfigurationRevisions(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)
	configArn := createCoverageConfig(t, h, "cfg-revisions")

	_ = configArn
	// Use backend directly for revision tests
	_, be2 := newTestHandlerWithBackend(t)

	cfg, err := be2.CreateConfiguration(
		context.Background(),
		"cfg2",
		"",
		[]string{"2.8.0"},
		"auto.create.topics.enable=false",
	)
	require.NoError(t, err)

	// DescribeConfigurationRevision
	revision, err := be2.DescribeConfigurationRevision(context.Background(), cfg.Arn, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), revision.Revision)

	// ListConfigurationRevisions
	revs, err := be2.ListConfigurationRevisions(context.Background(), cfg.Arn)
	require.NoError(t, err)
	assert.NotEmpty(t, revs)

	// UpdateConfiguration (description, serverProperties)
	updated, err := be2.UpdateConfiguration(context.Background(), cfg.Arn, "v2 desc", "auto.create.topics.enable=true")
	require.NoError(t, err)
	assert.NotEmpty(t, updated.Arn)
}
