package kafka_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// ----------------------------------------
// Refinement check 1 tests
// ----------------------------------------

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("c1", "2.8.0")
	b.AddConfigurationInternal("cfg1")

	require.Equal(t, 1, kafka.ClusterCount(b))
	require.Equal(t, 1, kafka.ConfigurationCount(b))

	b.Reset()

	assert.Equal(t, 0, kafka.ClusterCount(b))
	assert.Equal(t, 0, kafka.ConfigurationCount(b))
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)

	for range 3 {
		b.AddClusterInternal("c1", "2.8.0")
		b.Reset()
		assert.Equal(t, 0, kafka.ClusterCount(b))
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	backend.AddClusterInternal("c1", "2.8.0")
	require.Equal(t, 1, kafka.ClusterCount(backend))

	h.Reset()

	assert.Equal(t, 0, kafka.ClusterCount(backend))
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &kafka.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, kafka.ErrNilAppContext)
}

func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    map[string]any
		path    string
		method  string
		wantMsg string
	}{
		{
			name:   "create_cluster_empty_name",
			path:   "/v1/clusters",
			method: http.MethodPost,
			body: map[string]any{
				"clusterName":         "",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantMsg: "clusterName is required",
		},
		{
			name:   "create_replicator_empty_name",
			path:   "/replication/v1/replicators",
			method: http.MethodPost,
			body:   map[string]any{"replicatorName": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doKafkaRequest(t, h, tt.method, tt.path, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)

			if tt.wantMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantMsg)
			}
		})
	}
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)

	cl := b.AddClusterInternal("my-cluster", "2.8.0")
	require.NotNil(t, cl)
	assert.Equal(t, "my-cluster", cl.ClusterName)
	assert.Equal(t, kafka.ClusterStateActive, cl.State)

	cfg := b.AddConfigurationInternal("my-cfg")
	require.NotNil(t, cfg)
	assert.Equal(t, "my-cfg", cfg.Name)

	rep := b.AddReplicatorInternal("my-replicator")
	require.NotNil(t, rep)
	assert.Equal(t, "my-replicator", rep.ReplicatorName)

	topic := b.AddTopicInternal(cl.ClusterArn, "my-topic")
	require.NotNil(t, topic)
	assert.Equal(t, "my-topic", topic.TopicName)

	vpc := b.AddVpcConnectionInternal(cl.ClusterArn, "vpc-12345")
	require.NotNil(t, vpc)
	assert.Equal(t, "vpc-12345", vpc.VpcID)

	op := b.AddClusterOperationInternal(cl.ClusterArn, "UPDATE_BROKER_COUNT")
	require.NotNil(t, op)
	assert.Equal(t, "UPDATE_BROKER_COUNT", op.OperationType)
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)

	assert.Equal(t, 0, kafka.ClusterCount(b))
	assert.Equal(t, 0, kafka.ConfigurationCount(b))
	assert.Equal(t, 0, kafka.ReplicatorCount(b))
	assert.Equal(t, 0, kafka.TopicCount(b))
	assert.Equal(t, 0, kafka.VpcConnectionCount(b))
	assert.Equal(t, 0, kafka.ClusterOperationCount(b))
	assert.Equal(t, 0, kafka.ScramSecretCount(b))

	cl := b.AddClusterInternal("c1", "2.8.0")
	b.AddConfigurationInternal("cfg1")
	b.AddReplicatorInternal("rep1")
	b.AddTopicInternal(cl.ClusterArn, "t1")
	b.AddVpcConnectionInternal(cl.ClusterArn, "vpc-1")
	b.AddClusterOperationInternal(cl.ClusterArn, "UPDATE")

	assert.Equal(t, 1, kafka.ClusterCount(b))
	assert.Equal(t, 1, kafka.ConfigurationCount(b))
	assert.Equal(t, 1, kafka.ReplicatorCount(b))
	assert.Equal(t, 1, kafka.TopicCount(b))
	assert.Equal(t, 1, kafka.VpcConnectionCount(b))
	assert.Equal(t, 1, kafka.ClusterOperationCount(b))
}

func TestRefinement1_SortedListClusters(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("zzz-cluster", "2.8.0")
	b.AddClusterInternal("aaa-cluster", "2.8.0")
	b.AddClusterInternal("mmm-cluster", "2.8.0")

	clusters := b.ListClusters(context.Background())
	require.Len(t, clusters, 3)
	assert.Equal(t, "aaa-cluster", clusters[0].ClusterName)
	assert.Equal(t, "mmm-cluster", clusters[1].ClusterName)
	assert.Equal(t, "zzz-cluster", clusters[2].ClusterName)
}

func TestRefinement1_SortedListConfigurations(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	b.AddConfigurationInternal("zzz-cfg")
	b.AddConfigurationInternal("aaa-cfg")
	b.AddConfigurationInternal("mmm-cfg")

	cfgs := b.ListConfigurations(context.Background())
	require.Len(t, cfgs, 3)
	assert.Equal(t, "aaa-cfg", cfgs[0].Name)
	assert.Equal(t, "mmm-cfg", cfgs[1].Name)
	assert.Equal(t, "zzz-cfg", cfgs[2].Name)
}

func TestRefinement1_NonNilTags_Cluster(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")

	assert.NotNil(t, cl.Tags)
}

func TestRefinement1_NonNilTags_Replicator(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	rep := b.AddReplicatorInternal("rep1")

	assert.NotNil(t, rep.Tags)
}

func TestRefinement1_NonNilTags_VpcConnection(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")
	vpc := b.AddVpcConnectionInternal(cl.ClusterArn, "vpc-1")

	assert.NotNil(t, vpc.Tags)
}

func TestRefinement1_DeleteCluster_CascadesTopicsAndScram(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")

	b.AddTopicInternal(cl.ClusterArn, "t1")
	b.AddTopicInternal(cl.ClusterArn, "t2")

	_, err := b.BatchAssociateScramSecret(context.Background(),
		cl.ClusterArn,
		[]string{"arn:aws:secretsmanager:us-east-1:000000000000:secret:s1"},
	)
	require.NoError(t, err)

	require.Equal(t, 2, kafka.TopicCount(b))
	require.Equal(t, 1, kafka.ScramSecretCount(b))

	err = b.DeleteCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)

	assert.Equal(t, 0, kafka.TopicCount(b))
	assert.Equal(t, 0, kafka.ScramSecretCount(b))
}

func TestRefinement1_TagResource_Replicator(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	rep := b.AddReplicatorInternal("rep1")

	err := b.TagResource(context.Background(), rep.ReplicatorArn, map[string]string{"env": "prod"})
	require.NoError(t, err)

	tags, err := b.GetTags(context.Background(), rep.ReplicatorArn)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
}

func TestRefinement1_TagResource_VpcConnection(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")
	vpc := b.AddVpcConnectionInternal(cl.ClusterArn, "vpc-1")

	err := b.TagResource(context.Background(), vpc.VpcConnectionArn, map[string]string{"team": "infra"})
	require.NoError(t, err)

	tags, err := b.GetTags(context.Background(), vpc.VpcConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "infra", tags["team"])
}

func TestRefinement1_DeepCopy_ClusterDoesNotAlias(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")

	// Mutating the returned clone must not affect the stored cluster.
	cl.ClusterName = "mutated"
	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, "c1", described.ClusterName)
}

func TestRefinement1_DeepCopy_ConfigurationDoesNotAlias(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cfg := b.AddConfigurationInternal("cfg1")

	cfg.Name = "mutated"
	described, err := b.DescribeConfiguration(context.Background(), cfg.Arn)
	require.NoError(t, err)
	assert.Equal(t, "cfg1", described.Name)
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")
	b.AddConfigurationInternal("cfg1")
	b.AddReplicatorInternal("rep1")
	b.AddTopicInternal(cl.ClusterArn, "t1")
	b.AddClusterOperationInternal(cl.ClusterArn, "UPDATE")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, kafka.ClusterCount(b2))
	assert.Equal(t, 1, kafka.ConfigurationCount(b2))
	assert.Equal(t, 1, kafka.ReplicatorCount(b2))
	assert.Equal(t, 1, kafka.TopicCount(b2))
	assert.Equal(t, 1, kafka.ClusterOperationCount(b2))
	assert.Equal(t, testAccountID, b2.AccountID())
	assert.Equal(t, testRegion, b2.Region())
}

func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 0, kafka.ClusterCount(b2))
}

func TestRefinement1_CreateCluster_RequiresName(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateCluster(context.Background(), "", "2.8.0", 3, kafka.BrokerNodeGroupInfo{}, nil, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestRefinement1_CreateConfiguration_RequiresName(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateConfiguration(context.Background(), "", "", nil, "")

	require.Error(t, err)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestRefinement1_CreateReplicator_RequiresName(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateReplicator(context.Background(), "", "", "", nil)

	require.Error(t, err)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestRefinement1_CreateTopic_RequiresName(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")
	_, err := b.CreateTopic(context.Background(), cl.ClusterArn, "", 3, 1, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, kafka.ErrValidation)
}

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"BatchAssociateScramSecret",
		"BatchDisassociateScramSecret",
		"CreateCluster",
		"CreateClusterV2",
		"CreateConfiguration",
		"CreateReplicator",
		"CreateTopic",
		"CreateVpcConnection",
		"DeleteCluster",
		"DeleteClusterPolicy",
		"DeleteConfiguration",
		"DeleteReplicator",
		"DeleteTopic",
		"DeleteVpcConnection",
		"DescribeCluster",
		"DescribeClusterOperation",
		"DescribeClusterV2",
		"DescribeConfiguration",
		"GetBootstrapBrokers",
		"ListClusters",
		"ListClustersV2",
		"ListConfigurations",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}

func TestRefinement1_ScramSecretCount(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("c1", "2.8.0")

	secrets := []string{
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:s1",
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:s2",
	}
	_, err := b.BatchAssociateScramSecret(context.Background(), cl.ClusterArn, secrets)
	require.NoError(t, err)
	assert.Equal(t, 2, kafka.ScramSecretCount(b))

	_, err = b.BatchDisassociateScramSecret(context.Background(), cl.ClusterArn, secrets[:1])
	require.NoError(t, err)
	assert.Equal(t, 1, kafka.ScramSecretCount(b))
}

func TestRefinement1_HandlerSnapshot(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	backend.AddClusterInternal("c1", "2.8.0")

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2, backend2 := newTestHandlerWithBackend(t)
	err := h2.Restore(t.Context(), snap)
	require.NoError(t, err)
	assert.Equal(t, 1, kafka.ClusterCount(backend2))
}

func TestRefinement1_CreateClusterV2_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doKafkaRequest(t, h, http.MethodPost, "/api/v2/clusters", map[string]any{
		"clusterName": "v2-cluster",
		"provisioned": map[string]any{
			"kafkaVersion":        "3.5.0",
			"numberOfBrokerNodes": 3,
			"brokerNodeGroupInfo": map[string]any{
				"instanceType":  "kafka.m5.large",
				"clientSubnets": []string{"subnet-1"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "v2-cluster", resp["clusterName"])
	assert.Equal(t, "PROVISIONED", resp["clusterType"])
}

func TestRefinement1_BatchAssociateThenDisassociate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName": "scram-cluster", "kafkaVersion": "2.8.0", "numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{"instanceType": "kafka.m5.large", "clientSubnets": []string{"subnet-1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	clusterArn := cr["clusterArn"].(string)

	encoded := url.PathEscape(clusterArn)

	assocRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters/"+encoded+"/scram-secrets", map[string]any{
		"secretArnList": []string{"arn:aws:secretsmanager:us-east-1:000:secret:s1"},
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	disassocRec := doKafkaRequest(t, h, http.MethodPatch, "/v1/clusters/"+encoded+"/scram-secrets", map[string]any{
		"secretArnList": []string{"arn:aws:secretsmanager:us-east-1:000:secret:s1"},
	})
	assert.Equal(t, http.StatusOK, disassocRec.Code)
}

func TestRefinement1_ErrAlreadyExistsMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(*kafka.InMemoryBackend) error
		name string
	}{
		{
			name: "duplicate_cluster",
			fn: func(b *kafka.InMemoryBackend) error {
				b.AddClusterInternal("dup", "2.8.0")
				_, err := b.CreateCluster(
					context.Background(),
					"dup",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)

				return err
			},
		},
		{
			name: "duplicate_configuration",
			fn: func(b *kafka.InMemoryBackend) error {
				b.AddConfigurationInternal("dup-cfg")
				_, err := b.CreateConfiguration(context.Background(), "dup-cfg", "", nil, "")

				return err
			},
		},
		{
			name: "duplicate_replicator",
			fn: func(b *kafka.InMemoryBackend) error {
				b.AddReplicatorInternal("dup-rep")
				_, err := b.CreateReplicator(context.Background(), "dup-rep", "", "", nil)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			err := tt.fn(b)

			require.Error(t, err)
			require.ErrorIs(t, err, kafka.ErrAlreadyExists)
		})
	}
}
