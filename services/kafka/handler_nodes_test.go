package kafka_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestListNodes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "nodes-cluster")
	encoded := url.PathEscape(clusterArn)

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded+"/nodes", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

// TestKafkaCoverage2_RebootBroker covers RebootBroker handler.

func TestListKafkaVersions_IncludesKRaft(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	versions := b.ListKafkaVersions(context.Background())

	versionMap := make(map[string]string, len(versions))
	for _, v := range versions {
		versionMap[v.Version] = v.Status
	}

	tests := []struct {
		version    string
		wantStatus string
	}{
		{"3.8.0.kraft", "ACTIVE"},
		{"3.7.x.kraft", "ACTIVE"},
		{"3.6.0", "ACTIVE"},
		{"3.5.1", "ACTIVE"},
		{"3.4.0", "ACTIVE"},
		{"3.3.2", "ACTIVE"},
		{"3.3.1", "ACTIVE"},
		{"2.8.2.tiered", "ACTIVE"},
		{"2.8.1", "ACTIVE"},
		{"2.8.0", "DEPRECATED"},
		{"2.6.0", "DEPRECATED"},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			t.Parallel()

			status, ok := versionMap[tt.version]
			assert.True(t, ok, "version %q should be in ListKafkaVersions", tt.version)
			assert.Equal(t, tt.wantStatus, status, "version %q has unexpected status", tt.version)
		})
	}
}

// TestRefinement2_ListKafkaVersions_HTTP verifies the HTTP response.

func TestListKafkaVersions_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/kafka-versions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	versions, ok := resp["kafkaVersions"].([]any)
	require.True(t, ok)
	assert.Greater(t, len(versions), 5, "should return more than 5 versions")

	// Find KRaft variant.
	foundKRaft := false
	for _, v := range versions {
		ver := v.(map[string]any)
		if ver["version"] == "3.8.0.kraft" {
			foundKRaft = true
		}
	}

	assert.True(t, foundKRaft, "3.8.0.kraft should be in version list")
}

// TestRefinement2_GetCompatibleKafkaVersions_KRaftOnly verifies KRaft clusters get KRaft targets,
// grouped under the cluster's current version as SourceVersion (kafka@v1.57.2
// types.CompatibleKafkaVersion{SourceVersion, TargetVersions}).

func TestGetCompatibleKafkaVersions_KRaftOnly(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("kraft-cl", "3.7.x.kraft")

	groups, err := b.GetCompatibleKafkaVersions(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "3.7.x.kraft", groups[0].SourceVersion)

	// KRaft clusters should only get KRaft targets.
	for _, v := range groups[0].TargetVersions {
		assert.True(t, strings.HasSuffix(v, ".kraft") || v == "3.8.0.kraft",
			"KRaft cluster should only get KRaft-compatible versions, got %q", v)
	}

	assert.Contains(t, groups[0].TargetVersions, "3.8.0.kraft")
}

// TestRefinement2_GetCompatibleKafkaVersions_ZooKeeperNoKRaft verifies ZK clusters don't get KRaft.

func TestGetCompatibleKafkaVersions_ZooKeeperNoKRaft(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("zk-cl", "2.8.1")

	groups, err := b.GetCompatibleKafkaVersions(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "2.8.1", groups[0].SourceVersion)
	require.NotEmpty(t, groups[0].TargetVersions)

	for _, v := range groups[0].TargetVersions {
		assert.False(t, strings.HasSuffix(v, ".kraft"),
			"ZooKeeper cluster should not get KRaft versions, got %q", v)
	}
}

// TestRefinement2_GetCompatibleKafkaVersions_NotFound verifies error on missing cluster.

func TestGetCompatibleKafkaVersions_NotFound(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.GetCompatibleKafkaVersions(context.Background(), "arn:aws:kafka:us-east-1:123:cluster/nonexistent/abc")
	require.ErrorIs(t, err, kafka.ErrNotFound)
}

// TestGetCompatibleKafkaVersions_SDKRoundTrip drives GetCompatibleKafkaVersions
// through a real aws-sdk-go-v2 kafka client and asserts
// CompatibleKafkaVersions[0].SourceVersion/TargetVersions decode non-nil and
// non-empty (gopherstack-35gu). Before this fix the backend returned a flat
// []*MSKVersion{Version, Status}, which the real deserializer's
// awsRestjson1_deserializeDocumentCompatibleKafkaVersion (keys "sourceVersion"/
// "targetVersions") cannot match: every real client decoded an empty list.
func TestGetCompatibleKafkaVersions_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend(testAccountID, testRegion))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("compat-versions-sdk-cluster"),
		KafkaVersion:        aws.String("2.8.1"),
		NumberOfBrokerNodes: aws.Int32(3),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	out, err := client.GetCompatibleKafkaVersions(t.Context(), &kafkasdk.GetCompatibleKafkaVersionsInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.CompatibleKafkaVersions,
		"CompatibleKafkaVersions decoded empty: the real deserializer keys on sourceVersion/targetVersions")

	group := out.CompatibleKafkaVersions[0]
	assert.Equal(t, "2.8.1", aws.ToString(group.SourceVersion))
	assert.NotEmpty(t, group.TargetVersions)
}

// TestRefinement2_GetBootstrapBrokers_Variants tests bootstrap broker string generation.
