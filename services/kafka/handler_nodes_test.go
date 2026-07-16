package kafka_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

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

// TestRefinement2_GetCompatibleKafkaVersions_KRaftOnly verifies KRaft clusters get KRaft targets.

func TestGetCompatibleKafkaVersions_KRaftOnly(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("kraft-cl", "3.7.x.kraft")

	versions, err := b.GetCompatibleKafkaVersions(context.Background(), cl.ClusterArn)
	require.NoError(t, err)

	versionStrs := make([]string, 0, len(versions))
	for _, v := range versions {
		versionStrs = append(versionStrs, v.Version)
	}

	// KRaft clusters should only get KRaft targets.
	for _, v := range versions {
		assert.True(t, strings.HasSuffix(v.Version, ".kraft") || v.Version == "3.8.0.kraft",
			"KRaft cluster should only get KRaft-compatible versions, got %q", v.Version)
	}

	assert.Contains(t, versionStrs, "3.8.0.kraft")
}

// TestRefinement2_GetCompatibleKafkaVersions_ZooKeeperNoKRaft verifies ZK clusters don't get KRaft.

func TestGetCompatibleKafkaVersions_ZooKeeperNoKRaft(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("zk-cl", "2.8.1")

	versions, err := b.GetCompatibleKafkaVersions(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotEmpty(t, versions)

	for _, v := range versions {
		assert.False(t, strings.HasSuffix(v.Version, ".kraft"),
			"ZooKeeper cluster should not get KRaft versions, got %q", v.Version)
	}
}

// TestRefinement2_GetCompatibleKafkaVersions_NotFound verifies error on missing cluster.

func TestGetCompatibleKafkaVersions_NotFound(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.GetCompatibleKafkaVersions(context.Background(), "arn:aws:kafka:us-east-1:123:cluster/nonexistent/abc")
	require.ErrorIs(t, err, kafka.ErrNotFound)
}

// TestRefinement2_GetBootstrapBrokers_Variants tests bootstrap broker string generation.
