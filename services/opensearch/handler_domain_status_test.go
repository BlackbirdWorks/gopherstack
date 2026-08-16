package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

func TestOpenSearchHandler_DescribeDomainHealth(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create domain with specific cluster config.
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName":    "health-domain",
			"ClusterConfig": map[string]any{"InstanceType": "t3.small.search", "InstanceCount": 3},
		})
	resp.Body.Close()

	healthResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/health-domain/health", nil)
	defer healthResp.Body.Close()

	assert.Equal(t, http.StatusOK, healthResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(healthResp.Body).Decode(&out))

	assert.Equal(t, "Active", out["DomainState"])
	// 3 instances * 5 shards = 15
	totalShards, ok := out["TotalShards"].(float64)
	require.True(t, ok)
	assert.InDelta(t, float64(15), totalShards, 0)
	assert.Equal(t, out["TotalShards"], out["ActiveShards"])
}

func TestOpenSearchHandler_DescribeDomainNodes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName":    "nodes-domain",
			"ClusterConfig": map[string]any{"InstanceType": "r6g.large.search", "InstanceCount": 2},
		})
	resp.Body.Close()

	nodesResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/nodes-domain/nodes", nil)
	defer nodesResp.Body.Close()

	assert.Equal(t, http.StatusOK, nodesResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(nodesResp.Body).Decode(&out))

	nodes, ok := out["DomainNodesStatusList"].([]any)
	require.True(t, ok)
	assert.Len(t, nodes, 2)

	// Verify each node has expected fields.
	for _, node := range nodes {
		n, ok2 := node.(map[string]any)
		require.True(t, ok2)
		assert.NotEmpty(t, n["NodeId"])
		assert.Equal(t, "Data", n["NodeType"])
		assert.Equal(t, "r6g.large.search", n["InstanceType"])
		assert.Equal(t, "Active", n["NodeStatus"])
	}
}

func TestDescribeDomainStatusRoutes_UnknownDomain_404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "health", path: "/2021-01-01/opensearch/domain/missing/health"},
		{name: "nodes", path: "/2021-01-01/opensearch/domain/missing/nodes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			resp := doRequest(t, h, http.MethodGet, tt.path, nil)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
	}
}

func TestOpenSearchHandler_DescribeDomainChangeProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create domain.
	createTestDomain(t, h, "progress-domain")

	// Update config to generate a change ID.
	updateResp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/progress-domain/config",
		map[string]any{"EngineVersion": "OpenSearch_2.9"})
	updateResp.Body.Close()

	// Get change progress.
	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/progress-domain/progress", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	status, ok := out["ChangeProgressStatus"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, status["ChangeId"])
	assert.Equal(t, "COMPLETED", status["Status"])
}

func TestOpenSearchHandler_DescribeDryRunProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create domain.
	createTestDomain(t, h, "dryrun-domain")

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/dryrun-domain/dryRun", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, json.Unmarshal(bodyBytes, &out))

	status, ok := out["DryRunProgressStatus"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, status["DryRunId"])
	assert.Equal(t, "COMPLETED", status["DryRunStatus"])
}

func TestDescribeDomainHealth_AdditionalFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		clusterConfig     map[string]any
		name              string
		wantDataNodeCount float64
		wantDedicated     bool
	}{
		{
			name:              "default_single_node",
			clusterConfig:     map[string]any{"InstanceType": "t3.small.search", "InstanceCount": 1},
			wantDataNodeCount: 1,
			wantDedicated:     false,
		},
		{
			name: "multi_node_with_dedicated_master",
			clusterConfig: map[string]any{
				"InstanceType":           "r6g.large.search",
				"InstanceCount":          3,
				"DedicatedMasterEnabled": true,
				"DedicatedMasterType":    "m6g.large.search",
				"DedicatedMasterCount":   3,
			},
			wantDataNodeCount: 3,
			wantDedicated:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
				map[string]any{"DomainName": "health-domain", "ClusterConfig": tt.clusterConfig})
			resp.Body.Close()

			hResp := doRequest(t, h, http.MethodGet,
				"/2021-01-01/opensearch/domain/health-domain/health", nil)
			defer hResp.Body.Close()

			require.Equal(t, http.StatusOK, hResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(hResp.Body).Decode(&out))

			// Existing fields.
			assert.Equal(t, "Active", out["DomainState"])

			// New fields.
			unassigned, ok := out["UnAssignedShards"].(float64)
			require.True(t, ok, "UnAssignedShards must be present")
			assert.InDelta(t, float64(0), unassigned, 0)

			dataCount, ok := out["DataNodeCount"].(float64)
			require.True(t, ok, "DataNodeCount must be present")
			assert.InDelta(t, tt.wantDataNodeCount, dataCount, 0)

			dedicated, ok := out["DedicatedMaster"].(bool)
			require.True(t, ok, "DedicatedMaster must be present")
			assert.Equal(t, tt.wantDedicated, dedicated)

			_, hasAZCount := out["ActiveAvailabilityZoneCount"]
			assert.True(t, hasAZCount, "ActiveAvailabilityZoneCount must be present")
		})
	}
}

func TestDescribeDomainNodes_StorageAndAZ(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName":    "nodes-az-domain",
			"ClusterConfig": map[string]any{"InstanceType": "r6g.large.search", "InstanceCount": 2},
		})
	resp.Body.Close()

	nResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/nodes-az-domain/nodes", nil)
	defer nResp.Body.Close()

	require.Equal(t, http.StatusOK, nResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(nResp.Body).Decode(&out))

	nodes, ok := out["DomainNodesStatusList"].([]any)
	require.True(t, ok)
	require.Len(t, nodes, 2)

	for _, raw := range nodes {
		node, ok2 := raw.(map[string]any)
		require.True(t, ok2)

		assert.NotEmpty(t, node["StorageVolumeType"], "StorageVolumeType must be present")
		assert.NotEmpty(t, node["AvailabilityZone"], "AvailabilityZone must be present")
	}
}

func TestDescribeDomainChangeProgress_Timestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "progress-ts-domain")

	doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/progress-ts-domain/config",
		map[string]any{"EngineVersion": "OpenSearch_2.9"}).Body.Close()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/progress-ts-domain/progress", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	status, ok := out["ChangeProgressStatus"].(map[string]any)
	require.True(t, ok)

	assert.NotEmpty(t, status["StartTime"], "StartTime must be present")
	assert.NotEmpty(t, status["LastUpdatedTime"], "LastUpdatedTime must be present")
}

func TestDescribeDryRunProgress_ValidationFailures(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "dryrun-vf-domain")

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/dryrun-vf-domain/dryRun", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	status, ok := out["DryRunProgressStatus"].(map[string]any)
	require.True(t, ok)

	vf, hasVF := status["ValidationFailures"]
	assert.True(t, hasVF, "ValidationFailures must be present")

	failures, ok := vf.([]any)
	require.True(t, ok, "ValidationFailures must be a list")
	assert.Empty(t, failures)
}

func TestGetDomainHealth_WarmNodeCount(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("warm-domain", "OpenSearch_2.11")

	h := opensearch.NewHandler(b)

	doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/warm-domain/config",
		map[string]any{
			"ClusterConfig": map[string]any{
				"WarmEnabled": true,
				"WarmType":    "ultrawarm1.medium.search",
				"WarmCount":   3,
			},
		}).Body.Close()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/warm-domain/health", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	warmCount, ok := out["WarmNodeCount"].(float64)
	require.True(t, ok, "WarmNodeCount must be present")
	assert.InDelta(t, float64(3), warmCount, 0)
}
