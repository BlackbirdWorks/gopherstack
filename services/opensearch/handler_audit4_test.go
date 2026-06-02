package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// ---------------------------------------------------------------------------
// DescribeDomains returns full DomainStatus (issue: minimal map)
// ---------------------------------------------------------------------------

func TestAudit4_DescribeDomains_FullStatusShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
	}{
		{name: "default_version", engineVersion: ""},
		{name: "opensearch_211", engineVersion: "OpenSearch_2.11"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			b := h.Backend.(*opensearch.InMemoryBackend)
			b.AddDomainInternal("full-domain", tt.engineVersion)

			resp := doRequest(t, h, http.MethodGet,
				"/2021-01-01/opensearch/domain/describe",
				map[string]any{"DomainNames": []string{"full-domain"}})
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			list, ok := out["DomainStatusList"].([]any)
			require.True(t, ok)
			require.Len(t, list, 1)

			entry := list[0].(map[string]any)
			// Full shape must include ARN, ClusterConfig, Processing, DomainProcessingStatus.
			assert.NotEmpty(t, entry["ARN"])
			assert.NotEmpty(t, entry["DomainName"])
			_, hasCC := entry["ClusterConfig"]
			assert.True(t, hasCC, "ClusterConfig must be present")
			_, hasProc := entry["Processing"]
			assert.True(t, hasProc, "Processing must be present")
			_, hasDPS := entry["DomainProcessingStatus"]
			assert.True(t, hasDPS, "DomainProcessingStatus must be present")
		})
	}
}

// ---------------------------------------------------------------------------
// GetCompatibleVersions returns 404 when domain filter names unknown domain
// ---------------------------------------------------------------------------

func TestAudit4_GetCompatibleVersions_UnknownDomain_404(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/compatibleVersions?domainName=no-such-domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAudit4_GetCompatibleVersions_KnownDomain_200(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	b := h.Backend.(*opensearch.InMemoryBackend)
	b.AddDomainInternal("cv-domain", "OpenSearch_2.9")

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/compatibleVersions?domainName=cv-domain", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	versions, ok := out["CompatibleVersions"].([]any)
	require.True(t, ok)
	require.Len(t, versions, 1)

	entry := versions[0].(map[string]any)
	assert.Equal(t, "OpenSearch_2.9", entry["SourceVersion"])
}

func TestAudit4_GetCompatibleVersions_NoDomainFilter_AllVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/compatibleVersions", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	versions, ok := out["CompatibleVersions"].([]any)
	require.True(t, ok)
	assert.Greater(t, len(versions), 1, "no domain filter should return all versions")
}

// ---------------------------------------------------------------------------
// DescribeDomainHealth returns UnAssignedShards, DataNodeCount, DedicatedMaster
// ---------------------------------------------------------------------------

func TestAudit4_DescribeDomainHealth_AdditionalFields(t *testing.T) {
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

// ---------------------------------------------------------------------------
// DescribeDomainNodes returns StorageVolumeType and AvailabilityZone
// ---------------------------------------------------------------------------

func TestAudit4_DescribeDomainNodes_StorageAndAZ(t *testing.T) {
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

// ---------------------------------------------------------------------------
// DescribeDomainChangeProgress includes StartTime and LastUpdatedTime
// ---------------------------------------------------------------------------

func TestAudit4_DescribeDomainChangeProgress_Timestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "progress-ts-domain")

	doRequest(t, h, http.MethodPut,
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

// ---------------------------------------------------------------------------
// DescribeDryRunProgress includes ValidationFailures (empty slice)
// ---------------------------------------------------------------------------

func TestAudit4_DescribeDryRunProgress_ValidationFailures(t *testing.T) {
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

// ---------------------------------------------------------------------------
// ListInstanceTypeDetails includes AdvancedSecurityEnabled and InstanceRole
// ---------------------------------------------------------------------------

func TestAudit4_ListInstanceTypeDetails_InstanceRoleAndSecurity(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/instanceTypeDetails", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	details, ok := out["InstanceTypeDetails"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, details)

	for _, raw := range details {
		entry, ok2 := raw.(map[string]any)
		require.True(t, ok2)

		_, hasRole := entry["InstanceRole"]
		assert.True(t, hasRole, "InstanceRole must be present for %v", entry["InstanceType"])

		roles, ok3 := entry["InstanceRole"].([]any)
		require.True(t, ok3, "InstanceRole must be a list")
		assert.NotEmpty(t, roles)

		_, hasSec := entry["AdvancedSecurityEnabled"]
		assert.True(t, hasSec, "AdvancedSecurityEnabled must be present for %v", entry["InstanceType"])
	}
}

// ---------------------------------------------------------------------------
// Backend: GetDomainHealth warm node count
// ---------------------------------------------------------------------------

func TestAudit4_GetDomainHealth_WarmNodeCount(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("warm-domain", "OpenSearch_2.11")

	h := opensearch.NewHandler(b)

	doRequest(t, h, http.MethodPut,
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
