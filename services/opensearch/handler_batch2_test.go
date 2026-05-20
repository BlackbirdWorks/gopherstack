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

func createTestDomain(t *testing.T, h *opensearch.Handler, domainName string) {
	t.Helper()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": domainName})
	resp.Body.Close()
}

func TestOpenSearchHandler_DescribeDomains(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create two domains.
	for _, name := range []string{"domain-a", "domain-b"} {
		createTestDomain(t, h, name)
	}

	// Bulk describe with explicit names.
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/describe",
		map[string]any{"DomainNames": []string{"domain-a", "domain-b"}})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	list, ok := out["DomainStatusList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)

	// Verify required fields present in each entry.
	for _, item := range list {
		m, ok2 := item.(map[string]any)
		require.True(t, ok2)
		assert.NotEmpty(t, m["DomainName"])
		assert.NotEmpty(t, m["ARN"])
		assert.NotEmpty(t, m["EngineVersion"])
	}
}

func TestOpenSearchHandler_DescribeDomains_All(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"x-one", "x-two", "x-three"} {
		createTestDomain(t, h, name)
	}

	// GET with no body → returns all domains.
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/describe", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	list, ok := out["DomainStatusList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 3)
}

func TestOpenSearchHandler_UpdateDomainConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create domain.
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "config-domain", "EngineVersion": "OpenSearch_2.7"})
	resp.Body.Close()

	// Update config.
	upResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/config-domain/config",
		map[string]any{"EngineVersion": "OpenSearch_2.9"})
	defer upResp.Body.Close()

	assert.Equal(t, http.StatusOK, upResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(upResp.Body).Decode(&out))

	domainConfig, ok := out["DomainConfig"].(map[string]any)
	require.True(t, ok)
	engineVersion, ok := domainConfig["EngineVersion"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "OpenSearch_2.9", engineVersion["Options"])
}

func TestOpenSearchHandler_UpdateDomainConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/nonexistent/config",
		map[string]any{"EngineVersion": "OpenSearch_2.9"})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOpenSearchHandler_DefaultApplicationSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// GET before any settings → empty list.
	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/application/settings/default?ApplicationType=MyApp", nil)
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	settings0, ok := out["DefaultApplicationSettings"].([]any)
	require.True(t, ok)
	assert.Empty(t, settings0)

	// PUT settings.
	putResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/application/settings/default",
		map[string]any{
			"ApplicationType": "MyApp",
			"DefaultApplicationSettings": []map[string]any{
				{"Key": "theme", "Value": "dark"},
				{"Key": "locale", "Value": "en-US"},
			},
		})
	putResp.Body.Close()
	assert.Equal(t, http.StatusOK, putResp.StatusCode)

	// GET after PUT → should return stored settings.
	getResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/application/settings/default?ApplicationType=MyApp", nil)
	defer getResp.Body.Close()

	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out2))

	assert.Equal(t, "MyApp", out2["ApplicationType"])

	settings, ok := out2["DefaultApplicationSettings"].([]any)
	require.True(t, ok)
	assert.Len(t, settings, 2)
}

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

func TestOpenSearchHandler_DescribeDomainHealth_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/missing/health", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
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

func TestOpenSearchHandler_DescribeDomainNodes_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/missing/nodes", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOpenSearchHandler_GetCompatibleVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/compatibleVersions", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	versions, ok := out["CompatibleVersions"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, versions)

	// Verify structure.
	for _, v := range versions {
		entry, ok2 := v.(map[string]any)
		require.True(t, ok2)
		assert.NotEmpty(t, entry["SourceVersion"])
		_, hasTargets := entry["TargetVersions"]
		assert.True(t, hasTargets)
	}
}

func TestOpenSearchHandler_ListInstanceTypeDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/instanceTypeDetails", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	details, ok := out["InstanceTypeDetails"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, details)

	// Verify the first entry has expected fields.
	first, ok := details[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, first["InstanceType"])
}

func TestOpenSearchHandler_DissociatePackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create domain.
	createTestDomain(t, h, "dissoc-domain")

	// Create package.
	pkgResp := doRequest(t, h, http.MethodPost, "/2021-01-01/packages",
		map[string]any{"PackageName": "my-pkg", "PackageType": "TXT-DICTIONARY"})

	var pkgOut map[string]any
	require.NoError(t, json.NewDecoder(pkgResp.Body).Decode(&pkgOut))
	pkgResp.Body.Close()

	pkgDetails, ok := pkgOut["PackageDetails"].(map[string]any)
	require.True(t, ok)
	pkgID, ok := pkgDetails["PackageID"].(string)
	require.True(t, ok)

	// Associate the package.
	assocResp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/packages/associate/"+pkgID+"/dissoc-domain", nil)
	assocResp.Body.Close()
	require.Equal(t, http.StatusOK, assocResp.StatusCode)

	// Dissociate.
	dissocResp := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/packages/dissociate/"+pkgID+"/dissoc-domain", nil)
	defer dissocResp.Body.Close()

	assert.Equal(t, http.StatusOK, dissocResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(dissocResp.Body).Decode(&out))

	details2, ok := out["DomainPackageDetails"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, pkgID, details2["PackageID"])
	assert.Equal(t, "dissoc-domain", details2["DomainName"])
	assert.Equal(t, "DISSOCIATED", details2["DomainPackageStatus"])
}

func TestOpenSearchHandler_DissociatePackages(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create domain.
	createTestDomain(t, h, "multi-dissoc-domain")

	// Create two packages and associate them.
	pkgIDs := make([]string, 0, 2)

	for _, name := range []string{"pkg-one", "pkg-two"} {
		pkgResp := doRequest(t, h, http.MethodPost, "/2021-01-01/packages",
			map[string]any{"PackageName": name, "PackageType": "TXT-DICTIONARY"})
		var pkgOut map[string]any
		require.NoError(t, json.NewDecoder(pkgResp.Body).Decode(&pkgOut))
		pkgResp.Body.Close()

		id := pkgOut["PackageDetails"].(map[string]any)["PackageID"].(string)
		pkgIDs = append(pkgIDs, id)

		assocResp := doRequest(t, h, http.MethodPost,
			"/2021-01-01/packages/associate/"+id+"/multi-dissoc-domain", nil)
		assocResp.Body.Close()
	}

	// Dissociate multiple.
	pkgList := make([]map[string]any, 0, len(pkgIDs))
	for _, id := range pkgIDs {
		pkgList = append(pkgList, map[string]any{"PackageID": id})
	}

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/packages/dissociateMultiple",
		map[string]any{
			"DomainName":  "multi-dissoc-domain",
			"PackageList": pkgList,
		})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	list, ok := out["DomainPackageDetailsList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)

	for _, item := range list {
		entry, ok2 := item.(map[string]any)
		require.True(t, ok2)
		assert.Equal(t, "DISSOCIATED", entry["DomainPackageStatus"])
	}
}

func TestOpenSearchHandler_DescribeDomainChangeProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create domain.
	createTestDomain(t, h, "progress-domain")

	// Update config to generate a change ID.
	updateResp := doRequest(t, h, http.MethodPut,
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
