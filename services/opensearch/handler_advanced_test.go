package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

func TestOpenSearch_UpgradeDomain(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainAndGetARN(t, h, "upgradedom")

	// UpgradeDomain (POST to upgradeDomain path)
	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/upgradeDomain", map[string]any{
			"DomainName":       "upgradedom",
			"TargetVersion":    "OpenSearch_2.11",
			"PerformCheckOnly": false,
		})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// GetUpgradeStatus (GET /upgradeDomain/{name}/status)
	resp = doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/upgradeDomain/upgradedom/status", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// GetUpgradeHistory (GET /upgradeDomain/{name}/history)
	resp = doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/upgradeDomain/upgradedom/history", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestOpenSearch_AutoTune(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainAndGetARN(t, h, "autotunedom")

	// GetAutoTune (GET /domain/{name}/autoTunes)
	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/autotunedom/autoTunes", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestOpenSearch_InstanceTypeLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/instanceTypeLimits/OpenSearch_2.11/r6g.large.search?domainName=testdom", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
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

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/instanceTypeDetails/OpenSearch_2.11", nil)
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

func TestGetCompatibleVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		query        string
		seedDomain   string
		seedVersion  string
		wantVersion  string
		wantStatus   int
		wantMultiple bool
	}{
		{
			name:       "unknown_domain_404",
			query:      "?domainName=no-such-domain",
			wantStatus: http.StatusNotFound,
		},
		{
			name:        "known_domain_200",
			query:       "?domainName=cv-domain",
			seedDomain:  "cv-domain",
			seedVersion: "OpenSearch_2.9",
			wantStatus:  http.StatusOK,
			wantVersion: "OpenSearch_2.9",
		},
		{
			name:         "no_domain_filter_all_versions",
			query:        "",
			wantStatus:   http.StatusOK,
			wantMultiple: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.seedDomain != "" {
				b := h.Backend.(*opensearch.InMemoryBackend)
				b.AddDomainInternal(tt.seedDomain, tt.seedVersion)
			}

			resp := doRequest(t, h, http.MethodGet,
				"/2021-01-01/opensearch/compatibleVersions"+tt.query, nil)
			defer resp.Body.Close()

			require.Equal(t, tt.wantStatus, resp.StatusCode)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			versions, ok := out["CompatibleVersions"].([]any)
			require.True(t, ok)

			if tt.wantVersion != "" {
				require.Len(t, versions, 1)
				entry := versions[0].(map[string]any)
				assert.Equal(t, tt.wantVersion, entry["SourceVersion"])
			}

			if tt.wantMultiple {
				assert.Greater(t, len(versions), 1, "no domain filter should return all versions")
			}
		})
	}
}

func TestListInstanceTypeDetails_InstanceRoleAndSecurity(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/instanceTypeDetails/OpenSearch_2.11", nil)
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

func TestListVersions_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		query           string
		wantContains    []string
		wantMinVersions int
		wantNextToken   bool
	}{
		{
			name:            "default_returns_all_versions",
			query:           "",
			wantMinVersions: 10,
			wantNextToken:   false,
			wantContains:    []string{"OpenSearch_2.17", "OpenSearch_2.11", "Elasticsearch_7.10"},
		},
		{
			name:            "maxResults_limits_returned",
			query:           "?maxResults=3",
			wantMinVersions: 1,
			wantNextToken:   true,
		},
		{
			name:            "maxResults_1_returns_one",
			query:           "?maxResults=1",
			wantMinVersions: 1,
			wantNextToken:   true,
		},
		{
			name:            "maxResults_larger_than_total_no_next_token",
			query:           "?maxResults=1000",
			wantMinVersions: 10,
			wantNextToken:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/versions"+tt.query, nil)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			versions, ok := out["Versions"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(versions), tt.wantMinVersions)

			_, hasNext := out["NextToken"]
			assert.Equal(t, tt.wantNextToken, hasNext, "NextToken presence mismatch")

			for _, want := range tt.wantContains {
				found := false
				for _, v := range versions {
					if v.(string) == want {
						found = true

						break
					}
				}
				assert.True(t, found, "expected version %q in results", want)
			}
		})
	}
}

func TestListVersions_NextTokenContinuation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// First page: get 3 versions
	resp1 := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/versions?maxResults=3", nil)
	defer resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)

	var page1 map[string]any
	require.NoError(t, json.NewDecoder(resp1.Body).Decode(&page1))

	versions1 := page1["Versions"].([]any)
	assert.Len(t, versions1, 3)
	nextToken, hasNext := page1["NextToken"].(string)
	require.True(t, hasNext, "first page must have NextToken")

	// Second page: use nextToken
	resp2 := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/versions?nextToken="+nextToken, nil)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	var page2 map[string]any
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&page2))

	versions2 := page2["Versions"].([]any)
	assert.NotEmpty(t, versions2)

	// No overlap between pages.
	page1Set := make(map[string]bool, len(versions1))
	for _, v := range versions1 {
		page1Set[v.(string)] = true
	}
	for _, v := range versions2 {
		assert.False(t, page1Set[v.(string)], "version %q should not be in both pages", v)
	}
}

func TestGetUpgradeStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		domainName     string
		upgradeVersion string
		wantUpgrade    string
		wantStep       string
		seedDomain     bool
		wantErr        bool
	}{
		{
			name:           "upgrade_returns_last_step",
			domainName:     "upgrade-domain",
			upgradeVersion: "OpenSearch_2.17",
			seedDomain:     true,
			wantUpgrade:    "OpenSearch_2.17",
			wantStep:       "UPGRADE",
		},
		{
			name:        "no_history_returns_initial",
			domainName:  "no-upgrade",
			seedDomain:  true,
			wantUpgrade: "INITIAL",
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			if tt.seedDomain {
				b.AddDomainInternal(tt.domainName, "OpenSearch_2.11")
			}

			if tt.upgradeVersion != "" {
				err := b.UpgradeDomain(tt.domainName, tt.upgradeVersion)
				require.NoError(t, err)
			}

			upgradeName, status, step, err := b.GetUpgradeStatus(tt.domainName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			assert.Equal(t, tt.wantUpgrade, upgradeName)
			assert.Equal(t, "SUCCEEDED", status)

			if tt.wantStep != "" {
				assert.Equal(t, tt.wantStep, step)
			} else {
				assert.NotEmpty(t, step)
			}
		})
	}
}

func TestDescribeInstanceTypeLimits_ResponseFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		instanceType   string
		engineVersion  string
		wantRoleKeys   []string
		wantInnerKeys  []string
		wantAbsentKeys []string
	}{
		{
			name:           "r6g_large_has_correct_shape",
			instanceType:   "r6g.large.search",
			engineVersion:  "OpenSearch_2.11",
			wantRoleKeys:   []string{"data"},
			wantInnerKeys:  []string{"StorageTypes", "InstanceLimits"},
			wantAbsentKeys: []string{"InstanceType"},
		},
		{
			name:           "t3_small_has_correct_shape",
			instanceType:   "t3.small.search",
			engineVersion:  "OpenSearch_2.11",
			wantRoleKeys:   []string{"data"},
			wantInnerKeys:  []string{"StorageTypes", "InstanceLimits"},
			wantAbsentKeys: []string{"InstanceType"},
		},
		{
			name:           "m6g_large_has_correct_shape",
			instanceType:   "m6g.large.search",
			engineVersion:  "OpenSearch_2.11",
			wantRoleKeys:   []string{"data"},
			wantInnerKeys:  []string{"StorageTypes", "InstanceLimits"},
			wantAbsentKeys: []string{"InstanceType"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := "/2021-01-01/opensearch/instanceTypeLimits/" + tt.engineVersion + "/" + tt.instanceType
			resp := doRequest(t, h, http.MethodGet, path, nil)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			limitsByRole, ok := out["LimitsByRole"].(map[string]any)
			require.True(t, ok, "LimitsByRole must be an object")

			for _, key := range tt.wantRoleKeys {
				assert.Contains(t, limitsByRole, key, "LimitsByRole must contain %q", key)
			}

			if data, ok2 := limitsByRole["data"].(map[string]any); ok2 {
				for _, key := range tt.wantInnerKeys {
					assert.Contains(t, data, key, "data must contain %q", key)
				}

				for _, key := range tt.wantAbsentKeys {
					assert.NotContains(t, data, key, "data must NOT contain %q", key)
				}
			}
		})
	}
}

func TestUpgradeDomain_HTTPHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(h *opensearch.Handler)
		name          string
		domainName    string
		targetVersion string
		wantFields    []string
		wantCode      int
	}{
		{
			name: "success",
			setup: func(h *opensearch.Handler) {
				h.Backend.(*opensearch.InMemoryBackend).AddDomainInternal("upg-domain", "OpenSearch_2.11")
			},
			domainName:    "upg-domain",
			targetVersion: "OpenSearch_2.17",
			wantCode:      http.StatusOK,
			wantFields:    []string{"DomainName", "TargetVersion"},
		},
		{
			name:          "domain_not_found",
			setup:         func(_ *opensearch.Handler) {},
			domainName:    "missing-domain",
			targetVersion: "OpenSearch_2.17",
			wantCode:      http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.setup(h)

			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/upgradeDomain",
				map[string]any{
					"DomainName":    tt.domainName,
					"TargetVersion": tt.targetVersion,
				})
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantFields) > 0 {
				var out map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
				for _, field := range tt.wantFields {
					assert.Contains(t, out, field)
				}
			}
		})
	}
}

func TestGetCompatibleVersions_DomainFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
		wantNonEmpty  bool
	}{
		{
			name:          "opensearch_211_no_target_versions",
			engineVersion: "OpenSearch_2.11",
			wantNonEmpty:  true,
		},
		{
			name:          "opensearch_29_has_targets",
			engineVersion: "OpenSearch_2.9",
			wantNonEmpty:  true,
		},
		{
			name:          "opensearch_13_upgrades_to_27",
			engineVersion: "OpenSearch_1.3",
			wantNonEmpty:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.AddDomainInternal("cv-domain", tt.engineVersion)

			versions := b.GetCompatibleVersions("cv-domain")
			if tt.wantNonEmpty {
				require.NotEmpty(t, versions)
				assert.Equal(t, tt.engineVersion, versions[0]["SourceVersion"])
			}
		})
	}
}

func TestListInstanceTypeDetails_NonEmpty(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	details := b.ListInstanceTypeDetails("", "")

	require.NotEmpty(t, details, "ListInstanceTypeDetails should return at least one type")

	for _, d := range details {
		assert.Contains(t, d, "InstanceType", "each entry must have InstanceType")
	}
}

func TestListInstanceTypeDetails_HTTPHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/instanceTypeDetails/OpenSearch_2.11", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	details, ok := out["InstanceTypeDetails"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, details, "InstanceTypeDetails must not be empty")
}
