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
// Domain name validation (issue #6)
// ---------------------------------------------------------------------------

func TestAudit2_CreateDomain_DomainNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainName string
		wantCode   int
	}{
		{
			name:       "valid_lowercase_with_hyphens",
			domainName: "my-domain",
			wantCode:   http.StatusOK,
		},
		{
			name:       "valid_minimum_length",
			domainName: "abc",
			wantCode:   http.StatusOK,
		},
		{
			name:       "valid_maximum_length_28",
			domainName: "abcdefghijklmnopqrstuvwxyz-a",
			wantCode:   http.StatusOK,
		},
		{
			name:       "valid_starts_with_letter_contains_digits",
			domainName: "dom123",
			wantCode:   http.StatusOK,
		},
		{
			name:       "invalid_has_underscore",
			domainName: "my_domain",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "invalid_starts_with_digit",
			domainName: "1domain",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "invalid_uppercase",
			domainName: "MyDomain",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "invalid_too_short",
			domainName: "ab",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "invalid_too_long_29_chars",
			domainName: "abcdefghijklmnopqrstuvwxyz-ab",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "invalid_empty",
			domainName: "",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "invalid_starts_with_hyphen",
			domainName: "-invalid",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "invalid_special_chars",
			domainName: "dom@ain",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "invalid_spaces",
			domainName: "my domain",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{"DomainName": tt.domainName}
			if tt.domainName == "" {
				body = map[string]any{}
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode, "domain %q", tt.domainName)
		})
	}
}

// ---------------------------------------------------------------------------
// Engine version validation (issue #5)
// ---------------------------------------------------------------------------

func TestAudit2_CreateDomain_EngineVersionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
		wantCode      int
	}{
		{
			name:          "valid_opensearch_version",
			engineVersion: "OpenSearch_2.11",
			wantCode:      http.StatusOK,
		},
		{
			name:          "valid_opensearch_major_minor",
			engineVersion: "OpenSearch_1.3",
			wantCode:      http.StatusOK,
		},
		{
			name:          "valid_elasticsearch_version",
			engineVersion: "Elasticsearch_7.10",
			wantCode:      http.StatusOK,
		},
		{
			name:          "valid_elasticsearch_major_only",
			engineVersion: "Elasticsearch_8.11",
			wantCode:      http.StatusOK,
		},
		{
			name:          "empty_version_uses_default",
			engineVersion: "",
			wantCode:      http.StatusOK,
		},
		{
			name:          "invalid_version_no_prefix",
			engineVersion: "2.11",
			wantCode:      http.StatusBadRequest,
		},
		{
			name:          "invalid_version_wrong_prefix",
			engineVersion: "Solr_2.11",
			wantCode:      http.StatusBadRequest,
		},
		{
			name:          "invalid_version_missing_dot",
			engineVersion: "OpenSearch_211",
			wantCode:      http.StatusBadRequest,
		},
		{
			name:          "invalid_version_no_number",
			engineVersion: "OpenSearch_",
			wantCode:      http.StatusBadRequest,
		},
		{
			name:          "invalid_version_lowercase_prefix",
			engineVersion: "openSearch_2.11",
			wantCode:      http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{"DomainName": "eng-ver-td"}
			if tt.engineVersion != "" {
				body["EngineVersion"] = tt.engineVersion
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode, "engine version %q", tt.engineVersion)
		})
	}
}

// ---------------------------------------------------------------------------
// ListDomainNames engineType filter (issue #9)
// ---------------------------------------------------------------------------

func TestAudit2_ListDomainNames_EngineTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
		wantNames  []string
		wantAbsent []string
	}{
		{
			name:       "no_filter_returns_all",
			engineType: "",
			wantNames:  []string{"os-domain", "es-domain"},
		},
		{
			name:       "opensearch_filter",
			engineType: "OpenSearch",
			wantNames:  []string{"os-domain"},
			wantAbsent: []string{"es-domain"},
		},
		{
			name:       "elasticsearch_filter",
			engineType: "Elasticsearch",
			wantNames:  []string{"es-domain"},
			wantAbsent: []string{"os-domain"},
		},
		{
			name:       "unknown_filter_returns_empty",
			engineType: "Unknown",
			wantNames:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createTestDomainWithVersion(t, h, "os-domain", "OpenSearch_2.11")
			createTestDomainWithVersion(t, h, "es-domain", "Elasticsearch_7.10")

			path := "/2021-01-01/opensearch/domain"
			if tt.engineType != "" {
				path += "?engineType=" + tt.engineType
			}
			resp := doRequest(t, h, http.MethodGet, path, nil)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			domainNames, ok := out["DomainNames"].([]any)
			require.True(t, ok)

			returnedNames := make([]string, 0, len(domainNames))
			for _, entry := range domainNames {
				m, ok2 := entry.(map[string]any)
				require.True(t, ok2)
				if name, ok3 := m["DomainName"].(string); ok3 {
					returnedNames = append(returnedNames, name)
				}
			}

			for _, want := range tt.wantNames {
				assert.Contains(t, returnedNames, want, "should contain %s", want)
			}

			for _, absent := range tt.wantAbsent {
				assert.NotContains(t, returnedNames, absent, "should not contain %s", absent)
			}
		})
	}
}

func createTestDomainWithVersion(t *testing.T, h *opensearch.Handler, name, version string) {
	t.Helper()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": name, "EngineVersion": version})
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// DescribeDomains returns full DomainStatus (issue #8)
// ---------------------------------------------------------------------------

func TestAudit2_DescribeDomains_FullDomainStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createBody   map[string]any
		describeBody map[string]any
		wantFields   []string
		wantCount    int
	}{
		{
			name: "single_domain_full_fields",
			createBody: map[string]any{
				"DomainName":    "dd-full",
				"EngineVersion": "OpenSearch_2.11",
				"EBSOptions": map[string]any{
					"EBSEnabled": true,
					"VolumeType": "gp3",
					"VolumeSize": 100,
				},
			},
			describeBody: map[string]any{"DomainNames": []string{"dd-full"}},
			wantFields:   []string{"DomainName", "ARN", "EngineVersion", "ClusterConfig", "EBSOptions", "Endpoint"},
			wantCount:    1,
		},
		{
			name: "multiple_domains",
			createBody: map[string]any{
				"DomainName":    "dd-multi",
				"EngineVersion": "OpenSearch_2.11",
			},
			describeBody: map[string]any{"DomainNames": []string{"dd-multi"}},
			wantFields:   []string{"DomainName", "ARN", "EngineVersion"},
			wantCount:    1,
		},
		{
			name: "empty_names_returns_all",
			createBody: map[string]any{
				"DomainName": "dd-all",
			},
			describeBody: map[string]any{"DomainNames": []string{}},
			wantFields:   []string{},
			wantCount:    -1, // -1 = any count ≥ 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", tt.createBody)
			cr.Body.Close()

			resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/describe", tt.describeBody)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			list, ok := out["DomainStatusList"].([]any)
			require.True(t, ok)

			if tt.wantCount >= 0 {
				assert.Len(t, list, tt.wantCount)
			} else {
				assert.GreaterOrEqual(t, len(list), 1)
			}

			if len(list) > 0 {
				first, ok2 := list[0].(map[string]any)
				require.True(t, ok2)
				for _, field := range tt.wantFields {
					assert.Contains(t, first, field, "expected field %q in response", field)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ListVersions pagination (issue #20)
// ---------------------------------------------------------------------------

func TestAudit2_ListVersions_Pagination(t *testing.T) {
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

func TestAudit2_ListVersions_NextTokenContinuation(t *testing.T) {
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

// ---------------------------------------------------------------------------
// GetUpgradeStatus returns last completed step (issue #24)
// ---------------------------------------------------------------------------

func TestAudit2_GetUpgradeStatus_ReturnsLastStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		domainName     string
		upgradeVersion string
		wantUpgrade    string
		wantStep       string
	}{
		{
			name:           "upgrade_returns_last_step",
			domainName:     "upgrade-domain",
			upgradeVersion: "OpenSearch_2.17",
			wantUpgrade:    "OpenSearch_2.17",
			wantStep:       "UPGRADE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.AddDomainInternal(tt.domainName, "OpenSearch_2.11")

			err := b.UpgradeDomain(tt.domainName, tt.upgradeVersion)
			require.NoError(t, err)

			upgradeName, status, step, err := b.GetUpgradeStatus(tt.domainName)
			require.NoError(t, err)

			assert.Equal(t, tt.wantUpgrade, upgradeName)
			assert.Equal(t, "SUCCEEDED", status)
			assert.Equal(t, tt.wantStep, step)
		})
	}
}

func TestAudit2_GetUpgradeStatus_NoHistory(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("no-upgrade", "OpenSearch_2.11")

	upgradeName, status, step, err := b.GetUpgradeStatus("no-upgrade")
	require.NoError(t, err)

	assert.Equal(t, "INITIAL", upgradeName)
	assert.Equal(t, "SUCCEEDED", status)
	assert.NotEmpty(t, step)
}

func TestAudit2_GetUpgradeStatus_DomainNotFound(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, _, _, err := b.GetUpgradeStatus("nonexistent")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// DescribeInstanceTypeLimits response format (issue #22)
// ---------------------------------------------------------------------------

func TestAudit2_DescribeInstanceTypeLimits_ResponseFormat(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Persistence: all maps round-trip through snapshot/restore (issue #31)
// ---------------------------------------------------------------------------

func TestAudit2_Persistence_PackagesRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	pkg, err := b.CreatePackage("my-pkg", "TXT-DICTIONARY", "test package", nil, nil)
	require.NoError(t, err)
	pkgID := pkg.PackageID

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	pkgs, err := fresh.DescribePackages([]string{pkgID})
	require.NoError(t, err)
	require.Len(t, pkgs, 1)
	assert.Equal(t, "my-pkg", pkgs[0].PackageName)
	assert.Equal(t, "TXT-DICTIONARY", pkgs[0].PackageType)
}

func TestAudit2_Persistence_VpcEndpointsRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("vpc-persist", "")

	domARN := "arn:aws:es:us-east-1:123456789012:domain/vpc-persist"
	ep, err := b.CreateVpcEndpoint(domARN, map[string]any{
		"SubnetIds":        []string{"subnet-abc"},
		"SecurityGroupIds": []string{"sg-abc"},
	})
	require.NoError(t, err)
	epID := ep.VpcEndpointID

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	endpoints := fresh.ListVpcEndpoints()
	require.NotEmpty(t, endpoints)
	found := false
	for _, e := range endpoints {
		if e.VpcEndpointID == epID {
			found = true

			break
		}
	}
	assert.True(t, found, "VPC endpoint should persist through snapshot/restore")
}

func TestAudit2_Persistence_UpgradeHistoryRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("uh-persist", "OpenSearch_2.11")

	err := b.UpgradeDomain("uh-persist", "OpenSearch_2.17")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	history, err := fresh.GetUpgradeHistory("uh-persist")
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, "OpenSearch_2.17", history[0].UpgradeName)
}

func TestAudit2_Persistence_AutoTunesRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("at-persist", "")

	err := b.SetAutoTune("at-persist", "ENABLED", nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Domain should still exist.
	d, err := fresh.DescribeDomain("at-persist")
	require.NoError(t, err)
	assert.Equal(t, "at-persist", d.Name)

	// AutoTune entries should be present.
	tunes, err := fresh.GetAutoTune("at-persist")
	require.NoError(t, err)
	assert.NotEmpty(t, tunes)
}

func TestAudit2_Persistence_ReservedInstancesRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	ri, err := b.PurchaseReservedInstanceOffering("ri-offering-1", "my-reservation", 3)
	require.NoError(t, err)
	riID := ri.ReservedInstanceID

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	instances := fresh.DescribeReservedInstances()
	require.NotEmpty(t, instances)
	found := false
	for _, inst := range instances {
		if inst.ReservedInstanceID == riID {
			found = true
			assert.Equal(t, "my-reservation", inst.ReservationName)
			assert.Equal(t, 3, inst.InstanceCount)

			break
		}
	}
	assert.True(t, found, "reserved instance should persist through snapshot/restore")
}

func TestAudit2_Persistence_OutboundConnectionsRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	conn, err := b.CreateOutboundConnection(
		"test-alias",
		map[string]any{"DomainName": "local-domain"},
		map[string]any{"DomainName": "remote-domain"},
	)
	require.NoError(t, err)
	connID := conn.ConnectionID

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	conns := fresh.DescribeOutboundConnections()
	require.NotEmpty(t, conns)
	found := false
	for _, c := range conns {
		if c.ConnectionID == connID {
			found = true
			assert.Equal(t, "test-alias", c.ConnectionAlias)

			break
		}
	}
	assert.True(t, found, "outbound connection should persist through snapshot/restore")
}

func TestAudit2_Persistence_ScheduledActionsRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("sa-persist", "")

	action := &opensearch.ScheduledAction{
		ID:          "sa-001",
		Type:        "SERVICE_SOFTWARE_UPDATE",
		Severity:    "HIGH",
		ScheduledBy: "CUSTOMER",
		Status:      "PENDING_UPDATE",
	}
	_, err := b.UpdateScheduledAction("sa-persist", action)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	actions := fresh.ListScheduledActions("sa-persist")
	require.Len(t, actions, 1)
	assert.Equal(t, "sa-001", actions[0].ID)
	assert.Equal(t, "HIGH", actions[0].Severity)
}

func TestAudit2_Persistence_DomainIndexesRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("idx-persist", "")

	idx, err := b.CreateIndex(
		"idx-persist", "my-index",
		map[string]any{"properties": map[string]any{"field": "text"}},
		map[string]any{"number_of_shards": 1},
		map[string]any{},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-index", idx.IndexName)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	restoredIdx, err := fresh.GetIndex("idx-persist", "my-index")
	require.NoError(t, err)
	assert.Equal(t, "my-index", restoredIdx.IndexName)
}

func TestAudit2_Persistence_CountersRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	// Create items to bump counters.
	b.AddDomainInternal("counter-dom", "")
	_, err := b.CreateOutboundConnection("alias1", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateVpcEndpoint("arn:test", nil)
	require.NoError(t, err)
	_, err = b.CreatePackage("pkg-counter", "TXT-DICTIONARY", "", nil, nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Creating new items after restore should not reuse old IDs.
	conn2, err := fresh.CreateOutboundConnection("alias2", nil, nil)
	require.NoError(t, err)
	assert.NotEqual(t, "co-1", conn2.ConnectionID, "counter should be restored, new ID should not collide")

	ep2, err := fresh.CreateVpcEndpoint("arn:test2", nil)
	require.NoError(t, err)
	assert.NotEqual(t, "vpce-1", ep2.VpcEndpointID, "VPC endpoint counter should be restored")
}

// ---------------------------------------------------------------------------
// AssociatePackage validates package existence (issue #14)
// ---------------------------------------------------------------------------

func TestAudit2_AssociatePackage_PackageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(b *opensearch.InMemoryBackend)
		packageID  string
		domainName string
		wantErr    bool
	}{
		{
			name: "success_when_both_exist",
			setup: func(b *opensearch.InMemoryBackend) {
				b.AddDomainInternal("assoc-domain", "")
				b.AddPackageInternal("pkg-valid", "my-pkg", "TXT-DICTIONARY")
			},
			packageID:  "pkg-valid",
			domainName: "assoc-domain",
			wantErr:    false,
		},
		{
			name: "error_package_not_found",
			setup: func(b *opensearch.InMemoryBackend) {
				b.AddDomainInternal("assoc-domain-np", "")
			},
			packageID:  "pkg-missing",
			domainName: "assoc-domain-np",
			wantErr:    true,
		},
		{
			name: "error_domain_not_found",
			setup: func(b *opensearch.InMemoryBackend) {
				b.AddPackageInternal("pkg-nodomain", "my-pkg", "TXT-DICTIONARY")
			},
			packageID:  "pkg-nodomain",
			domainName: "missing-domain",
			wantErr:    true,
		},
		{
			name:       "error_both_not_found",
			setup:      func(_ *opensearch.InMemoryBackend) {},
			packageID:  "pkg-none",
			domainName: "dom-none",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(b)

			_, err := b.AssociatePackage(tt.packageID, tt.domainName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// UpgradeDomain HTTP handler wires through to backend (issue #23)
// ---------------------------------------------------------------------------

func TestAudit2_UpgradeDomain_HTTPHandler(t *testing.T) {
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

// ---------------------------------------------------------------------------
// UpdateDomainConfig actually mutates state (issue #7)
// ---------------------------------------------------------------------------

func TestAudit2_UpdateDomainConfig_MutatesState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		verifyFn   func(t *testing.T, domain map[string]any)
		name       string
	}{
		{
			name: "update_cluster_config",
			updateBody: map[string]any{
				"ClusterConfig": map[string]any{
					"InstanceType":  "m6g.large.search",
					"InstanceCount": 3,
				},
			},
			verifyFn: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc := status["DomainStatus"].(map[string]any)
				cc := dc["ClusterConfig"].(map[string]any)
				assert.Equal(t, "m6g.large.search", cc["InstanceType"])
				assert.InDelta(t, float64(3), cc["InstanceCount"], 0)
			},
		},
		{
			name: "update_ebs_options",
			updateBody: map[string]any{
				"EBSOptions": map[string]any{
					"EBSEnabled": true,
					"VolumeType": "gp3",
					"VolumeSize": 200,
				},
			},
			verifyFn: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc := status["DomainStatus"].(map[string]any)
				ebs := dc["EBSOptions"].(map[string]any)
				assert.Equal(t, true, ebs["EBSEnabled"])
				assert.Equal(t, "gp3", ebs["VolumeType"])
				assert.InDelta(t, float64(200), ebs["VolumeSize"], 0)
			},
		},
		{
			name: "update_access_policies",
			updateBody: map[string]any{
				"AccessPolicies": `{"Version":"2012-10-17","Statement":[]}`,
			},
			verifyFn: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc := status["DomainStatus"].(map[string]any)
				assert.NotEmpty(t, dc["AccessPolicies"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
				map[string]any{"DomainName": "mut-test"})
			cr.Body.Close()
			require.Equal(t, http.StatusOK, cr.StatusCode)

			upResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/mut-test/config",
				tt.updateBody)
			upResp.Body.Close()
			require.Equal(t, http.StatusOK, upResp.StatusCode)

			// Now describe to verify state was mutated.
			descResp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/mut-test", nil)
			defer descResp.Body.Close()
			require.Equal(t, http.StatusOK, descResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(descResp.Body).Decode(&out))
			tt.verifyFn(t, out)
		})
	}
}

// ---------------------------------------------------------------------------
// ListVpcEndpointAccess / RevokeVpcEndpointAccess lifecycle (issue #12)
// ---------------------------------------------------------------------------

func TestAudit2_VpcEndpointAccess_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		revokeAccount string
		accounts      []string
		wantCount     int
	}{
		{
			name:      "authorize_and_list",
			accounts:  []string{"111122223333"},
			wantCount: 1,
		},
		{
			name:          "authorize_then_revoke",
			accounts:      []string{"111122223333", "444455556666"},
			revokeAccount: "111122223333",
			wantCount:     1,
		},
		{
			name:      "multiple_authorize",
			accounts:  []string{"111122223333", "444455556666", "777788889999"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.AddDomainInternal("vpc-domain", "")

			for _, account := range tt.accounts {
				_, err := b.AuthorizeVpcEndpointAccess("vpc-domain", account, "")
				require.NoError(t, err)
			}

			if tt.revokeAccount != "" {
				err := b.RevokeVpcEndpointAccess("vpc-domain", tt.revokeAccount)
				require.NoError(t, err)
			}

			principals, err := b.ListVpcEndpointAccess("vpc-domain")
			require.NoError(t, err)
			assert.Len(t, principals, tt.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// DataSources lifecycle — ListDataSources and GetDataSource return backend data
// ---------------------------------------------------------------------------

func TestAudit2_DataSources_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		getSourceName  string
		addSources     []map[string]string
		wantListCount  int
		wantGetSuccess bool
	}{
		{
			name:           "single_source_listed",
			addSources:     []map[string]string{{"name": "ds1", "type": "S3GLUE", "desc": "test"}},
			wantListCount:  1,
			getSourceName:  "ds1",
			wantGetSuccess: true,
		},
		{
			name: "multiple_sources_listed",
			addSources: []map[string]string{
				{"name": "ds1", "type": "S3GLUE", "desc": ""},
				{"name": "ds2", "type": "CloudWatchLogs", "desc": ""},
				{"name": "ds3", "type": "SecurityLake", "desc": ""},
			},
			wantListCount:  3,
			getSourceName:  "ds2",
			wantGetSuccess: true,
		},
		{
			name:           "empty_domain_no_sources",
			addSources:     []map[string]string{},
			wantListCount:  0,
			getSourceName:  "missing",
			wantGetSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.AddDomainInternal("ds-domain", "")

			for _, src := range tt.addSources {
				_, err := b.AddDataSource("ds-domain", src["name"], src["desc"], src["type"])
				require.NoError(t, err)
			}

			sources, err := b.ListDataSources("ds-domain")
			require.NoError(t, err)
			assert.Len(t, sources, tt.wantListCount)

			_, getErr := b.GetDataSource("ds-domain", tt.getSourceName)
			if tt.wantGetSuccess {
				require.NoError(t, getErr)
			} else {
				require.Error(t, getErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Applications lifecycle — List/Get/Update/Delete read from backend
// ---------------------------------------------------------------------------

func TestAudit2_Applications_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		appName     string
		appConfigs  []map[string]string
		wantInList  bool
		wantGetOK   bool
		wantDeleted bool
	}{
		{
			name:       "create_and_list",
			appName:    "my-app",
			wantInList: true,
			wantGetOK:  true,
		},
		{
			name:        "create_and_delete",
			appName:     "del-app",
			wantInList:  true,
			wantDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

			app, err := b.CreateApplication(tt.appName, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, app.ID)
			assert.Equal(t, tt.appName, app.Name)

			apps := b.ListApplications()
			if tt.wantInList {
				require.NotEmpty(t, apps)
				found := false
				for _, a := range apps {
					if a.Name == tt.appName {
						found = true
					}
				}
				assert.True(t, found, "app should appear in list")
			}

			if tt.wantGetOK {
				got, getErr := b.GetApplication(app.ID)
				require.NoError(t, getErr)
				assert.Equal(t, tt.appName, got.Name)
			}

			if tt.wantDeleted {
				err = b.DeleteApplication(app.ID)
				require.NoError(t, err)

				apps = b.ListApplications()
				for _, a := range apps {
					assert.NotEqual(t, app.ID, a.ID, "deleted app should not appear in list")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Applications HTTP handler reads from backend (issue #19)
// ---------------------------------------------------------------------------

func TestAudit2_Applications_HTTPHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		wantFields []string
	}{
		{
			name:       "get_returns_real_app_data",
			appName:    "real-app",
			wantFields: []string{"Id", "Name", "Arn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create app via HTTP.
			cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application",
				map[string]any{"Name": tt.appName, "AppConfigs": []any{}, "DataSources": []any{}})
			cr.Body.Close()
			require.Equal(t, http.StatusOK, cr.StatusCode)

			// List apps via HTTP.
			lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/application", nil)
			defer lr.Body.Close()
			require.Equal(t, http.StatusOK, lr.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(lr.Body).Decode(&out))

			apps, ok := out["ApplicationSummaries"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, apps)

			first := apps[0].(map[string]any)
			for _, field := range tt.wantFields {
				assert.Contains(t, first, field, "expected field %q in application list", field)
			}
			assert.Equal(t, tt.appName, first["Name"])
		})
	}
}

// ---------------------------------------------------------------------------
// Persistence: domain maintenance persists (issue #31)
// ---------------------------------------------------------------------------

func TestAudit2_Persistence_DomainMaintenanceRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("maint-domain", "")

	m, err := b.StartDomainMaintenance("maint-domain", "REBOOT_NODE", "node-1")
	require.NoError(t, err)
	assert.NotEmpty(t, m.MaintenanceID)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	maintenances, err := fresh.ListDomainMaintenances("maint-domain")
	require.NoError(t, err)
	require.Len(t, maintenances, 1)
	assert.Equal(t, "REBOOT_NODE", maintenances[0].Action)
	assert.Equal(t, "node-1", maintenances[0].NodeID)
}

// ---------------------------------------------------------------------------
// Compatibility versions return correct data
// ---------------------------------------------------------------------------

func TestAudit2_GetCompatibleVersions_DomainFilter(t *testing.T) {
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

// ---------------------------------------------------------------------------
// ListInstanceTypeDetails returns non-empty list (issue #21)
// ---------------------------------------------------------------------------

func TestAudit2_ListInstanceTypeDetails_NonEmpty(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	details := b.ListInstanceTypeDetails("", "")

	require.NotEmpty(t, details, "ListInstanceTypeDetails should return at least one type")

	for _, d := range details {
		assert.Contains(t, d, "InstanceType", "each entry must have InstanceType")
	}
}

func TestAudit2_ListInstanceTypeDetails_HTTPHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/instanceTypeDetails", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	details, ok := out["InstanceTypeDetails"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, details, "InstanceTypeDetails must not be empty")
}

// ---------------------------------------------------------------------------
// Packages are validated during association via HTTP (issue #14)
// ---------------------------------------------------------------------------

func TestAudit2_AssociatePackage_HTTPValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, h *opensearch.Handler)
		packageID string
		domain    string
		wantCode  int
	}{
		{
			name: "success_package_exists",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "pkg-dom"})
				r.Body.Close()
				h.Backend.(*opensearch.InMemoryBackend).AddPackageInternal("pkg-real", "real-pkg", "TXT-DICTIONARY")
			},
			packageID: "pkg-real",
			domain:    "pkg-dom",
			wantCode:  http.StatusOK,
		},
		{
			name: "error_package_not_found",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "pkg-dom2"})
				r.Body.Close()
			},
			packageID: "pkg-ghost",
			domain:    "pkg-dom2",
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.setup(t, h)

			path := "/2021-01-01/packages/associate/" + tt.packageID + "/" + tt.domain
			resp := doRequest(t, h, http.MethodPost, path, nil)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}
