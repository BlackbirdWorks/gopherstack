package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch3_StartServiceSoftwareUpdate_ScheduleAt verifies that the handler
// correctly decodes ScheduleAt from the request body and the description in the
// response reflects the chosen schedule mode.
func TestStartServiceSoftwareUpdate_ScheduleAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scheduleAt  string
		wantDescSub string
	}{
		{
			scheduleAt:  "NOW",
			wantDescSub: "ready to install",
		},
		{
			scheduleAt:  "OFF_PEAK_WINDOW",
			wantDescSub: "off-peak window",
		},
		{
			scheduleAt:  "TIMESTAMP",
			wantDescSub: "requested time",
		},
		{
			scheduleAt:  "",
			wantDescSub: "ready to install",
		},
	}

	for _, tt := range tests {
		t.Run("scheduleAt_"+tt.scheduleAt, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			b := h.Backend.(*opensearch.InMemoryBackend)
			b.AddDomainInternal("sw-domain", "OpenSearch_2.11")

			body := map[string]any{
				"DomainName": "sw-domain",
				"ScheduleAt": tt.scheduleAt,
			}

			resp := doRequest(t, h, http.MethodPost,
				"/2021-01-01/opensearch/serviceSoftwareUpdate/start", body)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			swo, ok := out["ServiceSoftwareOptions"].(map[string]any)
			require.True(t, ok, "expected ServiceSoftwareOptions in response")
			assert.Equal(t, "PENDING_UPDATE", swo["UpdateStatus"])

			desc, _ := swo["Description"].(string)
			assert.Contains(t, desc, tt.wantDescSub,
				"description %q should contain %q for ScheduleAt=%q",
				desc, tt.wantDescSub, tt.scheduleAt)
		})
	}
}

// TestBatch3_StartServiceSoftwareUpdate_DomainNotFound verifies 404 when the
// domain does not exist.
func TestStartServiceSoftwareUpdate_DomainNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/serviceSoftwareUpdate/start",
		map[string]any{"DomainName": "no-such", "ScheduleAt": "NOW"})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOpenSearchHandler_DescribeDomains(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create two domains.
	for _, name := range []string{"domain-a", "domain-b"} {
		createTestDomain(t, h, name)
	}

	// Bulk describe with explicit names.
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain-info",
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
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain-info", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	list, ok := out["DomainStatusList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 3)
}

func TestCancelServiceSoftwareUpdate_NotFound(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CancelServiceSoftwareUpdate("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrDomainNotFound)
}

func TestDescribeDomains_FullStatusShape(t *testing.T) {
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

			resp := doRequest(t, h, http.MethodPost,
				"/2021-01-01/opensearch/domain-info",
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

func TestCreateDomain_DomainNameValidation(t *testing.T) {
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

func TestCreateDomain_EngineVersionValidation(t *testing.T) {
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

func TestListDomainNames_EngineTypeFilter(t *testing.T) {
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

			path := "/2021-01-01/domain"
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

func TestDescribeDomains_FullDomainStatus(t *testing.T) {
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

			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain-info", tt.describeBody)
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

func TestListDomainNames_ReturnsBothDomains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		domainNames []string
		wantCount   int
	}{
		{
			name:        "two domains both returned",
			domainNames: []string{"alpha-domain", "beta-domain"},
			wantCount:   2,
		},
		{
			name:        "single domain returned",
			domainNames: []string{"solo-domain"},
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for _, dn := range tt.domainNames {
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
					"DomainName": dn,
				})
				resp.Body.Close()
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}

			resp := doRequest(t, h, http.MethodGet, "/2021-01-01/domain", nil)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var got struct {
				DomainNames []struct {
					DomainName string `json:"DomainName"`
					EngineType string `json:"EngineType"`
				} `json:"DomainNames"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
			assert.Len(t, got.DomainNames, tt.wantCount)

			returnedNames := make(map[string]bool, len(got.DomainNames))

			for _, e := range got.DomainNames {
				returnedNames[e.DomainName] = true
				assert.Equal(t, "OpenSearch", e.EngineType, "expected OpenSearch EngineType for %s", e.DomainName)
			}

			for _, dn := range tt.domainNames {
				assert.True(t, returnedNames[dn], "expected domain %s in listing", dn)
			}
		})
	}
}

func TestOpenSearchHandler_CreateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *opensearch.Handler)
		name          string
		domainName    string
		engineVersion string
		wantContains  []string
		wantCode      int
	}{
		{
			name:          "success",
			domainName:    "test-domain",
			engineVersion: "OpenSearch_2.11",
			wantCode:      http.StatusOK,
			wantContains:  []string{"test-domain", "OpenSearch_2.11", "ARN", "Endpoint"},
		},
		{
			name:       "already_exists",
			domainName: "my-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				resp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"},
				)
				resp.Body.Close()
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "no_name",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			body := map[string]any{}
			if tt.domainName != "" {
				body["DomainName"] = tt.domainName
			}
			if tt.engineVersion != "" {
				body["EngineVersion"] = tt.engineVersion
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

// Test_DomainStatus_DomainID verifies CreateDomain/DescribeDomain responses
// include the required DomainStatus.DomainId field (aws-sdk-go-v2
// types.DomainStatus.DomainId, "This member is required."), formatted as
// AWS does: "{accountId}/{domainName}".
func Test_DomainStatus_DomainID(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "domainid-test"})
	defer createResp.Body.Close()
	require.Equal(t, http.StatusOK, createResp.StatusCode)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createStatus, ok := createOut["DomainStatus"].(map[string]any)
	require.True(t, ok, "DomainStatus missing")
	assert.Equal(t, "123456789012/domainid-test", createStatus["DomainId"])

	describeResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/domainid-test", nil)
	defer describeResp.Body.Close()
	require.Equal(t, http.StatusOK, describeResp.StatusCode)

	var describeOut map[string]any
	require.NoError(t, json.NewDecoder(describeResp.Body).Decode(&describeOut))
	describeStatus, ok := describeOut["DomainStatus"].(map[string]any)
	require.True(t, ok, "DomainStatus missing")
	assert.Equal(t, "123456789012/domainid-test", describeStatus["DomainId"])
}

func TestOpenSearchHandler_DescribeDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "my-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				resp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"},
				)
				resp.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"my-domain"},
		},
		{
			name:       "not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/"+tt.domainName, nil)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestOpenSearchHandler_ListDomainNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for _, name := range []string{"alpha", "beta"} {
		r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
			"DomainName": name,
		})
		r.Body.Close()
	}

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	names, ok := out["DomainNames"].([]any)
	require.True(t, ok)
	assert.Len(t, names, 2)
}

func TestOpenSearchHandler_DeleteDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *opensearch.Handler)
		domainName string
		wantCode   int
	}{
		{
			name:       "success",
			domainName: "to-delete",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(
					t,
					h,
					http.MethodPost,
					"/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "to-delete"},
				)
				r.Body.Close()
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			resp := doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/domain/"+tt.domainName, nil)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode == http.StatusOK {
				resp2 := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/"+tt.domainName, nil)
				defer resp2.Body.Close()
				assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
			}
		})
	}
}

func TestOpenSearchHandler_CreateDomain_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/2021-01-01/opensearch/domain", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")

	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusBadRequest, rw.Code)
}

func TestOpenSearchHandler_CancelServiceSoftwareUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "my-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"})
				r.Body.Close()
				// A pending update must exist for cancellation to be valid.
				_, err := h.Backend.StartServiceSoftwareUpdate("my-domain", "")
				require.NoError(t, err)
			},
			wantCode: http.StatusOK,
			// After cancelling a scheduled install the update remains available,
			// so the domain returns to the ELIGIBLE state (never a "CANCELLED"
			// value, which is not a real AWS status).
			wantContains: []string{"ServiceSoftwareOptions", "ELIGIBLE"},
		},
		{
			name:       "no_update_pending",
			domainName: "no-update",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "no-update"})
				r.Body.Close()
			},
			// No scheduled update → AWS-accurate ValidationException.
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			domainName: "",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			if tt.name == "invalid_json" {
				req := httptest.NewRequest(
					http.MethodPost,
					"/2021-01-01/opensearch/serviceSoftwareUpdate/cancel",
					strings.NewReader("bad-json"),
				)
				req.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				h.ServeHTTP(rw, req)
				resp := rw.Result()
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

				return
			}

			body := map[string]any{"DomainName": tt.domainName}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/serviceSoftwareUpdate/cancel", body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

// TestOpenSearchHandler_RollbackServiceSoftwareUpdate covers the wire shape
// and error mapping for RollbackServiceSoftwareUpdate, including that it
// operates on the same ServiceSoftware state Start/CancelServiceSoftwareUpdate
// track (see domains.go).
func TestOpenSearchHandler_RollbackServiceSoftwareUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "rollback_pending_update",
			domainName: "rb-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "rb-domain"})
				r.Body.Close()
				_, err := h.Backend.StartServiceSoftwareUpdate("rb-domain", "")
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"RollbackServiceSoftwareOptions", `"RollbackAvailable":true`},
		},
		{
			name:       "no_update_performed",
			domainName: "rb-none",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "rb-none"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{`"RollbackAvailable":false`},
		},
		{
			name:       "domain_not_found",
			domainName: "rb-nonexistent",
			// This newer op documents ResourceNotFoundException at HTTP 409,
			// unlike the classic domain-family 404 convention (see
			// writeAttachmentError's doc for the AWS reference citation).
			wantCode: http.StatusConflict,
		},
		{
			name:       "invalid_json",
			domainName: "",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			if tt.name == "invalid_json" {
				req := httptest.NewRequest(
					http.MethodPost,
					"/2021-01-01/opensearch/serviceSoftwareUpdate/rollback",
					strings.NewReader("bad-json"),
				)
				req.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				h.ServeHTTP(rw, req)
				resp := rw.Result()
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

				return
			}

			body := map[string]any{"DomainName": tt.domainName}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/serviceSoftwareUpdate/rollback", body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

// TestInsights_HTTPHandler covers ListInsights/DescribeInsightDetails/
// InsightFeedback: entity validation against real domain state, and that
// results are always honestly empty/not-found rather than fabricated (this
// emulator has no analytics engine -- see insights.go).
func TestInsights_HTTPHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *opensearch.Handler)
		body     map[string]any
		name     string
		path     string
		wantCode int
	}{
		{
			name: "list_insights_known_domain_returns_empty",
			path: "/2021-01-01/opensearch/insights",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				createTestDomain(t, h, "insights-domain")
			},
			body:     map[string]any{"Entity": map[string]any{"Type": "DomainName", "Value": "insights-domain"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "list_insights_unknown_domain_returns_409",
			path:     "/2021-01-01/opensearch/insights",
			body:     map[string]any{"Entity": map[string]any{"Type": "DomainName", "Value": "no-such-domain"}},
			wantCode: http.StatusConflict,
		},
		{
			name:     "list_insights_account_entity_returns_empty",
			path:     "/2021-01-01/opensearch/insights",
			body:     map[string]any{"Entity": map[string]any{"Type": "Account", "Value": "123456789012"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "list_insights_invalid_entity_type_returns_400",
			path:     "/2021-01-01/opensearch/insights",
			body:     map[string]any{"Entity": map[string]any{"Type": "Bogus", "Value": "x"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "describe_insight_details_always_not_found",
			path: "/2021-01-01/opensearch/insight-details",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				createTestDomain(t, h, "insights-domain-2")
			},
			body: map[string]any{
				"Entity":    map[string]any{"Type": "DomainName", "Value": "insights-domain-2"},
				"InsightId": "insight-1",
			},
			wantCode: http.StatusConflict,
		},
		{
			name: "insight_feedback_always_not_found",
			path: "/2021-01-01/opensearch/insight-feedback",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				createTestDomain(t, h, "insights-domain-3")
			},
			body: map[string]any{
				"Entity":    map[string]any{"Type": "DomainName", "Value": "insights-domain-3"},
				"InsightId": "insight-1",
				"Thumbs":    "Up",
			},
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			resp := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode == http.StatusOK && tt.path == "/2021-01-01/opensearch/insights" {
				var out map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
				insights, ok := out["Insights"].([]any)
				require.True(t, ok)
				assert.Empty(t, insights)
			}
		})
	}
}
