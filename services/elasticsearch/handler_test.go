package elasticsearch_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func newTestHandler() *elasticsearch.Handler {
	bk := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	return elasticsearch.NewHandler(bk)
}

func doRequest(t *testing.T, h *elasticsearch.Handler, method, path string, body any) *http.Response {
	t.Helper()

	var reqBody io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		reqBody = bytes.NewReader(b)
	}

	req := httptest.NewRequest(method, path, reqBody)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	return rw.Result()
}

func createDomainAndGetARN(t *testing.T, h *elasticsearch.Handler, domainName string) string {
	t.Helper()

	createResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": domainName,
	})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	status := createOut["DomainStatus"].(map[string]any)

	return status["ARN"].(string)
}

func newEchoContext(method, path string) *echo.Context {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()

	return e.NewContext(req, rec)
}

func TestElasticsearchHandler_CreateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(t *testing.T, h *elasticsearch.Handler)
		name                 string
		domainName           string
		elasticsearchVersion string
		wantContains         []string
		wantCode             int
	}{
		{
			name:                 "success",
			domainName:           "test-domain",
			elasticsearchVersion: "7.10",
			wantCode:             http.StatusOK,
			wantContains:         []string{"test-domain", "7.10", "ARN", "Endpoint"},
		},
		{
			name:       "already_exists",
			domainName: "my-domain",
			setup: func(t *testing.T, h *elasticsearch.Handler) {
				t.Helper()
				resp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain",
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
		{
			name:     "invalid_json",
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

			if tt.name == "invalid_json" {
				req := httptest.NewRequest(http.MethodPost, "/2015-01-01/es/domain", strings.NewReader("not-json"))
				req.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				h.ServeHTTP(rw, req)
				assert.Equal(t, tt.wantCode, rw.Code)

				return
			}

			body := map[string]any{}
			if tt.domainName != "" {
				body["DomainName"] = tt.domainName
			}

			if tt.elasticsearchVersion != "" {
				body["ElasticsearchVersion"] = tt.elasticsearchVersion
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", body)
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

func TestElasticsearchHandler_DescribeDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *elasticsearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "my-domain",
			setup: func(t *testing.T, h *elasticsearch.Handler) {
				t.Helper()
				resp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain",
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

			resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+tt.domainName, nil)
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

func TestElasticsearchHandler_DeleteDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *elasticsearch.Handler)
		domainName string
		wantCode   int
	}{
		{
			name:       "success",
			domainName: "to-delete",
			setup: func(t *testing.T, h *elasticsearch.Handler) {
				t.Helper()
				r := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain",
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

			resp := doRequest(t, h, http.MethodDelete, "/2015-01-01/es/domain/"+tt.domainName, nil)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode == http.StatusOK {
				resp2 := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+tt.domainName, nil)
				defer resp2.Body.Close()
				assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
			}
		})
	}
}

func TestElasticsearchHandler_ListDomainNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"alpha", "beta"} {
		r := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
			"DomainName": name,
		})
		r.Body.Close()
	}

	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	names, ok := out["DomainNames"].([]any)
	require.True(t, ok)
	assert.Len(t, names, 2)
}

func TestElasticsearchHandler_DescribeElasticsearchDomains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		domainNames []string
		query       []string
		wantCount   int
		wantCode    int
	}{
		{
			name:        "multiple_domains",
			domainNames: []string{"domain-a", "domain-b"},
			query:       []string{"domain-a", "domain-b"},
			wantCount:   2,
			wantCode:    http.StatusOK,
		},
		{
			name:        "nonexistent_filtered",
			domainNames: []string{"existing"},
			query:       []string{"existing", "missing"},
			wantCount:   1,
			wantCode:    http.StatusOK,
		},
		{
			name:      "empty_list",
			query:     []string{},
			wantCount: 0,
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			for _, name := range tt.domainNames {
				r := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
					"DomainName": name,
				})
				r.Body.Close()
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain-info", map[string]any{
				"DomainNames": tt.query,
			})
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			list, ok := out["DomainStatusList"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestElasticsearchHandler_UpdateElasticsearchDomainConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *elasticsearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "update-domain",
			setup: func(t *testing.T, h *elasticsearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
					"DomainName": "update-domain",
				})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DomainConfig"},
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

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/"+tt.domainName+"/config", map[string]any{
				"ElasticsearchClusterConfig": map[string]any{
					"InstanceType":  "r5.large.elasticsearch",
					"InstanceCount": 2,
				},
				"EBSOptions": map[string]any{
					"EBSEnabled": true,
					"VolumeSize": 20,
					"VolumeType": "gp2",
				},
			})
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

func TestElasticsearchHandler_DescribeDomainConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *elasticsearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "config-domain",
			setup: func(t *testing.T, h *elasticsearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain",
					map[string]any{"DomainName": "config-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DomainConfig", "ElasticsearchVersion", "ElasticsearchClusterConfig"},
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

			resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+tt.domainName+"/config", nil)
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

func TestElasticsearchHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		wantCount int
	}{
		{
			name:      "add_and_list_tags",
			operation: "add_list",
			wantCount: 2,
		},
		{
			name:      "remove_tag",
			operation: "remove",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			domainARN := createDomainAndGetARN(t, h, "tag-domain-"+tt.name)

			addResp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
				"ARN": domainARN,
				"TagList": []map[string]string{
					{"Key": "env", "Value": "prod"},
					{"Key": "team", "Value": "platform"},
				},
			})
			addResp.Body.Close()

			if tt.operation == "remove" {
				removeResp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags-removal", map[string]any{
					"ARN":     domainARN,
					"TagKeys": []string{"env"},
				})
				removeResp.Body.Close()
			}

			listResp := doRequest(t, h, http.MethodGet, "/2015-01-01/tags?arn="+domainARN, nil)
			defer listResp.Body.Close()
			assert.Equal(t, http.StatusOK, listResp.StatusCode)

			var listOut map[string]any
			require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))

			tagList, ok := listOut["TagList"].([]any)
			require.True(t, ok)
			assert.Len(t, tagList, tt.wantCount)
		})
	}
}

func TestElasticsearchHandler_Tags_InvalidBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantCode int
	}{
		{
			name:     "add_tags_invalid_json",
			path:     "/2015-01-01/tags",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "remove_tags_invalid_json",
			path:     "/2015-01-01/tags-removal",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader("not-json"))
			req.Header.Set("Content-Type", "application/json")

			rw := httptest.NewRecorder()
			h.ServeHTTP(rw, req)

			assert.Equal(t, tt.wantCode, rw.Code)
		})
	}
}

func TestElasticsearchBackend_DNSRegistrar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		domainName     string
		wantRegistered bool
		deleteAfter    bool
	}{
		{
			name:           "registers_on_create",
			domainName:     "my-domain",
			wantRegistered: true,
		},
		{
			name:           "deregisters_on_delete",
			domainName:     "del-domain",
			deleteAfter:    true,
			wantRegistered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			registrar := &mockDNSRegistrar{registered: make(map[string]bool)}
			b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.SetDNSRegistrar(registrar)

			domain, err := b.CreateDomain(tt.domainName, "", elasticsearch.ClusterConfig{}, elasticsearch.EBSOptions{})
			require.NoError(t, err)

			if tt.deleteAfter {
				_, err = b.DeleteDomain(tt.domainName)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantRegistered, registrar.registered[domain.Endpoint])
		})
	}
}

func TestElasticsearchHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "create_domain",
			method: http.MethodPost,
			path:   "/2015-01-01/es/domain",
			want:   "CreateElasticsearchDomain",
		},
		{
			name:   "list_domain_names",
			method: http.MethodGet,
			path:   "/2015-01-01/es/domain",
			want:   "ListDomainNames",
		},
		{
			name:   "describe_domain",
			method: http.MethodGet,
			path:   "/2015-01-01/es/domain/my-domain",
			want:   "DescribeElasticsearchDomain",
		},
		{
			name:   "delete_domain",
			method: http.MethodDelete,
			path:   "/2015-01-01/es/domain/my-domain",
			want:   "DeleteElasticsearchDomain",
		},
		{
			name:   "describe_domains",
			method: http.MethodPost,
			path:   "/2015-01-01/es/domain-info",
			want:   "DescribeElasticsearchDomains",
		},
		{
			name:   "update_config",
			method: http.MethodPost,
			path:   "/2015-01-01/es/domain/my-domain/config",
			want:   "UpdateElasticsearchDomainConfig",
		},
		{
			name:   "describe_config",
			method: http.MethodGet,
			path:   "/2015-01-01/es/domain/my-domain/config",
			want:   "DescribeElasticsearchDomainConfig",
		},
		{
			name:   "unknown_method",
			method: http.MethodPut,
			path:   "/2015-01-01/es/domain",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			c := newEchoContext(tt.method, tt.path)
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

// mockDNSRegistrar is a test double for elasticsearch.DNSRegistrar.
type mockDNSRegistrar struct {
	registered map[string]bool
}

func (m *mockDNSRegistrar) Register(hostname string) {
	m.registered[hostname] = true
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	delete(m.registered, hostname)
}

func TestElasticsearchHandler_Metadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	h.Region = "us-east-1"

	assert.Equal(t, "Elasticsearch", h.Name())
	assert.NotZero(t, h.MatchPriority())
	assert.Equal(t, "es", h.ChaosServiceName())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Len(t, h.GetSupportedOperations(), 7)

	c := newEchoContext(http.MethodGet, "/2015-01-01/es/domain/my-domain")
	assert.Equal(t, "my-domain", h.ExtractResource(c))

	c2 := newEchoContext(http.MethodGet, "/2015-01-01/es/domain")
	assert.Empty(t, h.ExtractResource(c2))

	matcher := h.RouteMatcher()
	c3 := newEchoContext(http.MethodGet, "/2015-01-01/es/domain")
	assert.True(t, matcher(c3))

	c4 := newEchoContext(http.MethodGet, "/other/path")
	assert.False(t, matcher(c4))

	echoCtx := newEchoContext(http.MethodGet, "/2015-01-01/es/domain")
	assert.NoError(t, h.Handle(echoCtx))
}

func TestElasticsearchHandler_CreateDomain_WithEBSOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName":           "ebs-domain",
		"ElasticsearchVersion": "7.10",
		"ElasticsearchClusterConfig": map[string]any{
			"InstanceType":  "r5.large.elasticsearch",
			"InstanceCount": 3,
		},
		"EBSOptions": map[string]any{
			"EBSEnabled": true,
			"VolumeSize": 20,
			"VolumeType": "gp2",
		},
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(bodyBytes), "r5.large.elasticsearch")
	assert.Contains(t, string(bodyBytes), "ebs-domain")
}

func TestElasticsearchHandler_ListTags_UnknownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	unknownARN := "arn:aws:es:us-east-1:123456789012:domain/nonexistent"
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/tags?arn="+unknownARN, nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	tagList, ok := out["TagList"].([]any)
	require.True(t, ok)
	assert.Empty(t, tagList)
}

func TestElasticsearchHandler_RouteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPut, "/2015-01-01/es/domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestElasticsearchHandler_PostDomainRoute_NotConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/some-domain/other", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestElasticsearchProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "default_init",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &elasticsearch.Provider{}
			assert.Equal(t, "Elasticsearch", p.Name())

			ctx := &service.AppContext{}
			handler, err := p.Init(ctx)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, handler)
		})
	}
}
