package opensearch_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/opensearch"
)

func newTestHandler() *opensearch.Handler {
	bk := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	return opensearch.NewHandler(bk, slog.Default())
}

func doRequest(t *testing.T, h *opensearch.Handler, method, path string, body any) *http.Response {
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

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain", nil)
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

func createDomainAndGetARN(t *testing.T, h *opensearch.Handler, domainName string) string {
	t.Helper()

	createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName": domainName,
	})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	status := createOut["DomainStatus"].(map[string]any)

	return status["ARN"].(string)
}

func TestOpenSearchHandler_AddTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainARN := createDomainAndGetARN(t, h, "tag-domain")

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags", map[string]any{
		"ARN": domainARN,
		"TagList": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	listResp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+domainARN, nil)
	defer listResp.Body.Close()

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))

	tagList, ok := listOut["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList, 2)
}

func TestOpenSearchHandler_RemoveTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainARN := createDomainAndGetARN(t, h, "remove-tag-domain")

	addResp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags", map[string]any{
		"ARN": domainARN,
		"TagList": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	addResp.Body.Close()

	removeResp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags-removal", map[string]any{
		"ARN":     domainARN,
		"TagKeys": []string{"env"},
	})
	defer removeResp.Body.Close()

	assert.Equal(t, http.StatusOK, removeResp.StatusCode)

	listResp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+domainARN, nil)
	defer listResp.Body.Close()

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))

	tagList, ok := listOut["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList, 1)

	tag := tagList[0].(map[string]any)
	assert.Equal(t, "team", tag["Key"])
	assert.Equal(t, "platform", tag["Value"])
}

func TestOpenSearchHandler_ListTags_EmptyDomain(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainARN := createDomainAndGetARN(t, h, "empty-tags-domain")

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+domainARN, nil)
	defer resp.Body.Close()

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	tagList, ok := out["TagList"].([]any)
	require.True(t, ok)
	assert.Empty(t, tagList)
}

func TestOpenSearchHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreateDomain")
	assert.Contains(t, ops, "DescribeDomain")
	assert.Contains(t, ops, "DeleteDomain")
	assert.Contains(t, ops, "ListDomainNames")
	assert.Len(t, ops, 4)
}

func newEchoContext(method, path string, body string) *echo.Context {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	e := echo.New()

	return e.NewContext(req, rec)
}

func TestOpenSearchHandler_ExtractOperation(t *testing.T) {
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
			path:   "/2021-01-01/opensearch/domain",
			want:   "CreateDomain",
		},
		{
			name:   "create_domain_trailing_slash",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/domain/",
			want:   "CreateDomain",
		},
		{
			name:   "list_domain_names",
			method: http.MethodGet,
			path:   "/2021-01-01/opensearch/domain",
			want:   "ListDomainNames",
		},
		{
			name:   "list_domain_names_trailing_slash",
			method: http.MethodGet,
			path:   "/2021-01-01/opensearch/domain/",
			want:   "ListDomainNames",
		},
		{
			name:   "describe_domain",
			method: http.MethodGet,
			path:   "/2021-01-01/opensearch/domain/my-domain",
			want:   "DescribeDomain",
		},
		{
			name:   "delete_domain",
			method: http.MethodDelete,
			path:   "/2021-01-01/opensearch/domain/my-domain",
			want:   "DeleteDomain",
		},
		{
			name:   "unknown_method_on_root",
			method: http.MethodPut,
			path:   "/2021-01-01/opensearch/domain",
			want:   "Unknown",
		},
		{
			name:   "unknown_method_on_domain",
			method: http.MethodPatch,
			path:   "/2021-01-01/opensearch/domain/my-domain",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			c := newEchoContext(tt.method, tt.path, "")
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestOpenSearchHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "domain_name",
			path: "/2021-01-01/opensearch/domain/my-domain",
			want: "my-domain",
		},
		{
			name: "domain_name_trailing_slash",
			path: "/2021-01-01/opensearch/domain/my-domain/",
			want: "my-domain",
		},
		{
			name: "root_path",
			path: "/2021-01-01/opensearch/domain",
			want: "",
		},
		{
			name: "unrelated_path",
			path: "/some/other/path",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			c := newEchoContext(http.MethodGet, tt.path, "")
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestOpenSearchHandler_ListTags_UnknownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	unknownARN := "arn:aws:es:us-east-1:123456789012:domain/nonexistent"
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+unknownARN, nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	tagList, ok := out["TagList"].([]any)
	require.True(t, ok)
	assert.Empty(t, tagList)
}

func TestOpenSearchHandler_AddTags_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/2021-01-01/tags", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")

	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusBadRequest, rw.Code)
}

func TestOpenSearchHandler_RemoveTags_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/2021-01-01/tags-removal", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")

	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusBadRequest, rw.Code)
}

func TestOpenSearchHandler_DescribeDomainConfig(t *testing.T) {
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
			domainName: "config-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "config-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DomainConfig", "EngineVersion", "ClusterConfig"},
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
			resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/"+tt.domainName+"/config", nil)
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

func TestOpenSearchHandler_RouteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
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

func TestOpenSearchHandler_CreateDomain_WithClusterConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName":    "cc-domain",
		"EngineVersion": "OpenSearch_2.11",
		"ClusterConfig": map[string]any{
			"InstanceType":  "r5.large.search",
			"InstanceCount": 3,
		},
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(bodyBytes), "r5.large.search")
}

// mockDNSRegistrar is a test double for opensearch.DNSRegistrar.
type mockDNSRegistrar struct {
	registered map[string]bool
}

func (m *mockDNSRegistrar) Register(hostname string) {
	m.registered[hostname] = true
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	delete(m.registered, hostname)
}

func TestOpenSearchBackend_DNSRegistrar(t *testing.T) {
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
			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.SetDNSRegistrar(registrar)

			domain, err := b.CreateDomain(tt.domainName, "", opensearch.ClusterConfig{})
			require.NoError(t, err)

			if tt.deleteAfter {
				_, err = b.DeleteDomain(tt.domainName)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantRegistered, registrar.registered[domain.Endpoint])
		})
	}
}
