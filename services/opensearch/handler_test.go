package opensearch_test

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

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

func newTestHandler() *opensearch.Handler {
	bk := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	return opensearch.NewHandler(bk)
}

func newTestHandlerAndBackend() (*opensearch.InMemoryBackend, *opensearch.Handler) {
	bk := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	return bk, opensearch.NewHandler(bk)
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
	assert.Contains(t, ops, "AcceptInboundConnection")
	assert.Contains(t, ops, "AddDataSource")
	assert.Contains(t, ops, "AddDirectQueryDataSource")
	assert.Contains(t, ops, "AddTags")
	assert.Contains(t, ops, "AssociatePackage")
	assert.Contains(t, ops, "AssociatePackages")
	assert.Contains(t, ops, "AuthorizeVpcEndpointAccess")
	assert.Contains(t, ops, "CancelDomainConfigChange")
	assert.Contains(t, ops, "CancelServiceSoftwareUpdate")
	assert.Contains(t, ops, "CreateApplication")
	assert.Len(t, ops, 104)
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

			domain, err := b.CreateDomain(opensearch.CreateDomainInput{Name: tt.domainName})
			require.NoError(t, err)

			if tt.deleteAfter {
				_, err = b.DeleteDomain(tt.domainName)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantRegistered, registrar.registered[domain.Endpoint])
		})
	}
}

func TestOpenSearchHandler_AcceptInboundConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		wantContains []string
		wantCode     int
		seedConn     bool
	}{
		{
			name:         "success",
			connectionID: "conn-123",
			wantCode:     http.StatusOK,
			wantContains: []string{"conn-123", "ACTIVE"},
			seedConn:     true,
		},
		{
			name:         "not_found",
			connectionID: "conn-nonexistent",
			wantCode:     http.StatusNotFound,
		},
		{
			name:         "empty_id",
			connectionID: "",
			wantCode:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			if tt.seedConn {
				opensearch.SeedInboundConnection(b, tt.connectionID)
			}
			h := opensearch.NewHandler(b)

			path := "/2021-01-01/opensearch/cc/inboundConnection/" + tt.connectionID + "/accept"
			resp := doRequest(t, h, http.MethodPut, path, nil)
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

func TestOpenSearchHandler_AddDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		dsName       string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "my-domain",
			dsName:     "my-datasource",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"Message"},
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent",
			dsName:     "ds",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "duplicate_datasource",
			domainName: "dup-domain",
			dsName:     "dup-ds",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "dup-domain"})
				r.Body.Close()
				r2 := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain/dup-domain/dataSource",
					map[string]any{"Name": "dup-ds", "DataSourceType": map[string]any{}})
				r2.Body.Close()
			},
			wantCode: http.StatusConflict,
		},
		{
			name:       "invalid_json",
			domainName: "my-domain",
			dsName:     "",
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

			var body any
			if tt.name == "invalid_json" {
				req := httptest.NewRequest(
					http.MethodPost,
					"/2021-01-01/opensearch/domain/my-domain/dataSource",
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

			if tt.dsName != "" {
				body = map[string]any{
					"Name":           tt.dsName,
					"DataSourceType": map[string]any{},
				}
			}

			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain/"+tt.domainName+"/dataSource", body)
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

func TestOpenSearchHandler_AddDirectQueryDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		dsName       string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			dsName:       "my-dq-source",
			wantCode:     http.StatusOK,
			wantContains: []string{"DataSourceArn"},
		},
		{
			name:     "no_name",
			dsName:   "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "duplicate",
			dsName:   "dup-source",
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.name == "duplicate" {
				r := doRequest(
					t,
					h,
					http.MethodPost,
					"/2021-01-01/opensearch/directQueryDataSource",
					map[string]any{
						"DataSourceName": "dup-source",
						"DataSourceType": map[string]any{},
						"OpenSearchArns": []string{},
					},
				)
				r.Body.Close()
			}

			body := map[string]any{
				"DataSourceName": tt.dsName,
				"DataSourceType": map[string]any{},
				"OpenSearchArns": []string{"arn:aws:opensearch:us-east-1:123456789012:domain/test"},
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/directQueryDataSource", body)
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

func TestOpenSearchHandler_AssociatePackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		packageID    string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			packageID:  "pkg-001",
			domainName: "my-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"})
				r.Body.Close()
				h.Backend.(*opensearch.InMemoryBackend).AddPackageInternal("pkg-001", "my-pkg", "TXT-DICTIONARY")
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"my-domain", "ACTIVE"},
		},
		{
			name:       "domain_not_found",
			packageID:  "pkg-002",
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

			path := "/2021-01-01/packages/associate/" + tt.packageID + "/" + tt.domainName
			resp := doRequest(t, h, http.MethodPost, path, nil)
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

func TestOpenSearchHandler_AssociatePackages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		packageIDs   []string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "my-domain",
			packageIDs: []string{"pkg-001", "pkg-002"},
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"})
				r.Body.Close()
				b := h.Backend.(*opensearch.InMemoryBackend)
				b.AddPackageInternal("pkg-001", "pkg-one", "TXT-DICTIONARY")
				b.AddPackageInternal("pkg-002", "pkg-two", "TXT-DICTIONARY")
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DomainPackageDetailsList"},
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent",
			packageIDs: []string{"pkg-001"},
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			domainName: "",
			packageIDs: nil,
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
					"/2021-01-01/packages/associateMultiple",
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

			pkgList := make([]map[string]string, 0, len(tt.packageIDs))
			for _, id := range tt.packageIDs {
				pkgList = append(pkgList, map[string]string{"PackageID": id})
			}

			body := map[string]any{
				"DomainName":  tt.domainName,
				"PackageList": pkgList,
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/packages/associateMultiple", body)
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

func TestOpenSearchHandler_AuthorizeVpcEndpointAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		account      string
		service      string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success_account",
			domainName: "vpc-domain",
			account:    "111122223333",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "vpc-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"111122223333", "AWS_ACCOUNT"},
		},
		{
			name:       "success_service",
			domainName: "svc-domain",
			service:    "delivery.logs.amazonaws.com",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "svc-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"delivery.logs.amazonaws.com", "AWS_SERVICE"},
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent",
			account:    "111122223333",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			domainName: "any-domain",
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

			path := "/2021-01-01/opensearch/domain/" + tt.domainName + "/authorizeVpcEndpointAccess"

			if tt.name == "invalid_json" {
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("bad-json"))
				req.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				h.ServeHTTP(rw, req)
				resp := rw.Result()
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

				return
			}

			body := map[string]any{
				"Account": tt.account,
				"Service": tt.service,
			}
			resp := doRequest(t, h, http.MethodPost, path, body)
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

func TestOpenSearchHandler_CancelDomainConfigChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
		dryRun       bool
	}{
		{
			name:       "success",
			domainName: "my-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CancelledChangeIds"},
		},
		{
			name:       "dry_run",
			domainName: "dr-domain",
			dryRun:     true,
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "dr-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"true"},
		},
		{
			name:       "domain_not_found",
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

			body := map[string]any{"DryRun": tt.dryRun}
			path := "/2021-01-01/opensearch/domain/" + tt.domainName + "/config/cancel"
			resp := doRequest(t, h, http.MethodPost, path, body)
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

func TestOpenSearchHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		appName      string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			appName:      "my-app",
			wantCode:     http.StatusOK,
			wantContains: []string{"my-app", "Id", "Arn"},
		},
		{
			name:     "no_name",
			appName:  "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "duplicate",
			appName:  "dup-app",
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.name == "duplicate" {
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application",
					map[string]any{"Name": "dup-app"})
				r.Body.Close()
			}

			body := map[string]any{"Name": tt.appName}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application", body)
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

func TestOpenSearchHandler_NewOps_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		method    string
		wantMatch bool
	}{
		{
			name:      "cc_inbound_connection",
			path:      "/2021-01-01/opensearch/cc/inboundConnection/conn-1/accept",
			method:    http.MethodPut,
			wantMatch: true,
		},
		{
			name:      "direct_query_datasource",
			path:      "/2021-01-01/opensearch/directQueryDataSource",
			method:    http.MethodPost,
			wantMatch: true,
		},
		{
			name:      "packages_associate",
			path:      "/2021-01-01/packages/associate/pkg-1/domain-1",
			method:    http.MethodPost,
			wantMatch: true,
		},
		{
			name:      "packages_associate_multiple",
			path:      "/2021-01-01/packages/associateMultiple",
			method:    http.MethodPost,
			wantMatch: true,
		},
		{
			name:      "service_software_cancel",
			path:      "/2021-01-01/opensearch/serviceSoftwareUpdate/cancel",
			method:    http.MethodPost,
			wantMatch: true,
		},
		{
			name:      "application",
			path:      "/2021-01-01/opensearch/application",
			method:    http.MethodPost,
			wantMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			c := newEchoContext(tt.method, tt.path, "")
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestOpenSearchHandler_NewOps_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		method       string
		path         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:     "package_route_not_found",
			method:   http.MethodGet,
			path:     "/2021-01-01/packages/unknown",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "service_software_route_not_found",
			method:   http.MethodGet,
			path:     "/2021-01-01/opensearch/serviceSoftwareUpdate/other",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "application_get_by_id_ok",
			method:   http.MethodGet,
			path:     "/2021-01-01/opensearch/application/some-id",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "cc_route_not_found",
			method:   http.MethodGet,
			path:     "/2021-01-01/opensearch/cc/unknown",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "direct_query_list_ok",
			method:   http.MethodGet,
			path:     "/2021-01-01/opensearch/directQueryDataSource",
			wantCode: http.StatusOK,
		},
		{
			name:     "cancel_domain_config_change_invalid_json",
			method:   http.MethodPost,
			path:     "/2021-01-01/opensearch/domain/my-domain/config/cancel",
			body:     "not-json",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "domain_subroute_not_found",
			method:   http.MethodPost,
			path:     "/2021-01-01/opensearch/domain/my-domain/unknownSuffix",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "create_application_invalid_json",
			method:   http.MethodPost,
			path:     "/2021-01-01/opensearch/application",
			body:     "not-json",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "add_direct_query_invalid_json",
			method:   http.MethodPost,
			path:     "/2021-01-01/opensearch/directQueryDataSource",
			body:     "not-json",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "authorize_vpc_access_invalid_body",
			method:   http.MethodPost,
			path:     "/2021-01-01/opensearch/domain/vpc-domain/authorizeVpcEndpointAccess",
			body:     "not-json",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "associate_packages_invalid_pkg_path",
			method:   http.MethodPost,
			path:     "/2021-01-01/packages/associate/only-one-segment",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "add_datasource_invalid_json",
			method:   http.MethodPost,
			path:     "/2021-01-01/opensearch/domain/my-domain/dataSource",
			body:     "not-json",
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

			var reader io.Reader
			if tt.body != "" {
				reader = strings.NewReader(tt.body)
			}

			req := httptest.NewRequest(tt.method, tt.path, reader)
			if tt.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}

			rw := httptest.NewRecorder()
			h.ServeHTTP(rw, req)
			resp := rw.Result()
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

func TestOpenSearchHandler_Persistence_NewOps(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	// Set up state with new ops
	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "snap-domain", EngineVersion: "OpenSearch_2.11"})
	require.NoError(t, err)

	opensearch.SeedInboundConnection(b, "conn-abc")

	_, err = b.AddDataSource("snap-domain", "my-ds", "desc", "S3GLUE")
	require.NoError(t, err)

	_, err = b.AddDirectQueryDataSource("my-dq", "desc", "CloudWatchLogs", []string{})
	require.NoError(t, err)

	b.AddPackageInternal("pkg-001", "test-pkg", "TXT-DICTIONARY")

	_, err = b.AssociatePackage("pkg-001", "snap-domain")
	require.NoError(t, err)

	_, err = b.AuthorizeVpcEndpointAccess("snap-domain", "111122223333", "")
	require.NoError(t, err)

	_, err = b.CreateApplication("my-app", nil, nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Verify domain persists
	domain, err := fresh.DescribeDomain("snap-domain")
	require.NoError(t, err)
	assert.Equal(t, "snap-domain", domain.Name)

	// Verify inbound connection persists
	conn, err := fresh.AcceptInboundConnection("conn-abc")
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", conn.Status)

	// Verify application persists
	app, err := fresh.CreateApplication("another-app", nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, app.ID)
}

func TestOpenSearchHandler_CreateApplication_WithConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := map[string]any{
		"Name": "configured-app",
		"AppConfigs": []map[string]string{
			{"Key": "opensearchDashboards.enabled", "Value": "true"},
		},
		"DataSources": []map[string]string{
			{"DataSourceArn": "arn:aws:opensearch:us-east-1:123456789012:domain/test"},
		},
	}

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application", body)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(bodyBytes), "configured-app")
	assert.Contains(t, string(bodyBytes), "opensearchDashboards.enabled")
}

func TestOpenSearchBackend_AddTags_DomainNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags", map[string]any{
		"ARN":     "arn:aws:es:us-east-1:123456789012:domain/nonexistent",
		"TagList": []map[string]string{{"Key": "k", "Value": "v"}},
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOpenSearchBackend_RemoveTags_DomainNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags-removal", map[string]any{
		"ARN":     "arn:aws:es:us-east-1:123456789012:domain/nonexistent",
		"TagKeys": []string{"env"},
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOpenSearchBackend_AddDataSource_MissingName(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "my-domain"})
	require.NoError(t, err)

	_, err = b.AddDataSource("my-domain", "", "desc", "S3GLUE")
	require.Error(t, err)
}
