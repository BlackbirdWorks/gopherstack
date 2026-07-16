package opensearch_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestDomain(t *testing.T, h *opensearch.Handler, domainName string) {
	t.Helper()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": domainName})
	resp.Body.Close()
}

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
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

func TestOpenSearchHandler_RouteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
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
