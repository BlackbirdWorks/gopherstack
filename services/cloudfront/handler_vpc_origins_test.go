package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestVpcOriginCRUD covers the full VPC Origin lifecycle via the HTTP handler.
func TestVpcOriginCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *cloudfront.Handler) string
		check       func(*testing.T, *httptest.ResponseRecorder, string)
		headersFunc func(*testing.T, *cloudfront.Handler, string) map[string]string
		name        string
		method      string
		path        string
		body        []byte
		wantStatus  int
	}{
		{
			name:   "create_vpc_origin",
			method: http.MethodPost,
			path:   "/2020-05-31/vpc-origin",
			body: []byte(
				`<CreateVpcOriginRequest>` +
					`<VpcOriginEndpointConfig><Name>my-vpc-origin</Name></VpcOriginEndpointConfig>` +
					`</CreateVpcOriginRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<VpcOrigin")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_vpc_origins",
			method: http.MethodGet,
			path:   "/2020-05-31/vpc-origin",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateVpcOrigin("list-vpc-origin", nil)
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<VpcOriginList")
			},
		},
		{
			name:   "get_vpc_origin",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				origin, err := h.Backend.CreateVpcOrigin("get-vpc-origin", nil)
				require.NoError(t, err)

				return "/2020-05-31/vpc-origin/" + origin.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<VpcOrigin")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "update_vpc_origin",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<UpdateVpcOriginRequest>` +
					`<VpcOriginEndpointConfig><Name>updated-vpc-origin</Name></VpcOriginEndpointConfig>` +
					`</UpdateVpcOriginRequest>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				origin, err := h.Backend.CreateVpcOrigin("old-vpc-origin", nil)
				require.NoError(t, err)

				return "/2020-05-31/vpc-origin/" + origin.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<VpcOrigin")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "delete_vpc_origin",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				origin, err := h.Backend.CreateVpcOrigin("del-vpc-origin", nil)
				require.NoError(t, err)

				return "/2020-05-31/vpc-origin/" + origin.ID
			},
			headersFunc: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				parts := strings.Split(strings.TrimRight(path, "/"), "/")
				id := parts[len(parts)-1]
				origin, err := h.Backend.GetVpcOrigin(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": origin.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_vpc_origin_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/vpc-origin/doesnotexist",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Error>")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headersFunc != nil {
				hdrs = tt.headersFunc(t, h, path)
			}
			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestInMemoryBackend_VpcOrigin tests VPC Origin backend operations directly.
func TestInMemoryBackend_VpcOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_get_list_update_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				origin, err := b.CreateVpcOrigin("vpc-origin-name", nil)
				require.NoError(t, err)
				assert.NotEmpty(t, origin.ID)

				got, err := b.GetVpcOrigin(origin.ID)
				require.NoError(t, err)
				assert.Equal(t, "vpc-origin-name", got.Name)

				list := b.ListVpcOrigins()
				assert.Len(t, list, 1)

				updated, err := b.UpdateVpcOrigin(origin.ID, "new-vpc-origin-name")
				require.NoError(t, err)
				assert.Equal(t, "new-vpc-origin-name", updated.Name)

				require.NoError(t, b.DeleteVpcOrigin(origin.ID))
				_, err = b.GetVpcOrigin(origin.ID)
				require.Error(t, err)
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetVpcOrigin("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "update_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateVpcOrigin("doesnotexist", "name")
				require.Error(t, err)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteVpcOrigin("doesnotexist")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
