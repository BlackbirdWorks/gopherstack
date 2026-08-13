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

// testVpcOriginEndpointConfig builds a valid VpcOriginEndpointConfig for backend-level tests.
func testVpcOriginEndpointConfig(name string) cloudfront.VpcOriginEndpointConfig {
	return cloudfront.VpcOriginEndpointConfig{
		Name:                 name,
		Arn:                  "arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-0123456789abcdef0",
		OriginProtocolPolicy: "https-only",
		HTTPPort:             80,
		HTTPSPort:            443,
	}
}

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
					`<VpcOriginEndpointConfig>` +
					`<Name>my-vpc-origin</Name>` +
					`<Arn>arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-0123456789abcdef0</Arn>` +
					`<HTTPPort>80</HTTPPort>` +
					`<HTTPSPort>443</HTTPSPort>` +
					`<OriginProtocolPolicy>https-only</OriginProtocolPolicy>` +
					`</VpcOriginEndpointConfig>` +
					`</CreateVpcOriginRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				body := rec.Body.String()
				assert.Contains(t, body, "<VpcOrigin")
				assert.Contains(
					t,
					body,
					"<Arn>arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-0123456789abcdef0</Arn>",
				)
				assert.Contains(t, body, "<HTTPPort>80</HTTPPort>")
				assert.Contains(t, body, "<HTTPSPort>443</HTTPSPort>")
				assert.Contains(t, body, "<OriginProtocolPolicy>https-only</OriginProtocolPolicy>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "create_vpc_origin_missing_arn_rejected",
			method: http.MethodPost,
			path:   "/2020-05-31/vpc-origin",
			body: []byte(
				`<CreateVpcOriginRequest>` +
					`<VpcOriginEndpointConfig>` +
					`<Name>no-arn-vpc-origin</Name>` +
					`<HTTPPort>80</HTTPPort>` +
					`<HTTPSPort>443</HTTPSPort>` +
					`<OriginProtocolPolicy>https-only</OriginProtocolPolicy>` +
					`</VpcOriginEndpointConfig>` +
					`</CreateVpcOriginRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name:   "list_vpc_origins",
			method: http.MethodGet,
			path:   "/2020-05-31/vpc-origin",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateVpcOrigin(testVpcOriginEndpointConfig("list-vpc-origin"), nil)
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
				origin, err := h.Backend.CreateVpcOrigin(testVpcOriginEndpointConfig("get-vpc-origin"), nil)
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
					`<VpcOriginEndpointConfig>` +
					`<Name>updated-vpc-origin</Name>` +
					`<Arn>arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/50dc6c495c0c9188</Arn>` +
					`<HTTPPort>8080</HTTPPort>` +
					`<HTTPSPort>8443</HTTPSPort>` +
					`<OriginProtocolPolicy>match-viewer</OriginProtocolPolicy>` +
					`</VpcOriginEndpointConfig>` +
					`</UpdateVpcOriginRequest>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				origin, err := h.Backend.CreateVpcOrigin(testVpcOriginEndpointConfig("old-vpc-origin"), nil)
				require.NoError(t, err)

				return "/2020-05-31/vpc-origin/" + origin.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				body := rec.Body.String()
				assert.Contains(t, body, "<VpcOrigin")
				assert.Contains(
					t,
					body,
					"<Arn>arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/50dc6c495c0c9188</Arn>",
				)
				assert.Contains(t, body, "<HTTPPort>8080</HTTPPort>")
				assert.Contains(t, body, "<HTTPSPort>8443</HTTPSPort>")
				assert.Contains(t, body, "<OriginProtocolPolicy>match-viewer</OriginProtocolPolicy>")
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
				origin, err := h.Backend.CreateVpcOrigin(testVpcOriginEndpointConfig("del-vpc-origin"), nil)
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
				origin, err := b.CreateVpcOrigin(testVpcOriginEndpointConfig("vpc-origin-name"), nil)
				require.NoError(t, err)
				assert.NotEmpty(t, origin.ID)
				assert.Equal(t, "https-only", origin.OriginProtocolPolicy)

				got, err := b.GetVpcOrigin(origin.ID)
				require.NoError(t, err)
				assert.Equal(t, "vpc-origin-name", got.Name)

				list := b.ListVpcOrigins()
				assert.Len(t, list, 1)

				updateCfg := testVpcOriginEndpointConfig("new-vpc-origin-name")
				updated, err := b.UpdateVpcOrigin(origin.ID, updateCfg)
				require.NoError(t, err)
				assert.Equal(t, "new-vpc-origin-name", updated.Name)

				require.NoError(t, b.DeleteVpcOrigin(origin.ID))
				_, err = b.GetVpcOrigin(origin.ID)
				require.Error(t, err)
			},
		},
		{
			name: "create_missing_required_member_rejected",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				cfg := testVpcOriginEndpointConfig("bad-vpc-origin")
				cfg.Arn = ""
				_, err := b.CreateVpcOrigin(cfg, nil)
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
				_, err := b.UpdateVpcOrigin("doesnotexist", testVpcOriginEndpointConfig("name"))
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
