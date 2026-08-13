package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
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
			// Real UpdateVpcOriginInput has no wrapping UpdateVpcOriginRequest element --
			// the root itself is VpcOriginEndpointConfig (cloudfront@v1.67.4
			// serializers.go: awsRestxml_serializeOpUpdateVpcOrigin's payloadRoot.Local).
			body: []byte(
				`<VpcOriginEndpointConfig>` +
					`<Name>updated-vpc-origin</Name>` +
					`<Arn>arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/50dc6c495c0c9188</Arn>` +
					`<HTTPPort>8080</HTTPPort>` +
					`<HTTPSPort>8443</HTTPSPort>` +
					`<OriginProtocolPolicy>match-viewer</OriginProtocolPolicy>` +
					`</VpcOriginEndpointConfig>`,
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

// TestUpdateVpcOrigin_RealClient is a regression test for gopherstack-ob1g:
// UpdateVpcOriginInput's real root element is VpcOriginEndpointConfig itself, with no
// wrapping UpdateVpcOriginRequest element (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpUpdateVpcOrigin's payloadRoot.Local). A handler expecting the
// wrong root discards the whole body via xml.Unmarshal's error, so the update
// silently no-ops -- driving this through the real SDK client is what catches it,
// since the SDK writes the exact wire shape regardless of what the handler expects.
func TestUpdateVpcOrigin_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateVpcOrigin(t.Context(), &cfsdk.CreateVpcOriginInput{
		VpcOriginEndpointConfig: &types.VpcOriginEndpointConfig{
			Name:                 aws.String("vpc-origin-rc"),
			Arn:                  aws.String("arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-rc1"),
			HTTPPort:             aws.Int32(80),
			HTTPSPort:            aws.Int32(443),
			OriginProtocolPolicy: types.OriginProtocolPolicyHttpsOnly,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.VpcOrigin)

	updated, err := client.UpdateVpcOrigin(t.Context(), &cfsdk.UpdateVpcOriginInput{
		Id:      created.VpcOrigin.Id,
		IfMatch: created.ETag,
		VpcOriginEndpointConfig: &types.VpcOriginEndpointConfig{
			Name:                 aws.String("vpc-origin-rc-updated"),
			Arn:                  aws.String("arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-rc2"),
			HTTPPort:             aws.Int32(8080),
			HTTPSPort:            aws.Int32(8443),
			OriginProtocolPolicy: types.OriginProtocolPolicyHttpOnly,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.VpcOrigin)
	require.NotNil(t, updated.VpcOrigin.VpcOriginEndpointConfig)

	cfg := updated.VpcOrigin.VpcOriginEndpointConfig
	assert.Equal(t, "vpc-origin-rc-updated", aws.ToString(cfg.Name))
	assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-rc2", aws.ToString(cfg.Arn))
	assert.Equal(t, int32(8080), aws.ToInt32(cfg.HTTPPort))
	assert.Equal(t, int32(8443), aws.ToInt32(cfg.HTTPSPort))
	assert.Equal(t, types.OriginProtocolPolicyHttpOnly, cfg.OriginProtocolPolicy)
}

// TestUpdateVpcOrigin_MalformedBodyHandled verifies a malformed request body is
// rejected with 400 MalformedXML instead of silently no-opping the update
// (gopherstack-ob1g: the previous handler discarded xml.Unmarshal's error).
func TestUpdateVpcOrigin_MalformedBodyHandled(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	created := cfOK(t, h, http.MethodPost, prefix+"vpc-origin",
		`<CreateVpcOriginRequest><VpcOriginEndpointConfig>`+
			`<Name>vpc-origin-malformed</Name>`+
			`<Arn>arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-malformed</Arn>`+
			`<HTTPPort>80</HTTPPort><HTTPSPort>443</HTTPSPort>`+
			`<OriginProtocolPolicy>https-only</OriginProtocolPolicy>`+
			`</VpcOriginEndpointConfig></CreateVpcOriginRequest>`)
	id := extractXMLID(t, created)

	rec := cfRequest(t, h, http.MethodPut, prefix+"vpc-origin/"+id, "<<<not xml")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}
