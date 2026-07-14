package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
)

// TestExtractOperationAndResource verifies ExtractOperation and ExtractResource.
func TestExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		method        string
		path          string
		wantOperation string
		wantResource  string
	}{
		{
			name:          "create_distribution",
			method:        http.MethodPost,
			path:          "/2020-05-31/distribution",
			wantOperation: "CreateDistribution",
			wantResource:  "",
		},
		{
			name:          "get_distribution",
			method:        http.MethodGet,
			path:          "/2020-05-31/distribution/ABCDE12345678F",
			wantOperation: "GetDistribution",
			wantResource:  "ABCDE12345678F",
		},
		{
			name:          "create_oai",
			method:        http.MethodPost,
			path:          "/2020-05-31/origin-access-identity/cloudfront",
			wantOperation: "CreateCloudFrontOriginAccessIdentity",
			wantResource:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOperation, h.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

// TestNewOperations_ExtractOperation verifies route parsing for new operations.
func TestNewOperations_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		method        string
		path          string
		wantOperation string
	}{
		{
			name:          "associate_alias",
			method:        http.MethodPut,
			path:          "/2020-05-31/distribution/ABCD1234/associate-alias",
			wantOperation: "AssociateAlias",
		},
		{
			name:          "associate_distribution_web_acl",
			method:        http.MethodPut,
			path:          "/2020-05-31/distribution/ABCD1234/associate-web-acl",
			wantOperation: "AssociateDistributionWebACL",
		},
		{
			name:          "associate_distribution_tenant_web_acl",
			method:        http.MethodPut,
			path:          "/2020-05-31/distribution-tenant/TENANT1234/associate-web-acl",
			wantOperation: "AssociateDistributionTenantWebACL",
		},
		{
			name:          "copy_distribution",
			method:        http.MethodPost,
			path:          "/2020-05-31/distribution/ABCD1234/copy",
			wantOperation: "CopyDistribution",
		},
		{
			name:          "create_anycast_ip_list",
			method:        http.MethodPost,
			path:          "/2020-05-31/anycast-ip-list",
			wantOperation: "CreateAnycastIpList",
		},
		{
			name:          "create_cache_policy",
			method:        http.MethodPost,
			path:          "/2020-05-31/cache-policy",
			wantOperation: "CreateCachePolicy",
		},
		{
			name:          "create_connection_function",
			method:        http.MethodPost,
			path:          "/2020-05-31/connection-function",
			wantOperation: "CreateConnectionFunction",
		},
		{
			name:          "create_connection_group",
			method:        http.MethodPost,
			path:          "/2020-05-31/connection-group",
			wantOperation: "CreateConnectionGroup",
		},
		{
			name:          "create_continuous_deployment_policy",
			method:        http.MethodPost,
			path:          "/2020-05-31/continuous-deployment-policy",
			wantOperation: "CreateContinuousDeploymentPolicy",
		},
		{
			name:          "get_cloudfront_oai",
			method:        http.MethodGet,
			path:          "/2020-05-31/origin-access-identity/cloudfront/OAIID123",
			wantOperation: "GetCloudFrontOriginAccessIdentity",
		},
		{
			name:          "get_cloudfront_oai_config",
			method:        http.MethodGet,
			path:          "/2020-05-31/origin-access-identity/cloudfront/OAIID123/config",
			wantOperation: "GetCloudFrontOriginAccessIdentityConfig",
		},
		{
			name:          "list_cloudfront_oais",
			method:        http.MethodGet,
			path:          "/2020-05-31/origin-access-identity/cloudfront",
			wantOperation: "ListCloudFrontOriginAccessIdentities",
		},
		{
			name:          "delete_cloudfront_oai",
			method:        http.MethodDelete,
			path:          "/2020-05-31/origin-access-identity/cloudfront/OAIID123",
			wantOperation: "DeleteCloudFrontOriginAccessIdentity",
		},
		{
			name:          "update_cloudfront_oai",
			method:        http.MethodPut,
			path:          "/2020-05-31/origin-access-identity/cloudfront/OAIID123/config",
			wantOperation: "UpdateCloudFrontOriginAccessIdentity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOperation, h.ExtractOperation(c))
		})
	}
}
