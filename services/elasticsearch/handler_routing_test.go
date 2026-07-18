package elasticsearch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
			path:   "/2015-01-01/domain",
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
		{
			name:   "add_tags",
			method: http.MethodPost,
			path:   "/2015-01-01/tags",
			want:   "AddTags",
		},
		{
			name:   "list_tags",
			method: http.MethodGet,
			path:   "/2015-01-01/tags",
			want:   "ListTags",
		},
		{
			name:   "remove_tags",
			method: http.MethodPost,
			path:   "/2015-01-01/tags-removal",
			want:   "RemoveTags",
		},
		{
			name:   "delete_service_role",
			method: http.MethodDelete,
			path:   "/2015-01-01/es/role",
			want:   "DeleteElasticsearchServiceRole",
		},
		{
			name:   "cancel_software_update",
			method: http.MethodPost,
			path:   "/2015-01-01/es/serviceSoftwareUpdate/cancel",
			want:   "CancelElasticsearchServiceSoftwareUpdate",
		},
		{
			name:   "accept_inbound_connection",
			method: http.MethodPut,
			path:   "/2015-01-01/es/ccs/inboundConnection/conn-001/accept",
			want:   "AcceptInboundCrossClusterSearchConnection",
		},
		{
			name:   "create_outbound_connection",
			method: http.MethodPost,
			path:   "/2015-01-01/es/ccs/outboundConnection",
			want:   "CreateOutboundCrossClusterSearchConnection",
		},
		{
			name:   "create_vpc_endpoint",
			method: http.MethodPost,
			path:   "/2015-01-01/es/vpcEndpoints",
			want:   "CreateVpcEndpoint",
		},
		{
			name:   "associate_package",
			method: http.MethodPost,
			path:   "/2015-01-01/packages/associate/F0000000001/my-domain",
			want:   "AssociatePackage",
		},
		{
			name:   "create_package",
			method: http.MethodPost,
			path:   "/2015-01-01/packages",
			want:   "CreatePackage",
		},
		{
			name:   "cancel_domain_config_change",
			method: http.MethodPost,
			path:   "/2015-01-01/es/domain/my-domain/config/cancel",
			want:   "CancelDomainConfigChange",
		},
		{
			name:   "authorize_vpc_endpoint_access",
			method: http.MethodPost,
			path:   "/2015-01-01/es/domain/my-domain/authorizeVpcEndpointAccess",
			want:   "AuthorizeVpcEndpointAccess",
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

func TestElasticsearchHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "service_role", path: "/2015-01-01/es/role", want: true},
		{name: "software_update_cancel", path: "/2015-01-01/es/serviceSoftwareUpdate/cancel", want: true},
		{name: "ccs_inbound", path: "/2015-01-01/es/ccs/inboundConnection/conn-001/accept", want: true},
		{name: "ccs_outbound", path: "/2015-01-01/es/ccs/outboundConnection", want: true},
		{name: "vpc_endpoints", path: "/2015-01-01/es/vpcEndpoints", want: true},
		{name: "packages", path: "/2015-01-01/packages", want: true},
		{name: "packages_associate", path: "/2015-01-01/packages/associate/F001/my-domain", want: true},
		{name: "unrelated", path: "/2015-01-01/other/path", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			matcher := h.RouteMatcher()
			c := newEchoContext(http.MethodGet, tt.path)
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}
