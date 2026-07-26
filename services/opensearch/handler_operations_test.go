package opensearch_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
)

// extractOperationForTest builds a minimal Echo context and calls ExtractOperation.
func extractOperationForTest(h *opensearch.Handler, method, path string) string {
	e := echo.New()
	req := httptest.NewRequest(method, path, http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return h.ExtractOperation(c)
}

func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend(testAccountID, testRegion))
	assert.Equal(t, 118, opensearch.HandlerOpsLen(h))
}

func TestExtractOperation_NewRoutes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "accept_inbound_connection",
			method: http.MethodPut,
			path:   "/2021-01-01/opensearch/cc/inboundConnection/conn-123/accept",
			wantOp: "AcceptInboundConnection",
		},
		{
			name:   "add_direct_query_ds",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/directQueryDataSource",
			wantOp: "AddDirectQueryDataSource",
		},
		{
			name:   "associate_package",
			method: http.MethodPost,
			path:   "/2021-01-01/packages/associate/pkg-1/my-domain",
			wantOp: "AssociatePackage",
		},
		{
			name:   "associate_packages",
			method: http.MethodPost,
			path:   "/2021-01-01/packages/associateMultiple",
			wantOp: "AssociatePackages",
		},
		{
			name:   "cancel_service_software_update",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/serviceSoftwareUpdate/cancel",
			wantOp: "CancelServiceSoftwareUpdate",
		},
		{
			name:   "create_application",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/application",
			wantOp: "CreateApplication",
		},
		{
			name:   "add_tags",
			method: http.MethodPost,
			path:   "/2021-01-01/tags",
			wantOp: "AddTags",
		},
		{
			name:   "list_tags",
			method: http.MethodGet,
			path:   "/2021-01-01/tags",
			wantOp: "ListTags",
		},
		{
			name:   "remove_tags",
			method: http.MethodPost,
			path:   "/2021-01-01/tags-removal",
			wantOp: "RemoveTags",
		},
		{
			name:   "add_data_source",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/domain/my-domain/dataSource",
			wantOp: "AddDataSource",
		},
		{
			name:   "authorize_vpc",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/domain/my-domain/authorizeVpcEndpointAccess",
			wantOp: "AuthorizeVpcEndpointAccess",
		},
		{
			name:   "cancel_config_change",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/domain/my-domain/config/cancel",
			wantOp: "CancelDomainConfigChange",
		},
		{
			name:   "create_domain",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/domain",
			wantOp: "CreateDomain",
		},
		{
			name:   "list_domain_names",
			method: http.MethodGet,
			path:   "/2021-01-01/opensearch/domain",
			wantOp: "ListDomainNames",
		},
		{
			name:   "describe_domain",
			method: http.MethodGet,
			path:   "/2021-01-01/opensearch/domain/my-domain",
			wantOp: "DescribeDomain",
		},
		{
			name:   "delete_domain",
			method: http.MethodDelete,
			path:   "/2021-01-01/opensearch/domain/my-domain",
			wantOp: "DeleteDomain",
		},
		{
			name:   "attach_data_source",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/application/app-1/attachDataSource",
			wantOp: "AttachDataSource",
		},
		{
			name:   "detach_data_source",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/application/app-1/detachDataSource",
			wantOp: "DetachDataSource",
		},
		{
			name:   "describe_data_source_attachment",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/application/app-1/describeDataSourceAttachment",
			wantOp: "DescribeDataSourceAttachment",
		},
		{
			name:   "list_data_source_attachments",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/application/app-1/listDataSourceAttachments",
			wantOp: "ListDataSourceAttachments",
		},
		{
			name:   "register_capability",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/application/app-1/capability/register",
			wantOp: "RegisterCapability",
		},
		{
			name:   "deregister_capability",
			method: http.MethodDelete,
			path:   "/2021-01-01/opensearch/application/app-1/capability/deregister/ai-capability",
			wantOp: "DeregisterCapability",
		},
		{
			name:   "get_capability",
			method: http.MethodGet,
			path:   "/2021-01-01/opensearch/application/app-1/capability/ai-capability",
			wantOp: "GetCapability",
		},
		{
			name:   "list_insights",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/insights",
			wantOp: "ListInsights",
		},
		{
			name:   "describe_insight_details",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/insight-details",
			wantOp: "DescribeInsightDetails",
		},
		{
			name:   "insight_feedback",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/insight-feedback",
			wantOp: "InsightFeedback",
		},
		{
			name:   "start_migration",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/app-migrations",
			wantOp: "StartMigration",
		},
		{
			name:   "list_migrations",
			method: http.MethodGet,
			path:   "/2021-01-01/opensearch/app-migrations",
			wantOp: "ListMigrations",
		},
		{
			name:   "get_migration",
			method: http.MethodGet,
			path:   "/2021-01-01/opensearch/app-migrations/migration-1",
			wantOp: "GetMigration",
		},
		{
			name:   "rollback_service_software_update",
			method: http.MethodPost,
			path:   "/2021-01-01/opensearch/serviceSoftwareUpdate/rollback",
			wantOp: "RollbackServiceSoftwareUpdate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op := extractOperationForTest(h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
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
	assert.Contains(t, ops, "AttachDataSource")
	assert.Contains(t, ops, "DetachDataSource")
	assert.Contains(t, ops, "DescribeDataSourceAttachment")
	assert.Contains(t, ops, "ListDataSourceAttachments")
	assert.Contains(t, ops, "RegisterCapability")
	assert.Contains(t, ops, "DeregisterCapability")
	assert.Contains(t, ops, "GetCapability")
	assert.Contains(t, ops, "ListInsights")
	assert.Contains(t, ops, "DescribeInsightDetails")
	assert.Contains(t, ops, "InsightFeedback")
	assert.Contains(t, ops, "StartMigration")
	assert.Contains(t, ops, "GetMigration")
	assert.Contains(t, ops, "ListMigrations")
	assert.Contains(t, ops, "RollbackServiceSoftwareUpdate")
	assert.Len(t, ops, 118)
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
