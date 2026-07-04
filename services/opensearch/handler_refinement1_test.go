package opensearch_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
)

// ----------------------------------------
// Refinement check 1 tests
// ----------------------------------------

// extractOperationForTest builds a minimal Echo context and calls ExtractOperation.
func extractOperationForTest(h *opensearch.Handler, method, path string) string {
	e := echo.New()
	req := httptest.NewRequest(method, path, http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return h.ExtractOperation(c)
}

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("d1", "")
	b.AddDomainInternal("d2", "")

	require.Equal(t, 2, opensearch.DomainCount(b))

	b.Reset()

	assert.Equal(t, 0, opensearch.DomainCount(b))
	assert.Equal(t, 0, opensearch.ARNIndexSize(b))
}

func TestRefinement1_MultipleResetCycles(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)

	for range 3 {
		b.AddDomainInternal("d1", "OpenSearch_2.11")
		b.Reset()
		assert.Equal(t, 0, opensearch.DomainCount(b))
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	h := opensearch.NewHandler(b)
	b.AddDomainInternal("d1", "")

	require.Equal(t, 1, opensearch.DomainCount(b))
	h.Reset()
	assert.Equal(t, 0, opensearch.DomainCount(b))
}

func TestRefinement1_BackendRegionAccountID(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)

	assert.Equal(t, testRegion, b.Region())
	assert.Equal(t, testAccountID, b.AccountID())
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &opensearch.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrNilAppContext)
}

func TestRefinement1_ARNIndexRebuiltAfterRestore(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("snap-domain", "OpenSearch_2.11")

	// Snapshot and restore into fresh backend.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Tag operations should work after restore (they rely on arnIndex).
	domain, err := fresh.DescribeDomain("snap-domain")
	require.NoError(t, err)

	err = fresh.AddTags(domain.ARN, map[string]string{"env": "test"})
	require.NoError(t, err)

	tags, err := fresh.ListTags(domain.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend(testAccountID, testRegion))
	assert.Equal(t, 104, opensearch.HandlerOpsLen(h))
}

func TestRefinement1_AddDomainInternal(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("seed-domain", "OpenSearch_2.3")

	d, err := b.DescribeDomain("seed-domain")
	require.NoError(t, err)
	assert.Equal(t, "OpenSearch_2.3", d.EngineVersion)
	assert.NotEmpty(t, d.ARN)
	assert.NotEmpty(t, d.Endpoint)
}

func TestRefinement1_AddDomainInternal_DefaultVersion(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("seed-domain", "")

	d, err := b.DescribeDomain("seed-domain")
	require.NoError(t, err)
	assert.Equal(t, "OpenSearch_2.11", d.EngineVersion)
}

func TestRefinement1_ExportCounts(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("d1", "")
	b.AddDomainInternal("d2", "")

	opensearch.SeedInboundConnection(b, "conn-1")

	_, err := b.AddDataSource("d1", "ds-1", "desc", "S3GLUE")
	require.NoError(t, err)

	_, err = b.AddDirectQueryDataSource("dq-1", "desc", "CloudWatchLogs", nil)
	require.NoError(t, err)

	_, err = b.CreateApplication("app-1", nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, opensearch.DomainCount(b))
	assert.Equal(t, 1, opensearch.ConnectionCount(b))
	assert.Equal(t, 1, opensearch.DataSourceCount(b))
	assert.Equal(t, 1, opensearch.DirectQueryDataSourceCount(b))
	assert.Equal(t, 1, opensearch.ApplicationCount(b))
	assert.Equal(t, 2, opensearch.ARNIndexSize(b))
}

func TestRefinement1_DeleteDomain_Cascade(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("my-domain", "")

	_, err := b.AddDataSource("my-domain", "ds1", "desc", "S3GLUE")
	require.NoError(t, err)

	b.AddPackageInternal("pkg-001", "test-pkg", "TXT-DICTIONARY")

	_, err = b.AssociatePackage("pkg-001", "my-domain")
	require.NoError(t, err)

	_, err = b.AuthorizeVpcEndpointAccess("my-domain", "111122223333", "")
	require.NoError(t, err)

	assert.Equal(t, 1, opensearch.DataSourceCount(b))

	_, err = b.DeleteDomain("my-domain")
	require.NoError(t, err)

	// Domain data sources cleaned up.
	assert.Equal(t, 0, opensearch.DataSourceCount(b))
	// ARN index cleaned up.
	assert.Equal(t, 0, opensearch.ARNIndexSize(b))
}

func TestRefinement1_ListDomainNames_Sorted(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("zebra", "")
	b.AddDomainInternal("apple", "")
	b.AddDomainInternal("mango", "")

	names := b.ListDomainNames()
	require.Len(t, names, 3)
	assert.Equal(t, []string{"apple", "mango", "zebra"}, names)
}

func TestRefinement1_AssociatePackages_EmptyList(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("my-domain", "")

	_, err := b.AssociatePackages("my-domain", []string{})
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrInvalidParameter)
}

func TestRefinement1_DescribeDomainConfig_RealData(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName":    "cfg-domain",
			"EngineVersion": "OpenSearch_2.3",
		})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cfgResp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/cfg-domain/config", nil)
	defer cfgResp.Body.Close()
	require.Equal(t, http.StatusOK, cfgResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(cfgResp.Body).Decode(&out))

	domainConfig, ok := out["DomainConfig"].(map[string]any)
	require.True(t, ok)
	ev, ok := domainConfig["EngineVersion"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "OpenSearch_2.3", ev["Options"])
}

func TestRefinement1_ExtractOperation_NewRoutes(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op := extractOperationForTest(h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_TagsAfterRestore_RoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("tagged-domain", "")

	domain, err := b.DescribeDomain("tagged-domain")
	require.NoError(t, err)

	require.NoError(t, b.AddTags(domain.ARN, map[string]string{"key": "value", "env": "prod"}))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Tags should survive round-trip
	tags, err := fresh.ListTags(domain.ARN)
	require.NoError(t, err)
	assert.Equal(t, "value", tags["key"])
	assert.Equal(t, "prod", tags["env"])
}

func TestRefinement1_AddDataSource_DomainNotFound(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.AddDataSource("nonexistent", "ds1", "desc", "S3GLUE")
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrDomainNotFound)
}

func TestRefinement1_AcceptInboundConnection_EmptyID(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.AcceptInboundConnection("")
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrInvalidParameter)
}

func TestRefinement1_CreateApplication_EmptyName(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateApplication("", nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrInvalidParameter)
}

func TestRefinement1_CreateApplication_Duplicate(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateApplication("my-app", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateApplication("my-app", nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrApplicationAlreadyExists)
}

func TestRefinement1_AuthorizeVpcEndpointAccess_ServicePrincipal(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("vpc-domain", "")

	principal, err := b.AuthorizeVpcEndpointAccess("vpc-domain", "", "vpc-endpoint.opensearch.amazonaws.com")
	require.NoError(t, err)
	assert.Equal(t, "AWS_SERVICE", principal.PrincipalType)
	assert.Equal(t, "vpc-endpoint.opensearch.amazonaws.com", principal.Principal)
}

func TestRefinement1_AuthorizeVpcEndpointAccess_AccountPrincipal(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("vpc-domain", "")

	principal, err := b.AuthorizeVpcEndpointAccess("vpc-domain", "111122223333", "")
	require.NoError(t, err)
	assert.Equal(t, "AWS_ACCOUNT", principal.PrincipalType)
	assert.Equal(t, "111122223333", principal.Principal)
}

func TestRefinement1_CancelDomainConfigChange_DryRun(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("my-domain", "")

	ids, dryRun, err := b.CancelDomainConfigChange("my-domain", true)
	require.NoError(t, err)
	assert.Empty(t, ids)
	assert.True(t, dryRun)
}

func TestRefinement1_CancelServiceSoftwareUpdate_NotFound(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CancelServiceSoftwareUpdate("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrDomainNotFound)
}

func TestRefinement1_AddDirectQueryDataSource_Duplicate(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.AddDirectQueryDataSource("ds-1", "desc", "CloudWatchLogs", nil)
	require.NoError(t, err)

	_, err = b.AddDirectQueryDataSource("ds-1", "desc2", "S3", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrDataSourceAlreadyExists)
}

func TestRefinement1_DirectQueryDataSource_ARN(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	dsARN, err := b.AddDirectQueryDataSource("my-dq", "desc", "CloudWatchLogs", nil)
	require.NoError(t, err)
	assert.Contains(t, dsARN, "directQueryDataSource/my-dq")
	assert.Contains(t, dsARN, testAccountID)
	assert.Contains(t, dsARN, testRegion)
}

func TestRefinement1_HTTPAddDataSource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "my-domain"})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain/my-domain/dataSource",
		map[string]any{
			"Name":           "my-ds",
			"Description":    "test",
			"DataSourceType": map[string]string{"S3GlueDataCatalog": ""},
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRefinement1_HTTPAssociatePackages_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/packages/associateMultiple", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
