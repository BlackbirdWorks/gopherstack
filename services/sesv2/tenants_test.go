package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTenantLifecycle tests the full Create/Get/List/Delete tenant lifecycle
// via HTTP, using the real SDK's RPC-style tenant paths (every op is POST to
// a fixed verb path with identifying fields in the JSON body).
func TestTenantLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tenantName := "TestTenant"

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{
		"TenantName": tenantName,
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	getRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/get", map[string]any{
		"TenantName": tenantName,
	})
	assert.Equal(t, http.StatusOK, getRec.Code)

	getResp := decodeJSON(t, getRec)
	tenant, ok := getResp["Tenant"].(map[string]any)
	require.True(t, ok, "GetTenant response missing Tenant wrapper: %s", getRec.Body)
	assert.Equal(t, tenantName, tenant["TenantName"])
	assert.NotEmpty(t, tenant["TenantId"])
	assert.NotEmpty(t, tenant["TenantArn"])
	assert.Equal(t, "ENABLED", tenant["SendingStatus"])
	assert.NotZero(t, tenant["CreatedTimestamp"])

	listRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/list", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	listResp := decodeJSON(t, listRec)
	items, ok := listResp["Tenants"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, tenantName, item["TenantName"])
	// ListTenants items are types.TenantInfo -- no SendingStatus field.
	_, hasSendingStatus := item["SendingStatus"]
	assert.False(t, hasSendingStatus)

	delRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/delete", map[string]any{
		"TenantName": tenantName,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	getAfterDelete := doRequest(t, h, http.MethodPost, "/v2/email/tenants/get", map[string]any{
		"TenantName": tenantName,
	})
	assert.Equal(t, http.StatusNotFound, getAfterDelete.Code)
}

// TestCreateTenant_AlreadyExists verifies AlreadyExistsException semantics.
func TestCreateTenant_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{"TenantName": "dup"})
	rec := doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{"TenantName": "dup"})

	assert.Equal(t, http.StatusConflict, rec.Code)
}

// TestTenantResourceAssociation tests CreateTenantResourceAssociation,
// ListTenantResources, and DeleteTenantResourceAssociation via the RPC-style
// paths (TenantName/ResourceArn travel in the body, not the URL).
func TestTenantResourceAssociation(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tenantName := "AssocTenant"
	const resourceArn = "arn:aws:ses:us-east-1:123456789012:configuration-set/MyCS"

	doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{
		"TenantName": tenantName,
	})

	assocRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/resources", map[string]any{
		"TenantName":  tenantName,
		"ResourceArn": resourceArn,
	})
	assert.Equal(t, http.StatusOK, assocRec.Code)

	listRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/resources/list", map[string]any{
		"TenantName": tenantName,
	})
	assert.Equal(t, http.StatusOK, listRec.Code)

	listResp := decodeJSON(t, listRec)
	items, ok := listResp["TenantResources"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, resourceArn, item["ResourceArn"])
	assert.Equal(t, "CONFIGURATION_SET", item["ResourceType"])

	delRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/resources/delete", map[string]any{
		"TenantName":  tenantName,
		"ResourceArn": resourceArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	listAfterDelete := doRequest(t, h, http.MethodPost, "/v2/email/tenants/resources/list", map[string]any{
		"TenantName": tenantName,
	})
	afterResp := decodeJSON(t, listAfterDelete)
	afterItems, _ := afterResp["TenantResources"].([]any)
	assert.Empty(t, afterItems)
}

// TestCreateTenantResourceAssociation_TenantNotFound verifies the
// NotFoundException path for associating a resource with a nonexistent tenant.
func TestCreateTenantResourceAssociation_TenantNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/resources", map[string]any{
		"TenantName":  "nope",
		"ResourceArn": "arn:aws:ses:us-east-1:123456789012:configuration-set/MyCS",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestListResourceTenants tests the ListResourceTenants operation, which
// lives under a distinct top-level path (/v2/email/resources/tenants/list)
// from the rest of the tenant family (/v2/email/tenants/...).
func TestListResourceTenants(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tenantName := "ResTenant"
	const resourceArn = "arn:aws:ses:us-east-1:123456789012:identity/example.com"

	doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{"TenantName": tenantName})
	doRequest(t, h, http.MethodPost, "/v2/email/tenants/resources", map[string]any{
		"TenantName":  tenantName,
		"ResourceArn": resourceArn,
	})

	rec := doRequest(t, h, http.MethodPost, "/v2/email/resources/tenants/list", map[string]any{
		"ResourceArn": resourceArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	resp := decodeJSON(t, rec)
	items, ok := resp["ResourceTenants"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, tenantName, item["TenantName"])
	assert.Equal(t, resourceArn, item["ResourceArn"])
}

// TestTenantCRUD verifies Create persists, Get retrieves the name, List includes it.
func TestTenantCRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{"TenantName": "acme"})
	require.Equal(t, http.StatusOK, rec.Code, "CreateTenant: %s", rec.Body)

	rec2 := doRequest(t, h, http.MethodPost, "/v2/email/tenants/get", map[string]any{"TenantName": "acme"})
	require.Equal(t, http.StatusOK, rec2.Code, "GetTenant: %s", rec2.Body)

	resp2 := decodeJSON(t, rec2)
	tenant, ok := resp2["Tenant"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "acme", tenant["TenantName"])

	rec3 := doRequest(t, h, http.MethodPost, "/v2/email/tenants/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec3.Code, "ListTenants: %s", rec3.Body)
}

// TestGetTenant_NotFound verifies NotFoundException for an unknown tenant.
func TestGetTenant_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/get", map[string]any{"TenantName": "ghost"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
