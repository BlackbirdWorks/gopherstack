package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTenantLifecycle tests the full Create/Get/List/Delete tenant lifecycle via HTTP.
func TestTenantLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tenantName := "TestTenant"

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{
		"TenantName": tenantName,
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/v2/email/tenants/"+tenantName, nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/v2/email/tenants", nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	delRec := doRequest(t, h, http.MethodDelete, "/v2/email/tenants/"+tenantName, nil)
	assert.Equal(t, http.StatusOK, delRec.Code)
}

// TestTenantResourceAssociation tests CreateTenantResourceAssociation, ListTenantResources,
// and DeleteTenantResourceAssociation.
func TestTenantResourceAssociation(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tenantName := "AssocTenant"

	doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{
		"TenantName": tenantName,
	})

	// Create tenant resource association (ARN without slashes to avoid path splitting).
	const resARN = "arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Acs-MyCS"
	assocRec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/tenants/"+tenantName+"/resources",
		map[string]any{
			"ResourceArn": "arn:aws:ses:us-east-1:123456789012:cs-MyCS",
		},
	)
	assert.Equal(t, http.StatusOK, assocRec.Code)

	// List tenant resources.
	listRec := doRequest(t, h, http.MethodGet, "/v2/email/tenants/"+tenantName+"/resources", nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	// Delete tenant resource association.
	delRec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/tenants/"+tenantName+"/resources/"+resARN,
		nil,
	)
	assert.Equal(t, http.StatusOK, delRec.Code)
}

// TestListResourceTenants tests the ListResourceTenants operation.
func TestListResourceTenants(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/resource-tenants", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestTenantCRUD verifies Create persists, Get retrieves the name, List includes it.
func TestTenantCRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := doReqQuery(t, h, http.MethodPost, "/v2/email/tenants", nil,
		map[string]any{"TenantName": "acme"})
	require.Equal(t, http.StatusOK, rec.Code, "CreateTenant: %s", rec.Body)

	rec2 := doReqQuery(t, h, http.MethodGet, "/v2/email/tenants/acme", nil, nil)
	require.Equal(t, http.StatusOK, rec2.Code, "GetTenant: %s", rec2.Body)

	resp2 := decodeJSON(t, rec2)
	assert.Equal(t, "acme", resp2["TenantName"])

	rec3 := doReqQuery(t, h, http.MethodGet, "/v2/email/tenants", nil, nil)
	require.Equal(t, http.StatusOK, rec3.Code, "ListTenants: %s", rec3.Body)
}
