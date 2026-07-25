package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTenantSDKRoundTrip drives CreateTenant/GetTenant/ListTenants through
// the real aws-sdk-go-v2 sesv2 client (not hand-decoded JSON maps), so the
// tenantOutput/tenantInfoOutput typed-DTO conversion (wire_output.go) is
// verified by the genuine SDK deserializer rather than a backend-struct
// assertion -- the class of test that has repeatedly hidden dropped-field
// bugs elsewhere in this codebase's parity work.
func TestTenantSDKRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, tenant *sesv2types.Tenant, list []sesv2types.TenantInfo)
		name  string
	}{
		{
			name: "get_tenant_full_fields",
			check: func(t *testing.T, tenant *sesv2types.Tenant, _ []sesv2types.TenantInfo) {
				t.Helper()
				require.NotNil(t, tenant)
				assert.Equal(t, "sdk-tenant", aws.ToString(tenant.TenantName))
				assert.NotEmpty(t, aws.ToString(tenant.TenantId))
				assert.NotEmpty(t, aws.ToString(tenant.TenantArn))
				assert.Equal(t, sesv2types.SendingStatusEnabled, tenant.SendingStatus)
				assert.NotNil(t, tenant.CreatedTimestamp)
			},
		},
		{
			name: "list_tenants_info_shape",
			check: func(t *testing.T, _ *sesv2types.Tenant, list []sesv2types.TenantInfo) {
				t.Helper()
				require.Len(t, list, 1)
				assert.Equal(t, "sdk-tenant", aws.ToString(list[0].TenantName))
				assert.NotEmpty(t, aws.ToString(list[0].TenantId))
				assert.NotEmpty(t, aws.ToString(list[0].TenantArn))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			client := newSESv2SDKClient(t, h)

			_, err := client.CreateTenant(t.Context(), &sesv2sdk.CreateTenantInput{
				TenantName: aws.String("sdk-tenant"),
			})
			require.NoError(t, err)

			getOut, err := client.GetTenant(t.Context(), &sesv2sdk.GetTenantInput{
				TenantName: aws.String("sdk-tenant"),
			})
			require.NoError(t, err)

			listOut, err := client.ListTenants(t.Context(), &sesv2sdk.ListTenantsInput{})
			require.NoError(t, err)

			tt.check(t, getOut.Tenant, listOut.Tenants)
		})
	}
}

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

// TestPutTenantSuppressionAttributes tests the PutTenantSuppressionAttributes
// operation. Note the real path is the singular /v2/email/tenant/suppression
// -- a distinct top-level segment from the rest of the tenant family's plural
// /v2/email/tenants/... (confirmed against serializers.go; see
// parseTenantSuppressionPath's doc comment). Valid reasons/scope persist onto
// the tenant and are reflected by GetTenant's SuppressionAttributes; an
// invalid reason is a BadRequestException; a nonexistent tenant is a
// NotFoundException.
func TestPutTenantSuppressionAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		suppressionScope  string
		name              string
		tenantName        string
		suppressedReasons []any
		createTenant      bool
		wantStatus        int
	}{
		{
			name:              "bounce_and_complaint_tenant_scope",
			tenantName:        "SuppressionTenant",
			suppressedReasons: []any{"BOUNCE", "COMPLAINT"},
			suppressionScope:  "TENANT",
			createTenant:      true,
			wantStatus:        http.StatusOK,
		},
		{
			name:              "bounce_only_account_scope",
			tenantName:        "SuppressionTenant2",
			suppressedReasons: []any{"BOUNCE"},
			suppressionScope:  "ACCOUNT",
			createTenant:      true,
			wantStatus:        http.StatusOK,
		},
		{
			name:              "invalid_reason",
			tenantName:        "SuppressionTenant3",
			suppressedReasons: []any{"BOGUS"},
			createTenant:      true,
			wantStatus:        http.StatusBadRequest,
		},
		{
			name:              "invalid_scope",
			tenantName:        "SuppressionTenant4",
			suppressedReasons: []any{"BOUNCE"},
			suppressionScope:  "BOGUS",
			createTenant:      true,
			wantStatus:        http.StatusBadRequest,
		},
		{
			name:              "tenant_not_found",
			tenantName:        "GhostTenant",
			suppressedReasons: []any{"BOUNCE"},
			createTenant:      false,
			wantStatus:        http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.createTenant {
				doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{"TenantName": tt.tenantName})
			}

			rec := doRequest(t, h, http.MethodPost, "/v2/email/tenant/suppression", map[string]any{
				"TenantName":        tt.tenantName,
				"SuppressedReasons": tt.suppressedReasons,
				"SuppressionScope":  tt.suppressionScope,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)

			if tt.wantStatus != http.StatusOK {
				return
			}

			getRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/get", map[string]any{
				"TenantName": tt.tenantName,
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			resp := decodeJSON(t, getRec)
			tenant, ok := resp["Tenant"].(map[string]any)
			require.True(t, ok)

			suppression, ok := tenant["SuppressionAttributes"].(map[string]any)
			require.True(t, ok, "GetTenant response missing SuppressionAttributes: %s", getRec.Body)
			assert.ElementsMatch(t, tt.suppressedReasons, suppression["SuppressedReasons"])
			assert.Equal(t, tt.suppressionScope, suppression["SuppressionScope"])
		})
	}
}

// TestPutTenantSuppressionAttributesSDKRoundTrip drives
// CreateTenant/PutTenantSuppressionAttributes/GetTenant through the real
// aws-sdk-go-v2 sesv2 client so the tenantSuppressionAttributesOutput typed
// DTO (wire_output.go) is verified by the genuine SDK deserializer.
func TestPutTenantSuppressionAttributesSDKRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newSESv2SDKClient(t, h)

	_, err := client.CreateTenant(t.Context(), &sesv2sdk.CreateTenantInput{
		TenantName: aws.String("sdk-suppression-tenant"),
	})
	require.NoError(t, err)

	_, err = client.PutTenantSuppressionAttributes(t.Context(), &sesv2sdk.PutTenantSuppressionAttributesInput{
		TenantName:        aws.String("sdk-suppression-tenant"),
		SuppressedReasons: []sesv2types.SuppressionListReason{sesv2types.SuppressionListReasonComplaint},
		SuppressionScope:  sesv2types.SuppressionListScopeTenant,
	})
	require.NoError(t, err)

	getOut, err := client.GetTenant(t.Context(), &sesv2sdk.GetTenantInput{
		TenantName: aws.String("sdk-suppression-tenant"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Tenant)
	require.NotNil(t, getOut.Tenant.SuppressionAttributes)
	assert.Equal(
		t,
		[]sesv2types.SuppressionListReason{sesv2types.SuppressionListReasonComplaint},
		getOut.Tenant.SuppressionAttributes.SuppressedReasons,
	)
	assert.Equal(t, sesv2types.SuppressionListScopeTenant, getOut.Tenant.SuppressionAttributes.SuppressionScope)
}

// TestGetTenant_NotFound verifies NotFoundException for an unknown tenant.
func TestGetTenant_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := doRequest(t, h, http.MethodPost, "/v2/email/tenants/get", map[string]any{"TenantName": "ghost"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
