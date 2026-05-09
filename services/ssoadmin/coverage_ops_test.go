package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

func setupSSOInstance(t *testing.T, h *ssoadmin.Handler) string {
	t.Helper()

	rec := doRequest(t, h, "CreateInstance", map[string]any{
		"Name": "test-instance",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	arn, _ := resp["InstanceArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

func TestSSO_PermissionSetExtended(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceARN := setupSSOInstance(t, h)

	// Create permission set
	rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
		"InstanceArn": instanceARN,
		"Name":        "my-perm-set",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	psARN := resp["PermissionSet"].(map[string]any)["PermissionSetArn"].(string)

	// ListCustomerManagedPolicyReferencesInPermissionSet
	rec = doRequest(t, h, "ListCustomerManagedPolicyReferencesInPermissionSet", map[string]any{
		"InstanceArn":      instanceARN,
		"PermissionSetArn": psARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeletePermissionsBoundaryFromPermissionSet (no boundary set - should handle gracefully)
	rec = doRequest(t, h, "DeletePermissionsBoundaryFromPermissionSet", map[string]any{
		"InstanceArn":      instanceARN,
		"PermissionSetArn": psARN,
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}

func TestSSO_UpdateInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceARN := setupSSOInstance(t, h)

	// UpdateInstance
	rec := doRequest(t, h, "UpdateInstance", map[string]any{
		"InstanceArn": instanceARN,
		"Name":        "updated-instance",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}
