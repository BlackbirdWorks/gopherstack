package acmpca_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestACMPCAHandler_PermissionLifecycle verifies the create/list/delete
// permission lifecycle via the handler dispatch path.
func TestACMPCAHandler_PermissionLifecycle(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	createRec := doACMPCARequest(t, h, "CreatePermission", map[string]any{
		"Actions":                 []string{"IssueCertificate"},
		"CertificateAuthorityArn": caARN,
		"Principal":               "acm.amazonaws.com",
		"SourceAccount":           testAccountID,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	listRec := doACMPCARequest(t, h, "ListPermissions", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	listResp := parseACMPCAResponse(t, listRec)
	perms, ok := listResp["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, perms, 1)

	deleteRec := doACMPCARequest(t, h, "DeletePermission", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Principal":               "acm.amazonaws.com",
		"SourceAccount":           testAccountID,
	})
	require.Equal(t, http.StatusOK, deleteRec.Code)
}

// TestACMPCAHandler_ListPermissions_RequiresCA verifies that ListPermissions
// without a CertificateAuthorityArn returns InvalidArnException, matching
// ListPermissions' own deserializeOpError.
func TestACMPCAHandler_ListPermissions_RequiresCA(t *testing.T) {
	t.Parallel()

	rec := doACMPCARequest(t, newACMPCAHandler(), "ListPermissions", map[string]any{})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := parseACMPCAResponse(t, rec)
	assert.Equal(t, "InvalidArnException", resp["__type"])
}

// TestACMPCAHandler_CreatePermission_Duplicate verifies that granting the same
// principal/source-account permission twice returns PermissionAlreadyExistsException
// on the wire, matching real AWS ACM PCA behavior.
func TestACMPCAHandler_CreatePermission_Duplicate(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	body := map[string]any{
		"Actions":                 []string{"IssueCertificate"},
		"CertificateAuthorityArn": caARN,
		"Principal":               "acm.amazonaws.com",
		"SourceAccount":           testAccountID,
	}

	firstRec := doACMPCARequest(t, h, "CreatePermission", body)
	require.Equal(t, http.StatusOK, firstRec.Code)

	secondRec := doACMPCARequest(t, h, "CreatePermission", body)
	require.Equal(t, http.StatusBadRequest, secondRec.Code)
	resp := parseACMPCAResponse(t, secondRec)
	assert.Equal(t, "PermissionAlreadyExistsException", resp["__type"])
}
