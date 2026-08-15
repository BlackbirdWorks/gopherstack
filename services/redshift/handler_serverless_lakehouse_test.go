package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerless_UpdateLakehouseConfiguration_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing namespace name",
			body:       map[string]any{"catalogName": "mycatalog"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "unknown namespace",
			body:       map[string]any{"namespaceName": "no-such-ns", "catalogName": "mycatalog"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, "UpdateLakehouseConfiguration", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}

// TestServerless_UpdateLakehouseConfiguration_DryRun proves DryRun validates
// without mutating state: the namespace's catalogArn/lakehouseRegistrationStatus
// stay empty afterward.
func TestServerless_UpdateLakehouseConfiguration_DryRun(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "lh-dryrun-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "UpdateLakehouseConfiguration", map[string]any{
		"namespaceName":         "lh-dryrun-ns",
		"catalogName":           "mycatalog",
		"lakehouseRegistration": "Register",
		"dryRun":                true,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "DryRunException", errResp["__type"])

	rec = doServerlessOp(t, h, "GetNamespace", map[string]any{"namespaceName": "lh-dryrun-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&getResp))
	ns, _ := getResp["namespace"].(map[string]any)
	require.NotNil(t, ns)
	assert.Empty(t, ns["catalogArn"], "dry run must not mutate the namespace's catalogArn")
	assert.Empty(t, ns["lakehouseRegistrationStatus"], "dry run must not mutate the namespace's registration status")
}

// TestServerless_UpdateLakehouseConfiguration_RegisterAndAssociate proves the
// real state changes UpdateLakehouseConfiguration makes are observable both
// on its own response and via a subsequent GetNamespace (for the two fields
// that are real Namespace members: catalogArn/lakehouseRegistrationStatus).
func TestServerless_UpdateLakehouseConfiguration_RegisterAndAssociate(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "lh-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "UpdateLakehouseConfiguration", map[string]any{
		"namespaceName":              "lh-ns",
		"catalogName":                "mycatalog",
		"lakehouseRegistration":      "Register",
		"lakehouseIdcApplicationArn": "arn:aws:sso::000000000000:application/idc-app-1",
		"lakehouseIdcRegistration":   "Associate",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "lh-ns", resp["namespaceName"])
	assert.Contains(t, resp["catalogArn"], "mycatalog")
	assert.Equal(t, "Registered", resp["lakehouseRegistrationStatus"])
	assert.Equal(t, "arn:aws:sso::000000000000:application/idc-app-1", resp["lakehouseIdcApplicationArn"])

	rec = doServerlessOp(t, h, "GetNamespace", map[string]any{"namespaceName": "lh-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&getResp))
	ns, _ := getResp["namespace"].(map[string]any)
	require.NotNil(t, ns)
	assert.Contains(t, ns["catalogArn"], "mycatalog")
	assert.Equal(t, "Registered", ns["lakehouseRegistrationStatus"])
	assert.NotContains(t, ns, "lakehouseIdcApplicationArn",
		"real GetNamespaceOutput's Namespace shape has no lakehouseIdcApplicationArn member")

	// A later call that changes only the registration status must not lose
	// the previously-associated IDC application ARN.
	rec = doServerlessOp(t, h, "UpdateLakehouseConfiguration", map[string]any{
		"namespaceName":         "lh-ns",
		"lakehouseRegistration": "Deregister",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp2 map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp2))
	assert.Equal(t, "Deregistered", resp2["lakehouseRegistrationStatus"])
	assert.Equal(t, "arn:aws:sso::000000000000:application/idc-app-1", resp2["lakehouseIdcApplicationArn"])

	// Disassociate must clear the stored IDC application ARN.
	rec = doServerlessOp(t, h, "UpdateLakehouseConfiguration", map[string]any{
		"namespaceName":            "lh-ns",
		"lakehouseIdcRegistration": "Disassociate",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp3 map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp3))
	assert.NotContains(t, resp3, "lakehouseIdcApplicationArn")
}
