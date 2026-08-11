package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerless_CustomDomainAssociationCRUD covers Create/Get/List/Update/
// Delete and proves GetCredentials.customDomainName resolves to the
// associated workgroup, and that the workgroup itself echoes the
// association's fields (real Workgroup carries them directly).
func TestServerless_CustomDomainAssociationCRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "cd-ns"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "cd-wg", "namespaceName": "cd-ns"})

	rec := doServerlessOp(t, h, "CreateCustomDomainAssociation", map[string]any{
		"customDomainName":           "db.example.com",
		"customDomainCertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc",
		"workgroupName":              "cd-wg",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	assert.Equal(t, "db.example.com", createResp["customDomainName"])
	assert.Equal(t, "cd-wg", createResp["workgroupName"])
	assert.NotContains(t, createResp, "association", "response is flat, not wrapped in an envelope key")

	// The association is mirrored onto the workgroup itself.
	rec = doServerlessOp(t, h, "GetWorkgroup", map[string]any{"workgroupName": "cd-wg"})
	require.Equal(t, http.StatusOK, rec.Code)

	var wgResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&wgResp))
	wgData, _ := wgResp["workgroup"].(map[string]any)
	require.NotNil(t, wgData)
	assert.Equal(t, "db.example.com", wgData["customDomainName"])
	assert.Equal(t, "arn:aws:acm:us-east-1:000000000000:certificate/abc", wgData["customDomainCertificateArn"])
	assert.Contains(t, wgData, "customDomainCertificateExpiryTime")

	rec = doServerlessOp(t, h, "GetCustomDomainAssociation", map[string]any{
		"customDomainName": "db.example.com",
		"workgroupName":    "cd-wg",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ListCustomDomainAssociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	assocs, _ := listResp["associations"].([]any)
	require.Len(t, assocs, 1)

	// GetCredentials resolves workgroupName via customDomainName alone.
	rec = doServerlessOp(t, h, "GetCredentials", map[string]any{"customDomainName": "db.example.com"})
	require.Equal(t, http.StatusOK, rec.Code)

	var credResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&credResp))
	dbUser, _ := credResp["dbUser"].(string)
	assert.NotEmpty(t, dbUser)

	rec = doServerlessOp(t, h, "UpdateCustomDomainAssociation", map[string]any{
		"customDomainName":           "db.example.com",
		"workgroupName":              "cd-wg",
		"customDomainCertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/def",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&updateResp))
	assert.Equal(t, "arn:aws:acm:us-east-1:000000000000:certificate/def", updateResp["customDomainCertificateArn"])

	rec = doServerlessOp(t, h, "DeleteCustomDomainAssociation", map[string]any{
		"customDomainName": "db.example.com",
		"workgroupName":    "cd-wg",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var deleteResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&deleteResp))
	assert.Empty(t, deleteResp, "DeleteCustomDomainAssociationResponse has zero members on the real wire")

	// The mirrored workgroup fields are cleared too.
	rec = doServerlessOp(t, h, "GetWorkgroup", map[string]any{"workgroupName": "cd-wg"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&wgResp))
	wgData, _ = wgResp["workgroup"].(map[string]any)
	assert.NotContains(t, wgData, "customDomainName")

	rec = doServerlessOp(t, h, "GetCustomDomainAssociation", map[string]any{
		"customDomainName": "db.example.com",
		"workgroupName":    "cd-wg",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestServerless_CustomDomainAssociation_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "missing required fields",
			body: map[string]any{
				"customDomainName": "db.example.com",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name: "workgroup not found",
			body: map[string]any{
				"customDomainName":           "db.example.com",
				"customDomainCertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc",
				"workgroupName":              "no-such-workgroup",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, "CreateCustomDomainAssociation", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}

	t.Run("conflict when domain already associated", func(t *testing.T) {
		t.Parallel()

		h := newServerlessHandler()

		doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "cd-ns2"})
		doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "cd-wg2", "namespaceName": "cd-ns2"})
		doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "cd-wg3", "namespaceName": "cd-ns2"})

		rec := doServerlessOp(t, h, "CreateCustomDomainAssociation", map[string]any{
			"customDomainName":           "dup.example.com",
			"customDomainCertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc",
			"workgroupName":              "cd-wg2",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		rec = doServerlessOp(t, h, "CreateCustomDomainAssociation", map[string]any{
			"customDomainName":           "dup.example.com",
			"customDomainCertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/def",
			"workgroupName":              "cd-wg3",
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "ConflictException", errResp["__type"])
	})

	t.Run("get credentials requires workgroup or custom domain name", func(t *testing.T) {
		t.Parallel()

		h := newServerlessHandler()

		rec := doServerlessOp(t, h, "GetCredentials", map[string]any{})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "ValidationException", errResp["__type"])
	})

	t.Run("get credentials unknown custom domain name", func(t *testing.T) {
		t.Parallel()

		h := newServerlessHandler()

		rec := doServerlessOp(t, h, "GetCredentials", map[string]any{"customDomainName": "nope.example.com"})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
	})
}
