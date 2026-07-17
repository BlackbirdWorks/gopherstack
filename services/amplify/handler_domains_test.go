package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DomainAssociationCRUD verifies domain association lifecycle via the HTTP handler.
func TestHandler_DomainAssociationCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_domain_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "DomainApp")

			subDomains := []map[string]any{{"prefix": "www", "branchName": "main"}}

			// Create.
			rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/domains",
				map[string]any{"domainName": "example.com", "subDomainSettings": subDomains})
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			da := createResp["domainAssociation"].(map[string]any)
			assert.Equal(t, "example.com", da["domainName"])

			// Get.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/domains/example.com", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// List.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/domains", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp["domainAssociations"].([]any), 1)

			// Update.
			rec = doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/domains/example.com",
				map[string]any{"subDomainSettings": subDomains, "enableAutoSubDomain": true})
			require.Equal(t, http.StatusOK, rec.Code)

			var updateResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
			updated := updateResp["domainAssociation"].(map[string]any)
			assert.Equal(t, true, updated["enableAutoSubDomain"])

			// Delete.
			rec = doRequest(t, h, http.MethodDelete, "/apps/"+app.AppID+"/domains/example.com", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Confirm gone.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/domains/example.com", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestHandler_CreateDomainAssociation_DuplicateIsBadRequest verifies that a
// duplicate domain association create yields a BadRequestException-shaped error.
func TestHandler_CreateDomainAssociation_DuplicateIsBadRequest(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	app, err := b.CreateApp("TestApp", "", "", "", nil)
	require.NoError(t, err)

	body := map[string]any{"domainName": "example.com"}
	rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/domains", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/domains", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "BadRequestException", rec.Header().Get("X-Amzn-Errortype"))
}
