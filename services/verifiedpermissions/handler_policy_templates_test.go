package verifiedpermissions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVPHandler_PolicyTemplateCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "full CRUD lifecycle",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)

			// Create policy store
			rec := doVPRequest(
				t,
				h,
				"CreatePolicyStore",
				map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var storeResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
			storeID := storeResp["policyStoreId"].(string)

			// Create template
			rec = doVPRequest(t, h, "CreatePolicyTemplate", map[string]any{
				"policyStoreId": storeID,
				"description":   "My template",
				"statement":     "permit(principal == ?principal, action, resource);",
			})
			require.Equal(t, tt.wantCode, rec.Code)

			var tplResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tplResp))
			templateID := tplResp["policyTemplateId"].(string)
			assert.NotEmpty(t, templateID)

			// Get template
			rec = doVPRequest(t, h, "GetPolicyTemplate", map[string]any{
				"policyStoreId":    storeID,
				"policyTemplateId": templateID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// List templates
			rec = doVPRequest(t, h, "ListPolicyTemplates", map[string]any{
				"policyStoreId": storeID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Update template
			rec = doVPRequest(t, h, "UpdatePolicyTemplate", map[string]any{
				"policyStoreId":    storeID,
				"policyTemplateId": templateID,
				"description":      "Updated",
				"statement":        "forbid(principal == ?principal, action, resource);",
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete template
			rec = doVPRequest(t, h, "DeletePolicyTemplate", map[string]any{
				"policyStoreId":    storeID,
				"policyTemplateId": templateID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestVPHandler_PolicyTemplateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "create template missing store id",
			action:   "CreatePolicyTemplate",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create template missing statement",
			action:   "CreatePolicyTemplate",
			body:     map[string]any{"policyStoreId": "store-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get template missing store id",
			action:   "GetPolicyTemplate",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get template missing template id",
			action:   "GetPolicyTemplate",
			body:     map[string]any{"policyStoreId": "store-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list templates missing store id",
			action:   "ListPolicyTemplates",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update template missing store id",
			action:   "UpdatePolicyTemplate",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update template missing template id",
			action:   "UpdatePolicyTemplate",
			body:     map[string]any{"policyStoreId": "store-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "update template missing statement",
			action: "UpdatePolicyTemplate",
			body: map[string]any{
				"policyStoreId":    "store-1",
				"policyTemplateId": "tpl-1",
				"description":      "desc only, no statement",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete template missing store id",
			action:   "DeletePolicyTemplate",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete template missing template id",
			action:   "DeletePolicyTemplate",
			body:     map[string]any{"policyStoreId": "store-1"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			rec := doVPRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_ListPolicyTemplates_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	rec := doVPRequest(t, h, "ListPolicyTemplates", map[string]any{
		"policyStoreId": "nonexistent-store",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_UpdatePolicyTemplate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Create a store but use nonexistent template
	rec := doVPRequest(
		t,
		h,
		"CreatePolicyStore",
		map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
	)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
	storeID := storeResp["policyStoreId"].(string)

	rec = doVPRequest(t, h, "UpdatePolicyTemplate", map[string]any{
		"policyStoreId":    storeID,
		"policyTemplateId": "nonexistent-template",
		"statement":        "permit(principal == ?principal, action, resource);",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_DeletePolicyTemplate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Create a store but use nonexistent template
	rec := doVPRequest(
		t,
		h,
		"CreatePolicyStore",
		map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
	)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
	storeID := storeResp["policyStoreId"].(string)

	rec = doVPRequest(t, h, "DeletePolicyTemplate", map[string]any{
		"policyStoreId":    storeID,
		"policyTemplateId": "nonexistent-template",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
