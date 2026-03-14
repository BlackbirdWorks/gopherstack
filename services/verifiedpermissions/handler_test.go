package verifiedpermissions_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

func newTestVPHandler(t *testing.T) *verifiedpermissions.Handler {
	t.Helper()

	return verifiedpermissions.NewHandler(verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1"))
}

func doVPRequest(
	t *testing.T,
	h *verifiedpermissions.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "VerifiedPermissions."+target)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestVPHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	assert.Equal(t, "VerifiedPermissions", h.Name())
}

func TestVPHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreatePolicyStore")
	assert.Contains(t, ops, "GetPolicyStore")
	assert.Contains(t, ops, "ListPolicyStores")
	assert.Contains(t, ops, "UpdatePolicyStore")
	assert.Contains(t, ops, "DeletePolicyStore")
	assert.Contains(t, ops, "CreatePolicy")
	assert.Contains(t, ops, "GetPolicy")
	assert.Contains(t, ops, "ListPolicies")
	assert.Contains(t, ops, "UpdatePolicy")
	assert.Contains(t, ops, "DeletePolicy")
	assert.Contains(t, ops, "CreatePolicyTemplate")
	assert.Contains(t, ops, "GetPolicyTemplate")
	assert.Contains(t, ops, "ListPolicyTemplates")
	assert.Contains(t, ops, "UpdatePolicyTemplate")
	assert.Contains(t, ops, "DeletePolicyTemplate")
}

func TestVPHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matches VerifiedPermissions target",
			target: "VerifiedPermissions.CreatePolicyStore",
			want:   true,
		},
		{
			name:   "does not match wrong prefix",
			target: "TransferService.CreateServer",
			want:   false,
		},
		{
			name:   "empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestVPHandler_CreatePolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name:     "create with description",
			body:     map[string]any{"description": "My test store"},
			wantCode: http.StatusOK,
			wantKey:  "policyStoreId",
		},
		{
			name:     "create without description",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			wantKey:  "policyStoreId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			rec := doVPRequest(t, h, "CreatePolicyStore", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, tt.wantKey)
		})
	}
}

func TestVPHandler_GetPolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "get existing store",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["policyStoreId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "get non-existent store",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "nonexistent-id"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			id := tt.setup(t, h)

			rec := doVPRequest(t, h, "GetPolicyStore", map[string]any{"policyStoreId": id})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_ListPolicyStores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numStores int
		wantCode  int
	}{
		{
			name:      "list empty",
			numStores: 0,
			wantCode:  http.StatusOK,
		},
		{
			name:      "list with stores",
			numStores: 2,
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)

			for range tt.numStores {
				doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
			}

			rec := doVPRequest(t, h, "ListPolicyStores", map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			stores := resp["policyStores"].([]any)
			assert.Len(t, stores, tt.numStores)
		})
	}
}

func TestVPHandler_DeletePolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "delete existing",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["policyStoreId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete non-existent",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "nonexistent-id"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			id := tt.setup(t, h)

			rec := doVPRequest(t, h, "DeletePolicyStore", map[string]any{"policyStoreId": id})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_PolicyCRUD(t *testing.T) {
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
			rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
			require.Equal(t, http.StatusOK, rec.Code)

			var storeResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
			storeID := storeResp["policyStoreId"].(string)

			// Create policy
			rec = doVPRequest(t, h, "CreatePolicy", map[string]any{
				"policyStoreId": storeID,
				"definition": map[string]any{
					"static": map[string]any{
						"statement": "permit(principal, action, resource);",
					},
				},
			})
			require.Equal(t, tt.wantCode, rec.Code)

			var policyResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &policyResp))
			policyID := policyResp["policyId"].(string)
			assert.NotEmpty(t, policyID)

			// Get policy
			rec = doVPRequest(t, h, "GetPolicy", map[string]any{
				"policyStoreId": storeID,
				"policyId":      policyID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// List policies
			rec = doVPRequest(t, h, "ListPolicies", map[string]any{
				"policyStoreId": storeID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Update policy
			rec = doVPRequest(t, h, "UpdatePolicy", map[string]any{
				"policyStoreId": storeID,
				"policyId":      policyID,
				"definition": map[string]any{
					"static": map[string]any{
						"statement": "forbid(principal, action, resource);",
					},
				},
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete policy
			rec = doVPRequest(t, h, "DeletePolicy", map[string]any{
				"policyStoreId": storeID,
				"policyId":      policyID,
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

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
			rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
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

func TestVPHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	rec := doVPRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "valid target",
			target: "VerifiedPermissions.CreatePolicyStore",
			want:   "CreatePolicyStore",
		},
		{
			name:   "empty target",
			target: "",
			want:   "Unknown",
		},
		{
			name:   "wrong prefix",
			target: "SomeOther.Operation",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			h := newTestVPHandler(t)
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestVPHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		want string
	}{
		{
			name: "policy store and policy id",
			body: map[string]any{"policyStoreId": "store-1", "policyId": "policy-1"},
			want: "store-1/policy-1",
		},
		{
			name: "policy store and template id",
			body: map[string]any{"policyStoreId": "store-1", "policyTemplateId": "tpl-1"},
			want: "store-1/tpl-1",
		},
		{
			name: "policy store id only",
			body: map[string]any{"policyStoreId": "store-1"},
			want: "store-1",
		},
		{
			name: "empty body",
			body: map[string]any{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			h := newTestVPHandler(t)
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestVPHandler_UpdatePolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "update existing store",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "original"})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["policyStoreId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "update non-existent store",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "nonexistent-id"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing policyStoreId",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			id := tt.setup(t, h)

			rec := doVPRequest(t, h, "UpdatePolicyStore", map[string]any{
				"policyStoreId": id,
				"description":   "updated",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_PolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "create policy missing store id",
			action:   "CreatePolicy",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "create policy template linked",
			action: "CreatePolicy",
			body: map[string]any{
				"policyStoreId": "",
				"definition": map[string]any{
					"templateLinked": map[string]any{"policyTemplateId": "tpl-1"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get policy missing store id",
			action:   "GetPolicy",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get policy missing policy id",
			action:   "GetPolicy",
			body:     map[string]any{"policyStoreId": "store-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list policies missing store id",
			action:   "ListPolicies",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update policy missing store id",
			action:   "UpdatePolicy",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update policy missing policy id",
			action:   "UpdatePolicy",
			body:     map[string]any{"policyStoreId": "store-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete policy missing store id",
			action:   "DeletePolicy",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete policy missing policy id",
			action:   "DeletePolicy",
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
