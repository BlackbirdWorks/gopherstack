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

	"github.com/blackbirdworks/gopherstack/pkgs/service"
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

	return doVPRequestRaw(t, h, target, bodyBytes)
}

func doVPRequestRaw(
	t *testing.T,
	h *verifiedpermissions.Handler,
	target string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
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
			name: "create with description",
			body: map[string]any{
				"description":        "My test store",
				"validationSettings": map[string]any{"mode": "OFF"},
			},
			wantCode: http.StatusOK,
			wantKey:  "policyStoreId",
		},
		{
			name:     "create without description",
			body:     map[string]any{"validationSettings": map[string]any{"mode": "OFF"}},
			wantCode: http.StatusOK,
			wantKey:  "policyStoreId",
		},
		{
			name:     "create without validationSettings",
			body:     map[string]any{"description": "no validation settings"},
			wantCode: http.StatusBadRequest,
			wantKey:  "__type",
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

				rec := doVPRequest(
					t,
					h,
					"CreatePolicyStore",
					map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
				)
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
				doVPRequest(
					t,
					h,
					"CreatePolicyStore",
					map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
				)
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

				rec := doVPRequest(
					t,
					h,
					"CreatePolicyStore",
					map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
				)
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

				rec := doVPRequest(
					t,
					h,
					"CreatePolicyStore",
					map[string]any{"description": "original", "validationSettings": map[string]any{"mode": "OFF"}},
				)
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
			name:   "create policy missing definition",
			action: "CreatePolicy",
			body: map[string]any{
				"policyStoreId": "store-1",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "create policy both static and template linked",
			action: "CreatePolicy",
			body: map[string]any{
				"policyStoreId": "store-1",
				"definition": map[string]any{
					"static":         map[string]any{"statement": "permit(principal, action, resource);"},
					"templateLinked": map[string]any{"policyTemplateId": "tpl-1"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "create policy static empty statement",
			action: "CreatePolicy",
			body: map[string]any{
				"policyStoreId": "store-1",
				"definition": map[string]any{
					"static": map[string]any{"statement": ""},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "create policy template linked empty policy template id",
			action: "CreatePolicy",
			body: map[string]any{
				"policyStoreId": "store-1",
				"definition": map[string]any{
					"templateLinked": map[string]any{"policyTemplateId": ""},
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
			name:   "update policy template linked definition rejected",
			action: "UpdatePolicy",
			body: map[string]any{
				"policyStoreId": "store-1",
				"policyId":      "policy-1",
				"definition": map[string]any{
					"templateLinked": map[string]any{"policyTemplateId": "tpl-1"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "update policy empty static statement",
			action: "UpdatePolicy",
			body: map[string]any{
				"policyStoreId": "store-1",
				"policyId":      "policy-1",
				"definition": map[string]any{
					"static": map[string]any{"statement": ""},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "update policy missing definition",
			action: "UpdatePolicy",
			body: map[string]any{
				"policyStoreId": "store-1",
				"policyId":      "policy-1",
			},
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

func TestVPHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		tags     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "tag existing resource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(
					t,
					h,
					"CreatePolicyStore",
					map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
				)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["arn"].(string)
			},
			tags:     map[string]any{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name: "tag non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/nonexistent"
			},
			tags:     map[string]any{"key": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return ""
			},
			tags:     map[string]any{"key": "value"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			arn := tt.setup(t, h)

			rec := doVPRequest(t, h, "TagResource", map[string]any{
				"resourceArn": arn,
				"tags":        tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		name     string
		tagKeys  []string
		wantCode int
	}{
		{
			name: "untag existing resource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
					"description":        "test",
					"tags":               map[string]any{"env": "prod"},
					"validationSettings": map[string]any{"mode": "OFF"},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["arn"].(string)
			},
			tagKeys:  []string{"env"},
			wantCode: http.StatusOK,
		},
		{
			name: "untag non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/nonexistent"
			},
			tagKeys:  []string{"key"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return ""
			},
			tagKeys:  []string{"key"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			arn := tt.setup(t, h)

			rec := doVPRequest(t, h, "UntagResource", map[string]any{
				"resourceArn": arn,
				"tagKeys":     tt.tagKeys,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		name     string
		wantCode int
		wantTags bool
	}{
		{
			name: "list tags for existing resource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
					"description":        "test",
					"tags":               map[string]any{"env": "prod"},
					"validationSettings": map[string]any{"mode": "OFF"},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["arn"].(string)
			},
			wantCode: http.StatusOK,
			wantTags: true,
		},
		{
			name: "list tags for non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
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
			arn := tt.setup(t, h)

			rec := doVPRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": arn,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantTags {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "tags")
			}
		})
	}
}

func TestVPHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	assert.Equal(t, "verifiedpermissions", h.ChaosServiceName())
}

func TestVPHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	ops := h.ChaosOperations()
	assert.Equal(t, h.GetSupportedOperations(), ops)
}

func TestVPHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	regions := h.ChaosRegions()
	assert.NotEmpty(t, regions)
}

func TestVPHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestVPHandler_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numStores int
	}{
		{name: "empty handler", numStores: 0},
		{name: "handler with data", numStores: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)

			for range tt.numStores {
				doVPRequest(
					t,
					h,
					"CreatePolicyStore",
					map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
				)
			}

			snap := h.Snapshot()
			require.NotNil(t, snap)

			h2 := newTestVPHandler(t)
			require.NoError(t, h2.Restore(snap))

			rec := doVPRequest(t, h2, "ListPolicyStores", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			stores := resp["policyStores"].([]any)
			assert.Len(t, stores, tt.numStores)
		})
	}
}

func TestVPProvider_Name(t *testing.T) {
	t.Parallel()

	p := &verifiedpermissions.Provider{}
	assert.Equal(t, "VerifiedPermissions", p.Name())
}

func TestVPProvider_Init(t *testing.T) {
	t.Parallel()

	p := &verifiedpermissions.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, "VerifiedPermissions", reg.Name())
}

func TestVPHandler_GetPolicyStore_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	rec := doVPRequest(t, h, "GetPolicyStore", map[string]any{"policyStoreId": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_DeletePolicyStore_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	rec := doVPRequest(t, h, "DeletePolicyStore", map[string]any{"policyStoreId": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_UpdatePolicyStore_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	rec := doVPRequest(t, h, "UpdatePolicyStore", map[string]any{
		"policyStoreId": "nonexistent-id",
		"description":   "updated",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestVPHandler_GetPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	rec := doVPRequest(
		t,
		h,
		"CreatePolicyStore",
		map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
	)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
	storeID := storeResp["policyStoreId"].(string)

	rec = doVPRequest(t, h, "GetPolicy", map[string]any{
		"policyStoreId": storeID,
		"policyId":      "nonexistent-policy",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_DeletePolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	rec := doVPRequest(
		t,
		h,
		"CreatePolicyStore",
		map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
	)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
	storeID := storeResp["policyStoreId"].(string)

	rec = doVPRequest(t, h, "DeletePolicy", map[string]any{
		"policyStoreId": storeID,
		"policyId":      "nonexistent-policy",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_CreatePolicy_TemplateLinked(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Create store
	rec := doVPRequest(
		t,
		h,
		"CreatePolicyStore",
		map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
	)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
	storeID := storeResp["policyStoreId"].(string)

	// Create a policy template first.
	tmplRec := doVPRequest(t, h, "CreatePolicyTemplate", map[string]any{
		"policyStoreId": storeID,
		"statement":     "permit(principal == ?principal, action, resource);",
		"description":   "test template",
	})
	require.Equal(t, http.StatusOK, tmplRec.Code)
	var tmplResp map[string]any
	require.NoError(t, json.Unmarshal(tmplRec.Body.Bytes(), &tmplResp))
	templateID := tmplResp["policyTemplateId"].(string)

	// Create template-linked policy
	rec = doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": storeID,
		"definition": map[string]any{
			"templateLinked": map[string]any{
				"policyTemplateId": templateID,
				"principal":        map[string]any{"entityType": "User", "entityId": "alice"},
				"resource":         map[string]any{"entityType": "Document", "entityId": "doc1"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var policyResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &policyResp))
	assert.Equal(t, "TEMPLATE_LINKED", policyResp["policyType"])
}

func TestVPHandler_CreatePolicyStore_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"description":        "tagged store",
		"tags":               map[string]any{"env": "prod", "team": "platform"},
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["policyStoreId"])
	assert.NotEmpty(t, resp["arn"])
}

// createTestPolicyStore is a helper to create a policy store and return its ID.
func createTestPolicyStore(t *testing.T, h *verifiedpermissions.Handler) string {
	t.Helper()

	rec := doVPRequest(
		t,
		h,
		"CreatePolicyStore",
		map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["policyStoreId"].(string)
}

func TestVPHandler_BatchGetPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		wantCode int
	}{
		{
			name: "batch get existing policies",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				rec := doVPRequest(t, h, "CreatePolicy", map[string]any{
					"policyStoreId": storeID,
					"definition": map[string]any{
						"static": map[string]any{"statement": "permit(principal, action, resource);"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var pResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pResp))
				policyID := pResp["policyId"].(string)

				return map[string]any{
					"requests": []any{
						map[string]any{"policyStoreId": storeID, "policyId": policyID},
					},
				}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				results := resp["results"].([]any)
				assert.Len(t, results, 1)
				assert.Empty(t, resp["errors"])
			},
		},
		{
			name: "batch get with missing policy",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"requests": []any{
						map[string]any{"policyStoreId": storeID, "policyId": "nonexistent-policy"},
					},
				}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				errors := resp["errors"].([]any)
				assert.Len(t, errors, 1)
			},
		},
		{
			name: "batch get missing required fields",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) map[string]any {
				return map[string]any{
					"requests": []any{
						map[string]any{"policyStoreId": "", "policyId": ""},
					},
				}
			},
			wantCode: http.StatusBadRequest,
			check:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			body := tt.setup(t, h)

			rec := doVPRequest(t, h, "BatchGetPolicy", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

func TestVPHandler_BatchIsAuthorized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "batch authorization with requests",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"policyStoreId": storeID,
					"requests": []any{
						map[string]any{
							"principal": map[string]any{"entityType": "User", "entityId": "alice"},
							"action":    map[string]any{"actionType": "Action", "actionId": "view"},
							"resource":  map[string]any{"entityType": "Photo", "entityId": "photo1"},
						},
					},
				}
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name: "missing policyStoreId",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) map[string]any {
				return map[string]any{
					"requests": []any{},
				}
			},
			wantCode: http.StatusBadRequest,
			wantLen:  0,
		},
		{
			name: "non-existent policy store",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) map[string]any {
				return map[string]any{
					"policyStoreId": "nonexistent",
					"requests":      []any{},
				}
			},
			wantCode: http.StatusBadRequest,
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			body := tt.setup(t, h)

			rec := doVPRequest(t, h, "BatchIsAuthorized", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantLen > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				results := resp["results"].([]any)
				assert.Len(t, results, tt.wantLen)

				first := results[0].(map[string]any)
				assert.Contains(t, []string{"ALLOW", "DENY"}, first["decision"])
			}
		})
	}
}

func TestVPHandler_BatchIsAuthorizedWithToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) map[string]any
		name     string
		wantCode int
	}{
		{
			name: "with access token",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"policyStoreId": storeID,
					"accessToken":   "fake-access-token",
					"requests": []any{
						map[string]any{
							"action":   map[string]any{"actionType": "Action", "actionId": "view"},
							"resource": map[string]any{"entityType": "Photo", "entityId": "photo1"},
						},
					},
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "with identity token",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"policyStoreId": storeID,
					"identityToken": "fake-identity-token",
					"requests":      []any{},
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing token",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) map[string]any {
				t.Helper()

				storeID := createTestPolicyStore(t, h)

				return map[string]any{
					"policyStoreId": storeID,
					"requests":      []any{},
				}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing policyStoreId",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) map[string]any {
				return map[string]any{
					"accessToken": "fake-token",
					"requests":    []any{},
				}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			body := tt.setup(t, h)

			rec := doVPRequest(t, h, "BatchIsAuthorizedWithToken", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_IdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*testing.T, *verifiedpermissions.Handler) (string, string)
		check     func(*testing.T, map[string]any)
		name      string
		operation string
		wantCode  int
	}{
		{
			name:      "create with Cognito config",
			operation: "CreateIdentitySource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), ""
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				assert.NotEmpty(t, resp["identitySourceId"])
				assert.NotEmpty(t, resp["policyStoreId"])
			},
		},
		{
			name:      "create with OIDC config",
			operation: "CreateIdentitySource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), "oidc"
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				assert.NotEmpty(t, resp["identitySourceId"])
			},
		},
		{
			name:      "create missing policyStoreId",
			operation: "CreateIdentitySource",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) (string, string) {
				return "", ""
			},
			wantCode: http.StatusBadRequest,
			check:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID, configType := tt.setup(t, h)

			var body map[string]any

			switch {
			case storeID == "":
				body = map[string]any{
					"configuration": map[string]any{},
				}
			case configType == "oidc":
				body = map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"openIdConnectConfiguration": map[string]any{
							"issuer": "https://example.com",
						},
					},
				}
			default:
				body = map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
							"clientIds":   []string{"client1"},
						},
					},
				}
			}

			rec := doVPRequest(t, h, tt.operation, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

func TestVPHandler_GetIdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) (string, string)
		name     string
		wantCode int
	}{
		{
			name: "get existing identity source",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				storeID := createTestPolicyStore(t, h)
				rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var isResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &isResp))

				return storeID, isResp["identitySourceId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "get non-existent identity source",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), "nonexistent-id"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing identitySourceId",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID, isID := tt.setup(t, h)

			rec := doVPRequest(t, h, "GetIdentitySource", map[string]any{
				"policyStoreId":    storeID,
				"identitySourceId": isID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_DeleteIdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) (string, string)
		name     string
		wantCode int
	}{
		{
			name: "delete existing identity source",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				storeID := createTestPolicyStore(t, h)
				rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var isResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &isResp))

				return storeID, isResp["identitySourceId"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete non-existent identity source",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, string) {
				t.Helper()

				return createTestPolicyStore(t, h), "nonexistent-id"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID, isID := tt.setup(t, h)

			rec := doVPRequest(t, h, "DeleteIdentitySource", map[string]any{
				"policyStoreId":    storeID,
				"identitySourceId": isID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_ListIdentitySources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numSrcs   int
		wantCode  int
		wantCount int
	}{
		{
			name:      "list empty",
			numSrcs:   0,
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name:      "list with identity sources",
			numSrcs:   2,
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID := createTestPolicyStore(t, h)

			for range tt.numSrcs {
				doVPRequest(t, h, "CreateIdentitySource", map[string]any{
					"policyStoreId":       storeID,
					"principalEntityType": "MyCorp::User",
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
						},
					},
				})
			}

			rec := doVPRequest(t, h, "ListIdentitySources", map[string]any{"policyStoreId": storeID})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			sources := resp["identitySources"].([]any)
			assert.Len(t, sources, tt.wantCount)
		})
	}
}

func TestVPHandler_PutSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "put schema successfully",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				return createTestPolicyStore(t, h)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing policyStoreId",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing cedarJson",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				return createTestPolicyStore(t, h)
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID := tt.setup(t, h)

			var body map[string]any

			switch tt.name {
			case "missing policyStoreId":
				body = map[string]any{
					"definition": map[string]any{"cedarJson": `{"namespace": "MyCorp"}`},
				}
			case "missing cedarJson":
				body = map[string]any{
					"policyStoreId": storeID,
					"definition":    map[string]any{"cedarJson": ""},
				}
			default:
				body = map[string]any{
					"policyStoreId": storeID,
					"definition":    map[string]any{"cedarJson": `{"namespace": "MyCorp"}`},
				}
			}

			rec := doVPRequest(t, h, "PutSchema", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, storeID, resp["policyStoreId"])
				assert.NotEmpty(t, resp["createdDate"])
				assert.NotEmpty(t, resp["lastUpdatedDate"])
			}
		})
	}
}

func TestVPHandler_GetSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) (string, bool)
		name     string
		wantCode int
	}{
		{
			name: "get schema after put",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, bool) {
				t.Helper()

				storeID := createTestPolicyStore(t, h)
				rec := doVPRequest(t, h, "PutSchema", map[string]any{
					"policyStoreId": storeID,
					"definition":    map[string]any{"cedarJson": `{"namespace": "MyCorp"}`},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return storeID, true
			},
			wantCode: http.StatusOK,
		},
		{
			name: "get schema when none exists",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) (string, bool) {
				t.Helper()

				return createTestPolicyStore(t, h), false
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "get schema for non-existent store",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) (string, bool) {
				return "nonexistent-store", false
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing policyStoreId",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) (string, bool) {
				return "", false
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID, hasPut := tt.setup(t, h)

			rec := doVPRequest(t, h, "GetSchema", map[string]any{"policyStoreId": storeID})
			assert.Equal(t, tt.wantCode, rec.Code)

			if hasPut && tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, storeID, resp["policyStoreId"])
				assert.JSONEq(t, `{"namespace": "MyCorp"}`, resp["schema"].(string))
			}
		})
	}
}

func TestVPHandler_NewOperations_InSupportedList(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"BatchGetPolicy",
		"BatchIsAuthorized",
		"BatchIsAuthorizedWithToken",
		"CreateIdentitySource",
		"DeleteIdentitySource",
		"GetIdentitySource",
		"GetSchema",
		"ListIdentitySources",
		"PutSchema",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestVPHandler_Snapshot_Restore_WithNewResources(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	// Create identity source
	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "MyCorp::User",
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var isResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &isResp))
	isID := isResp["identitySourceId"].(string)

	// Put schema
	rec = doVPRequest(t, h, "PutSchema", map[string]any{
		"policyStoreId": storeID,
		"definition":    map[string]any{"cedarJson": `{"ns": "test"}`},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Snapshot
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	// Restore to new handler
	h2 := newTestVPHandler(t)
	require.NoError(t, h2.Restore(snap))

	// Verify identity source persisted
	rec = doVPRequest(t, h2, "GetIdentitySource", map[string]any{
		"policyStoreId":    storeID,
		"identitySourceId": isID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify schema persisted
	rec = doVPRequest(t, h2, "GetSchema", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, rec.Code)

	var schemaResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &schemaResp))
	assert.JSONEq(t, `{"ns": "test"}`, schemaResp["schema"].(string))
}

func TestVPHandler_PutSchema_UpdateSchema(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	// Put schema first time
	rec := doVPRequest(t, h, "PutSchema", map[string]any{
		"policyStoreId": storeID,
		"definition":    map[string]any{"cedarJson": `{"v": 1}`},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update schema
	rec = doVPRequest(t, h, "PutSchema", map[string]any{
		"policyStoreId": storeID,
		"definition":    map[string]any{"cedarJson": `{"v": 2}`},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get updated schema
	rec = doVPRequest(t, h, "GetSchema", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, `{"v": 2}`, resp["schema"])
}

func TestVPHandler_CreateIdentitySource_MissingConfig(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "MyCorp::User",
		"configuration":       map[string]any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_CreateIdentitySource_MissingUserPoolArn(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "MyCorp::User",
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "",
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_BatchGetPolicy_EmptyRequests(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	rec := doVPRequest(t, h, "BatchGetPolicy", map[string]any{
		"requests": []any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["results"])
	assert.Empty(t, resp["errors"])
}

func TestVPHandler_DeletePolicyStore_CascadesIdentitySources(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	// Create identity source
	rec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       storeID,
		"principalEntityType": "MyCorp::User",
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete the policy store
	rec = doVPRequest(t, h, "DeletePolicyStore", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, rec.Code)

	// Try to list identity sources - policy store should be gone
	rec = doVPRequest(t, h, "ListIdentitySources", map[string]any{"policyStoreId": storeID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPHandler_GetSupportedOperations_UpdatedList(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "ListTagsForResource")
}

func TestVPHandler_ServiceSummary(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
	assert.Equal(t, "verifiedpermissions", h.ChaosServiceName())
}
