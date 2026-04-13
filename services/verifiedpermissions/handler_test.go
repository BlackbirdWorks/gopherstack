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

				rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
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
					"description": "test",
					"tags":        map[string]any{"env": "prod"},
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
					"description": "test",
					"tags":        map[string]any{"env": "prod"},
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
				doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
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
	rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
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
	rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
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

	rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
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

	rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
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
	rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{"description": "test"})
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
	storeID := storeResp["policyStoreId"].(string)

	// Create template-linked policy
	rec = doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": storeID,
		"definition": map[string]any{
			"templateLinked": map[string]any{
				"policyTemplateId": "some-template-id",
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
		"description": "tagged store",
		"tags":        map[string]any{"env": "prod", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["policyStoreId"])
	assert.NotEmpty(t, resp["arn"])
}
