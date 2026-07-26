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

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			h2 := newTestVPHandler(t)
			require.NoError(t, h2.Restore(t.Context(), snap))

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
		"CreatePolicyStoreAlias",
		"GetPolicyStoreAlias",
		"ListPolicyStoreAliases",
		"DeletePolicyStoreAlias",
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
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore to new handler
	h2 := newTestVPHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

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

// TestVPHandler_CreateIdentitySource_OIDCIdentityTokenClientIDs verifies the
// identityTokenOnly wire field is "clientIds", not "audiences" (the real SDK
// uses different field names for identityTokenOnly vs accessTokenOnly, but
// both round-trip through the same internal representation here).

func TestVPHandler_Reset(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, _ = b.CreatePolicyStore("store", nil, "OFF", "", "")

	require.Equal(t, 1, verifiedpermissions.PolicyStoreCount(b))

	h := verifiedpermissions.NewHandler(b)
	h.Reset()

	assert.Equal(t, 0, verifiedpermissions.PolicyStoreCount(b))
}

func TestVPProvider_InitNilContext(t *testing.T) {
	t.Parallel()

	p := &verifiedpermissions.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, verifiedpermissions.ErrNilAppContext)
}

func TestVPHandler_HandleError_ValidationException(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Passing raw malformed JSON triggers a JSON syntax error → ValidationException (400).
	rec := doVPRequestRaw(t, h, "GetPolicyStore", []byte("{bad json}"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

func TestVPHandler_OpsLength(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := verifiedpermissions.NewHandler(b)
	ops := h.GetSupportedOperations()

	assert.GreaterOrEqual(t, len(ops), 30)

	// Verify all new ops are present.
	for _, op := range []string{"IsAuthorized", "IsAuthorizedWithToken", "UpdateIdentitySource"} {
		assert.Contains(t, ops, op)
	}
}

func TestVPHandler_UnknownOp_ReturnsUnknownOperationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
	}{
		{name: "completely_unknown_op", target: "FrobnicatePolicy"},
		{name: "misspelled_op", target: "CreatPolicyStore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			rec := doVPRequest(t, h, tt.target, map[string]any{})

			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "UnknownOperationException", resp["__type"], "body: %s", rec.Body.String())
		})
	}
}
