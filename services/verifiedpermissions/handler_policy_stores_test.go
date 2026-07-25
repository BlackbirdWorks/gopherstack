package verifiedpermissions_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestVPHandler_DeletePolicyStore_DeletionProtection_ConflictException(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	createRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
		"deletionProtection": "ENABLED",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	storeID := createResp["policyStoreId"].(string)

	rec := doVPRequest(t, h, "DeletePolicyStore", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ConflictException", resp["__type"])
}

// TestVPHandler_TagResource_TooManyTags verifies exceeding the 50-tag limit
// via TagResource surfaces the real SDK's TooManyTagsException wire type.

func TestVPHandler_CreatePolicyStore_DescriptionBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		descLen  int
		wantCode int
	}{
		{name: "at_bound_ok", descLen: 150, wantCode: http.StatusOK},
		{name: "over_bound_rejected", descLen: 151, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
				"validationSettings": map[string]any{"mode": "OFF"},
				"description":        strings.Repeat("d", tt.descLen),
			})

			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestVPHandler_CreatePolicyStore_WireShape locks in a wire-shape bug fix:
// the real SDK's CreatePolicyStoreOutput has no validationSettings field at
// all (only arn/createdDate/lastUpdatedDate/policyStoreId) -- gopherstack
// previously echoed the input validationSettings back, a field the real
// client-side deserializer never expects here.
func TestVPHandler_CreatePolicyStore_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotContains(t, resp, "validationSettings")
	assert.NotEmpty(t, resp["policyStoreId"])
	assert.NotEmpty(t, resp["arn"])
	assert.NotEmpty(t, resp["createdDate"])
	assert.NotEmpty(t, resp["lastUpdatedDate"])
}

// TestVPHandler_GetPolicyStore_CedarVersion verifies GetPolicyStore always
// populates the optional cedarVersion field (Amazon Verified Permissions'
// Cedar v4 FAQ) -- gopherstack's cedar-go evaluation engine implements
// Cedar 4, so every policy store reports CEDAR_4.
func TestVPHandler_GetPolicyStore_CedarVersion(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "GetPolicyStore", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "CEDAR_4", resp["cedarVersion"])
}

// TestVPHandler_ListPolicyStores_WireShape locks in a wire-shape bug fix:
// the real SDK's PolicyStoreItem (ListPolicyStores) is a leaner shape than
// GetPolicyStoreOutput -- no validationSettings or deletionProtection --
// gopherstack previously echoed both, fields the real item type doesn't
// declare at all.
func TestVPHandler_ListPolicyStores_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "ListPolicyStores", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, _ := resp["policyStores"].([]any)
	require.NotEmpty(t, items)
	item, _ := items[0].(map[string]any)
	assert.NotContains(t, item, "validationSettings")
	assert.NotContains(t, item, "deletionProtection")
	assert.NotContains(t, item, "cedarVersion")
}

// TestVPHandler_UpdatePolicyStore_WireShape locks in a wire-shape bug fix:
// the real SDK's UpdatePolicyStoreOutput requires createdDate (the store
// already existed) and, like CreatePolicyStoreOutput, has no
// validationSettings field -- gopherstack previously omitted createdDate
// (a required field) and echoed validationSettings (an invented one).
func TestVPHandler_UpdatePolicyStore_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	rec := doVPRequest(t, h, "UpdatePolicyStore", map[string]any{
		"policyStoreId": storeID,
		"description":   "updated",
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotContains(t, resp, "validationSettings")
	assert.NotEmpty(t, resp["createdDate"], "createdDate is a required UpdatePolicyStoreOutput field")
	assert.NotEmpty(t, resp["lastUpdatedDate"])
	assert.Equal(t, storeID, resp["policyStoreId"])
}

// TestVPHandler_CreatePolicyStore_ClientTokenIdempotency verifies
// CreatePolicyStore's ClientToken idempotency semantics documented on
// CreatePolicyStoreInput.ClientToken: a retry with the same token and the
// same parameters replays the original policy store (no duplicate created);
// a retry with the same token but different parameters fails with
// ConflictException.
func TestVPHandler_CreatePolicyStore_ClientTokenIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	body := func(description string) map[string]any {
		return map[string]any{
			"validationSettings": map[string]any{"mode": "OFF"},
			"description":        description,
			"clientToken":        "fixed-token",
		}
	}

	rec1 := doVPRequest(t, h, "CreatePolicyStore", body("first"))
	require.Equal(t, http.StatusOK, rec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	// Same token, same parameters: replays the original policy store.
	rec2 := doVPRequest(t, h, "CreatePolicyStore", body("first"))
	require.Equal(t, http.StatusOK, rec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	assert.Equal(t, resp1["policyStoreId"], resp2["policyStoreId"])
	assert.Equal(t, resp1["createdDate"], resp2["createdDate"])

	// Same token, different parameters: ConflictException.
	rec3 := doVPRequest(t, h, "CreatePolicyStore", body("different"))
	assert.Equal(t, http.StatusBadRequest, rec3.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &errResp))
	assert.Equal(t, "ConflictException", errResp["__type"])

	// A different token creates a genuinely new policy store.
	rec4 := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
		"description":        "first",
		"clientToken":        "another-token",
	})
	require.Equal(t, http.StatusOK, rec4.Code)
	var resp4 map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &resp4))
	assert.NotEqual(t, resp1["policyStoreId"], resp4["policyStoreId"])
}
