package verifiedpermissions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
