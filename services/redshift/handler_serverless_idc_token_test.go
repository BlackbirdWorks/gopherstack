package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerless_GetIdentityCenterAuthToken(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "idc-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateWorkgroup", map[string]any{
		"workgroupName": "idc-wg", "namespaceName": "idc-ns",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetIdentityCenterAuthToken", map[string]any{
		"workgroupNames": []string{"idc-wg"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	token1, _ := resp["token"].(string)
	require.NotEmpty(t, token1)
	require.NotEmpty(t, resp["expirationTime"])

	rec = doServerlessOp(t, h, "GetIdentityCenterAuthToken", map[string]any{
		"workgroupNames": []string{"idc-wg"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp2 map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp2))
	token2, _ := resp2["token"].(string)
	require.NotEmpty(t, token2)

	assert.NotEqual(t, token1, token2, "each call must mint a fresh token")
}

func TestServerless_GetIdentityCenterAuthToken_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing workgroup names",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "empty workgroup names",
			body:       map[string]any{"workgroupNames": []string{}},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "unknown workgroup",
			body:       map[string]any{"workgroupNames": []string{"no-such-wg"}},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, "GetIdentityCenterAuthToken", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}

// TestServerless_GetIdentityCenterAuthToken_OneUnknownAmongMany proves every
// named workgroup is validated, not just the first.
func TestServerless_GetIdentityCenterAuthToken_OneUnknownAmongMany(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "idc-ns2"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateWorkgroup", map[string]any{
		"workgroupName": "idc-wg2", "namespaceName": "idc-ns2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "GetIdentityCenterAuthToken", map[string]any{
		"workgroupNames": []string{"idc-wg2", "no-such-wg"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
}
