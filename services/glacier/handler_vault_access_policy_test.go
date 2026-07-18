package glacier_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccessPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		policyBody string
	}{
		{
			name:       "set_get_delete",
			vaultName:  "policy-vault",
			policyBody: `{"Policy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Set access policy
			rec = doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName+"/access-policy", tt.policyBody)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get access policy
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/access-policy", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete access policy
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName+"/access-policy", "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete = 404
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/access-policy", "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestAccessPolicy_SetGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "iam_policy_roundtrip", policy: `{"Version":"2012-10-17","Statement":[]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "access-policy-vault")

			// GET before set → 404.
			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/access-policy-vault/access-policy", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// SET.
			body := `{"Policy":"` + strings.ReplaceAll(tt.policy, `"`, `\"`) + `"}`
			rec = doRequestWithHeaders(t, h, http.MethodPut,
				"/"+testAccountID+"/vaults/access-policy-vault/access-policy", body, nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GET.
			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/access-policy-vault/access-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var policyResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &policyResp))
			assert.NotEmpty(t, policyResp["Policy"])

			// DELETE.
			rec = doRequestWithHeaders(t, h, http.MethodDelete,
				"/"+testAccountID+"/vaults/access-policy-vault/access-policy", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GET after delete → 404.
			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/access-policy-vault/access-policy", "", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 18. DescribeJob response fidelity
// ─────────────────────────────────────────────────────────────────────────────
