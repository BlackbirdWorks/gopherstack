package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestAdministratorMaster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_administrator_nil_when_none",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/administrator", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Nil(t, resp["administrator"])
			},
		},
		{
			name: "accept_then_disassociate_administrator",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/invitations/accept", map[string]any{
					"administratorAccountId": "admin-account",
					"invitationId":           "inv-1",
				})

				// GetAdministratorAccount
				rec := doRequest(t, h, http.MethodGet, "/administrator", nil)
				var adminResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &adminResp))
				assert.NotNil(t, adminResp["administrator"])

				// DisassociateFromAdministratorAccount
				rec = doRequest(t, h, http.MethodPost, "/administrator/disassociate", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify cleared
				rec = doRequest(t, h, http.MethodGet, "/administrator", nil)
				var afterResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterResp))
				assert.Nil(t, afterResp["administrator"])
			},
		},
		{
			name: "get_master_account_legacy",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/invitations/accept", map[string]any{
					"administratorAccountId": "legacy-admin",
					"invitationId":           "inv-2",
				})

				rec := doRequest(t, h, http.MethodGet, "/master", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				master, _ := resp["master"].(map[string]any)
				assert.Equal(t, "legacy-admin", master["accountId"])
			},
		},
		{
			name: "disassociate_from_master_legacy",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/invitations/accept", map[string]any{
					"administratorAccountId": "legacy-admin",
					"invitationId":           "inv-3",
				})

				rec := doRequest(t, h, http.MethodPost, "/master/disassociate", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/master", nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Nil(t, resp["master"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}
