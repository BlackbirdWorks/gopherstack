package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrganization(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "DescribeOrganizationConfiguration returns defaults",
			steps: []step{
				{
					name:   "describe",
					method: http.MethodGet,
					path:   "/organization/configuration",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["AutoEnableStandards"])
					},
				},
			},
		},
		{
			name: "UpdateOrganizationConfiguration and read back",
			steps: []step{
				{
					name:   "update",
					method: http.MethodPost,
					path:   "/organization/configuration",
					body: map[string]any{
						"AutoEnable":          true,
						"AutoEnableStandards": "DEFAULT",
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "describe updated",
					method: http.MethodGet,
					path:   "/organization/configuration",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, true, resp["AutoEnable"])
						assert.Equal(t, "DEFAULT", resp["AutoEnableStandards"])
					},
				},
			},
		},
		{
			name: "EnableDisableOrganizationAdminAccount and ListOrganizationAdminAccounts",
			steps: []step{
				{
					name:   "enable admin",
					method: http.MethodPost,
					path:   "/organization/admin/enable",
					body:   map[string]any{"AdminAccountId": "777777777777"},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "list admin accounts",
					method: http.MethodGet,
					path:   "/organization/admin",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						accounts, _ := resp["AdminAccounts"].([]any)
						assert.Len(t, accounts, 1)
						a, _ := accounts[0].(map[string]any)
						assert.Equal(t, "777777777777", a["AccountId"])
						assert.Equal(t, "ENABLED", a["Status"])
					},
				},
				{
					name:   "disable admin",
					method: http.MethodPost,
					path:   "/organization/admin/disable",
					body:   map[string]any{"AdminAccountId": "777777777777"},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "list after disable is empty",
					method: http.MethodGet,
					path:   "/organization/admin",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						accounts, _ := resp["AdminAccounts"].([]any)
						assert.Empty(t, accounts)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}
