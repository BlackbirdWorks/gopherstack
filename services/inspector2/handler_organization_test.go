package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelegatedAdminAccountLifecycle(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Enable/Get/Disable/List full cycle",
			steps: []step{
				{
					name:   "enable",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/enable",
					body:   map[string]any{"delegatedAdminAccountId": "333333333333"},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						assert.Equal(t, "333333333333", resp["delegatedAdminAccountId"])
						assert.Equal(t, "ENABLED", resp["status"])
					},
				},
				{
					name:   "get delegated admin",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/get",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						d, _ := resp["delegatedAdmin"].(map[string]any)
						assert.Equal(t, "333333333333", d["accountId"])
					},
				},
				{
					name:   "list delegated admins",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						admins, _ := resp["delegatedAdminAccounts"].([]any)
						assert.Len(t, admins, 1)
					},
				},
				{
					name:   "disable delegated admin",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/disable",
					body:   map[string]any{"delegatedAdminAccountId": "333333333333"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after disable returns 404",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/get",
					body:   map[string]any{},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
		{
			name: "Duplicate enable returns conflict",
			steps: []step{
				{
					name:   "first enable",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/enable",
					body:   map[string]any{"delegatedAdminAccountId": "444444444444"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "second enable same account",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/enable",
					body:   map[string]any{"delegatedAdminAccountId": "444444444444"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusConflict, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}

func TestOrganizationConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "Describe returns defaults",
			method: http.MethodPost,
			path:   "/organizationconfiguration/describe",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["autoEnable"]
				assert.True(t, ok)
			},
		},
		{
			name:   "Update returns OK",
			method: http.MethodPost,
			path:   "/organizationconfiguration/update",
			body:   map[string]any{"autoEnable": map[string]any{"ec2": true}},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}
