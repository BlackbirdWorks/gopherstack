package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMembersLifecycle(t *testing.T) {
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
			name: "AssociateMember returns accountId",
			steps: []step{
				{
					name:   "associate",
					method: http.MethodPost,
					path:   "/members/associate",
					body:   map[string]any{"accountId": "111111111111"},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						assert.Equal(t, "111111111111", resp["accountId"])
					},
				},
			},
		},
		{
			name: "Associate/Get/Disassociate full cycle",
			steps: []step{
				{
					name:   "associate",
					method: http.MethodPost,
					path:   "/members/associate",
					body:   map[string]any{"accountId": "222222222222"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after associate",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"accountId": "222222222222"},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						m, _ := resp["member"].(map[string]any)
						assert.Equal(t, "222222222222", m["accountId"])
					},
				},
				{
					name:   "list shows member",
					method: http.MethodPost,
					path:   "/members/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						members, _ := resp["members"].([]any)
						assert.Len(t, members, 1)
					},
				},
				{
					name:   "disassociate",
					method: http.MethodPost,
					path:   "/members/disassociate",
					body:   map[string]any{"accountId": "222222222222"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after disassociate returns 404",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"accountId": "222222222222"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
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
