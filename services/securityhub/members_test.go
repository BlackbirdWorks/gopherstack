package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMembers(t *testing.T) {
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
			name: "CreateMembers returns empty unprocessed on success",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "111111111111", "Email": "a@example.com"},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, []any{}, resp["UnprocessedAccounts"])
					},
				},
			},
		},
		{
			name: "CreateDeleteGetMembers full cycle",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "222222222222", "Email": "b@example.com"},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) { //nolint:revive // existing issue.
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"AccountIds": []any{"222222222222"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						members, _ := resp["Members"].([]any)
						require.Len(t, members, 1)
						m, _ := members[0].(map[string]any)
						assert.Equal(t, "222222222222", m["AccountId"])
					},
				},
				{
					name:   "delete",
					method: http.MethodPost,
					path:   "/members/delete",
					body:   map[string]any{"AccountIds": []any{"222222222222"}},
					check: func(t *testing.T, code int, resp map[string]any) { //nolint:revive // existing issue.
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after delete returns unprocessed",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"AccountIds": []any{"222222222222"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						members, _ := resp["Members"].([]any)
						assert.Empty(t, members)
						unprocessed, _ := resp["UnprocessedAccounts"].([]any)
						assert.Len(t, unprocessed, 1)
					},
				},
			},
		},
		{
			name: "InviteListMembers cycle",
			steps: []step{
				{
					name:   "create first",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "333333333333", "Email": "c@example.com"},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) { //nolint:revive // existing issue.
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "invite",
					method: http.MethodPost,
					path:   "/members/invite",
					body:   map[string]any{"AccountIds": []any{"333333333333"}},
					check: func(t *testing.T, code int, resp map[string]any) { //nolint:revive // existing issue.
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/members",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						members, _ := resp["Members"].([]any)
						assert.NotEmpty(t, members)
					},
				},
			},
		},
		{
			name: "DisassociateMembers sets status Removed",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "444444444444", "Email": "d@example.com"},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "disassociate",
					method: http.MethodPost,
					path:   "/members/disassociate",
					body:   map[string]any{"AccountIds": []any{"444444444444"}},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get shows Removed status",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"AccountIds": []any{"444444444444"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						members, _ := resp["Members"].([]any)
						require.Len(t, members, 1)
						m, _ := members[0].(map[string]any)
						assert.Equal(t, "Removed", m["MemberStatus"])
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

// TestParity_InviteMembers_UnknownAccountUnprocessed verifies InviteMembers
// rejects account IDs that were never created via CreateMembers instead of
// silently succeeding for every account ID, matching real AWS behavior
// (InviteMembers requires a prior CreateMembers call).
func TestInviteMembers_UnknownAccountUnprocessed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantUnproc    int
		preCreate     bool
		wantListedMem bool
	}{
		{name: "known_member_invited", preCreate: true, wantUnproc: 0, wantListedMem: true},
		{name: "unknown_account_unprocessed", preCreate: false, wantUnproc: 1, wantListedMem: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			const accountID = "222222222222"

			if tc.preCreate {
				createRec := doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"AccountDetails": []any{
						map[string]any{"AccountId": accountID, "Email": "m@example.com"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
			}

			inviteRec := doRequest(t, h, http.MethodPost, "/members/invite", map[string]any{
				"AccountIds": []any{accountID},
			})
			require.Equal(t, http.StatusOK, inviteRec.Code)

			var inviteResp map[string]any
			require.NoError(t, json.Unmarshal(inviteRec.Body.Bytes(), &inviteResp))

			unprocessed, _ := inviteResp["UnprocessedAccounts"].([]any)
			assert.Len(t, unprocessed, tc.wantUnproc)

			if tc.wantListedMem {
				membersRec := doRequest(t, h, http.MethodGet, "/members", nil)
				require.Equal(t, http.StatusOK, membersRec.Code)

				var membersResp map[string]any
				require.NoError(t, json.Unmarshal(membersRec.Body.Bytes(), &membersResp))

				members, _ := membersResp["Members"].([]any)
				require.Len(t, members, 1)

				m, _ := members[0].(map[string]any)
				assert.Equal(t, "Invited", m["MemberStatus"])
			}
		})
	}
}
