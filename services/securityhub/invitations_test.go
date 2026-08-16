package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

func TestInvitations(t *testing.T) {
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
			name: "AcceptAdministratorInvitation and GetAdministratorAccount",
			steps: []step{
				{
					name:   "accept",
					method: http.MethodPost,
					path:   "/administrator",
					body:   map[string]any{"AdministratorId": "999999999999", "InvitationId": "inv-001"},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get admin account",
					method: http.MethodGet,
					path:   "/administrator",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						admin, _ := resp["Administrator"].(map[string]any)
						assert.Equal(t, "999999999999", admin["AccountId"])
						assert.Equal(t, "inv-001", admin["InvitationId"])
					},
				},
				{
					name:   "disassociate from admin",
					method: http.MethodPost,
					path:   "/administrator/disassociate",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after disassociate returns nil",
					method: http.MethodGet,
					path:   "/administrator",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Nil(t, resp["Administrator"])
					},
				},
			},
		},
		{
			name: "AcceptInvitation and GetMasterAccount",
			steps: []step{
				{
					name:   "accept via master endpoint",
					method: http.MethodPost,
					path:   "/master",
					body:   map[string]any{"MasterId": "888888888888", "InvitationId": "inv-002"},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get master account",
					method: http.MethodGet,
					path:   "/master",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						master, _ := resp["Master"].(map[string]any)
						assert.Equal(t, "888888888888", master["AccountId"])
					},
				},
				{
					name:   "disassociate from master",
					method: http.MethodPost,
					path:   "/master/disassociate",
					body:   nil,
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
			},
		},
		{
			name: "GetInvitationsCount and ListInvitations",
			steps: []step{
				{
					name:   "create members to invite",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "555555555555", "Email": "e@example.com"},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "invite members",
					method: http.MethodPost,
					path:   "/members/invite",
					body:   map[string]any{"AccountIds": []any{"555555555555"}},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get invitations count",
					method: http.MethodGet,
					path:   "/invitations/count",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						count, _ := resp["InvitationsCount"].(float64)
						assert.Positive(t, int(count))
					},
				},
				{
					name:   "list invitations",
					method: http.MethodGet,
					path:   "/invitations",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						invs, _ := resp["Invitations"].([]any)
						assert.NotEmpty(t, invs)
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

func TestBackend_DeclineInvitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		preInvite    bool
		wantDeclined int
		wantUnproc   int
	}{
		{
			name:         "decline existing invitation",
			preInvite:    true,
			wantDeclined: 1,
			wantUnproc:   0,
		},
		{
			name:         "decline non-existent invitation",
			preInvite:    false,
			wantDeclined: 0,
			wantUnproc:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			if tc.preInvite {
				_, _ = b.CreateMembers([]map[string]any{
					{"AccountId": "111111111111", "Email": "a@test.com"},
				})
				b.InviteMembers([]string{"111111111111"})
			}

			declined, unprocessed := b.DeclineInvitations([]string{"111111111111"})
			assert.Len(t, declined, tc.wantDeclined)
			assert.Len(t, unprocessed, tc.wantUnproc)
		})
	}
}

func TestBackend_DeleteInvitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		preInvite  bool
		wantDel    int
		wantUnproc int
	}{
		{
			name:       "delete existing invitation",
			preInvite:  true,
			wantDel:    1,
			wantUnproc: 0,
		},
		{
			name:       "delete non-existent invitation",
			preInvite:  false,
			wantDel:    0,
			wantUnproc: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			if tc.preInvite {
				_, _ = b.CreateMembers([]map[string]any{
					{"AccountId": "222222222222", "Email": "b@test.com"},
				})
				b.InviteMembers([]string{"222222222222"})
			}

			deleted, unprocessed := b.DeleteInvitations([]string{"222222222222"})
			assert.Len(t, deleted, tc.wantDel)
			assert.Len(t, unprocessed, tc.wantUnproc)
		})
	}
}

func TestHandler_DeclineDeleteInvitations(t *testing.T) {
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
			name: "decline invitations",
			steps: []step{
				{
					name:   "create member",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "999999000001", "Email": "inv@test.com"},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "invite member",
					method: http.MethodPost,
					path:   "/members/invite",
					body:   map[string]any{"AccountIds": []any{"999999000001"}},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "decline invitation",
					method: http.MethodPost,
					path:   "/invitations/decline",
					body:   map[string]any{"AccountIds": []any{"999999000001"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						unproc, _ := resp["UnprocessedAccounts"].([]any)
						assert.Empty(t, unproc)
					},
				},
			},
		},
		{
			name: "delete invitations",
			steps: []step{
				{
					name:   "create member",
					method: http.MethodPost,
					path:   "/members",
					body: map[string]any{
						"AccountDetails": []any{
							map[string]any{"AccountId": "999999000002", "Email": "del@test.com"},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "invite member",
					method: http.MethodPost,
					path:   "/members/invite",
					body:   map[string]any{"AccountIds": []any{"999999000002"}},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "delete invitation",
					method: http.MethodPost,
					path:   "/invitations/delete",
					body:   map[string]any{"AccountIds": []any{"999999000002"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						unproc, _ := resp["UnprocessedAccounts"].([]any)
						assert.Empty(t, unproc)
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
