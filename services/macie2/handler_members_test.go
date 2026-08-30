package macie2_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "create_get_list_delete",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// CreateMember
				rec := doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{
						"accountId": "111111111111",
						"email":     "member@example.com",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetMember
				rec = doRequest(t, h, http.MethodGet, "/members/111111111111", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getMem map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getMem))
				assert.Equal(t, "111111111111", getMem["accountId"])
				assert.Equal(t, "member@example.com", getMem["email"])
				assert.Equal(t, "Created", getMem["relationshipStatus"])
				// Real GetMemberOutput always includes arn and masterAccountId
				// (the deprecated wire name for administratorAccountId).
				assert.Contains(t, getMem["arn"], "arn:aws:macie2:")
				assert.NotEmpty(t, getMem["masterAccountId"])

				// ListMembers
				rec = doRequest(t, h, http.MethodGet, "/members", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				members, _ := listResp["members"].([]any)
				assert.Len(t, members, 1)

				// DeleteMember
				rec = doRequest(t, h, http.MethodDelete, "/members/111111111111", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = doRequest(t, h, http.MethodGet, "/members", nil)
				var afterList map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterList))
				membersAfter, _ := afterList["members"].([]any)
				assert.Empty(t, membersAfter)
			},
		},
		{
			name: "disassociate_member",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{
						"accountId": "222222222222",
						"email":     "other@example.com",
					},
				})

				// DisassociateMember
				rec := doRequest(t, h, http.MethodPost, "/members/disassociate/222222222222", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify status changed
				rec = doRequest(t, h, http.MethodGet, "/members/222222222222", nil)
				var mem map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mem))
				assert.Equal(t, "DISASSOCIATED", mem["relationshipStatus"])
			},
		},
		{
			name: "update_member_session",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{
						"accountId": "333333333333",
						"email":     "session@example.com",
					},
				})

				// UpdateMemberSession via /macie/members/{id}
				rec := doRequest(t, h, http.MethodPatch, "/macie/members/333333333333", map[string]any{
					"status": "PAUSED",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/members/333333333333", nil)
				var mem map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mem))
				assert.Equal(t, "PAUSED", mem["relationshipStatus"])
			},
		},
		{
			name: "duplicate_member_returns_conflict",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{"accountId": "444444444444", "email": "dup@example.com"},
				})

				rec := doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{"accountId": "444444444444", "email": "dup@example.com"},
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "get_missing_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/members/999999999999", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
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

// TestListMembersOnlyAssociatedAndPagination locks the ListMembers gap fix:
// onlyAssociated must default to true (hiding DISASSOCIATED members) and
// honor "false" to include them, and maxResults/nextToken must actually
// page results instead of always returning every member in one page.
func TestListMembersOnlyAssociatedAndPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/members", map[string]any{
		"account": map[string]string{"accountId": "111111111111", "email": "a@example.com"},
	})
	doRequest(t, h, http.MethodPost, "/members", map[string]any{
		"account": map[string]string{"accountId": "222222222222", "email": "b@example.com"},
	})

	disassociateRec := doRequest(t, h, http.MethodPost, "/members/disassociate/222222222222", nil)
	require.Equal(t, http.StatusOK, disassociateRec.Code)

	t.Run("default onlyAssociated=true hides disassociated members", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodGet, "/members", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		members, ok := resp["members"].([]any)
		require.True(t, ok)
		require.Len(t, members, 1)

		member, ok := members[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "111111111111", member["accountId"])
	})

	t.Run("onlyAssociated=false includes disassociated members", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodGet, "/members?onlyAssociated=false", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		members, ok := resp["members"].([]any)
		require.True(t, ok)
		assert.Len(t, members, 2)
	})

	t.Run("maxResults paginates and nextToken advances", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodGet, "/members?onlyAssociated=false&maxResults=1", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		members, ok := resp["members"].([]any)
		require.True(t, ok)
		require.Len(t, members, 1)
		require.NotEmpty(t, resp["nextToken"])

		nextToken, ok := resp["nextToken"].(string)
		require.True(t, ok)

		rec2 := doRequest(
			t, h, http.MethodGet,
			"/members?onlyAssociated=false&maxResults=1&nextToken="+url.QueryEscape(nextToken), nil,
		)
		require.Equal(t, http.StatusOK, rec2.Code)

		var resp2 map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
		members2, ok := resp2["members"].([]any)
		require.True(t, ok)
		require.Len(t, members2, 1)
		assert.Empty(t, resp2["nextToken"])
	})
}

// --- invitations ---

func TestInvitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "create_list_count",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// CreateInvitations
				rec := doRequest(t, h, http.MethodPost, "/invitations", map[string]any{
					"accountIds": []string{"111111111111", "222222222222"},
					"message":    "Join Macie",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetInvitationsCount
				rec = doRequest(t, h, http.MethodGet, "/invitations/count", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var countResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &countResp))
				count, _ := countResp["invitationsCount"].(float64)
				assert.InDelta(t, 2, count, 0.0001)

				// ListInvitations
				rec = doRequest(t, h, http.MethodGet, "/invitations", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				invitations, _ := listResp["invitations"].([]any)
				assert.Len(t, invitations, 2)
			},
		},
		{
			name: "accept_invitation",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/invitations/accept", map[string]any{
					"administratorAccountId": "123456789012",
					"invitationId":           "inv-abc",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify administrator is set
				rec = doRequest(t, h, http.MethodGet, "/administrator", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var adminResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &adminResp))
				admin, _ := adminResp["administrator"].(map[string]any)
				assert.Equal(t, "123456789012", admin["accountId"])
			},
		},
		{
			name: "decline_and_delete_invitations",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/invitations", map[string]any{
					"accountIds": []string{"111111111111", "222222222222"},
				})

				// DeclineInvitations
				rec := doRequest(t, h, http.MethodPost, "/invitations/decline", map[string]any{
					"accountIds": []string{"111111111111"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var declineResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &declineResp))
				assert.Empty(t, declineResp["unprocessedAccounts"])

				// DeleteInvitations
				rec = doRequest(t, h, http.MethodPost, "/invitations/delete", map[string]any{
					"accountIds": []string{"222222222222"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
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
