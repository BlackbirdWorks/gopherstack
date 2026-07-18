package identitystore_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListGroupMembershipsPagination verifies ListGroupMemberships MaxResults + NextToken pagination.
func TestListGroupMembershipsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	groupRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "Paginated Membership Group",
	})
	require.Equal(t, http.StatusOK, groupRec.Code)
	groupID := parseResponse(t, groupRec)["GroupId"].(string)

	for i := range 4 {
		uRec := doRequest(t, h, "CreateUser", map[string]any{
			"IdentityStoreId": testStoreID,
			"UserName":        fmt.Sprintf("mem.page.user.%d", i),
		})
		require.Equal(t, http.StatusOK, uRec.Code)
		userID := parseResponse(t, uRec)["UserId"].(string)

		mRec := doRequest(t, h, "CreateGroupMembership", map[string]any{
			"IdentityStoreId": testStoreID,
			"GroupId":         groupID,
			"MemberId":        map[string]any{"UserId": userID},
		})
		require.Equal(t, http.StatusOK, mRec.Code)
	}

	rec1 := doRequest(t, h, "ListGroupMemberships", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"MaxResults":      2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResponse(t, rec1)
	m1, _ := resp1["GroupMemberships"].([]any)
	assert.Len(t, m1, 2)
	token, hasToken := resp1["NextToken"].(string)
	require.True(t, hasToken)

	rec2 := doRequest(t, h, "ListGroupMemberships", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"MaxResults":      2,
		"NextToken":       token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResponse(t, rec2)
	m2, _ := resp2["GroupMemberships"].([]any)
	assert.Len(t, m2, 2)
	assert.Nil(t, resp2["NextToken"])
}

// TestListGroupMembershipsMaxResultsBound verifies ListGroupMemberships validates MaxResults.
func TestListGroupMembershipsMaxResultsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults any
		name       string
		wantStatus int
	}{
		{name: "unset_ok", maxResults: nil, wantStatus: http.StatusOK},
		{name: "in_range_ok", maxResults: 10, wantStatus: http.StatusOK},
		{name: "over_bound_rejected", maxResults: 101, wantStatus: http.StatusBadRequest},
		{name: "zero_as_unset_ok", maxResults: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a group to satisfy the required GroupId.
			createRec := doRequest(t, h, "CreateGroup", map[string]any{
				"IdentityStoreId": testStoreID,
				"DisplayName":     "parity-mb-group-" + tt.name,
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			groupID := parseResponse(t, createRec)["GroupId"].(string)

			body := map[string]any{
				"IdentityStoreId": testStoreID,
				"GroupId":         groupID,
			}
			if tt.maxResults != nil {
				body["MaxResults"] = tt.maxResults
			}

			rec := doRequest(t, h, "ListGroupMemberships", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestListMembershipsForMemberPagination verifies ListGroupMembershipsForMember
// MaxResults + NextToken pagination.
func TestListMembershipsForMemberPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	uRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "memforpaged.user",
	})
	require.Equal(t, http.StatusOK, uRec.Code)
	userID := parseResponse(t, uRec)["UserId"].(string)

	for i := range 4 {
		gRec := doRequest(t, h, "CreateGroup", map[string]any{
			"IdentityStoreId": testStoreID,
			"DisplayName":     fmt.Sprintf("ForMember Paged Group %d", i),
		})
		require.Equal(t, http.StatusOK, gRec.Code)
		groupID := parseResponse(t, gRec)["GroupId"].(string)

		mRec := doRequest(t, h, "CreateGroupMembership", map[string]any{
			"IdentityStoreId": testStoreID,
			"GroupId":         groupID,
			"MemberId":        map[string]any{"UserId": userID},
		})
		require.Equal(t, http.StatusOK, mRec.Code)
	}

	rec1 := doRequest(t, h, "ListGroupMembershipsForMember", map[string]any{
		"IdentityStoreId": testStoreID,
		"MemberId":        map[string]any{"UserId": userID},
		"MaxResults":      2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResponse(t, rec1)
	m1, _ := resp1["GroupMemberships"].([]any)
	assert.Len(t, m1, 2)
	token, hasToken := resp1["NextToken"].(string)
	require.True(t, hasToken)

	rec2 := doRequest(t, h, "ListGroupMembershipsForMember", map[string]any{
		"IdentityStoreId": testStoreID,
		"MemberId":        map[string]any{"UserId": userID},
		"MaxResults":      2,
		"NextToken":       token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	m2, _ := parseResponse(t, rec2)["GroupMemberships"].([]any)
	assert.Len(t, m2, 2)
}

// TestListGroupMembershipsForMemberMaxResultsBound verifies ListGroupMembershipsForMember
// validates MaxResults.
func TestListGroupMembershipsForMemberMaxResultsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults any
		name       string
		wantStatus int
	}{
		{name: "unset_ok", maxResults: nil, wantStatus: http.StatusOK},
		{name: "in_range_ok", maxResults: 10, wantStatus: http.StatusOK},
		{name: "over_bound_rejected", maxResults: 200, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a user to satisfy the required MemberId.
			createRec := doRequest(t, h, "CreateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserName":        "parity-lfm-" + tt.name,
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			userID := parseResponse(t, createRec)["UserId"].(string)

			body := map[string]any{
				"IdentityStoreId": testStoreID,
				"MemberId":        map[string]string{"UserId": userID},
			}
			if tt.maxResults != nil {
				body["MaxResults"] = tt.maxResults
			}

			rec := doRequest(t, h, "ListGroupMembershipsForMember", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestIsMemberInGroupsMemberID verifies that IsMemberInGroups results include MemberId.
func TestIsMemberInGroupsMemberID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "result_includes_member_id",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "ismember.user",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "IsMemberGroup",
				})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MemberId":        map[string]any{"UserId": userID},
				})

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        []string{groupID},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				results := parseResponse(t, rec)["Results"].([]any)
				require.Len(t, results, 1)

				result := results[0].(map[string]any)
				assert.Equal(t, groupID, result["GroupId"])
				assert.Equal(t, true, result["MembershipExists"])

				memberID, ok := result["MemberId"].(map[string]any)
				require.True(t, ok, "MemberId should be present in result")
				assert.Equal(t, userID, memberID["UserId"])
			},
		},
		{
			name: "non_member_result_includes_member_id",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "nonmember.user",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "NoMemberGroup",
				})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        []string{groupID},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				results := parseResponse(t, rec)["Results"].([]any)
				require.Len(t, results, 1)

				result := results[0].(map[string]any)
				assert.Equal(t, false, result["MembershipExists"])

				memberID, ok := result["MemberId"].(map[string]any)
				require.True(t, ok, "MemberId should be present even for non-members")
				assert.Equal(t, userID, memberID["UserId"])
			},
		},
		{
			name: "multiple_groups_all_have_member_id",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "multi.member",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				g1Rec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "MultiGroup1",
				})
				g1ID := parseResponse(t, g1Rec)["GroupId"].(string)

				g2Rec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "MultiGroup2",
				})
				g2ID := parseResponse(t, g2Rec)["GroupId"].(string)

				doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         g1ID,
					"MemberId":        map[string]any{"UserId": userID},
				})

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        []string{g1ID, g2ID},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				results := parseResponse(t, rec)["Results"].([]any)
				require.Len(t, results, 2)

				for _, r := range results {
					result := r.(map[string]any)
					memberID, ok := result["MemberId"].(map[string]any)
					require.True(t, ok, "MemberId should be present for every result")
					assert.Equal(t, userID, memberID["UserId"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestIsMemberInGroupsValidation verifies empty GroupIds returns a validation error.
func TestIsMemberInGroupsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		groupID string
		wantMsg string
	}{
		{
			name:    "empty_group_ids",
			groupID: "",
			wantMsg: "GroupIds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			userRec := doRequest(t, h, "CreateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserName":        "ismember.val",
			})
			require.Equal(t, http.StatusOK, userRec.Code)
			userID := parseResponse(t, userRec)["UserId"].(string)

			rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
				"IdentityStoreId": testStoreID,
				"MemberId":        map[string]any{"UserId": userID},
				"GroupIds":        []string{},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			_ = tt.wantMsg
		})
	}
}

// TestMembershipIDFormat verifies that generated membership IDs are UUID format.
func TestMembershipIDFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	userRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "uuid.member",
	})
	userID := parseResponse(t, userRec)["UserId"].(string)

	groupRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "UUIDMembership",
	})
	groupID := parseResponse(t, groupRec)["GroupId"].(string)

	rec := doRequest(t, h, "CreateGroupMembership", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"MemberId":        map[string]any{"UserId": userID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	membershipID := parseResponse(t, rec)["MembershipId"].(string)
	assert.Len(t, membershipID, 36, "MembershipID should be UUID format (36 chars)")
}

// TestMembershipListSorting verifies ListGroupMemberships and ListGroupMembershipsForMember
// return deterministic sorted results.
func TestMembershipListSorting(t *testing.T) {
	t.Parallel()

	t.Run("list_group_memberships_is_deterministic", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler()

		groupRec := doRequest(t, h, "CreateGroup", map[string]any{
			"IdentityStoreId": testStoreID,
			"DisplayName":     "SortMemberships",
		})
		groupID := parseResponse(t, groupRec)["GroupId"].(string)

		for i := range 5 {
			userRec := doRequest(t, h, "CreateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserName":        fmt.Sprintf("member%d", i),
			})
			userID := parseResponse(t, userRec)["UserId"].(string)
			doRequest(t, h, "CreateGroupMembership", map[string]any{
				"IdentityStoreId": testStoreID,
				"GroupId":         groupID,
				"MemberId":        map[string]any{"UserId": userID},
			})
		}

		rec1 := doRequest(t, h, "ListGroupMemberships", map[string]any{
			"IdentityStoreId": testStoreID,
			"GroupId":         groupID,
		})
		rec2 := doRequest(t, h, "ListGroupMemberships", map[string]any{
			"IdentityStoreId": testStoreID,
			"GroupId":         groupID,
		})

		require.Equal(t, http.StatusOK, rec1.Code)
		require.Equal(t, http.StatusOK, rec2.Code)

		m1 := parseResponse(t, rec1)["GroupMemberships"].([]any)
		m2 := parseResponse(t, rec2)["GroupMemberships"].([]any)

		require.Len(t, m1, 5)
		for i := range m1 {
			id1 := m1[i].(map[string]any)["MembershipId"].(string)
			id2 := m2[i].(map[string]any)["MembershipId"].(string)
			assert.Equal(t, id1, id2, "ListGroupMemberships order must be deterministic")
		}
	})

	t.Run("list_memberships_for_member_is_deterministic", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler()

		userRec := doRequest(t, h, "CreateUser", map[string]any{
			"IdentityStoreId": testStoreID,
			"UserName":        "sort.member",
		})
		userID := parseResponse(t, userRec)["UserId"].(string)

		for i := range 4 {
			groupRec := doRequest(t, h, "CreateGroup", map[string]any{
				"IdentityStoreId": testStoreID,
				"DisplayName":     fmt.Sprintf("SortGroup%d", i),
			})
			groupID := parseResponse(t, groupRec)["GroupId"].(string)
			doRequest(t, h, "CreateGroupMembership", map[string]any{
				"IdentityStoreId": testStoreID,
				"GroupId":         groupID,
				"MemberId":        map[string]any{"UserId": userID},
			})
		}

		rec1 := doRequest(t, h, "ListGroupMembershipsForMember", map[string]any{
			"IdentityStoreId": testStoreID,
			"MemberId":        map[string]any{"UserId": userID},
		})
		rec2 := doRequest(t, h, "ListGroupMembershipsForMember", map[string]any{
			"IdentityStoreId": testStoreID,
			"MemberId":        map[string]any{"UserId": userID},
		})

		require.Equal(t, http.StatusOK, rec1.Code)
		require.Equal(t, http.StatusOK, rec2.Code)

		m1 := parseResponse(t, rec1)["GroupMemberships"].([]any)
		m2 := parseResponse(t, rec2)["GroupMemberships"].([]any)

		require.Len(t, m1, 4)
		for i := range m1 {
			id1 := m1[i].(map[string]any)["MembershipId"].(string)
			id2 := m2[i].(map[string]any)["MembershipId"].(string)
			assert.Equal(t, id1, id2, "ListGroupMembershipsForMember order must be deterministic")
		}
	})
}
