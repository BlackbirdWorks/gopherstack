package identitystore_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// TestMembershipCRUD exercises CreateGroupMembership, DescribeGroupMembership,
// ListGroupMemberships, and DeleteGroupMembership.
func TestMembershipCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystore.Handler)
		name string
	}{
		{
			name: "create_and_describe_membership",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "member.user",
					"DisplayName":     "Member",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Test Group",
				})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				memberRec := doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MemberId":        map[string]any{"UserId": userID},
				})

				assert.Equal(t, http.StatusOK, memberRec.Code)
				memberResp := parseResponse(t, memberRec)
				membershipID := memberResp["MembershipId"].(string)
				assert.NotEmpty(t, membershipID)

				descRec := doRequest(t, h, "DescribeGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"MembershipId":    membershipID,
				})
				assert.Equal(t, http.StatusOK, descRec.Code)
				descResp := parseResponse(t, descRec)
				assert.Equal(t, membershipID, descResp["MembershipId"])
				assert.Equal(t, groupID, descResp["GroupId"])
			},
		},
		{
			name: "list_group_memberships",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "List Membership Group",
				})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				for i := range 2 {
					userNames := []string{"list.user0", "list.user1"}
					userRec := doRequest(t, h, "CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        userNames[i],
					})
					require.Equal(t, http.StatusOK, userRec.Code)
					userID := parseResponse(t, userRec)["UserId"].(string)

					doRequest(t, h, "CreateGroupMembership", map[string]any{
						"IdentityStoreId": testStoreID,
						"GroupId":         groupID,
						"MemberId":        map[string]any{"UserId": userID},
					})
				}

				rec := doRequest(t, h, "ListGroupMemberships", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				memberships, ok := resp["GroupMemberships"].([]any)
				require.True(t, ok)
				assert.Len(t, memberships, 2)
			},
		},
		{
			name: "delete_membership",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "del.member",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Del Group",
				})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				memberRec := doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MemberId":        map[string]any{"UserId": userID},
				})
				require.Equal(t, http.StatusOK, memberRec.Code)
				membershipID := parseResponse(t, memberRec)["MembershipId"].(string)

				delRec := doRequest(t, h, "DeleteGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"MembershipId":    membershipID,
				})
				assert.Equal(t, http.StatusOK, delRec.Code)

				descRec := doRequest(t, h, "DescribeGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"MembershipId":    membershipID,
				})
				assert.Equal(t, http.StatusNotFound, descRec.Code)
			},
		},
		{
			name: "list_memberships_for_member",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "multi.group.user",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupNames := []string{"Member Group 0", "Member Group 1"}
				for i := range 2 {
					groupRec := doRequest(t, h, "CreateGroup", map[string]any{
						"IdentityStoreId": testStoreID,
						"DisplayName":     groupNames[i],
					})
					require.Equal(t, http.StatusOK, groupRec.Code)
					groupID := parseResponse(t, groupRec)["GroupId"].(string)

					doRequest(t, h, "CreateGroupMembership", map[string]any{
						"IdentityStoreId": testStoreID,
						"GroupId":         groupID,
						"MemberId":        map[string]any{"UserId": userID},
					})
				}

				rec := doRequest(t, h, "ListGroupMembershipsForMember", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				memberships, ok := resp["GroupMemberships"].([]any)
				require.True(t, ok)
				assert.Len(t, memberships, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newTestHandler())
		})
	}
}

// TestGetGroupMembershipID covers GetGroupMembershipId.
func TestGetGroupMembershipID(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	userRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "gm.user",
	})
	require.Equal(t, http.StatusOK, userRec.Code)
	userID := parseResponse(t, userRec)["UserId"].(string)

	groupRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "GM Group",
	})
	require.Equal(t, http.StatusOK, groupRec.Code)
	groupID := parseResponse(t, groupRec)["GroupId"].(string)

	memberRec := doRequest(t, h, "CreateGroupMembership", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"MemberId":        map[string]any{"UserId": userID},
	})
	require.Equal(t, http.StatusOK, memberRec.Code)
	wantMembershipID := parseResponse(t, memberRec)["MembershipId"].(string)

	rec := doRequest(t, h, "GetGroupMembershipId", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"MemberId":        map[string]any{"UserId": userID},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.Equal(t, wantMembershipID, resp["MembershipId"])
}

// TestIsMemberInGroups verifies the IsMemberInGroups endpoint.
func TestIsMemberInGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	userRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "ismember.user",
	})
	require.Equal(t, http.StatusOK, userRec.Code)
	userID := parseResponse(t, userRec)["UserId"].(string)

	groupNames := []string{"IsGroup 0", "IsGroup 1", "IsGroup 2"}
	groupIDs := make([]string, 3)
	for i := range 3 {
		gr := doRequest(t, h, "CreateGroup", map[string]any{
			"IdentityStoreId": testStoreID,
			"DisplayName":     groupNames[i],
		})
		require.Equal(t, http.StatusOK, gr.Code)
		groupIDs[i] = parseResponse(t, gr)["GroupId"].(string)
	}

	// Add user to first two groups only.
	for _, gid := range groupIDs[:2] {
		doRequest(t, h, "CreateGroupMembership", map[string]any{
			"IdentityStoreId": testStoreID,
			"GroupId":         gid,
			"MemberId":        map[string]any{"UserId": userID},
		})
	}

	rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
		"IdentityStoreId": testStoreID,
		"MemberId":        map[string]any{"UserId": userID},
		"GroupIds":        groupIDs,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	results, ok := resp["Results"].([]any)
	require.True(t, ok)
	require.Len(t, results, 3)
}

// TestDeleteGroupMembership verifies deleting a membership.
func TestDeleteGroupMembership(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	userRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "del.mem.user",
	})
	require.Equal(t, http.StatusOK, userRec.Code)
	userID := parseResponse(t, userRec)["UserId"].(string)

	groupRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "Del Mem Group",
	})
	require.Equal(t, http.StatusOK, groupRec.Code)
	groupID := parseResponse(t, groupRec)["GroupId"].(string)

	memRec := doRequest(t, h, "CreateGroupMembership", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"MemberId":        map[string]any{"UserId": userID},
	})
	require.Equal(t, http.StatusOK, memRec.Code)
	membershipID := parseResponse(t, memRec)["MembershipId"].(string)

	delRec := doRequest(t, h, "DeleteGroupMembership", map[string]any{
		"IdentityStoreId": testStoreID,
		"MembershipId":    membershipID,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

// TestMembershipErrors covers 404/400 error paths and required-field validation for
// GroupMembership operations, including IsMemberInGroups bounds.
func TestMembershipErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystore.Handler)
		name string
	}{
		{
			name: "describe_nonexistent_membership",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DescribeGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"MembershipId":    "does-not-exist",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "resource_not_found_has_resource_type_group_membership",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DescribeGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"MembershipId":    "nonexistent-membership",
				})
				require.Equal(t, http.StatusNotFound, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
				assert.Equal(t, "GROUP_MEMBERSHIP", resp["ResourceType"])
			},
		},
		{
			name: "create_membership_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "CreateGroupMembership", map[string]any{
					"GroupId":  "group-1",
					"MemberId": map[string]any{"UserId": "user-1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_membership_missing_group_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": "user-1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_membership_missing_user_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         "group-1",
					"MemberId":        map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_membership_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DescribeGroupMembership", map[string]any{
					"MembershipId": "mem-001",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_membership_missing_membership_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DescribeGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_membership_missing_membership_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DeleteGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_group_memberships_missing_group_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "ListGroupMemberships", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_group_memberships_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "ListGroupMemberships", map[string]any{
					"GroupId": "group-001",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_group_membership_id_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "GetGroupMembershipId", map[string]any{
					"GroupId":  "g-1",
					"MemberId": map[string]any{"UserId": "u-1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_group_membership_id_missing_group_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "GetGroupMembershipId", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": "u-1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_group_membership_id_missing_user_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "GetGroupMembershipId", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         "g-1",
					"MemberId":        map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_memberships_for_member_missing_user_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "ListGroupMembershipsForMember", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "is_member_in_groups_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"MemberId": map[string]any{"UserId": "u-1"},
					"GroupIds": []string{"g-1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "is_member_in_groups_missing_user_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{},
					"GroupIds":        []string{"g1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "is_member_in_groups_empty_group_ids",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": "user-1"},
					"GroupIds":        []string{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "is_member_in_groups_exceeds_100_ids",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				groupIDs := make([]string, 101)
				for i := range 101 {
					groupIDs[i] = fmt.Sprintf("g-%d", i)
				}

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": "u-1"},
					"GroupIds":        groupIDs,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, "ValidationException", resp["__type"])
			},
		},
		{
			name: "is_member_in_groups_exactly_100_ids_ok",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				uRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "max100.user",
				})
				require.Equal(t, http.StatusOK, uRec.Code)
				userID := parseResponse(t, uRec)["UserId"].(string)

				groupIDs := make([]string, 100)
				for i := range 100 {
					groupIDs[i] = fmt.Sprintf("fake-group-%d", i)
				}

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        groupIDs,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				results, _ := parseResponse(t, rec)["Results"].([]any)
				assert.Len(t, results, 100)
			},
		},
		{
			name: "is_member_in_groups_uses_index",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				uRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "idx.user",
				})
				require.Equal(t, http.StatusOK, uRec.Code)
				userID := parseResponse(t, uRec)["UserId"].(string)

				gRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Idx Group",
				})
				require.Equal(t, http.StatusOK, gRec.Code)
				groupID := parseResponse(t, gRec)["GroupId"].(string)

				doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MemberId":        map[string]any{"UserId": userID},
				})

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        []string{groupID, "other-group"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				results, ok := resp["Results"].([]any)
				require.True(t, ok)
				require.Len(t, results, 2)

				first := results[0].(map[string]any)
				assert.Equal(t, groupID, first["GroupId"])
				assert.Equal(t, true, first["MembershipExists"])

				second := results[1].(map[string]any)
				assert.Equal(t, "other-group", second["GroupId"])
				assert.Equal(t, false, second["MembershipExists"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newTestHandler())
		})
	}
}
