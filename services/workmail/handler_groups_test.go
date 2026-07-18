package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- Groups ---

func TestWorkMail_Groups_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "create_group_returns_group_id",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "grporg")
				groupID := createTestGroup(t, h, orgID, "engineering")
				assert.NotEmpty(t, groupID)
			},
		},
		{
			name: "describe_group_returns_fields",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "descgrporg")
				groupID := createTestGroup(t, h, orgID, "finance")
				rec := doOp(t, h, "DescribeGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				assert.Equal(t, groupID, m["GroupId"])
				assert.Equal(t, "finance", m["Name"])
				assert.Equal(t, "DISABLED", m["State"])
			},
		},
		{
			name: "list_groups",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "listgrporg")
				createTestGroup(t, h, orgID, "grp-a")
				createTestGroup(t, h, orgID, "grp-b")
				rec := doOp(t, h, "ListGroups", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				groups, ok := m["Groups"].([]any)
				require.True(t, ok)
				assert.Len(t, groups, 2)
				g := groups[0].(map[string]any)
				assert.NotEmpty(t, g["Id"])
				assert.NotEmpty(t, g["Name"])
				assert.NotEmpty(t, g["State"])
			},
		},
		{
			name: "delete_group",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "delgrporg")
				groupID := createTestGroup(t, h, orgID, "todelete")
				rec := doOp(t, h, "DeleteGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "DescribeGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
			},
		},
		{
			name: "associate_and_list_group_members",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "memorg")
				groupID := createTestGroup(t, h, orgID, "team")
				userID := createTestUser(t, h, orgID, "member1", "Member One")
				rec := doOp(t, h, "AssociateMemberToGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "ListGroupMembers", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := decodeJSON(t, rec2)
				members, ok := m["Members"].([]any)
				require.True(t, ok)
				require.Len(t, members, 1)
				mem := members[0].(map[string]any)
				assert.Equal(t, userID, mem["Id"])
				assert.Equal(t, "USER", mem["Type"])
				assert.Equal(t, "ENABLED", mem["State"])
			},
		},
		{
			name: "disassociate_member_from_group",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "disassocorg")
				groupID := createTestGroup(t, h, orgID, "squad")
				userID := createTestUser(t, h, orgID, "member2", "Member Two")
				doOp(t, h, "AssociateMemberToGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				rec := doOp(t, h, "DisassociateMemberFromGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "ListGroupMembers", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				m := decodeJSON(t, rec2)
				members := m["Members"].([]any)
				assert.Empty(t, members)
			},
		},
		{
			name: "disassociate_nonmember_returns_not_found",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "disnmorg")
				groupID := createTestGroup(t, h, orgID, "crew")
				userID := createTestUser(t, h, orgID, "nonmember", "Non Member")
				rec := doOp(t, h, "DisassociateMemberFromGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := decodeJSON(t, rec)
				assert.Contains(t, m["__type"].(string), "EntityNotFoundException")
			},
		},
		{
			name: "list_groups_for_entity",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "lgeorg")
				userID := createTestUser(t, h, orgID, "lgeuser", "LGE User")
				groupID := createTestGroup(t, h, orgID, "lgegrp")
				doOp(t, h, "AssociateMemberToGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				rec := doOp(t, h, "ListGroupsForEntity", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				groups, ok := m["Groups"].([]any)
				require.True(t, ok)
				assert.Len(t, groups, 1)
				assert.Equal(t, groupID, groups[0].(map[string]any)["GroupId"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.run(t, h)
		})
	}
}

func TestCreateGroup_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCode   string
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "empty_name_fails",
			body:       `{"OrganizationId":"%s","Name":""}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterException",
		},
		{
			name:       "valid_name_succeeds",
			body:       `{"OrganizationId":"%s","Name":"mygroup"}`,
			wantStatus: http.StatusOK,
		},
	}

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "group-val-org")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doOp(t, h, "CreateGroup", fmt.Sprintf(tc.body, orgID))
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantCode != "" {
				m := decodeJSON(t, rec)
				assert.Equal(t, tc.wantCode, m["__type"])
			}
		})
	}
}

func TestDescribeGroup_HiddenFromGlobalAddressList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		hidden string
		want   bool
	}{
		{
			name:   "hidden_true",
			hidden: "true",
			want:   true,
		},
		{
			name:   "hidden_false",
			hidden: "false",
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "hidden-group-org-"+tc.name)

			rec := doOp(t, h, "CreateGroup", fmt.Sprintf(
				`{"OrganizationId":%q,"Name":"grp1","HiddenFromGlobalAddressList":%s}`,
				orgID, tc.hidden,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			m := decodeJSON(t, rec)
			groupID := m["GroupId"].(string)

			rec = doOp(t, h, "DescribeGroup", fmt.Sprintf(
				`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			m = decodeJSON(t, rec)
			hidden, _ := m["HiddenFromGlobalAddressList"].(bool)
			assert.Equal(t, tc.want, hidden)
		})
	}
}
