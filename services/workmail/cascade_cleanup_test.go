package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for the cascade-cleanup leaks fixed this pass (see
// cascadeCleanEntity/deleteTagsForOrg/deleteGlobalAliasesForOrg in
// store.go): deleting a user, group, or resource previously left ghost rows
// behind in aliases/globalAliases, mailbox permissions (as either the
// target entity or the grantee), group memberships, resource delegate
// lists, and tags; deleting an organization previously left the org's own
// tag entry, every contained user/group/resource's tag entry, and every
// globalAliases row (primary emails and CreateAlias-created aliases alike)
// behind entirely (DeleteOrganization's own doc comment said as much:
// "tags ... deliberately left untouched").

// TestDeleteUser_CascadeCleansEveryCollection locks that deleting a
// (disabled) user removes it from every collection that references it by
// ID: its own CreateAlias-created aliases (plus their globalAliases
// reverse-index rows -- previously left as ghost rows that would
// permanently block reuse of that alias by any other entity in the org),
// its mailbox-permission grant as a grantee on another entity, its group
// membership, its resource delegate listing, and its tags.
func TestDeleteUser_CascadeCleansEveryCollection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "cascade-user-org")

	userA := createTestUser(t, h, orgID, "cascade-a", "Cascade A")
	require.Equal(t, http.StatusOK, doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"Email":"cascade-a@example.com"}`, orgID, userA,
	)).Code)

	const extraAlias = "cascade-extra@example.com"
	require.Equal(t, http.StatusOK, doOp(t, h, "CreateAlias", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"Alias":%q}`, orgID, userA, extraAlias,
	)).Code)

	groupID := createTestGroup(t, h, orgID, "cascade-group")
	require.Equal(t, http.StatusOK, doOp(t, h, "AssociateMemberToGroup", fmt.Sprintf(
		`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userA,
	)).Code)

	resourceID := createTestResource(t, h, orgID, "cascade-resource", "ROOM")
	require.Equal(t, http.StatusOK, doOp(t, h, "AssociateDelegateToResource", fmt.Sprintf(
		`{"OrganizationId":%q,"ResourceId":%q,"EntityId":%q}`, orgID, resourceID, userA,
	)).Code)

	userC := createTestUser(t, h, orgID, "cascade-c", "Cascade C")
	require.Equal(t, http.StatusOK, doOp(t, h, "PutMailboxPermissions", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"GranteeId":%q,"PermissionValues":["FULL_ACCESS"]}`,
		orgID, userC, userA,
	)).Code)

	// DescribeUserOutput has no ARN field (real WorkMail doesn't return
	// one), so build the user's ARN from the org's ARN the same way
	// entityARN does internally: "<orgARN>/user/<userID>".
	setupRec := doOp(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, setupRec.Code)
	orgARN := decodeJSON(t, setupRec)["ARN"].(string)
	userAARNStr := orgARN + "/user/" + userA
	require.Equal(t, http.StatusOK, doOp(t, h, "TagResource", fmt.Sprintf(
		`{"ResourceARN":%q,"Tags":[{"Key":"env","Value":"test"}]}`, userAARNStr,
	)).Code)

	// userA must be disabled before it can be deleted.
	require.Equal(t, http.StatusOK, doOp(t, h, "DeregisterFromWorkMail", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q}`, orgID, userA,
	)).Code)
	require.Equal(t, http.StatusOK, doOp(t, h, "DeleteUser", fmt.Sprintf(
		`{"OrganizationId":%q,"UserId":%q}`, orgID, userA,
	)).Code)

	t.Run("extra_alias_reusable_by_another_entity", func(t *testing.T) {
		t.Parallel()

		userB := createTestUser(t, h, orgID, "cascade-b", "Cascade B")
		rec := doOp(t, h, "CreateAlias", fmt.Sprintf(
			`{"OrganizationId":%q,"EntityId":%q,"Alias":%q}`, orgID, userB, extraAlias,
		))
		assert.Equal(t, http.StatusOK, rec.Code, "extra alias should be reusable after owning user is deleted")
	})

	t.Run("group_membership_removed", func(t *testing.T) {
		t.Parallel()

		rec := doOp(t, h, "ListGroupMembers", fmt.Sprintf(
			`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
		))
		require.Equal(t, http.StatusOK, rec.Code)
		members, _ := decodeJSON(t, rec)["Members"].([]any)
		assert.Empty(t, members)
	})

	t.Run("resource_delegate_removed", func(t *testing.T) {
		t.Parallel()

		rec := doOp(t, h, "ListResourceDelegates", fmt.Sprintf(
			`{"OrganizationId":%q,"ResourceId":%q}`, orgID, resourceID,
		))
		require.Equal(t, http.StatusOK, rec.Code)
		delegates, _ := decodeJSON(t, rec)["Delegates"].([]any)
		assert.Empty(t, delegates)
	})

	t.Run("grantee_permission_removed", func(t *testing.T) {
		t.Parallel()

		rec := doOp(t, h, "ListMailboxPermissions", fmt.Sprintf(
			`{"OrganizationId":%q,"EntityId":%q}`, orgID, userC,
		))
		require.Equal(t, http.StatusOK, rec.Code)
		perms, _ := decodeJSON(t, rec)["Permissions"].([]any)
		assert.Empty(t, perms)
	})

	t.Run("tags_removed", func(t *testing.T) {
		t.Parallel()

		rec := doOp(t, h, "ListTagsForResource", fmt.Sprintf(`{"ResourceARN":%q}`, userAARNStr))
		require.Equal(t, http.StatusOK, rec.Code)
		tags, _ := decodeJSON(t, rec)["Tags"].([]any)
		assert.Empty(t, tags)
	})
}

// TestDeleteOrganization_CascadeCleansTagsAndGlobalAliases locks that
// deleting an organization removes the org's own tag entry, its contained
// user's tag entry, and every globalAliases row (primary email and
// CreateAlias-created alias alike) belonging to it -- previously all left
// behind as ghost rows with no path to ever being cleaned (the org, and
// every entity that could reference them, was already gone).
func TestDeleteOrganization_CascadeCleansTagsAndGlobalAliases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "cascade-org-delete")

	setupRec := doOp(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, setupRec.Code)
	orgARN := decodeJSON(t, setupRec)["ARN"].(string)
	require.Equal(t, http.StatusOK, doOp(t, h, "TagResource", fmt.Sprintf(
		`{"ResourceARN":%q,"Tags":[{"Key":"team","Value":"platform"}]}`, orgARN,
	)).Code)

	userID := createTestUser(t, h, orgID, "org-cascade-user", "Org Cascade User")
	const sharedEmail = "shared-across-orgs@example.com"
	require.Equal(t, http.StatusOK, doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"Email":%q}`, orgID, userID, sharedEmail,
	)).Code)

	require.Equal(t, http.StatusOK, doOp(t, h, "DeleteOrganization", fmt.Sprintf(
		`{"OrganizationId":%q,"DeleteDirectory":false}`, orgID,
	)).Code)

	t.Run("org_tags_removed", func(t *testing.T) {
		t.Parallel()

		rec := doOp(t, h, "ListTagsForResource", fmt.Sprintf(`{"ResourceARN":%q}`, orgARN))
		require.Equal(t, http.StatusOK, rec.Code)
		tags, _ := decodeJSON(t, rec)["Tags"].([]any)
		assert.Empty(t, tags)
	})

	t.Run("global_alias_reusable_by_a_new_organization", func(t *testing.T) {
		t.Parallel()

		newOrgID := createTestOrg(t, h, "cascade-org-reuse")
		newUserID := createTestUser(t, h, newOrgID, "org-cascade-user2", "Org Cascade User 2")
		rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
			`{"OrganizationId":%q,"EntityId":%q,"Email":%q}`, newOrgID, newUserID, sharedEmail,
		))
		assert.Equal(t, http.StatusOK, rec.Code,
			"email freed by a deleted org's globalAliases entries should be reusable in a new org")
	})
}
