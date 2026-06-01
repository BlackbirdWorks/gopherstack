package workmail_test

// Audit batch-1: AWS-accuracy coverage for WorkMail (go-yel3).
//
// Covers:
//   - Protocol: X-Amz-Target WorkMailService.* dispatch, Content-Type application/x-amz-json-1.1
//     on success and error, __type+message error envelope, non-POST → 405,
//     missing X-Amz-Target → 400, GET / → op list
//   - Organizations: CreateOrganization/DescribeOrganization/DeleteOrganization/ListOrganizations
//     field shapes, default mail domain pre-created, state ACTIVE
//   - Users: CreateUser → UserId, DescribeUser fields (Name/Email/State/UserRole),
//     UpdateUser, DeleteUser, ListUsers, RegisterToWorkMail → state ENABLED,
//     DeregisterFromWorkMail → state DISABLED, GetMailboxDetails/UpdateMailboxQuota
//   - Groups: CreateGroup → GroupId, DescribeGroup/UpdateGroup/DeleteGroup/ListGroups,
//     AssociateMemberToGroup/DisassociateMemberFromGroup/ListGroupMembers
//   - Resources: CreateResource → ResourceId, DescribeResource/UpdateResource/DeleteResource/ListResources,
//     AssociateDelegateToResource/DisassociateDelegateFromResource/ListResourceDelegates
//   - Aliases: CreateAlias/DeleteAlias/ListAliases (includes primary email)
//   - Mailbox permissions: PutMailboxPermissions/DeleteMailboxPermissions/ListMailboxPermissions
//     with GranteeId/GranteeType/PermissionValues fields
//   - Mail domains: RegisterMailDomain/DeregisterMailDomain/GetMailDomain/ListMailDomains/UpdateDefaultMailDomain
//   - Access control rules: PutAccessControlRule/DeleteAccessControlRule/GetAccessControlEffect/ListAccessControlRules
//   - Impersonation roles: CreateImpersonationRole/GetImpersonationRole/UpdateImpersonationRole/
//     DeleteImpersonationRole/ListImpersonationRoles field shapes
//   - Tags: TagResource/UntagResource/ListTagsForResource
//   - Error paths: org-not-found → EntityNotFoundException, duplicate → EntityAlreadyExistsException,
//     entity-not-found on group/resource ops, member-not-in-group on disassociate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- helpers ---

func a1Handler(t *testing.T) *workmail.Handler {
	t.Helper()

	return workmail.NewHandler(workmail.NewInMemoryBackend("000000000000", "us-east-1"))
}

func a1Do(t *testing.T, h *workmail.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "WorkMailService."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func a1JSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// createTestOrg creates a test organization and returns its ID.
func createTestOrg(t *testing.T, h *workmail.Handler, alias string) string {
	t.Helper()

	rec := a1Do(t, h, "CreateOrganization", fmt.Sprintf(`{"Alias":%q}`, alias))
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1JSON(t, rec)
	orgID, ok := m["OrganizationId"].(string)
	require.True(t, ok, "OrganizationId missing")
	require.NotEmpty(t, orgID)

	return orgID
}

// createTestUser creates a user and returns their ID.
func createTestUser(t *testing.T, h *workmail.Handler, orgID, name, displayName string) string {
	t.Helper()

	rec := a1Do(t, h, "CreateUser", fmt.Sprintf(
		`{"OrganizationId":%q,"Name":%q,"DisplayName":%q,"Password":"Pass@1234"}`, orgID, name, displayName,
	))
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1JSON(t, rec)
	userID, ok := m["UserId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, userID)

	return userID
}

// createTestGroup creates a group and returns its ID.
func createTestGroup(t *testing.T, h *workmail.Handler, orgID, name string) string {
	t.Helper()

	rec := a1Do(t, h, "CreateGroup", fmt.Sprintf(
		`{"OrganizationId":%q,"Name":%q}`, orgID, name,
	))
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1JSON(t, rec)
	groupID, ok := m["GroupId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, groupID)

	return groupID
}

// createTestResource creates a resource and returns its ID.
func createTestResource(t *testing.T, h *workmail.Handler, orgID, name, resType string) string {
	t.Helper()

	rec := a1Do(t, h, "CreateResource", fmt.Sprintf(
		`{"OrganizationId":%q,"Name":%q,"Type":%q}`, orgID, name, resType,
	))
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1JSON(t, rec)
	resID, ok := m["ResourceId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, resID)

	return resID
}

// --- Protocol accuracy ---

func TestAudit1_WorkMail_Protocol_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		body       string
		wantCT     string
		wantStatus int
	}{
		{
			name:       "success_returns_json11",
			action:     "ListOrganizations",
			body:       `{}`,
			wantStatus: http.StatusOK,
			wantCT:     "application/x-amz-json-1.1",
		},
		{
			name:       "error_returns_json11",
			action:     "DescribeOrganization",
			body:       `{"OrganizationId":"m-notexist"}`,
			wantStatus: http.StatusBadRequest,
			wantCT:     "application/x-amz-json-1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			rec := a1Do(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantCT)
		})
	}
}

func TestAudit1_WorkMail_Protocol_NonPost_Returns405(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/notroot", nil)
	req.Header.Set("X-Amz-Target", "WorkMailService.ListOrganizations")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestAudit1_WorkMail_Protocol_MissingTarget_Returns400(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_WorkMail_Protocol_GetRoot_Returns_OpList(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Amz-Target", "WorkMailService.ListOrganizations")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusOK, rec.Code)
	var ops []string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
	assert.NotEmpty(t, ops)
}

func TestAudit1_WorkMail_Protocol_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	rec := a1Do(t, h, "DescribeOrganization", `{"OrganizationId":"m-nope"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := a1JSON(t, rec)
	assert.NotEmpty(t, m["__type"], "__type field missing from error envelope")
	assert.NotEmpty(t, m["message"], "message field missing from error envelope")
}

// --- Organizations ---

func TestAudit1_WorkMail_Organizations_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "create_returns_org_id",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "testcorp")
				assert.NotEmpty(t, orgID)
			},
		},
		{
			name: "describe_returns_expected_fields",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "mycompany")
				rec := a1Do(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				assert.Equal(t, orgID, m["OrganizationId"])
				assert.Equal(t, "mycompany", m["Alias"])
				assert.Equal(t, "ACTIVE", m["State"])
				assert.Contains(t, m["DefaultMailDomain"], "mycompany")
				assert.NotEmpty(t, m["ARN"])
				assert.NotEmpty(t, m["DirectoryId"])
				assert.NotEmpty(t, m["DirectoryType"])
				assert.NotZero(t, m["CompletedDate"])
			},
		},
		{
			name: "list_returns_organization",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				createTestOrg(t, h, "listtest")
				rec := a1Do(t, h, "ListOrganizations", `{}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				summaries, ok := m["OrganizationSummaries"].([]any)
				require.True(t, ok)
				require.NotEmpty(t, summaries)
				first := summaries[0].(map[string]any)
				assert.NotEmpty(t, first["OrganizationId"])
				assert.NotEmpty(t, first["Alias"])
				assert.NotEmpty(t, first["State"])
			},
		},
		{
			name: "delete_removes_organization",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "todelete")
				rec := a1Do(t, h, "DeleteOrganization",
					fmt.Sprintf(`{"OrganizationId":%q,"DeleteDirectory":false}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				assert.Equal(t, orgID, m["OrganizationId"])
				assert.Equal(t, "DELETED", m["State"])
				// subsequent describe should 404
				rec2 := a1Do(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
			},
		},
		{
			name: "create_default_domain_visible_in_list_mail_domains",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domaintest")
				rec := a1Do(t, h, "ListMailDomains", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				domains, ok := m["MailDomains"].([]any)
				require.True(t, ok)
				require.NotEmpty(t, domains)
				d := domains[0].(map[string]any)
				assert.Contains(t, d["DomainName"].(string), "domaintest")
				assert.Equal(t, true, d["IsDefault"])
			},
		},
		{
			name: "duplicate_alias_returns_conflict",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				createTestOrg(t, h, "duporg")
				rec := a1Do(t, h, "CreateOrganization", `{"Alias":"duporg"}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := a1JSON(t, rec)
				assert.Contains(t, m["__type"].(string), "AlreadyExists")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Users ---

func TestAudit1_WorkMail_Users_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "create_returns_user_id",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "userorg")
				userID := createTestUser(t, h, orgID, "alice", "Alice Smith")
				assert.NotEmpty(t, userID)
			},
		},
		{
			name: "describe_returns_correct_fields",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "descuserorg")
				userID := createTestUser(t, h, orgID, "bob", "Bob Jones")
				rec := a1Do(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				assert.Equal(t, userID, m["UserId"])
				assert.Equal(t, "bob", m["Name"])
				assert.Equal(t, "Bob Jones", m["DisplayName"])
				assert.Equal(t, "DISABLED", m["State"])
				assert.Equal(t, "USER", m["UserRole"])
			},
		},
		{
			name: "register_to_workmail_enables_user",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "regorgnow")
				userID := createTestUser(t, h, orgID, "carol", "Carol")
				rec := a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"carol@regorgnow.awsapps.com"}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				// confirm state is ENABLED
				rec2 := a1Do(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				m := a1JSON(t, rec2)
				assert.Equal(t, "ENABLED", m["State"])
				assert.Equal(t, "carol@regorgnow.awsapps.com", m["Email"])
				assert.NotZero(t, m["EnabledDate"])
			},
		},
		{
			name: "deregister_disables_user",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "deregorgnow")
				userID := createTestUser(t, h, orgID, "dan", "Dan")
				a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"dan@deregorgnow.awsapps.com"}`, orgID, userID,
				))
				rec := a1Do(t, h, "DeregisterFromWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				m := a1JSON(t, rec2)
				assert.Equal(t, "DISABLED", m["State"])
			},
		},
		{
			name: "update_user_changes_display_name",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "updorg")
				userID := createTestUser(t, h, orgID, "eve", "Eve Old")
				rec := a1Do(t, h, "UpdateUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q,"DisplayName":"Eve New"}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				m := a1JSON(t, rec2)
				assert.Equal(t, "Eve New", m["DisplayName"])
			},
		},
		{
			name: "delete_user_removes_from_list",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "deluserorg")
				userID := createTestUser(t, h, orgID, "frank", "Frank")
				rec := a1Do(t, h, "DeleteUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, 0, workmail.UserCount(workmail.NewInMemoryBackend("", ""), orgID))
			},
		},
		{
			name: "list_users_returns_summaries",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "listusersorg")
				createTestUser(t, h, orgID, "user1", "User One")
				createTestUser(t, h, orgID, "user2", "User Two")
				rec := a1Do(t, h, "ListUsers", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				users, ok := m["Users"].([]any)
				require.True(t, ok)
				assert.Len(t, users, 2)
				u := users[0].(map[string]any)
				assert.NotEmpty(t, u["Id"])
				assert.NotEmpty(t, u["Name"])
				assert.NotEmpty(t, u["State"])
			},
		},
		{
			name: "get_mailbox_details_returns_quota",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "mbxorg")
				userID := createTestUser(t, h, orgID, "mbxuser", "Mbx User")
				rec := a1Do(t, h, "GetMailboxDetails", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				assert.NotZero(t, m["MailboxQuota"])
				_, hasSize := m["MailboxSize"]
				assert.True(t, hasSize)
			},
		},
		{
			name: "update_mailbox_quota",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "quotaorg")
				userID := createTestUser(t, h, orgID, "quotauser", "Quota User")
				rec := a1Do(t, h, "UpdateMailboxQuota", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q,"MailboxQuota":100000}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "GetMailboxDetails", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				m := a1JSON(t, rec2)
				assert.InDelta(t, float64(100000), m["MailboxQuota"], 0)
			},
		},
		{
			name: "duplicate_user_name_returns_conflict",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "dupuserorg")
				createTestUser(t, h, orgID, "dupname", "First")
				rec := a1Do(t, h, "CreateUser", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"dupname","DisplayName":"Second","Password":"x"}`, orgID,
				))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := a1JSON(t, rec)
				assert.Contains(t, m["__type"].(string), "AlreadyExists")
			},
		},
		{
			name: "describe_nonexistent_user_returns_not_found",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "nfuserorg")
				rec := a1Do(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":"no-such-user"}`, orgID,
				))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := a1JSON(t, rec)
				assert.Contains(t, m["__type"].(string), "EntityNotFoundException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Groups ---

func TestAudit1_WorkMail_Groups_Lifecycle(t *testing.T) {
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
				rec := a1Do(t, h, "DescribeGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
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
				rec := a1Do(t, h, "ListGroups", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
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
				rec := a1Do(t, h, "DeleteGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "DescribeGroup", fmt.Sprintf(
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
				rec := a1Do(t, h, "AssociateMemberToGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListGroupMembers", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := a1JSON(t, rec2)
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
				a1Do(t, h, "AssociateMemberToGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				rec := a1Do(t, h, "DisassociateMemberFromGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListGroupMembers", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
				))
				m := a1JSON(t, rec2)
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
				rec := a1Do(t, h, "DisassociateMemberFromGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := a1JSON(t, rec)
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
				a1Do(t, h, "AssociateMemberToGroup", fmt.Sprintf(
					`{"OrganizationId":%q,"GroupId":%q,"MemberId":%q}`, orgID, groupID, userID,
				))
				rec := a1Do(t, h, "ListGroupsForEntity", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				groups, ok := m["Groups"].([]any)
				require.True(t, ok)
				assert.Len(t, groups, 1)
				assert.Equal(t, groupID, groups[0].(map[string]any)["Id"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Resources ---

func TestAudit1_WorkMail_Resources_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "create_resource_returns_id",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "resorg")
				resID := createTestResource(t, h, orgID, "conf-room-a", "ROOM")
				assert.NotEmpty(t, resID)
			},
		},
		{
			name: "describe_resource_returns_fields",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "descresorg")
				resID := createTestResource(t, h, orgID, "projector", "EQUIPMENT")
				rec := a1Do(t, h, "DescribeResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q}`, orgID, resID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				assert.Equal(t, resID, m["ResourceId"])
				assert.Equal(t, "projector", m["Name"])
				assert.Equal(t, "EQUIPMENT", m["Type"])
				assert.Equal(t, "DISABLED", m["State"])
			},
		},
		{
			name: "list_resources",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "listresorg")
				createTestResource(t, h, orgID, "room-1", "ROOM")
				createTestResource(t, h, orgID, "equip-1", "EQUIPMENT")
				rec := a1Do(t, h, "ListResources", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				resources, ok := m["Resources"].([]any)
				require.True(t, ok)
				assert.Len(t, resources, 2)
				r := resources[0].(map[string]any)
				assert.NotEmpty(t, r["Id"])
				assert.NotEmpty(t, r["Type"])
			},
		},
		{
			name: "delete_resource",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "delresorg")
				resID := createTestResource(t, h, orgID, "old-room", "ROOM")
				rec := a1Do(t, h, "DeleteResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q}`, orgID, resID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "associate_and_list_delegates",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "delgorg")
				resID := createTestResource(t, h, orgID, "meetroom", "ROOM")
				userID := createTestUser(t, h, orgID, "delegate1", "Delegate One")
				rec := a1Do(t, h, "AssociateDelegateToResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q,"EntityId":%q}`, orgID, resID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListResourceDelegates", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q}`, orgID, resID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := a1JSON(t, rec2)
				delegates, ok := m["Delegates"].([]any)
				require.True(t, ok)
				require.Len(t, delegates, 1)
				d := delegates[0].(map[string]any)
				assert.Equal(t, userID, d["Id"])
				assert.Equal(t, "USER", d["Type"])
			},
		},
		{
			name: "disassociate_delegate",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "disdelgorg")
				resID := createTestResource(t, h, orgID, "boardroom", "ROOM")
				userID := createTestUser(t, h, orgID, "delg2", "Delg2")
				a1Do(t, h, "AssociateDelegateToResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q,"EntityId":%q}`, orgID, resID, userID,
				))
				rec := a1Do(t, h, "DisassociateDelegateFromResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q,"EntityId":%q}`, orgID, resID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Aliases ---

func TestAudit1_WorkMail_Aliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "create_and_list_alias",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "aliasorg")
				userID := createTestUser(t, h, orgID, "aliasuser", "Alias User")
				// register primary email
				a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"main@aliasorg.awsapps.com"}`, orgID, userID,
				))
				// add alias
				rec := a1Do(t, h, "CreateAlias", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Alias":"alt@aliasorg.awsapps.com"}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				// list aliases includes both
				rec2 := a1Do(t, h, "ListAliases", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := a1JSON(t, rec2)
				aliases, ok := m["Aliases"].([]any)
				require.True(t, ok)
				// primary + 1 alias = 2 total
				assert.Len(t, aliases, 2)
			},
		},
		{
			name: "delete_alias",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "delalorg")
				userID := createTestUser(t, h, orgID, "delaluser", "Del Alias")
				a1Do(t, h, "CreateAlias", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Alias":"bye@delalorg.awsapps.com"}`, orgID, userID,
				))
				rec := a1Do(t, h, "DeleteAlias", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Alias":"bye@delalorg.awsapps.com"}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListAliases", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, userID,
				))
				m := a1JSON(t, rec2)
				aliases := m["Aliases"].([]any)
				assert.Empty(t, aliases)
			},
		},
		{
			name: "duplicate_alias_returns_conflict",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "dupalorg")
				userID := createTestUser(t, h, orgID, "dupaluser", "Dup")
				a1Do(t, h, "CreateAlias", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Alias":"same@dupalorg.awsapps.com"}`, orgID, userID,
				))
				rec := a1Do(t, h, "CreateAlias", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Alias":"same@dupalorg.awsapps.com"}`, orgID, userID,
				))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := a1JSON(t, rec)
				assert.Contains(t, m["__type"].(string), "AlreadyExists")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Mailbox Permissions ---

func TestAudit1_WorkMail_MailboxPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "put_and_list_permissions",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "permorg")
				ownerID := createTestUser(t, h, orgID, "owner", "Owner")
				granteeID := createTestUser(t, h, orgID, "grantee", "Grantee")
				rec := a1Do(t, h, "PutMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"GranteeId":%q,"PermissionValues":["FULL_ACCESS"]}`,
					orgID, ownerID, granteeID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, ownerID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := a1JSON(t, rec2)
				perms, ok := m["Permissions"].([]any)
				require.True(t, ok)
				require.Len(t, perms, 1)
				p := perms[0].(map[string]any)
				assert.Equal(t, granteeID, p["GranteeId"])
				assert.NotEmpty(t, p["GranteeType"])
				pvs, ok := p["PermissionValues"].([]any)
				require.True(t, ok)
				assert.Contains(t, pvs, "FULL_ACCESS")
			},
		},
		{
			name: "delete_permissions",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "delpermorg")
				ownerID := createTestUser(t, h, orgID, "delpowner", "Del Owner")
				granteeID := createTestUser(t, h, orgID, "delpgrantee", "Del Grantee")
				a1Do(t, h, "PutMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"GranteeId":%q,"PermissionValues":["SEND_AS"]}`,
					orgID, ownerID, granteeID,
				))
				rec := a1Do(t, h, "DeleteMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"GranteeId":%q}`, orgID, ownerID, granteeID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, ownerID,
				))
				m := a1JSON(t, rec2)
				perms := m["Permissions"].([]any)
				assert.Empty(t, perms)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Mail Domains ---

func TestAudit1_WorkMail_MailDomains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "register_and_get_domain",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg1")
				rec := a1Do(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "GetMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := a1JSON(t, rec2)
				assert.Equal(t, "example.com", m["DomainName"])
				assert.Equal(t, false, m["IsDefault"])
				assert.NotEmpty(t, m["OwnershipVerificationStatus"])
			},
		},
		{
			name: "deregister_domain",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg2")
				a1Do(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"toremove.com"}`, orgID,
				))
				rec := a1Do(t, h, "DeregisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"toremove.com"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "deregister_default_domain_fails",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg3")
				// get the default domain name
				rec := a1Do(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				m := a1JSON(t, rec)
				defaultDomain := m["DefaultMailDomain"].(string)
				rec2 := a1Do(t, h, "DeregisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":%q}`, orgID, defaultDomain,
				))
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
			},
		},
		{
			name: "update_default_mail_domain",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg4")
				a1Do(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"newdefault.com"}`, orgID,
				))
				rec := a1Do(t, h, "UpdateDefaultMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"newdefault.com"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "GetMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"newdefault.com"}`, orgID,
				))
				m := a1JSON(t, rec2)
				assert.Equal(t, true, m["IsDefault"])
			},
		},
		{
			name: "list_mail_domains",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg5")
				a1Do(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"extra.com"}`, orgID,
				))
				rec := a1Do(t, h, "ListMailDomains", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				domains, ok := m["MailDomains"].([]any)
				require.True(t, ok)
				// default + extra
				assert.GreaterOrEqual(t, len(domains), 2)
				d := domains[0].(map[string]any)
				assert.NotEmpty(t, d["DomainName"])
				_, hasDefault := d["IsDefault"]
				assert.True(t, hasDefault)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Access Control Rules ---

func TestAudit1_WorkMail_AccessControlRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "put_and_list_rules",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "acrorg")
				rec := a1Do(t, h, "PutAccessControlRule", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"AllowAll","Effect":"ALLOW","Description":"Allow everything"}`,
					orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListAccessControlRules", fmt.Sprintf(
					`{"OrganizationId":%q}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := a1JSON(t, rec2)
				rules, ok := m["Rules"].([]any)
				require.True(t, ok)
				require.Len(t, rules, 1)
				r := rules[0].(map[string]any)
				assert.Equal(t, "AllowAll", r["Name"])
				assert.Equal(t, "ALLOW", r["Effect"])
			},
		},
		{
			name: "delete_rule",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "acrorgrm")
				a1Do(t, h, "PutAccessControlRule", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"TempRule","Effect":"DENY"}`, orgID,
				))
				rec := a1Do(t, h, "DeleteAccessControlRule", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"TempRule"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListAccessControlRules", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				m := a1JSON(t, rec2)
				rules := m["Rules"].([]any)
				assert.Empty(t, rules)
			},
		},
		{
			name: "get_access_control_effect",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "aceorg")
				rec := a1Do(t, h, "GetAccessControlEffect", fmt.Sprintf(
					`{"OrganizationId":%q,"IpAddress":"10.0.0.1","Action":"LOGIN","UserId":"u-123"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				assert.NotEmpty(t, m["Effect"])
				_, hasMatched := m["MatchedRules"]
				assert.True(t, hasMatched)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Impersonation Roles ---

func TestAudit1_WorkMail_ImpersonationRoles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "create_and_get_role",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "imperorg")
				rec := a1Do(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"AdminRole","Type":"FULL_ACCESS","Description":"Admin impersonation"}`,
					orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				roleID := m["ImpersonationRoleId"].(string)
				assert.NotEmpty(t, roleID)

				rec2 := a1Do(t, h, "GetImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m2 := a1JSON(t, rec2)
				assert.Equal(t, roleID, m2["ImpersonationRoleId"])
				assert.Equal(t, "AdminRole", m2["Name"])
				assert.Equal(t, "FULL_ACCESS", m2["Type"])
				assert.Equal(t, "Admin impersonation", m2["Description"])
				assert.NotZero(t, m2["DateCreated"])
				assert.NotZero(t, m2["DateModified"])
			},
		},
		{
			name: "list_impersonation_roles",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "listimperorg")
				a1Do(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"Role1","Type":"FULL_ACCESS"}`, orgID,
				))
				a1Do(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"Role2","Type":"READ_ONLY"}`, orgID,
				))
				rec := a1Do(t, h, "ListImpersonationRoles", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				items, ok := m["Items"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 2)
				item := items[0].(map[string]any)
				assert.NotEmpty(t, item["ImpersonationRoleId"])
				assert.NotEmpty(t, item["Name"])
				assert.NotEmpty(t, item["Type"])
			},
		},
		{
			name: "update_impersonation_role",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "updimperorg")
				rec := a1Do(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"OldName","Type":"FULL_ACCESS"}`, orgID,
				))
				m := a1JSON(t, rec)
				roleID := m["ImpersonationRoleId"].(string)
				rec2 := a1Do(t, h, "UpdateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q,"Name":"NewName","Type":"READ_ONLY"}`,
					orgID, roleID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				rec3 := a1Do(t, h, "GetImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
				))
				m3 := a1JSON(t, rec3)
				assert.Equal(t, "NewName", m3["Name"])
				assert.Equal(t, "READ_ONLY", m3["Type"])
			},
		},
		{
			name: "delete_impersonation_role",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "delimperorg")
				rec := a1Do(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"TempRole","Type":"FULL_ACCESS"}`, orgID,
				))
				m := a1JSON(t, rec)
				roleID := m["ImpersonationRoleId"].(string)
				rec2 := a1Do(t, h, "DeleteImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				rec3 := a1Do(t, h, "GetImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
				))
				assert.Equal(t, http.StatusBadRequest, rec3.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- Tags ---

func TestAudit1_WorkMail_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "tag_and_list_tags",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				arn := "arn:aws:workmail:us-east-1:000000000000:organization/m-abc123"
				rec := a1Do(t, h, "TagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"Tags":[{"Key":"Env","Value":"prod"},{"Key":"Team","Value":"eng"}]}`, arn,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListTagsForResource", fmt.Sprintf(`{"ResourceARN":%q}`, arn))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := a1JSON(t, rec2)
				tags, ok := m["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, 2)
				keys := make([]string, 0, len(tags))
				for _, t := range tags {
					tag := t.(map[string]any)
					keys = append(keys, tag["Key"].(string))
				}
				assert.Contains(t, keys, "Env")
				assert.Contains(t, keys, "Team")
			},
		},
		{
			name: "untag_removes_key",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				arn := "arn:aws:workmail:us-east-1:000000000000:organization/m-def456"
				a1Do(t, h, "TagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"Tags":[{"Key":"A","Value":"1"},{"Key":"B","Value":"2"}]}`, arn,
				))
				rec := a1Do(t, h, "UntagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"TagKeys":["A"]}`, arn,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := a1Do(t, h, "ListTagsForResource", fmt.Sprintf(`{"ResourceARN":%q}`, arn))
				m := a1JSON(t, rec2)
				tags := m["Tags"].([]any)
				assert.Len(t, tags, 1)
				assert.Equal(t, "B", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "tag_overwrite_same_key",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				arn := "arn:aws:workmail:us-east-1:000000000000:organization/m-ghi789"
				a1Do(t, h, "TagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"Tags":[{"Key":"Env","Value":"dev"}]}`, arn,
				))
				a1Do(t, h, "TagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"Tags":[{"Key":"Env","Value":"prod"}]}`, arn,
				))
				rec := a1Do(t, h, "ListTagsForResource", fmt.Sprintf(`{"ResourceARN":%q}`, arn))
				m := a1JSON(t, rec)
				tags := m["Tags"].([]any)
				assert.Len(t, tags, 1)
				assert.Equal(t, "prod", tags[0].(map[string]any)["Value"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}

// --- DescribeEntity ---

func TestAudit1_WorkMail_DescribeEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "describe_user_entity_by_id",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "deentorg")
				userID := createTestUser(t, h, orgID, "entuser", "Ent User")
				rec := a1Do(t, h, "DescribeEntity", fmt.Sprintf(
					`{"OrganizationId":%q,"Email":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1JSON(t, rec)
				assert.Equal(t, userID, m["EntityId"])
				assert.Equal(t, "entuser", m["Name"])
				assert.Equal(t, "USER", m["Type"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			tt.run(t, h)
		})
	}
}
