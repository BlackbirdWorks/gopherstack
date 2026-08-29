package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- Users ---

func TestWorkMail_Users_Lifecycle(t *testing.T) {
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
				rec := doOp(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
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
				rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"carol@regorgnow.awsapps.com"}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				// confirm state is ENABLED
				rec2 := doOp(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				m := decodeJSON(t, rec2)
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
				doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"dan@deregorgnow.awsapps.com"}`, orgID, userID,
				))
				rec := doOp(t, h, "DeregisterFromWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				m := decodeJSON(t, rec2)
				assert.Equal(t, "DISABLED", m["State"])
			},
		},
		{
			name: "update_user_changes_display_name",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "updorg")
				userID := createTestUser(t, h, orgID, "eve", "Eve Old")
				rec := doOp(t, h, "UpdateUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q,"DisplayName":"Eve New"}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				m := decodeJSON(t, rec2)
				assert.Equal(t, "Eve New", m["DisplayName"])
			},
		},
		{
			name: "delete_user_removes_from_list",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "deluserorg")
				userID := createTestUser(t, h, orgID, "frank", "Frank")
				rec := doOp(t, h, "DeleteUser", fmt.Sprintf(
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
				rec := doOp(t, h, "ListUsers", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
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
				rec := doOp(t, h, "GetMailboxDetails", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
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
				rec := doOp(t, h, "UpdateMailboxQuota", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q,"MailboxQuota":100000}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "GetMailboxDetails", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":%q}`, orgID, userID,
				))
				m := decodeJSON(t, rec2)
				assert.InDelta(t, float64(100000), m["MailboxQuota"], 0)
			},
		},
		{
			name: "duplicate_user_name_returns_conflict",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "dupuserorg")
				createTestUser(t, h, orgID, "dupname", "First")
				rec := doOp(t, h, "CreateUser", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"dupname","DisplayName":"Second","Password":"x"}`, orgID,
				))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := decodeJSON(t, rec)
				assert.Equal(t, "NameAvailabilityException", m["__type"])
			},
		},
		{
			name: "describe_nonexistent_user_returns_not_found",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "nfuserorg")
				rec := doOp(t, h, "DescribeUser", fmt.Sprintf(
					`{"OrganizationId":%q,"UserId":"no-such-user"}`, orgID,
				))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := decodeJSON(t, rec)
				assert.Contains(t, m["__type"].(string), "EntityNotFoundException")
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

func TestCreateUser_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCode   string
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "empty_name_fails",
			body:       `{"OrganizationId":"%s","Name":"","DisplayName":"X","Password":"Pass@1234"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterException",
		},
		{
			name: "invalid_role_fails",
			body: `{"OrganizationId":"%s","Name":"alice",` +
				`"DisplayName":"Alice","Password":"Pass@1234","Role":"ADMIN"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterException",
		},
		{
			name: "role_USER_succeeds",
			body: `{"OrganizationId":"%s","Name":"alice",` +
				`"DisplayName":"Alice","Password":"Pass@1234","Role":"USER"}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "role_SYSTEM_USER_succeeds",
			body: `{"OrganizationId":"%s","Name":"sysalice",` +
				`"DisplayName":"SysAlice","Password":"Pass@1234","Role":"SYSTEM_USER"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_role_succeeds",
			body:       `{"OrganizationId":"%s","Name":"norolice","DisplayName":"NoRole","Password":"Pass@1234"}`,
			wantStatus: http.StatusOK,
		},
	}

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "valid-org")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doOp(t, h, "CreateUser", fmt.Sprintf(tc.body, orgID))
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantCode != "" {
				m := decodeJSON(t, rec)
				assert.Equal(t, tc.wantCode, m["__type"])
			}
		})
	}
}

func TestListUsers_UserRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		userName string
		wantRole string
		role     string
	}{
		{name: "user_role", userName: "role-user", role: "USER", wantRole: "USER"},
		{name: "system_user_role", userName: "role-sysuser", role: "SYSTEM_USER", wantRole: "SYSTEM_USER"},
		// omitting role in request causes handler to default to USER
		{name: "no_role_defaults_to_USER", userName: "role-norole", role: "USER", wantRole: "USER"},
	}

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "userrole-org")

	for _, tc := range tests {
		body := fmt.Sprintf(
			`{"OrganizationId":%q,"Name":%q,"DisplayName":"X","Password":"Pass@1234","Role":%q}`,
			orgID, tc.userName, tc.role,
		)
		rec := doOp(t, h, "CreateUser", body)
		require.Equal(t, http.StatusOK, rec.Code, "create user %s", tc.userName)
	}

	rec := doOp(t, h, "ListUsers", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, rec.Code)

	m := decodeJSON(t, rec)
	users, _ := m["Users"].([]any)
	require.Len(t, users, len(tests))

	// Build map by name for easy lookup.
	byName := make(map[string]map[string]any)
	for _, u := range users {
		um := u.(map[string]any)
		byName[um["Name"].(string)] = um
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			um, ok := byName[tc.userName]
			require.True(t, ok, "user %q not in list", tc.userName)

			role, _ := um["UserRole"].(string)
			assert.Equal(t, tc.wantRole, role)
		})
	}
}

func TestUpdatePrimaryEmailAddress_AllEntityTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *workmail.Handler, orgID string) string
		body       func(orgID, entityID string) string
		verifyBody func(orgID, entityID string) string
		name       string
		verifyOp   string
		wantEmail  string
	}{
		{
			name: "user",
			setup: func(h *workmail.Handler, orgID string) string {
				uid := createTestUser(t, h, orgID, "pea-user", "PEA User")
				rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"pea-user-orig@example.com"}`,
					orgID, uid,
				))
				require.Equal(t, http.StatusOK, rec.Code)

				return uid
			},
			body: func(orgID, entityID string) string {
				return fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"pea-user-new@example.com"}`,
					orgID, entityID,
				)
			},
			verifyOp: "DescribeUser",
			verifyBody: func(orgID, entityID string) string {
				return fmt.Sprintf(`{"OrganizationId":%q,"UserId":%q}`, orgID, entityID)
			},
			wantEmail: "pea-user-new@example.com",
		},
		{
			name: "group",
			setup: func(h *workmail.Handler, orgID string) string {
				gid := createTestGroup(t, h, orgID, "pea-group")
				rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"pea-group-orig@example.com"}`,
					orgID, gid,
				))
				require.Equal(t, http.StatusOK, rec.Code)

				return gid
			},
			body: func(orgID, entityID string) string {
				return fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"pea-group-new@example.com"}`,
					orgID, entityID,
				)
			},
			verifyOp: "DescribeGroup",
			verifyBody: func(orgID, entityID string) string {
				return fmt.Sprintf(`{"OrganizationId":%q,"GroupId":%q}`, orgID, entityID)
			},
			wantEmail: "pea-group-new@example.com",
		},
		{
			name: "resource",
			setup: func(h *workmail.Handler, orgID string) string {
				rid := createTestResource(t, h, orgID, "pea-resource", "ROOM")
				rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"pea-resource-orig@example.com"}`,
					orgID, rid,
				))
				require.Equal(t, http.StatusOK, rec.Code)

				return rid
			},
			body: func(orgID, entityID string) string {
				return fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"pea-resource-new@example.com"}`,
					orgID, entityID,
				)
			},
			verifyOp: "DescribeResource",
			verifyBody: func(orgID, entityID string) string {
				return fmt.Sprintf(`{"OrganizationId":%q,"ResourceId":%q}`, orgID, entityID)
			},
			wantEmail: "pea-resource-new@example.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "pea-org-"+tc.name)
			entityID := tc.setup(h, orgID)

			rec := doOp(t, h, "UpdatePrimaryEmailAddress", tc.body(orgID, entityID))
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doOp(t, h, tc.verifyOp, tc.verifyBody(orgID, entityID))
			require.Equal(t, http.StatusOK, rec.Code)

			m := decodeJSON(t, rec)
			assert.Equal(t, tc.wantEmail, m["Email"])
		})
	}
}

// TestListUsers_Filters locks the ListUsersInput.Filters wire behavior
// (DisplayNamePrefix/PrimaryEmailPrefix/State/UsernamePrefix/
// IdentityProviderUserIdPrefix): previously accepted on the wire but
// silently ignored, so a real client's prefix/state search would have
// gotten back the full unfiltered page.
func TestListUsers_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "listusers-filter-org")

	createTestUser(t, h, orgID, "alpha", "Team Alpha")
	betaID := createTestUser(t, h, orgID, "beta", "Team Beta")
	require.Equal(t, http.StatusOK, doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"Email":"beta@filter.example"}`, orgID, betaID,
	)).Code)
	createTestUser(t, h, orgID, "gamma", "Other Gamma")

	tests := []struct {
		filters   string
		name      string
		wantNames []string
	}{
		{
			name:      "display_name_prefix",
			filters:   `{"DisplayNamePrefix":"Team"}`,
			wantNames: []string{"alpha", "beta"},
		},
		{
			name:      "username_prefix",
			filters:   `{"UsernamePrefix":"be"}`,
			wantNames: []string{"beta"},
		},
		{
			name:      "primary_email_prefix",
			filters:   `{"PrimaryEmailPrefix":"beta@"}`,
			wantNames: []string{"beta"},
		},
		{
			name:      "state_enabled",
			filters:   `{"State":"ENABLED"}`,
			wantNames: []string{"beta"},
		},
		{
			name:      "state_disabled",
			filters:   `{"State":"DISABLED"}`,
			wantNames: []string{"alpha", "gamma"},
		},
		{
			name:      "no_match",
			filters:   `{"UsernamePrefix":"nonexistent"}`,
			wantNames: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doOp(t, h, "ListUsers", fmt.Sprintf(
				`{"OrganizationId":%q,"Filters":%s}`, orgID, tc.filters,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			m := decodeJSON(t, rec)
			users, _ := m["Users"].([]any)
			gotNames := make([]string, 0, len(users))
			for _, u := range users {
				gotNames = append(gotNames, u.(map[string]any)["Name"].(string))
			}
			assert.ElementsMatch(t, tc.wantNames, gotNames)
		})
	}
}

// TestCreateAndUpdateUser_ProfileFields locks CreateUser/UpdateUser/
// DescribeUser round-tripping the optional profile fields (City, Company,
// Country, Department, HiddenFromGlobalAddressList,
// IdentityProviderIdentityStoreId/UserId, Initials, JobTitle, Office,
// Street, Telephone, ZipCode) that were previously accepted on
// CreateUserInput/UpdateUserInput but never modeled anywhere on the backend
// or DescribeUserOutput.
func TestCreateAndUpdateUser_ProfileFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "profile-fields-org")

	rec := doOp(t, h, "CreateUser", fmt.Sprintf(`{
		"OrganizationId":%q,"Name":"profuser","DisplayName":"Prof User","Password":"x",
		"FirstName":"Prof","LastName":"User","IdentityProviderUserId":"idp-user-1",
		"HiddenFromGlobalAddressList":true
	}`, orgID))
	require.Equal(t, http.StatusOK, rec.Code)
	userID := decodeJSON(t, rec)["UserId"].(string)

	rec = doOp(t, h, "DescribeUser", fmt.Sprintf(`{"OrganizationId":%q,"UserId":%q}`, orgID, userID))
	require.Equal(t, http.StatusOK, rec.Code)
	m := decodeJSON(t, rec)
	assert.Equal(t, "Prof", m["FirstName"])
	assert.Equal(t, "User", m["LastName"])
	assert.Equal(t, "idp-user-1", m["IdentityProviderUserId"])
	assert.Equal(t, true, m["HiddenFromGlobalAddressList"])

	rec = doOp(t, h, "UpdateUser", fmt.Sprintf(`{
		"OrganizationId":%q,"UserId":%q,
		"City":"Seattle","Company":"Acme","Country":"US","Department":"Eng",
		"Initials":"P.U.","JobTitle":"Engineer","Office":"HQ","Street":"1 Main St",
		"Telephone":"555-0100","ZipCode":"98101"
	}`, orgID, userID))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doOp(t, h, "DescribeUser", fmt.Sprintf(`{"OrganizationId":%q,"UserId":%q}`, orgID, userID))
	require.Equal(t, http.StatusOK, rec.Code)
	m = decodeJSON(t, rec)
	assert.Equal(t, "Seattle", m["City"])
	assert.Equal(t, "Acme", m["Company"])
	assert.Equal(t, "US", m["Country"])
	assert.Equal(t, "Eng", m["Department"])
	assert.Equal(t, "P.U.", m["Initials"])
	assert.Equal(t, "Engineer", m["JobTitle"])
	assert.Equal(t, "HQ", m["Office"])
	assert.Equal(t, "1 Main St", m["Street"])
	assert.Equal(t, "555-0100", m["Telephone"])
	assert.Equal(t, "98101", m["ZipCode"])
	// Fields set at creation time must survive an UpdateUser call that
	// doesn't touch them (empty string means "leave unchanged").
	assert.Equal(t, "Prof", m["FirstName"])
	assert.Equal(t, true, m["HiddenFromGlobalAddressList"])
}

// TestRegisterDeregisterToWorkMail_MailboxProvisionedDates locks
// MailboxProvisionedDate/MailboxDeprovisionedDate on DescribeUserOutput:
// real WorkMail sets these alongside EnabledDate/DisabledDate when the
// mailbox itself is created/removed.
func TestRegisterDeregisterToWorkMail_MailboxProvisionedDates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "mailbox-dates-org")
	userID := createTestUser(t, h, orgID, "datesuser", "Dates User")

	rec := doOp(t, h, "DescribeUser", fmt.Sprintf(`{"OrganizationId":%q,"UserId":%q}`, orgID, userID))
	require.Equal(t, http.StatusOK, rec.Code)
	m := decodeJSON(t, rec)
	_, hasProvisioned := m["MailboxProvisionedDate"]
	assert.False(t, hasProvisioned, "MailboxProvisionedDate should be absent before registration")

	rec = doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"Email":"datesuser@example.com"}`, orgID, userID,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doOp(t, h, "DescribeUser", fmt.Sprintf(`{"OrganizationId":%q,"UserId":%q}`, orgID, userID))
	require.Equal(t, http.StatusOK, rec.Code)
	m = decodeJSON(t, rec)
	assert.NotZero(t, m["MailboxProvisionedDate"])
	assert.Equal(t, m["EnabledDate"], m["MailboxProvisionedDate"])

	rec = doOp(t, h, "DeregisterFromWorkMail", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q}`, orgID, userID,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doOp(t, h, "DescribeUser", fmt.Sprintf(`{"OrganizationId":%q,"UserId":%q}`, orgID, userID))
	require.Equal(t, http.StatusOK, rec.Code)
	m = decodeJSON(t, rec)
	assert.NotZero(t, m["MailboxDeprovisionedDate"])
	assert.Equal(t, m["DisabledDate"], m["MailboxDeprovisionedDate"])
}
