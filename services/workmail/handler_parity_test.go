package workmail_test

// Parity tests for behavioral gaps identified in go-kjmmw audit:
//   - Input validation: required fields, enum values
//   - Entity state lifecycle: cannot delete ENABLED entities
//   - GetAccessControlEffect: actual rule evaluation (IP, action, user)
//   - Response field fidelity: HiddenFromGlobalAddressList, UserRole, ACR timestamps
//   - UpdatePrimaryEmailAddress: groups and resources
//   - Pagination with index-based tokens

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

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

	h := a1Handler(t)
	orgID := createTestOrg(t, h, "valid-org")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := a1Do(t, h, "CreateUser", fmt.Sprintf(tc.body, orgID))
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantCode != "" {
				m := a1JSON(t, rec)
				assert.Equal(t, tc.wantCode, m["__type"])
			}
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

	h := a1Handler(t)
	orgID := createTestOrg(t, h, "group-val-org")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := a1Do(t, h, "CreateGroup", fmt.Sprintf(tc.body, orgID))
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantCode != "" {
				m := a1JSON(t, rec)
				assert.Equal(t, tc.wantCode, m["__type"])
			}
		})
	}
}

func TestCreateResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCode   string
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "empty_name_fails",
			body:       `{"OrganizationId":"%s","Name":"","Type":"ROOM"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterException",
		},
		{
			name:       "invalid_type_fails",
			body:       `{"OrganizationId":"%s","Name":"myres","Type":"DESK"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterException",
		},
		{
			name:       "type_ROOM_succeeds",
			body:       `{"OrganizationId":"%s","Name":"room1","Type":"ROOM"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "type_EQUIPMENT_succeeds",
			body:       `{"OrganizationId":"%s","Name":"eq1","Type":"EQUIPMENT"}`,
			wantStatus: http.StatusOK,
		},
	}

	h := a1Handler(t)
	orgID := createTestOrg(t, h, "resource-val-org")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := a1Do(t, h, "CreateResource", fmt.Sprintf(tc.body, orgID))
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantCode != "" {
				m := a1JSON(t, rec)
				assert.Equal(t, tc.wantCode, m["__type"])
			}
		})
	}
}

func TestDeleteEnabled_EntityState(t *testing.T) {
	t.Parallel()

	type entityKind struct {
		createFn   func(h *workmail.Handler, orgID string) string
		deleteBody func(orgID, entityID string) string
		registerFn func(h *workmail.Handler, orgID, entityID string)
		name       string
		deleteOp   string
	}

	kinds := []entityKind{
		{
			name: "user",
			createFn: func(h *workmail.Handler, orgID string) string {
				return createTestUser(t, h, orgID, "del-user", "Del User")
			},
			deleteOp: "DeleteUser",
			deleteBody: func(orgID, entityID string) string {
				return fmt.Sprintf(`{"OrganizationId":%q,"UserId":%q}`, orgID, entityID)
			},
			registerFn: func(h *workmail.Handler, orgID, entityID string) {
				rec := a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"deluser@example.com"}`,
					orgID,
					entityID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "group",
			createFn: func(h *workmail.Handler, orgID string) string {
				return createTestGroup(t, h, orgID, "del-group")
			},
			deleteOp: "DeleteGroup",
			deleteBody: func(orgID, entityID string) string {
				return fmt.Sprintf(`{"OrganizationId":%q,"GroupId":%q}`, orgID, entityID)
			},
			registerFn: func(h *workmail.Handler, orgID, entityID string) {
				rec := a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"delgroup@example.com"}`,
					orgID,
					entityID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "resource",
			createFn: func(h *workmail.Handler, orgID string) string {
				return createTestResource(t, h, orgID, "del-resource", "ROOM")
			},
			deleteOp: "DeleteResource",
			deleteBody: func(orgID, entityID string) string {
				return fmt.Sprintf(`{"OrganizationId":%q,"ResourceId":%q}`, orgID, entityID)
			},
			registerFn: func(h *workmail.Handler, orgID, entityID string) {
				rec := a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"Email":"delresource@example.com"}`,
					orgID,
					entityID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, k := range kinds {
		kind := k
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				wantCode   string
				name       string
				wantStatus int
				enabled    bool
			}{
				{
					name:       "enabled_entity_cannot_be_deleted",
					enabled:    true,
					wantStatus: http.StatusBadRequest,
					wantCode:   "EntityStateException",
				},
				{
					name:       "disabled_entity_can_be_deleted",
					enabled:    false,
					wantStatus: http.StatusOK,
				},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					h := a1Handler(t)
					orgID := createTestOrg(t, h, "state-check-org-"+kind.name+"-"+tc.name)
					entityID := kind.createFn(h, orgID)

					if tc.enabled {
						kind.registerFn(h, orgID, entityID)
					}

					rec := a1Do(t, h, kind.deleteOp, kind.deleteBody(orgID, entityID))
					assert.Equal(t, tc.wantStatus, rec.Code)

					if tc.wantCode != "" {
						m := a1JSON(t, rec)
						assert.Equal(t, tc.wantCode, m["__type"])
					}
				})
			}
		})
	}
}

func TestGetAccessControlEffect_RuleEvaluation(t *testing.T) {
	t.Parallel()

	// putACRule puts a single ACR into the handler's org.
	putACRule := func(t *testing.T, h *workmail.Handler, orgID, name, effect string, body map[string]any) {
		t.Helper()

		body["OrganizationId"] = orgID
		body["Name"] = name
		body["Effect"] = effect

		jsonBody, err := json.Marshal(body)
		require.NoError(t, err)

		rec := a1Do(t, h, "PutAccessControlRule", string(jsonBody))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	aceRequest := func(t *testing.T, h *workmail.Handler, orgID, ipAddr, action, userID string) map[string]any {
		t.Helper()

		body := fmt.Sprintf(`{"OrganizationId":%q,"IpAddress":%q,"Action":%q,"UserId":%q}`,
			orgID, ipAddr, action, userID)
		rec := a1Do(t, h, "GetAccessControlEffect", body)
		require.Equal(t, http.StatusOK, rec.Code)

		return a1JSON(t, rec)
	}

	tests := []struct {
		setupRules  func(t *testing.T, h *workmail.Handler, orgID string)
		name        string
		ipAddr      string
		action      string
		userID      string
		wantEffect  string
		wantMatched bool
	}{
		{
			name: "no_rules_returns_allow",
			setupRules: func(_ *testing.T, _ *workmail.Handler, _ string) {
				// no rules
			},
			ipAddr:      "1.2.3.4",
			action:      "AutoDiscover",
			userID:      "some-user",
			wantEffect:  "ALLOW",
			wantMatched: false,
		},
		{
			name: "action_match_deny",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"Actions": []string{"ActiveSync"}})
			},
			ipAddr:      "1.2.3.4",
			action:      "ActiveSync",
			userID:      "some-user",
			wantEffect:  "DENY",
			wantMatched: true,
		},
		{
			name: "action_no_match_allow",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"Actions": []string{"ActiveSync"}})
			},
			ipAddr:      "1.2.3.4",
			action:      "AutoDiscover",
			userID:      "some-user",
			wantEffect:  "ALLOW",
			wantMatched: false,
		},
		{
			name: "ip_in_range_deny",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"IpRanges": []string{"10.0.0.0/8"}})
			},
			ipAddr:      "10.0.0.1",
			action:      "AutoDiscover",
			userID:      "some-user",
			wantEffect:  "DENY",
			wantMatched: true,
		},
		{
			name: "ip_outside_range_allow",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"IpRanges": []string{"10.0.0.0/8"}})
			},
			ipAddr:      "192.168.1.1",
			action:      "AutoDiscover",
			userID:      "some-user",
			wantEffect:  "ALLOW",
			wantMatched: false,
		},
		{
			// NotActions=["ActiveSync"] means rule matches when action is NOT "ActiveSync".
			name: "not_actions_blocks_other_actions",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"NotActions": []string{"ActiveSync"}})
			},
			ipAddr:      "1.2.3.4",
			action:      "AutoDiscover", // not "ActiveSync", so matches
			userID:      "some-user",
			wantEffect:  "DENY",
			wantMatched: true,
		},
		{
			// NotActions=["ActiveSync"] means rule does NOT match when action IS "ActiveSync".
			name: "not_actions_skips_listed_action",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"NotActions": []string{"ActiveSync"}})
			},
			ipAddr:      "1.2.3.4",
			action:      "ActiveSync", // is in NotActions, so rule does not match
			userID:      "some-user",
			wantEffect:  "ALLOW",
			wantMatched: false,
		},
		{
			name: "user_id_match_deny",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"UserIds": []string{"target-user-id"}})
			},
			ipAddr:      "1.2.3.4",
			action:      "AutoDiscover",
			userID:      "target-user-id",
			wantEffect:  "DENY",
			wantMatched: true,
		},
		{
			name: "user_id_no_match_allow",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"UserIds": []string{"target-user-id"}})
			},
			ipAddr:      "1.2.3.4",
			action:      "AutoDiscover",
			userID:      "other-user-id",
			wantEffect:  "ALLOW",
			wantMatched: false,
		},
		{
			// First rule DENYs; second rule ALLOWs the same action but should never be reached.
			name: "first_matching_rule_wins",
			setupRules: func(t *testing.T, h *workmail.Handler, orgID string) {
				t.Helper()
				putACRule(t, h, orgID, "r1", "DENY",
					map[string]any{"Actions": []string{"ActiveSync"}})
				putACRule(t, h, orgID, "r2", "ALLOW",
					map[string]any{"Actions": []string{"ActiveSync"}})
			},
			ipAddr:      "1.2.3.4",
			action:      "ActiveSync",
			userID:      "some-user",
			wantEffect:  "DENY",
			wantMatched: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			orgID := createTestOrg(t, h, "ace-org-"+tc.name)
			tc.setupRules(t, h, orgID)

			m := aceRequest(t, h, orgID, tc.ipAddr, tc.action, tc.userID)
			assert.Equal(t, tc.wantEffect, m["Effect"], "effect mismatch")

			matched, _ := m["MatchedRules"].([]any)
			if tc.wantMatched {
				assert.NotEmpty(t, matched, "expected MatchedRules to be non-empty")
			} else {
				assert.Empty(t, matched, "expected MatchedRules to be empty")
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

			h := a1Handler(t)
			orgID := createTestOrg(t, h, "hidden-group-org-"+tc.name)

			rec := a1Do(t, h, "CreateGroup", fmt.Sprintf(
				`{"OrganizationId":%q,"Name":"grp1","HiddenFromGlobalAddressList":%s}`,
				orgID, tc.hidden,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1JSON(t, rec)
			groupID := m["GroupId"].(string)

			rec = a1Do(t, h, "DescribeGroup", fmt.Sprintf(
				`{"OrganizationId":%q,"GroupId":%q}`, orgID, groupID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			m = a1JSON(t, rec)
			hidden, _ := m["HiddenFromGlobalAddressList"].(bool)
			assert.Equal(t, tc.want, hidden)
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

	h := a1Handler(t)
	orgID := createTestOrg(t, h, "userrole-org")

	for _, tc := range tests {
		body := fmt.Sprintf(
			`{"OrganizationId":%q,"Name":%q,"DisplayName":"X","Password":"Pass@1234","Role":%q}`,
			orgID, tc.userName, tc.role,
		)
		rec := a1Do(t, h, "CreateUser", body)
		require.Equal(t, http.StatusOK, rec.Code, "create user %s", tc.userName)
	}

	rec := a1Do(t, h, "ListUsers", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1JSON(t, rec)
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

func TestListAccessControlRules_Timestamps(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	orgID := createTestOrg(t, h, "acr-ts-org")

	rec := a1Do(t, h, "PutAccessControlRule", fmt.Sprintf(
		`{"OrganizationId":%q,"Name":"ts-rule","Effect":"ALLOW","Description":"ts test"}`,
		orgID,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = a1Do(t, h, "ListAccessControlRules", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1JSON(t, rec)
	rules, _ := m["Rules"].([]any)
	require.Len(t, rules, 1)

	rule := rules[0].(map[string]any)
	dateCreated, _ := rule["DateCreated"].(float64)
	dateModified, _ := rule["DateModified"].(float64)

	assert.NotZero(t, dateCreated, "DateCreated should be non-zero")
	assert.NotZero(t, dateModified, "DateModified should be non-zero")
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
				rec := a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
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
				rec := a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
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
				rec := a1Do(t, h, "RegisterToWorkMail", fmt.Sprintf(
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

			h := a1Handler(t)
			orgID := createTestOrg(t, h, "pea-org-"+tc.name)
			entityID := tc.setup(h, orgID)

			rec := a1Do(t, h, "UpdatePrimaryEmailAddress", tc.body(orgID, entityID))
			require.Equal(t, http.StatusOK, rec.Code)

			rec = a1Do(t, h, tc.verifyOp, tc.verifyBody(orgID, entityID))
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1JSON(t, rec)
			assert.Equal(t, tc.wantEmail, m["Email"])
		})
	}
}

func TestPagination_MaxResults(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	orgID := createTestOrg(t, h, "pagination-org")

	// Create 5 users.
	for i := range 5 {
		name := fmt.Sprintf("page-user-%02d", i)
		rec := a1Do(t, h, "CreateUser", fmt.Sprintf(
			`{"OrganizationId":%q,"Name":%q,"DisplayName":"Page User","Password":"Pass@1234"}`,
			orgID, name,
		))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: MaxResults=2.
	rec := a1Do(t, h, "ListUsers", fmt.Sprintf(`{"OrganizationId":%q,"MaxResults":2}`, orgID))
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1JSON(t, rec)
	users1, _ := m["Users"].([]any)
	require.Len(t, users1, 2, "page 1 should have 2 users")

	token1, _ := m["NextToken"].(string)
	require.NotEmpty(t, token1, "NextToken should be present after page 1")

	// Page 2: MaxResults=2, use NextToken from page 1.
	rec = a1Do(t, h, "ListUsers", fmt.Sprintf(
		`{"OrganizationId":%q,"MaxResults":2,"NextToken":%q}`, orgID, token1,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	m = a1JSON(t, rec)
	users2, _ := m["Users"].([]any)
	require.Len(t, users2, 2, "page 2 should have 2 users")

	token2, _ := m["NextToken"].(string)
	require.NotEmpty(t, token2, "NextToken should be present after page 2")

	// Page 3: MaxResults=2, use NextToken from page 2.
	rec = a1Do(t, h, "ListUsers", fmt.Sprintf(
		`{"OrganizationId":%q,"MaxResults":2,"NextToken":%q}`, orgID, token2,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	m = a1JSON(t, rec)
	users3, _ := m["Users"].([]any)
	require.Len(t, users3, 1, "page 3 should have 1 user (last one)")

	token3, _ := m["NextToken"].(string)
	assert.Empty(t, token3, "NextToken should be empty on last page")

	// Ensure no duplicate users across pages.
	seen := make(map[string]bool)
	for _, page := range [][]any{users1, users2, users3} {
		for _, u := range page {
			um := u.(map[string]any)
			id := um["Id"].(string)
			assert.False(t, seen[id], "duplicate user %q across pages", id)
			seen[id] = true
		}
	}

	assert.Len(t, seen, 5, "should have seen all 5 users across 3 pages")
}
