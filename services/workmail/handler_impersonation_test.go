package workmail_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- Impersonation Roles ---

func TestWorkMail_ImpersonationRoles(t *testing.T) {
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
				rec := doOp(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"AdminRole","Type":"FULL_ACCESS","Description":"Admin impersonation"}`,
					orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				roleID := m["ImpersonationRoleId"].(string)
				assert.NotEmpty(t, roleID)

				rec2 := doOp(t, h, "GetImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m2 := decodeJSON(t, rec2)
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
				doOp(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"Role1","Type":"FULL_ACCESS"}`, orgID,
				))
				doOp(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"Role2","Type":"READ_ONLY"}`, orgID,
				))
				rec := doOp(t, h, "ListImpersonationRoles", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				items, ok := m["Roles"].([]any)
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
				rec := doOp(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"OldName","Type":"FULL_ACCESS"}`, orgID,
				))
				m := decodeJSON(t, rec)
				roleID := m["ImpersonationRoleId"].(string)
				rec2 := doOp(t, h, "UpdateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q,"Name":"NewName","Type":"READ_ONLY"}`,
					orgID, roleID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				rec3 := doOp(t, h, "GetImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
				))
				m3 := decodeJSON(t, rec3)
				assert.Equal(t, "NewName", m3["Name"])
				assert.Equal(t, "READ_ONLY", m3["Type"])
			},
		},
		{
			name: "delete_impersonation_role",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "delimperorg")
				rec := doOp(t, h, "CreateImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"Name":"TempRole","Type":"FULL_ACCESS"}`, orgID,
				))
				m := decodeJSON(t, rec)
				roleID := m["ImpersonationRoleId"].(string)
				rec2 := doOp(t, h, "DeleteImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				rec3 := doOp(t, h, "GetImpersonationRole", fmt.Sprintf(
					`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
				))
				assert.Equal(t, http.StatusBadRequest, rec3.Code)
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

// ---- Impersonation Role Effect ----

func TestGetImpersonationRoleEffect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ruleEffect  string
		queryUser   string
		wantEffect  string
		targetUsers []string
	}{
		{
			name:        "allow specific user",
			ruleEffect:  "ALLOW",
			targetUsers: []string{"user-alpha"},
			queryUser:   "user-alpha",
			wantEffect:  "ALLOW",
		},
		{
			name:        "deny unmatched user",
			ruleEffect:  "ALLOW",
			targetUsers: []string{"user-alpha"},
			queryUser:   "user-beta",
			wantEffect:  "DENY",
		},
		{
			name:        "deny rule matched",
			ruleEffect:  "DENY",
			targetUsers: []string{"user-gamma"},
			queryUser:   "user-gamma",
			wantEffect:  "DENY",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "imp-effect-org")

			targetJSON, _ := json.Marshal(tc.targetUsers)
			roleBody := fmt.Sprintf(
				`{"OrganizationId":%q,"Name":"test-role","Type":"FULL_ACCESS","Rules":[{"ImpersonationRuleId":"rule-1","Name":"rule-one","Effect":%q,"TargetUsers":%s}]}`, //nolint:lll // existing issue.
				orgID,
				tc.ruleEffect,
				string(targetJSON),
			)
			rec := doOp(t, h, "CreateImpersonationRole", roleBody)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m := decodeJSON(t, rec)
			roleID := m["ImpersonationRoleId"].(string)

			rec = doOp(t, h, "GetImpersonationRoleEffect", fmt.Sprintf(
				`{"OrganizationId":%q,"ImpersonationRoleId":%q,"TargetUser":%q}`,
				orgID, roleID, tc.queryUser,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m = decodeJSON(t, rec)
			assert.Equal(t, tc.wantEffect, m["Effect"])
			assert.Equal(t, "FULL_ACCESS", m["Type"])
		})
	}
}

// ---- Assume Impersonation Role ----

func TestAssumeImpersonationRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		roleType  string
		wantToken bool
	}{
		{name: "full access role", roleType: "FULL_ACCESS", wantToken: true},
		{name: "read only role", roleType: "READ_ONLY", wantToken: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "assume-imp-org")

			rec := doOp(t, h, "CreateImpersonationRole", fmt.Sprintf(
				`{"OrganizationId":%q,"Name":"test-role","Type":%q,"Rules":[]}`, orgID, tc.roleType,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m := decodeJSON(t, rec)
			roleID := m["ImpersonationRoleId"].(string)

			rec = doOp(t, h, "AssumeImpersonationRole", fmt.Sprintf(
				`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, orgID, roleID,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m = decodeJSON(t, rec)
			if tc.wantToken {
				token, ok := m["Token"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, token)
				expiresIn, ok := m["ExpiresIn"].(float64)
				require.True(t, ok)
				assert.Greater(t, expiresIn, float64(0))
			}
		})
	}
}

func TestAssumeImpersonationRoleErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		orgID     string
		roleID    string
		wantError string
	}{
		{
			// AssumeImpersonationRole's own error model declares
			// OrganizationNotFoundException for this, not the shared
			// EntityNotFoundException sentinel (gopherstack-6flj/uox6).
			name:      "org not found",
			orgID:     "org-nonexistent0001",
			roleID:    "role-abc",
			wantError: "OrganizationNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doOp(t, h, "AssumeImpersonationRole", fmt.Sprintf(
				`{"OrganizationId":%q,"ImpersonationRoleId":%q}`, tc.orgID, tc.roleID,
			))
			require.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeJSON(t, rec)
			assert.Contains(t, m["__type"], tc.wantError)
		})
	}
}
