package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- Resources ---

func TestWorkMail_Resources_Lifecycle(t *testing.T) {
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
				rec := doOp(t, h, "DescribeResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q}`, orgID, resID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
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
				rec := doOp(t, h, "ListResources", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
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
				rec := doOp(t, h, "DeleteResource", fmt.Sprintf(
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
				rec := doOp(t, h, "AssociateDelegateToResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q,"EntityId":%q}`, orgID, resID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "ListResourceDelegates", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q}`, orgID, resID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := decodeJSON(t, rec2)
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
				doOp(t, h, "AssociateDelegateToResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q,"EntityId":%q}`, orgID, resID, userID,
				))
				rec := doOp(t, h, "DisassociateDelegateFromResource", fmt.Sprintf(
					`{"OrganizationId":%q,"ResourceId":%q,"EntityId":%q}`, orgID, resID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
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

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "resource-val-org")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doOp(t, h, "CreateResource", fmt.Sprintf(tc.body, orgID))
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantCode != "" {
				m := decodeJSON(t, rec)
				assert.Equal(t, tc.wantCode, m["__type"])
			}
		})
	}
}

// TestListResources_Filters locks the ListResourcesInput.Filters wire
// behavior (NamePrefix/PrimaryEmailPrefix/State): previously accepted on the
// wire but silently ignored, so a real client's prefix/state search would
// have gotten back the full unfiltered page.
func TestListResources_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "listresources-filter-org")

	createTestResource(t, h, orgID, "room-north", "ROOM")
	southID := createTestResource(t, h, orgID, "room-south", "ROOM")
	require.Equal(t, http.StatusOK, doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"Email":"room-south@filter.example"}`, orgID, southID,
	)).Code)
	createTestResource(t, h, orgID, "projector-1", "EQUIPMENT")

	tests := []struct {
		filters   string
		name      string
		wantNames []string
	}{
		{name: "name_prefix", filters: `{"NamePrefix":"room-"}`, wantNames: []string{"room-north", "room-south"}},
		{name: "email_prefix", filters: `{"PrimaryEmailPrefix":"room-south@"}`, wantNames: []string{"room-south"}},
		{name: "state_enabled", filters: `{"State":"ENABLED"}`, wantNames: []string{"room-south"}},
		{
			name: "state_disabled", filters: `{"State":"DISABLED"}`,
			wantNames: []string{"room-north", "projector-1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doOp(t, h, "ListResources", fmt.Sprintf(`{"OrganizationId":%q,"Filters":%s}`, orgID, tc.filters))
			require.Equal(t, http.StatusOK, rec.Code)

			m := decodeJSON(t, rec)
			resources, _ := m["Resources"].([]any)
			gotNames := make([]string, 0, len(resources))
			for _, r := range resources {
				gotNames = append(gotNames, r.(map[string]any)["Name"].(string))
			}
			assert.ElementsMatch(t, tc.wantNames, gotNames)
		})
	}
}
