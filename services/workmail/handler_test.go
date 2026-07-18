package workmail_test

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

func newTestHandler(t *testing.T) *workmail.Handler {
	t.Helper()

	return workmail.NewHandler(workmail.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doOp(t *testing.T, h *workmail.Handler, action, body string) *httptest.ResponseRecorder {
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

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// createTestOrg creates a test organization and returns its ID.
func createTestOrg(t *testing.T, h *workmail.Handler, alias string) string {
	t.Helper()

	rec := doOp(t, h, "CreateOrganization", fmt.Sprintf(`{"Alias":%q}`, alias))
	require.Equal(t, http.StatusOK, rec.Code)
	m := decodeJSON(t, rec)
	orgID, ok := m["OrganizationId"].(string)
	require.True(t, ok, "OrganizationId missing")
	require.NotEmpty(t, orgID)

	return orgID
}

// createTestUser creates a user and returns their ID.
func createTestUser(t *testing.T, h *workmail.Handler, orgID, name, displayName string) string {
	t.Helper()

	rec := doOp(t, h, "CreateUser", fmt.Sprintf(
		`{"OrganizationId":%q,"Name":%q,"DisplayName":%q,"Password":"Pass@1234"}`, orgID, name, displayName,
	))
	require.Equal(t, http.StatusOK, rec.Code)
	m := decodeJSON(t, rec)
	userID, ok := m["UserId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, userID)

	return userID
}

// createTestGroup creates a group and returns its ID.
func createTestGroup(t *testing.T, h *workmail.Handler, orgID, name string) string {
	t.Helper()

	rec := doOp(t, h, "CreateGroup", fmt.Sprintf(
		`{"OrganizationId":%q,"Name":%q}`, orgID, name,
	))
	require.Equal(t, http.StatusOK, rec.Code)
	m := decodeJSON(t, rec)
	groupID, ok := m["GroupId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, groupID)

	return groupID
}

// createTestResource creates a resource and returns its ID.
func createTestResource(t *testing.T, h *workmail.Handler, orgID, name, resType string) string {
	t.Helper()

	rec := doOp(t, h, "CreateResource", fmt.Sprintf(
		`{"OrganizationId":%q,"Name":%q,"Type":%q}`, orgID, name, resType,
	))
	require.Equal(t, http.StatusOK, rec.Code)
	m := decodeJSON(t, rec)
	resID, ok := m["ResourceId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, resID)

	return resID
}

// --- Protocol accuracy ---

func TestWorkMail_Protocol_ContentType(t *testing.T) {
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

			h := newTestHandler(t)
			rec := doOp(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantCT)
		})
	}
}

func TestWorkMail_Protocol_NonPost_Returns405(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/notroot", nil)
	req.Header.Set("X-Amz-Target", "WorkMailService.ListOrganizations")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestWorkMail_Protocol_MissingTarget_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWorkMail_Protocol_GetRoot_Returns_OpList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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

func TestWorkMail_Protocol_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doOp(t, h, "DescribeOrganization", `{"OrganizationId":"m-nope"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := decodeJSON(t, rec)
	assert.NotEmpty(t, m["__type"], "__type field missing from error envelope")
	assert.NotEmpty(t, m["message"], "message field missing from error envelope")
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
				rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
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
				rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
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
				rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
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

					h := newTestHandler(t)
					orgID := createTestOrg(t, h, "state-check-org-"+kind.name+"-"+tc.name)
					entityID := kind.createFn(h, orgID)

					if tc.enabled {
						kind.registerFn(h, orgID, entityID)
					}

					rec := doOp(t, h, kind.deleteOp, kind.deleteBody(orgID, entityID))
					assert.Equal(t, tc.wantStatus, rec.Code)

					if tc.wantCode != "" {
						m := decodeJSON(t, rec)
						assert.Equal(t, tc.wantCode, m["__type"])
					}
				})
			}
		})
	}
}

func TestPagination_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "pagination-org")

	// Create 5 users.
	for i := range 5 {
		name := fmt.Sprintf("page-user-%02d", i)
		rec := doOp(t, h, "CreateUser", fmt.Sprintf(
			`{"OrganizationId":%q,"Name":%q,"DisplayName":"Page User","Password":"Pass@1234"}`,
			orgID, name,
		))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: MaxResults=2.
	rec := doOp(t, h, "ListUsers", fmt.Sprintf(`{"OrganizationId":%q,"MaxResults":2}`, orgID))
	require.Equal(t, http.StatusOK, rec.Code)

	m := decodeJSON(t, rec)
	users1, _ := m["Users"].([]any)
	require.Len(t, users1, 2, "page 1 should have 2 users")

	token1, _ := m["NextToken"].(string)
	require.NotEmpty(t, token1, "NextToken should be present after page 1")

	// Page 2: MaxResults=2, use NextToken from page 1.
	rec = doOp(t, h, "ListUsers", fmt.Sprintf(
		`{"OrganizationId":%q,"MaxResults":2,"NextToken":%q}`, orgID, token1,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	m = decodeJSON(t, rec)
	users2, _ := m["Users"].([]any)
	require.Len(t, users2, 2, "page 2 should have 2 users")

	token2, _ := m["NextToken"].(string)
	require.NotEmpty(t, token2, "NextToken should be present after page 2")

	// Page 3: MaxResults=2, use NextToken from page 2.
	rec = doOp(t, h, "ListUsers", fmt.Sprintf(
		`{"OrganizationId":%q,"MaxResults":2,"NextToken":%q}`, orgID, token2,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	m = decodeJSON(t, rec)
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
