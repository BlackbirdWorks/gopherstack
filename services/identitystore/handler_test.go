package identitystore_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

const testStoreID = "d-1234567890"

func newTestHandler() *identitystore.Handler {
	backend := identitystore.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return identitystore.NewHandler(backend)
}

// doRequest sends a JSON protocol request with X-Amz-Target: AWSIdentityStore.{op}.
func doRequest(
	t *testing.T,
	h *identitystore.Handler,
	op string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Target", "AWSIdentityStore."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestHandlerMetadata verifies Name, GetSupportedOperations, and routing methods.
func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, "IdentityStore", h.Name())
	assert.Equal(t, "identitystore", h.ChaosServiceName())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.Len(t, h.GetSupportedOperations(), 19)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

// TestRouteMatcher verifies that RouteMatcher accepts requests with the correct X-Amz-Target header.
func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "valid_amz_target",
			target: "AWSIdentityStore.CreateUser",
			want:   true,
		},
		{
			name:   "other_service_target",
			target: "DynamoDB_20120810.GetItem",
			want:   false,
		},
		{
			name:   "no_target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)

			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestExtractOperationAndResource verifies ExtractOperation and ExtractResource.
func TestExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		body         map[string]any
		name         string
		op           string
		wantOp       string
		wantResource string
	}{
		{
			name:         "create_user",
			op:           "CreateUser",
			body:         map[string]any{"IdentityStoreId": testStoreID},
			wantOp:       "CreateUser",
			wantResource: testStoreID,
		},
		{
			name:         "describe_user",
			op:           "DescribeUser",
			body:         map[string]any{"IdentityStoreId": testStoreID, "UserId": "user-001"},
			wantOp:       "DescribeUser",
			wantResource: testStoreID,
		},
		{
			name:         "create_group",
			op:           "CreateGroup",
			body:         map[string]any{"IdentityStoreId": testStoreID},
			wantOp:       "CreateGroup",
			wantResource: testStoreID,
		},
		{
			name:         "list_memberships_for_member",
			op:           "ListGroupMembershipsForMember",
			body:         map[string]any{"IdentityStoreId": testStoreID},
			wantOp:       "ListGroupMembershipsForMember",
			wantResource: testStoreID,
		},
		{
			name:         "is_member_in_groups",
			op:           "IsMemberInGroups",
			body:         map[string]any{"IdentityStoreId": testStoreID},
			wantOp:       "IsMemberInGroups",
			wantResource: testStoreID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("X-Amz-Target", "AWSIdentityStore."+tt.op)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)

			op := h.ExtractOperation(c)
			resource := h.ExtractResource(c)

			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

// TestInvalidBodyErrors verifies bad JSON returns 400 across every operation.
func TestInvalidBodyErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name string
		op   string
	}{
		{"create_user_bad_body", "CreateUser"},
		{"update_user_bad_body", "UpdateUser"},
		{"get_user_id_bad_body", "GetUserId"},
		{"create_group_bad_body", "CreateGroup"},
		{"update_group_bad_body", "UpdateGroup"},
		{"get_group_id_bad_body", "GetGroupId"},
		{"create_membership_bad_body", "CreateGroupMembership"},
		{"get_membership_id_bad_body", "GetGroupMembershipId"},
		{"is_member_bad_body", "IsMemberInGroups"},
		{"list_memberships_for_member_bad_body", "ListGroupMembershipsForMember"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString("{bad json"))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Amz-Target", "AWSIdentityStore."+tt.op)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestUnknownOperation verifies an unrecognized X-Amz-Target operation returns 400.
func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AWSIdentityStore.UnknownOp")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestMissingAmzTarget verifies a request with no X-Amz-Target header returns 400.
func TestMissingAmzTarget(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestPersistenceSnapshotRestore verifies Snapshot/Restore round-trips users and groups,
// including the byUserName index used by GetUserId.
func TestPersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "persist.user",
	})
	doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "Persist Group",
	})

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	listRec := doRequest(t, h2, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	assert.Equal(t, http.StatusOK, listRec.Code)
	users := parseResponse(t, listRec)["Users"].([]any)
	assert.Len(t, users, 1)

	// Verify name index works after restore.
	rec := doRequest(t, h2, "GetUserId", map[string]any{
		"IdentityStoreId": testStoreID,
		"AlternateIdentifier": map[string]any{
			"UniqueAttribute": map[string]any{
				"AttributePath":  "userName",
				"AttributeValue": "persist.user",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
