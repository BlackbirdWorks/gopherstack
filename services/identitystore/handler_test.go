package identitystore_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func doRequest(
	t *testing.T,
	h *identitystore.Handler,
	method, path string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
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

// TestUserCRUD exercises CreateUser, DescribeUser, ListUsers, UpdateUser and DeleteUser.
func TestUserCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystore.Handler)
		name string
	}{
		{
			name: "create_user",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{
						"UserName":    "john.doe",
						"DisplayName": "John Doe",
						"Name": map[string]any{
							"GivenName":  "John",
							"FamilyName": "Doe",
						},
					})

				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				assert.NotEmpty(t, resp["UserId"])
				assert.Equal(t, testStoreID, resp["IdentityStoreId"])
			},
		},
		{
			name: "describe_user",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "jane.doe", "DisplayName": "Jane Doe"})
				require.Equal(t, http.StatusOK, createRec.Code)

				createResp := parseResponse(t, createRec)
				userID := createResp["UserId"].(string)

				rec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/users/%s", testStoreID, userID), nil)

				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, userID, resp["UserId"])
				assert.Equal(t, "jane.doe", resp["UserName"])
				assert.Equal(t, "Jane Doe", resp["DisplayName"])
			},
		},
		{
			name: "describe_user_not_found",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/users/nonexistent", testStoreID), nil)

				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_users",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "user1", "DisplayName": "User One"})
				doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "user2", "DisplayName": "User Two"})

				rec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/users", testStoreID), nil)

				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				users, ok := resp["Users"].([]any)
				require.True(t, ok)
				assert.Len(t, users, 2)
			},
		},
		{
			name: "update_user",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "update.me", "DisplayName": "Old Name"})
				require.Equal(t, http.StatusOK, createRec.Code)

				createResp := parseResponse(t, createRec)
				userID := createResp["UserId"].(string)

				rec := doRequest(t, h, http.MethodPatch,
					fmt.Sprintf("/identitystores/%s/users/%s", testStoreID, userID),
					map[string]any{
						"Operations": []map[string]any{
							{"AttributePath": "displayName", "AttributeValue": "New Name"},
						},
					})

				assert.Equal(t, http.StatusOK, rec.Code)

				descRec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/users/%s", testStoreID, userID), nil)
				descResp := parseResponse(t, descRec)
				assert.Equal(t, "New Name", descResp["DisplayName"])
			},
		},
		{
			name: "delete_user",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "delete.me"})
				require.Equal(t, http.StatusOK, createRec.Code)

				createResp := parseResponse(t, createRec)
				userID := createResp["UserId"].(string)

				rec := doRequest(t, h, http.MethodDelete,
					fmt.Sprintf("/identitystores/%s/users/%s", testStoreID, userID), nil)

				assert.Equal(t, http.StatusNoContent, rec.Code)

				descRec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/users/%s", testStoreID, userID), nil)
				assert.Equal(t, http.StatusNotFound, descRec.Code)
			},
		},
		{
			name: "get_user_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "lookup.me", "DisplayName": "Lookup User"})
				require.Equal(t, http.StatusOK, createRec.Code)

				createResp := parseResponse(t, createRec)
				wantUserID := createResp["UserId"].(string)

				rec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users/id", testStoreID),
					map[string]any{
						"AlternateIdentifier": map[string]any{
							"UniqueAttribute": map[string]any{
								"AttributePath":  "userName",
								"AttributeValue": "lookup.me",
							},
						},
					})

				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, wantUserID, resp["UserId"])
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

// TestGroupCRUD exercises CreateGroup, DescribeGroup, ListGroups, UpdateGroup and DeleteGroup.
func TestGroupCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystore.Handler)
		name string
	}{
		{
			name: "create_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{
						"DisplayName": "Engineering",
						"Description": "Engineering team",
					})

				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				assert.NotEmpty(t, resp["GroupId"])
				assert.Equal(t, testStoreID, resp["IdentityStoreId"])
			},
		},
		{
			name: "describe_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Product", "Description": "Product team"})
				require.Equal(t, http.StatusOK, createRec.Code)

				createResp := parseResponse(t, createRec)
				groupID := createResp["GroupId"].(string)

				rec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/groups/%s", testStoreID, groupID), nil)

				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, groupID, resp["GroupId"])
				assert.Equal(t, "Product", resp["DisplayName"])
			},
		},
		{
			name: "list_groups",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Team A"})
				doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Team B"})

				rec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID), nil)

				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				groups, ok := resp["Groups"].([]any)
				require.True(t, ok)
				assert.Len(t, groups, 2)
			},
		},
		{
			name: "delete_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Temp Group"})
				require.Equal(t, http.StatusOK, createRec.Code)

				createResp := parseResponse(t, createRec)
				groupID := createResp["GroupId"].(string)

				rec := doRequest(t, h, http.MethodDelete,
					fmt.Sprintf("/identitystores/%s/groups/%s", testStoreID, groupID), nil)

				assert.Equal(t, http.StatusNoContent, rec.Code)
			},
		},
		{
			name: "get_group_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Lookup Group"})
				require.Equal(t, http.StatusOK, createRec.Code)

				createResp := parseResponse(t, createRec)
				wantGroupID := createResp["GroupId"].(string)

				rec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups/id", testStoreID),
					map[string]any{
						"AlternateIdentifier": map[string]any{
							"UniqueAttribute": map[string]any{
								"AttributePath":  "displayName",
								"AttributeValue": "Lookup Group",
							},
						},
					})

				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, wantGroupID, resp["GroupId"])
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

				userRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "member.user", "DisplayName": "Member"})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Test Group"})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				memberRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/memberships", testStoreID),
					map[string]any{
						"GroupId":  groupID,
						"MemberId": map[string]any{"UserId": userID},
					})

				assert.Equal(t, http.StatusOK, memberRec.Code)
				memberResp := parseResponse(t, memberRec)
				membershipID := memberResp["MembershipId"].(string)
				assert.NotEmpty(t, membershipID)

				descRec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/memberships/%s", testStoreID, membershipID), nil)
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

				groupRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "List Membership Group"})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				for i := range 2 {
					userRec := doRequest(t, h, http.MethodPost,
						fmt.Sprintf("/identitystores/%s/users", testStoreID),
						map[string]any{"UserName": fmt.Sprintf("list.user%d", i)})
					require.Equal(t, http.StatusOK, userRec.Code)
					userID := parseResponse(t, userRec)["UserId"].(string)

					doRequest(t, h, http.MethodPost,
						fmt.Sprintf("/identitystores/%s/memberships", testStoreID),
						map[string]any{
							"GroupId":  groupID,
							"MemberId": map[string]any{"UserId": userID},
						})
				}

				rec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/groups/%s/memberships", testStoreID, groupID), nil)
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

				userRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "del.member"})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Del Group"})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				memberRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/memberships", testStoreID),
					map[string]any{
						"GroupId":  groupID,
						"MemberId": map[string]any{"UserId": userID},
					})
				require.Equal(t, http.StatusOK, memberRec.Code)
				membershipID := parseResponse(t, memberRec)["MembershipId"].(string)

				delRec := doRequest(t, h, http.MethodDelete,
					fmt.Sprintf("/identitystores/%s/memberships/%s", testStoreID, membershipID), nil)
				assert.Equal(t, http.StatusNoContent, delRec.Code)

				descRec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/memberships/%s", testStoreID, membershipID), nil)
				assert.Equal(t, http.StatusNotFound, descRec.Code)
			},
		},
		{
			name: "list_memberships_for_member",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				userRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "multi.group.user"})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				for i := range 2 {
					groupRec := doRequest(t, h, http.MethodPost,
						fmt.Sprintf("/identitystores/%s/groups", testStoreID),
						map[string]any{"DisplayName": fmt.Sprintf("Member Group %d", i)})
					require.Equal(t, http.StatusOK, groupRec.Code)
					groupID := parseResponse(t, groupRec)["GroupId"].(string)

					doRequest(t, h, http.MethodPost,
						fmt.Sprintf("/identitystores/%s/memberships", testStoreID),
						map[string]any{
							"GroupId":  groupID,
							"MemberId": map[string]any{"UserId": userID},
						})
				}

				rec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/memberships-for-member", testStoreID),
					map[string]any{
						"MemberId": map[string]any{"UserId": userID},
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

// TestHandlerMetadata verifies Name, GetSupportedOperations, and routing methods.
func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, "IdentityStore", h.Name())
	assert.Equal(t, "identitystore", h.ChaosServiceName())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.Len(t, h.GetSupportedOperations(), 19)
	assert.Equal(t, service.PriorityPathVersioned, h.MatchPriority())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

// TestRouteMatcher verifies that RouteMatcher accepts /identitystores/ paths.
func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "identitystores_path",
			path: "/identitystores/d-1234567890/users",
			want: true,
		},
		{
			name: "other_path",
			path: "/backup-vaults",
			want: false,
		},
		{
			name: "partial_prefix",
			path: "/s3/buckets",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
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
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
	}{
		{
			name:         "create_user",
			method:       http.MethodPost,
			path:         fmt.Sprintf("/identitystores/%s/users", testStoreID),
			wantOp:       "CreateUser",
			wantResource: testStoreID,
		},
		{
			name:         "describe_user",
			method:       http.MethodGet,
			path:         fmt.Sprintf("/identitystores/%s/users/user-001", testStoreID),
			wantOp:       "DescribeUser",
			wantResource: "user-001",
		},
		{
			name:         "create_group",
			method:       http.MethodPost,
			path:         fmt.Sprintf("/identitystores/%s/groups", testStoreID),
			wantOp:       "CreateGroup",
			wantResource: testStoreID,
		},
		{
			name:         "list_memberships_for_member",
			method:       http.MethodPost,
			path:         fmt.Sprintf("/identitystores/%s/memberships-for-member", testStoreID),
			wantOp:       "ListGroupMembershipsForMember",
			wantResource: testStoreID,
		},
		{
			name:         "is_member_in_groups",
			method:       http.MethodPost,
			path:         fmt.Sprintf("/identitystores/%s/IsMemberInGroups", testStoreID),
			wantOp:       "IsMemberInGroups",
			wantResource: testStoreID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
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

// TestUpdateGroupAndGetGroupMembershipID covers UpdateGroup and GetGroupMembershipId.
func TestUpdateGroupAndGetGroupMembershipID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystore.Handler)
		name string
	}{
		{
			name: "update_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "OldName", "Description": "Old"})
				require.Equal(t, http.StatusOK, createRec.Code)
				groupID := parseResponse(t, createRec)["GroupId"].(string)

				patchRec := doRequest(t, h, http.MethodPatch,
					fmt.Sprintf("/identitystores/%s/groups/%s", testStoreID, groupID),
					map[string]any{
						"Operations": []map[string]any{
							{"AttributePath": "displayName", "AttributeValue": "NewName"},
							{"AttributePath": "description", "AttributeValue": "New desc"},
						},
					})
				assert.Equal(t, http.StatusOK, patchRec.Code)

				descRec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/groups/%s", testStoreID, groupID), nil)
				desc := parseResponse(t, descRec)
				assert.Equal(t, "NewName", desc["DisplayName"])
				assert.Equal(t, "New desc", desc["Description"])
			},
		},
		{
			name: "get_group_membership_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				userRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "gm.user"})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "GM Group"})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				memberRec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/memberships", testStoreID),
					map[string]any{
						"GroupId":  groupID,
						"MemberId": map[string]any{"UserId": userID},
					})
				require.Equal(t, http.StatusOK, memberRec.Code)
				wantMembershipID := parseResponse(t, memberRec)["MembershipId"].(string)

				rec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/memberships/id", testStoreID),
					map[string]any{
						"GroupId":  groupID,
						"MemberId": map[string]any{"UserId": userID},
					})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResponse(t, rec)
				assert.Equal(t, wantMembershipID, resp["MembershipId"])
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

// TestIsMemberInGroups verifies the IsMemberInGroups endpoint.
func TestIsMemberInGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create user and two groups.
	userRec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/users", testStoreID),
		map[string]any{"UserName": "ismember.user"})
	require.Equal(t, http.StatusOK, userRec.Code)
	userID := parseResponse(t, userRec)["UserId"].(string)

	groupIDs := make([]string, 3)
	for i := range 3 {
		gr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/identitystores/%s/groups", testStoreID),
			map[string]any{"DisplayName": fmt.Sprintf("IsGroup %d", i)})
		require.Equal(t, http.StatusOK, gr.Code)
		groupIDs[i] = parseResponse(t, gr)["GroupId"].(string)
	}

	// Add user to first two groups only.
	for _, gid := range groupIDs[:2] {
		doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/identitystores/%s/memberships", testStoreID),
			map[string]any{
				"GroupId":  gid,
				"MemberId": map[string]any{"UserId": userID},
			})
	}

	rec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/IsMemberInGroups", testStoreID),
		map[string]any{
			"MemberId": map[string]any{"UserId": userID},
			"GroupIds": groupIDs,
		})
	assert.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	results, ok := resp["Results"].([]any)
	require.True(t, ok)
	require.Len(t, results, 3)
}

// TestErrorCases covers 404 and conflict error paths.
func TestErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run        func(t *testing.T, h *identitystore.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "describe_nonexistent_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/groups/does-not-exist", testStoreID), nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_nonexistent_user",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodDelete,
					fmt.Sprintf("/identitystores/%s/users/does-not-exist", testStoreID), nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_nonexistent_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodDelete,
					fmt.Sprintf("/identitystores/%s/groups/does-not-exist", testStoreID), nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "describe_nonexistent_membership",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet,
					fmt.Sprintf("/identitystores/%s/memberships/does-not-exist", testStoreID), nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "duplicate_user",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "dup.user"})

				rec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/users", testStoreID),
					map[string]any{"UserName": "dup.user"})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "duplicate_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Dup Group"})

				rec := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/identitystores/%s/groups", testStoreID),
					map[string]any{"DisplayName": "Dup Group"})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "unsupported_path",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet,
					"/identitystores/", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
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

// TestUpdateUserAttributes verifies updating specific user name attributes.
func TestUpdateUserAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		attrPath  string
		attrValue string
	}{
		{name: "update_nickname", attrPath: "nickName", attrValue: "nick"},
		{name: "update_title", attrPath: "title", attrValue: "Dr."},
		{name: "update_locale", attrPath: "locale", attrValue: "en-US"},
		{name: "update_preferredLanguage", attrPath: "preferredLanguage", attrValue: "English"},
		{name: "update_timezone", attrPath: "timezone", attrValue: "UTC"},
		{name: "update_userType", attrPath: "userType", attrValue: "employee"},
		{name: "update_name_givenName", attrPath: "name.givenName", attrValue: "Bob"},
		{name: "update_name_familyName", attrPath: "name.familyName", attrValue: "Smith"},
		{name: "update_name_middleName", attrPath: "name.middleName", attrValue: "M"},
		{name: "update_name_formatted", attrPath: "name.formatted", attrValue: "Bob M Smith"},
		{name: "update_name_honorificPrefix", attrPath: "name.honorificPrefix", attrValue: "Mr."},
		{name: "update_name_honorificSuffix", attrPath: "name.honorificSuffix", attrValue: "Jr."},
		{name: "update_profileUrl", attrPath: "profileUrl", attrValue: "http://example.com"},
		{name: "update_username", attrPath: "username", attrValue: "new.name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			createRec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/identitystores/%s/users", testStoreID),
				map[string]any{"UserName": "attr-user-" + tt.name, "DisplayName": "Attr User"})
			require.Equal(t, http.StatusOK, createRec.Code)
			userID := parseResponse(t, createRec)["UserId"].(string)

			patchRec := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/identitystores/%s/users/%s", testStoreID, userID),
				map[string]any{
					"Operations": []map[string]any{
						{"AttributePath": tt.attrPath, "AttributeValue": tt.attrValue},
					},
				})
			assert.Equal(t, http.StatusOK, patchRec.Code)
		})
	}
}

// TestGetUserID_WithUniqueAttribute verifies GetUserId with UniqueAttribute.
func TestGetUserID_WithUniqueAttribute(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create user first.
	createRec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/users", testStoreID),
		map[string]any{"UserName": "unique.user"})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/users/id", testStoreID),
		map[string]any{
			"AlternateIdentifier": map[string]any{
				"UniqueAttribute": map[string]any{
					"AttributePath":  "userName",
					"AttributeValue": "unique.user",
				},
			},
		})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.NotEmpty(t, resp["UserId"])
}

// TestGetGroupID_WithUniqueAttribute verifies GetGroupId with UniqueAttribute.
func TestGetGroupID_WithUniqueAttribute(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create group first.
	createRec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/groups", testStoreID),
		map[string]any{"DisplayName": "Unique Group"})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/groups/id", testStoreID),
		map[string]any{
			"AlternateIdentifier": map[string]any{
				"UniqueAttribute": map[string]any{
					"AttributePath":  "displayName",
					"AttributeValue": "Unique Group",
				},
			},
		})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.NotEmpty(t, resp["GroupId"])
}

// TestDeleteGroupMembership verifies deleting a membership.
func TestDeleteGroupMembership(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	userRec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/users", testStoreID),
		map[string]any{"UserName": "del.mem.user"})
	require.Equal(t, http.StatusOK, userRec.Code)
	userID := parseResponse(t, userRec)["UserId"].(string)

	groupRec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/groups", testStoreID),
		map[string]any{"DisplayName": "Del Mem Group"})
	require.Equal(t, http.StatusOK, groupRec.Code)
	groupID := parseResponse(t, groupRec)["GroupId"].(string)

	memRec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/identitystores/%s/memberships", testStoreID),
		map[string]any{
			"GroupId":  groupID,
			"MemberId": map[string]any{"UserId": userID},
		})
	require.Equal(t, http.StatusOK, memRec.Code)
	membershipID := parseResponse(t, memRec)["MembershipId"].(string)

	delRec := doRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/identitystores/%s/memberships/%s", testStoreID, membershipID), nil)
	assert.Equal(t, http.StatusNoContent, delRec.Code)
}

// TestInvalidBodyErrors verifies bad JSON returns 400.
func TestInvalidBodyErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"create_user_bad_body", http.MethodPost, fmt.Sprintf("/identitystores/%s/users", testStoreID)},
		{"update_user_bad_body", http.MethodPatch, fmt.Sprintf("/identitystores/%s/users/u-1", testStoreID)},
		{"get_user_id_bad_body", http.MethodPost, fmt.Sprintf("/identitystores/%s/users/id", testStoreID)},
		{"create_group_bad_body", http.MethodPost, fmt.Sprintf("/identitystores/%s/groups", testStoreID)},
		{"update_group_bad_body", http.MethodPatch, fmt.Sprintf("/identitystores/%s/groups/g-1", testStoreID)},
		{"get_group_id_bad_body", http.MethodPost, fmt.Sprintf("/identitystores/%s/groups/id", testStoreID)},
		{"create_membership_bad_body", http.MethodPost, fmt.Sprintf("/identitystores/%s/memberships", testStoreID)},
		{"get_membership_id_bad_body", http.MethodPost, fmt.Sprintf("/identitystores/%s/memberships/id", testStoreID)},
		{"is_member_bad_body", http.MethodPost, fmt.Sprintf("/identitystores/%s/IsMemberInGroups", testStoreID)},
		{
			"list_memberships_for_member_bad_body",
			http.MethodPost,
			fmt.Sprintf("/identitystores/%s/memberships-for-member", testStoreID),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewBufferString("{bad json"))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
