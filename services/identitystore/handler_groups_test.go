package identitystore_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

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

				rec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Engineering",
					"Description":     "Engineering team",
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

				createRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Product",
					"Description":     "Product team",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				groupID := parseResponse(t, createRec)["GroupId"].(string)

				rec := doRequest(t, h, "DescribeGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
				})

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

				doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Team A",
				})
				doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Team B",
				})

				rec := doRequest(t, h, "ListGroups", map[string]any{
					"IdentityStoreId": testStoreID,
				})

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

				createRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Temp Group",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				groupID := parseResponse(t, createRec)["GroupId"].(string)

				rec := doRequest(t, h, "DeleteGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
				})

				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "get_group_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Lookup Group",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				wantGroupID := parseResponse(t, createRec)["GroupId"].(string)

				rec := doRequest(t, h, "GetGroupId", map[string]any{
					"IdentityStoreId": testStoreID,
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

// TestUpdateGroup covers UpdateGroup applying DisplayName and Description changes.
func TestUpdateGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "OldName",
		"Description":     "Old",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	groupID := parseResponse(t, createRec)["GroupId"].(string)

	patchRec := doRequest(t, h, "UpdateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"Operations": []map[string]any{
			{"AttributePath": "displayName", "AttributeValue": "NewName"},
			{"AttributePath": "description", "AttributeValue": "New desc"},
		},
	})
	assert.Equal(t, http.StatusOK, patchRec.Code)

	descRec := doRequest(t, h, "DescribeGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
	})
	desc := parseResponse(t, descRec)
	assert.Equal(t, "NewName", desc["DisplayName"])
	assert.Equal(t, "New desc", desc["Description"])
}

// TestGetGroupID_WithUniqueAttribute verifies GetGroupId with UniqueAttribute.
func TestGetGroupID_WithUniqueAttribute(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "Unique Group",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, "GetGroupId", map[string]any{
		"IdentityStoreId": testStoreID,
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

// TestGroupErrors covers 404/409/400 error paths and required-field validation for Group operations.
func TestGroupErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystore.Handler)
		name string
	}{
		{
			name: "describe_nonexistent_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DescribeGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         "does-not-exist",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_nonexistent_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DeleteGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         "does-not-exist",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "duplicate_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Dup Group",
				})

				rec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Dup Group",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "create_group_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "CreateGroup", map[string]any{
					"DisplayName": "No Store Group",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			// DisplayName is NOT in CreateGroupRequest's "required" list in the
			// real AWS smithy model -- only IdentityStoreId is. A group may be
			// created with no DisplayName.
			name: "create_group_missing_display_name_is_allowed",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				resp := parseResponse(t, rec)
				assert.NotEmpty(t, resp["GroupId"])
			},
		},
		{
			name: "update_group_duplicate_display_name",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				r1 := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "GroupA",
				})
				require.Equal(t, http.StatusOK, r1.Code)
				groupID := parseResponse(t, r1)["GroupId"].(string)

				r2 := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "GroupB",
				})
				require.Equal(t, http.StatusOK, r2.Code)

				rec := doRequest(t, h, "UpdateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"Operations": []map[string]any{
						{"AttributePath": "displayName", "AttributeValue": "GroupB"},
					},
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "delete_group_removes_memberships",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				uRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "del.group.cascade.user",
				})
				require.Equal(t, http.StatusOK, uRec.Code)
				userID := parseResponse(t, uRec)["UserId"].(string)

				gRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Del Group Cascade",
				})
				require.Equal(t, http.StatusOK, gRec.Code)
				groupID := parseResponse(t, gRec)["GroupId"].(string)

				mRec := doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MemberId":        map[string]any{"UserId": userID},
				})
				require.Equal(t, http.StatusOK, mRec.Code)
				membershipID := parseResponse(t, mRec)["MembershipId"].(string)

				delRec := doRequest(t, h, "DeleteGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
				})
				assert.Equal(t, http.StatusOK, delRec.Code)

				descMem := doRequest(t, h, "DescribeGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"MembershipId":    membershipID,
				})
				assert.Equal(t, http.StatusNotFound, descMem.Code)
			},
		},
		{
			name: "list_groups_preallocated",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				for i := range 5 {
					doRequest(t, h, "CreateGroup", map[string]any{
						"IdentityStoreId": testStoreID,
						"DisplayName":     fmt.Sprintf("Pre Group %d", i),
					})
				}

				rec := doRequest(t, h, "ListGroups", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				groups := parseResponse(t, rec)["Groups"].([]any)
				assert.Len(t, groups, 5)
			},
		},
		{
			name: "resource_not_found_has_resource_type_group",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DescribeGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         "nonexistent-group",
				})
				require.Equal(t, http.StatusNotFound, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
				assert.Equal(t, "GROUP", resp["ResourceType"])
			},
		},
		{
			name: "describe_group_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DescribeGroup", map[string]any{
					"GroupId": "group-001",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_group_missing_group_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DescribeGroup", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_group_missing_group_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "DeleteGroup", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "update_group_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "UpdateGroup", map[string]any{
					"GroupId":    "group-001",
					"Operations": []map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "update_group_missing_group_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "UpdateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"Operations":      []map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_group_id_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "GetGroupId", map[string]any{
					"AlternateIdentifier": map[string]any{
						"UniqueAttribute": map[string]any{
							"AttributePath":  "displayName",
							"AttributeValue": "SomeGroup",
						},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_groups_missing_store_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "ListGroups", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
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
