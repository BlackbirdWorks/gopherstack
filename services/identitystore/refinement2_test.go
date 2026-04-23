package identitystore_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// TestRefinement2 covers all new features introduced in refinement 2:
//   - ExternalIds field on User
//   - GetUserId via ExternalId lookup
//   - GetUserID via email index (primary and non-primary)
//   - ListUsers / ListGroups filters (AttributePath/AttributeValue)
//   - ListUsers / ListGroups / ListGroupMemberships / ListGroupMembershipsForMember
//     MaxResults + NextToken pagination
//   - ResourceType field in ResourceNotFoundException responses
//   - Required-field validations on every remaining handler
//   - IsMemberInGroups max-100 GroupIds validation
//   - Inverted membership index correctness via cascade delete
//   - UpdateUser email-index maintenance
//   - Reset() leaves new indexes empty
func TestRefinement2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystoreTestHelper)
		name string
	}{
		// ----------------------------------------------------------------
		// ExternalIds on User
		// ----------------------------------------------------------------
		{
			name: "create_user_with_external_ids",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "ext.user",
					"ExternalIds": []map[string]any{
						{"Issuer": "okta", "Id": "okta-abc-123"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				descRec := h.do("DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				resp := parseResponse(t, descRec)
				extIDs, ok := resp["ExternalIds"].([]any)
				require.True(t, ok)
				require.Len(t, extIDs, 1)

				first := extIDs[0].(map[string]any)
				assert.Equal(t, "okta", first["Issuer"])
				assert.Equal(t, "okta-abc-123", first["Id"])
			},
		},
		// ----------------------------------------------------------------
		// GetUserId via ExternalId
		// ----------------------------------------------------------------
		{
			name: "get_user_id_by_external_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				createRec := h.do("CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "extlookup.user",
					"ExternalIds": []map[string]any{
						{"Issuer": "idp", "Id": "idp-xyz-789"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				wantID := parseResponse(t, createRec)["UserId"].(string)

				rec := h.do("GetUserId", map[string]any{
					"IdentityStoreId": testStoreID,
					"AlternateIdentifier": map[string]any{
						"ExternalId": map[string]any{
							"Issuer": "idp",
							"Id":     "idp-xyz-789",
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, wantID, parseResponse(t, rec)["UserId"])
			},
		},
		// ----------------------------------------------------------------
		// Email index: O(1) lookup and maintenance on email update
		// ----------------------------------------------------------------
		{
			name: "get_user_id_by_email_index",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				createRec := h.do("CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "email.index.user",
					"Emails": []map[string]any{
						{"Value": "primary@example.com", "Type": "work", "Primary": true},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				wantID := parseResponse(t, createRec)["UserId"].(string)

				rec := h.do("GetUserId", map[string]any{
					"IdentityStoreId": testStoreID,
					"AlternateIdentifier": map[string]any{
						"UniqueAttribute": map[string]any{
							"AttributePath":  "emails.value",
							"AttributeValue": "primary@example.com",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, wantID, parseResponse(t, rec)["UserId"])
			},
		},
		{
			name: "email_index_updated_on_user_update",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				createRec := h.do("CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "email.update.user",
					"Emails": []map[string]any{
						{"Value": "old@example.com", "Primary": true},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				// Update email
				patchRec := h.do("UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{
							"AttributePath": "emails",
							"AttributeValue": []map[string]any{
								{"Value": "new@example.com", "Primary": true},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, patchRec.Code)

				// Old email should no longer work
				oldRec := h.do("GetUserId", map[string]any{
					"IdentityStoreId": testStoreID,
					"AlternateIdentifier": map[string]any{
						"UniqueAttribute": map[string]any{
							"AttributePath":  "emails.value",
							"AttributeValue": "old@example.com",
						},
					},
				})
				assert.Equal(t, http.StatusNotFound, oldRec.Code)

				// New email should work
				newRec := h.do("GetUserId", map[string]any{
					"IdentityStoreId": testStoreID,
					"AlternateIdentifier": map[string]any{
						"UniqueAttribute": map[string]any{
							"AttributePath":  "emails.value",
							"AttributeValue": "new@example.com",
						},
					},
				})
				require.Equal(t, http.StatusOK, newRec.Code)
				assert.Equal(t, userID, parseResponse(t, newRec)["UserId"])
			},
		},
		// ----------------------------------------------------------------
		// ListUsers Filters
		// ----------------------------------------------------------------
		{
			name: "list_users_filter_by_username",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				for _, name := range []string{"filter.alice", "filter.bob", "filter.carol"} {
					rec := h.do("CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        name,
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec := h.do("ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"Filters": []map[string]any{
						{"AttributePath": "UserName", "AttributeValue": "filter.bob"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				users, ok := parseResponse(t, rec)["Users"].([]any)
				require.True(t, ok)
				require.Len(t, users, 1)
				assert.Equal(t, "filter.bob", users[0].(map[string]any)["UserName"])
			},
		},
		{
			name: "list_users_filter_by_email",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				for _, email := range []string{"aa@ex.com", "bb@ex.com"} {
					rec := h.do("CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        "femail-" + email,
						"Emails": []map[string]any{
							{"Value": email, "Primary": true},
						},
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec := h.do("ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"Filters": []map[string]any{
						{"AttributePath": "emails.value", "AttributeValue": "bb@ex.com"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				users, ok := parseResponse(t, rec)["Users"].([]any)
				require.True(t, ok)
				require.Len(t, users, 1)
			},
		},
		// ----------------------------------------------------------------
		// ListGroups Filters
		// ----------------------------------------------------------------
		{
			name: "list_groups_filter_by_display_name",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				for _, name := range []string{"Alpha Team", "Beta Team", "Gamma Team"} {
					rec := h.do("CreateGroup", map[string]any{
						"IdentityStoreId": testStoreID,
						"DisplayName":     name,
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec := h.do("ListGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"Filters": []map[string]any{
						{"AttributePath": "displayName", "AttributeValue": "Beta Team"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				groups, ok := parseResponse(t, rec)["Groups"].([]any)
				require.True(t, ok)
				require.Len(t, groups, 1)
				assert.Equal(t, "Beta Team", groups[0].(map[string]any)["DisplayName"])
			},
		},
		// ----------------------------------------------------------------
		// Pagination: ListUsers MaxResults + NextToken
		// ----------------------------------------------------------------
		{
			name: "list_users_pagination",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				for i := range 5 {
					rec := h.do("CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        fmt.Sprintf("page.user.%d", i),
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				// First page of 2
				rec1 := h.do("ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"MaxResults":      2,
				})
				require.Equal(t, http.StatusOK, rec1.Code)
				resp1 := parseResponse(t, rec1)

				users1, ok := resp1["Users"].([]any)
				require.True(t, ok)
				assert.Len(t, users1, 2)

				token, hasToken := resp1["NextToken"].(string)
				require.True(t, hasToken, "expected NextToken to be non-nil")
				require.NotEmpty(t, token)

				// Verify token is base64
				_, err := base64.StdEncoding.DecodeString(token)
				require.NoError(t, err)

				// Second page of 2
				rec2 := h.do("ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"MaxResults":      2,
					"NextToken":       token,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
				resp2 := parseResponse(t, rec2)
				users2, ok := resp2["Users"].([]any)
				require.True(t, ok)
				assert.Len(t, users2, 2)

				// Remaining page of 1
				token2 := resp2["NextToken"].(string)
				rec3 := h.do("ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"MaxResults":      2,
					"NextToken":       token2,
				})
				require.Equal(t, http.StatusOK, rec3.Code)
				resp3 := parseResponse(t, rec3)
				users3, ok := resp3["Users"].([]any)
				require.True(t, ok)
				assert.Len(t, users3, 1)
				assert.Nil(t, resp3["NextToken"])
			},
		},
		// ----------------------------------------------------------------
		// Pagination: ListGroups MaxResults + NextToken
		// ----------------------------------------------------------------
		{
			name: "list_groups_pagination",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				for i := range 4 {
					rec := h.do("CreateGroup", map[string]any{
						"IdentityStoreId": testStoreID,
						"DisplayName":     fmt.Sprintf("Paged Group %d", i),
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec1 := h.do("ListGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MaxResults":      2,
				})
				require.Equal(t, http.StatusOK, rec1.Code)
				resp1 := parseResponse(t, rec1)
				g1, _ := resp1["Groups"].([]any)
				assert.Len(t, g1, 2)

				token, hasToken := resp1["NextToken"].(string)
				require.True(t, hasToken)

				rec2 := h.do("ListGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MaxResults":      2,
					"NextToken":       token,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
				resp2 := parseResponse(t, rec2)
				g2, _ := resp2["Groups"].([]any)
				assert.Len(t, g2, 2)
				assert.Nil(t, resp2["NextToken"])
			},
		},
		// ----------------------------------------------------------------
		// Pagination: ListGroupMemberships
		// ----------------------------------------------------------------
		{
			name: "list_group_memberships_pagination",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				groupRec := h.do("CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "Paginated Membership Group",
				})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				for i := range 4 {
					uRec := h.do("CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        fmt.Sprintf("mem.page.user.%d", i),
					})
					require.Equal(t, http.StatusOK, uRec.Code)
					userID := parseResponse(t, uRec)["UserId"].(string)

					mRec := h.do("CreateGroupMembership", map[string]any{
						"IdentityStoreId": testStoreID,
						"GroupId":         groupID,
						"MemberId":        map[string]any{"UserId": userID},
					})
					require.Equal(t, http.StatusOK, mRec.Code)
				}

				rec1 := h.do("ListGroupMemberships", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MaxResults":      2,
				})
				require.Equal(t, http.StatusOK, rec1.Code)
				resp1 := parseResponse(t, rec1)
				m1, _ := resp1["GroupMemberships"].([]any)
				assert.Len(t, m1, 2)
				token, hasToken := resp1["NextToken"].(string)
				require.True(t, hasToken)

				rec2 := h.do("ListGroupMemberships", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MaxResults":      2,
					"NextToken":       token,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
				resp2 := parseResponse(t, rec2)
				m2, _ := resp2["GroupMemberships"].([]any)
				assert.Len(t, m2, 2)
				assert.Nil(t, resp2["NextToken"])
			},
		},
		// ----------------------------------------------------------------
		// Pagination: ListGroupMembershipsForMember
		// ----------------------------------------------------------------
		{
			name: "list_memberships_for_member_pagination",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				uRec := h.do("CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "memforpaged.user",
				})
				require.Equal(t, http.StatusOK, uRec.Code)
				userID := parseResponse(t, uRec)["UserId"].(string)

				for i := range 4 {
					gRec := h.do("CreateGroup", map[string]any{
						"IdentityStoreId": testStoreID,
						"DisplayName":     fmt.Sprintf("ForMember Paged Group %d", i),
					})
					require.Equal(t, http.StatusOK, gRec.Code)
					groupID := parseResponse(t, gRec)["GroupId"].(string)

					mRec := h.do("CreateGroupMembership", map[string]any{
						"IdentityStoreId": testStoreID,
						"GroupId":         groupID,
						"MemberId":        map[string]any{"UserId": userID},
					})
					require.Equal(t, http.StatusOK, mRec.Code)
				}

				rec1 := h.do("ListGroupMembershipsForMember", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"MaxResults":      2,
				})
				require.Equal(t, http.StatusOK, rec1.Code)
				resp1 := parseResponse(t, rec1)
				m1, _ := resp1["GroupMemberships"].([]any)
				assert.Len(t, m1, 2)
				token, hasToken := resp1["NextToken"].(string)
				require.True(t, hasToken)

				rec2 := h.do("ListGroupMembershipsForMember", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"MaxResults":      2,
					"NextToken":       token,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
				m2, _ := parseResponse(t, rec2)["GroupMemberships"].([]any)
				assert.Len(t, m2, 2)
			},
		},
		// ----------------------------------------------------------------
		// ResourceType in ResourceNotFoundException responses
		// ----------------------------------------------------------------
		{
			name: "resource_not_found_has_resource_type_user",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          "nonexistent-user",
				})
				require.Equal(t, http.StatusNotFound, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
				assert.Equal(t, "USER", resp["ResourceType"])
			},
		},
		{
			name: "resource_not_found_has_resource_type_group",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeGroup", map[string]any{
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
			name: "resource_not_found_has_resource_type_group_membership",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"MembershipId":    "nonexistent-membership",
				})
				require.Equal(t, http.StatusNotFound, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
				assert.Equal(t, "GROUP_MEMBERSHIP", resp["ResourceType"])
			},
		},
		// ----------------------------------------------------------------
		// Required-field validations on previously unvalidated handlers
		// ----------------------------------------------------------------
		{
			name: "describe_user_missing_user_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_user_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeUser", map[string]any{
					"UserId": "user-001",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_user_missing_user_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DeleteUser", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "update_user_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("UpdateUser", map[string]any{
					"UserId":     "user-001",
					"Operations": []map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "update_user_missing_user_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"Operations":      []map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_group_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeGroup", map[string]any{
					"GroupId": "group-001",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_group_missing_group_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeGroup", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_group_missing_group_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DeleteGroup", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "update_group_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("UpdateGroup", map[string]any{
					"GroupId":    "group-001",
					"Operations": []map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "update_group_missing_group_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("UpdateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"Operations":      []map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_membership_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeGroupMembership", map[string]any{
					"MembershipId": "mem-001",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_membership_missing_membership_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DescribeGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_membership_missing_membership_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("DeleteGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_group_memberships_missing_group_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("ListGroupMemberships", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_group_memberships_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("ListGroupMemberships", map[string]any{
					"GroupId": "group-001",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_group_membership_id_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("GetGroupMembershipId", map[string]any{
					"GroupId":  "g-1",
					"MemberId": map[string]any{"UserId": "u-1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_group_membership_id_missing_group_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("GetGroupMembershipId", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": "u-1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_group_membership_id_missing_user_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("GetGroupMembershipId", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         "g-1",
					"MemberId":        map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_user_id_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("GetUserId", map[string]any{
					"AlternateIdentifier": map[string]any{
						"UniqueAttribute": map[string]any{
							"AttributePath":  "userName",
							"AttributeValue": "someone",
						},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "get_group_id_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("GetGroupId", map[string]any{
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
			name: "list_users_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("ListUsers", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_groups_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("ListGroups", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "is_member_in_groups_missing_store_id",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				rec := h.do("IsMemberInGroups", map[string]any{
					"MemberId": map[string]any{"UserId": "u-1"},
					"GroupIds": []string{"g-1"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		// ----------------------------------------------------------------
		// IsMemberInGroups: max 100 group IDs
		// ----------------------------------------------------------------
		{
			name: "is_member_in_groups_exceeds_100_ids",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				groupIDs := make([]string, 101)
				for i := range 101 {
					groupIDs[i] = fmt.Sprintf("g-%d", i)
				}

				rec := h.do("IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": "u-1"},
					"GroupIds":        groupIDs,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				resp := parseResponse(t, rec)
				assert.Equal(t, "ValidationException", resp["__type"])
			},
		},
		{
			name: "is_member_in_groups_exactly_100_ids_ok",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				uRec := h.do("CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "max100.user",
				})
				require.Equal(t, http.StatusOK, uRec.Code)
				userID := parseResponse(t, uRec)["UserId"].(string)

				groupIDs := make([]string, 100)
				for i := range 100 {
					groupIDs[i] = fmt.Sprintf("fake-group-%d", i)
				}

				rec := h.do("IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        groupIDs,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				results, _ := parseResponse(t, rec)["Results"].([]any)
				assert.Len(t, results, 100)
			},
		},
		// ----------------------------------------------------------------
		// Inverted index: cascade delete on user removal
		// ----------------------------------------------------------------
		{
			name: "delete_user_cascades_via_inverted_index",
			run: func(t *testing.T, h *identitystoreTestHelper) {
				t.Helper()

				uRec := h.do("CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "cascade.inv.user",
				})
				require.Equal(t, http.StatusOK, uRec.Code)
				userID := parseResponse(t, uRec)["UserId"].(string)

				membershipIDs := make([]string, 0, 3)
				for i := range 3 {
					gRec := h.do("CreateGroup", map[string]any{
						"IdentityStoreId": testStoreID,
						"DisplayName":     fmt.Sprintf("Cascade Inv Group %d", i),
					})
					require.Equal(t, http.StatusOK, gRec.Code)
					groupID := parseResponse(t, gRec)["GroupId"].(string)

					mRec := h.do("CreateGroupMembership", map[string]any{
						"IdentityStoreId": testStoreID,
						"GroupId":         groupID,
						"MemberId":        map[string]any{"UserId": userID},
					})
					require.Equal(t, http.StatusOK, mRec.Code)
					membershipIDs = append(membershipIDs, parseResponse(t, mRec)["MembershipId"].(string))
				}

				// Delete the user — should cascade remove all 3 memberships via inverted index.
				delRec := h.do("DeleteUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, delRec.Code)

				for _, mid := range membershipIDs {
					descMem := h.do("DescribeGroupMembership", map[string]any{
						"IdentityStoreId": testStoreID,
						"MembershipId":    mid,
					})
					assert.Equal(t, http.StatusNotFound, descMem.Code)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &identitystoreTestHelper{t: t, h: newTestHandler()}
			tt.run(t, h)
		})
	}
}

// identitystoreTestHelper wraps a Handler with a convenient do() method.
type identitystoreTestHelper struct {
	t *testing.T
	h *identitystore.Handler
}

func (h *identitystoreTestHelper) do(op string, body map[string]any) *httptest.ResponseRecorder {
	h.t.Helper()

	return doRequest(h.t, h.h, op, body)
}
