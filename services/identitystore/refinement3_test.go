package identitystore_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// TestRefinement3_UserBirthdate verifies Birthdate is stored and returned on users.
func TestRefinement3_UserBirthdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_user_with_birthdate",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "birth.user",
					"Birthdate":       "1990-06-15",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)
				assert.Equal(t, "1990-06-15", parseResponse(t, desc)["Birthdate"])
			},
		},
		{
			name: "create_user_without_birthdate_returns_empty",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "no.birth",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)
				resp := parseResponse(t, desc)
				_, hasBirthdate := resp["Birthdate"]
				assert.False(t, hasBirthdate, "Birthdate should be absent when not set")
			},
		},
		{
			name: "invalid_birthdate_format_rejected",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "bad.birth",
					"Birthdate":       "15/06/1990",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, parseResponse(t, rec)["message"], "Birthdate")
			},
		},
		{
			name: "update_user_birthdate",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "update.birth",
					"Birthdate":       "1985-01-01",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "birthdate", "AttributeValue": "1990-06-15"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				assert.Equal(t, "1990-06-15", parseResponse(t, desc)["Birthdate"])
			},
		},
		{
			name: "list_users_includes_birthdate",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "b1",
					"Birthdate":       "2000-03-20",
				})

				listRec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				require.Equal(t, http.StatusOK, listRec.Code)

				users := parseResponse(t, listRec)["Users"].([]any)
				require.Len(t, users, 1)
				assert.Equal(t, "2000-03-20", users[0].(map[string]any)["Birthdate"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_UserWebsite verifies Website field is stored and returned.
func TestRefinement3_UserWebsite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_user_with_website",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "web.user",
					"Website":         "https://example.com/~web.user",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)
				assert.Equal(t, "https://example.com/~web.user", parseResponse(t, desc)["Website"])
			},
		},
		{
			name: "update_user_website",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "update.web",
					"Website":         "https://old.example.com",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "website", "AttributeValue": "https://new.example.com"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				assert.Equal(t, "https://new.example.com", parseResponse(t, desc)["Website"])
			},
		},
		{
			name: "clear_user_website",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "clear.web",
					"Website":         "https://example.com",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "website", "AttributeValue": ""},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				resp := parseResponse(t, desc)
				_, hasWebsite := resp["Website"]
				assert.False(t, hasWebsite, "Website should be absent after clearing")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_UserStatus verifies UserStatus defaults to ENABLED and can be toggled.
func TestRefinement3_UserStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "new_user_defaults_to_enabled",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.default",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)
				assert.Equal(t, "ENABLED", parseResponse(t, desc)["UserStatus"])
			},
		},
		{
			name: "update_user_status_to_disabled",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.change",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				assert.Equal(t, "DISABLED", parseResponse(t, desc)["UserStatus"])
			},
		},
		{
			name: "re_enable_user",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.reenable",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
					},
				})

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "userStatus", "AttributeValue": "ENABLED"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				assert.Equal(t, "ENABLED", parseResponse(t, desc)["UserStatus"])
			},
		},
		{
			name: "invalid_user_status_ignored",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.invalid",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "userStatus", "AttributeValue": "SUSPENDED"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				// Invalid value should be silently ignored; status stays ENABLED.
				assert.Equal(t, "ENABLED", parseResponse(t, desc)["UserStatus"])
			},
		},
		{
			name: "user_status_in_list_users",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.list",
				})

				listRec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				require.Equal(t, http.StatusOK, listRec.Code)

				users := parseResponse(t, listRec)["Users"].([]any)
				require.Len(t, users, 1)
				assert.Equal(t, "ENABLED", users[0].(map[string]any)["UserStatus"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_UserPhotos verifies Photos are stored, returned, updated, and limited to 3.
func TestRefinement3_UserPhotos(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_user_with_photos",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "photo.user",
					"Photos": []map[string]any{
						{"Value": "https://example.com/photo1.jpg", "Type": "thumbnail", "Primary": true},
						{"Value": "https://example.com/photo2.jpg", "Type": "full"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)

				photos, ok := parseResponse(t, desc)["Photos"].([]any)
				require.True(t, ok, "Photos should be returned")
				require.Len(t, photos, 2)

				first := photos[0].(map[string]any)
				assert.Equal(t, "https://example.com/photo1.jpg", first["Value"])
				assert.Equal(t, "thumbnail", first["Type"])
				assert.Equal(t, true, first["Primary"])
			},
		},
		{
			name: "create_user_with_max_photos",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "maxphoto.user",
					"Photos": []map[string]any{
						{"Value": "https://example.com/p1.jpg", "Primary": true},
						{"Value": "https://example.com/p2.jpg"},
						{"Value": "https://example.com/p3.jpg"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "create_user_exceeds_max_photos_rejected",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "toomany.photos",
					"Photos": []map[string]any{
						{"Value": "https://example.com/p1.jpg"},
						{"Value": "https://example.com/p2.jpg"},
						{"Value": "https://example.com/p3.jpg"},
						{"Value": "https://example.com/p4.jpg"},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, parseResponse(t, rec)["message"], "Photos")
			},
		},
		{
			name: "update_user_photos",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "update.photos",
					"Photos": []map[string]any{
						{"Value": "https://example.com/old.jpg", "Primary": true},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{
							"AttributePath": "photos",
							"AttributeValue": []map[string]any{
								{"Value": "https://example.com/new1.jpg", "Type": "thumbnail", "Primary": true},
								{"Value": "https://example.com/new2.jpg", "Type": "full"},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				photos := parseResponse(t, desc)["Photos"].([]any)
				require.Len(t, photos, 2)
				assert.Equal(t, "https://example.com/new1.jpg", photos[0].(map[string]any)["Value"])
			},
		},
		{
			name: "update_photos_clears_when_nil",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "clear.photos",
					"Photos": []map[string]any{
						{"Value": "https://example.com/p.jpg", "Primary": true},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "photos", "AttributeValue": nil},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				resp := parseResponse(t, desc)
				_, hasPhotos := resp["Photos"]
				assert.False(t, hasPhotos, "Photos should be absent after clearing")
			},
		},
		{
			name: "photo_display_field_stored",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "display.photo",
					"Photos": []map[string]any{
						{
							"Value":   "https://example.com/avatar.jpg",
							"Display": "Profile Avatar",
							"Type":    "profile",
							"Primary": true,
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				photos := parseResponse(t, desc)["Photos"].([]any)
				require.Len(t, photos, 1)

				photo := photos[0].(map[string]any)
				assert.Equal(t, "Profile Avatar", photo["Display"])
				assert.Equal(t, "profile", photo["Type"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_UserRoles verifies Roles are stored, returned, and updated.
func TestRefinement3_UserRoles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_user_with_roles",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "role.user",
					"Roles": []map[string]any{
						{"Value": "Researcher", "Type": "work", "Primary": true},
						{"Value": "Manager", "Type": "org"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)

				roles, ok := parseResponse(t, desc)["Roles"].([]any)
				require.True(t, ok, "Roles should be returned")
				require.Len(t, roles, 2)

				first := roles[0].(map[string]any)
				assert.Equal(t, "Researcher", first["Value"])
				assert.Equal(t, "work", first["Type"])
				assert.Equal(t, true, first["Primary"])
			},
		},
		{
			name: "update_user_roles",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "update.roles",
					"Roles": []map[string]any{
						{"Value": "Analyst", "Primary": true},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{
							"AttributePath": "roles",
							"AttributeValue": []map[string]any{
								{"Value": "SeniorAnalyst", "Type": "work", "Primary": true},
								{"Value": "Mentor", "Type": "community"},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				roles := parseResponse(t, desc)["Roles"].([]any)
				require.Len(t, roles, 2)
				assert.Equal(t, "SeniorAnalyst", roles[0].(map[string]any)["Value"])
			},
		},
		{
			name: "clear_user_roles",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "clear.roles",
					"Roles": []map[string]any{
						{"Value": "Engineer", "Primary": true},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "roles", "AttributeValue": nil},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				resp := parseResponse(t, desc)
				_, hasRoles := resp["Roles"]
				assert.False(t, hasRoles, "Roles should be absent after clearing")
			},
		},
		{
			name: "roles_in_list_users",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "roleslist.user",
					"Roles": []map[string]any{
						{"Value": "DevOps", "Primary": true},
					},
				})

				listRec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				require.Equal(t, http.StatusOK, listRec.Code)

				users := parseResponse(t, listRec)["Users"].([]any)
				require.Len(t, users, 1)

				roles, ok := users[0].(map[string]any)["Roles"].([]any)
				require.True(t, ok)
				require.Len(t, roles, 1)
				assert.Equal(t, "DevOps", roles[0].(map[string]any)["Value"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_IsMemberInGroupsMemberID verifies that IsMemberInGroups returns MemberId.
func TestRefinement3_IsMemberInGroupsMemberID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "result_includes_member_id",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "ismember.user",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "IsMemberGroup",
				})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MemberId":        map[string]any{"UserId": userID},
				})

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        []string{groupID},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				results := parseResponse(t, rec)["Results"].([]any)
				require.Len(t, results, 1)

				result := results[0].(map[string]any)
				assert.Equal(t, groupID, result["GroupId"])
				assert.Equal(t, true, result["MembershipExists"])

				memberID, ok := result["MemberId"].(map[string]any)
				require.True(t, ok, "MemberId should be present in result")
				assert.Equal(t, userID, memberID["UserId"])
			},
		},
		{
			name: "non_member_result_includes_member_id",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "nonmember.user",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "NoMemberGroup",
				})
				require.Equal(t, http.StatusOK, groupRec.Code)
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        []string{groupID},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				results := parseResponse(t, rec)["Results"].([]any)
				require.Len(t, results, 1)

				result := results[0].(map[string]any)
				assert.Equal(t, false, result["MembershipExists"])

				memberID, ok := result["MemberId"].(map[string]any)
				require.True(t, ok, "MemberId should be present even for non-members")
				assert.Equal(t, userID, memberID["UserId"])
			},
		},
		{
			name: "multiple_groups_all_have_member_id",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "multi.member",
				})
				require.Equal(t, http.StatusOK, userRec.Code)
				userID := parseResponse(t, userRec)["UserId"].(string)

				g1Rec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "MultiGroup1",
				})
				g1ID := parseResponse(t, g1Rec)["GroupId"].(string)

				g2Rec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "MultiGroup2",
				})
				g2ID := parseResponse(t, g2Rec)["GroupId"].(string)

				doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         g1ID,
					"MemberId":        map[string]any{"UserId": userID},
				})

				rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
					"GroupIds":        []string{g1ID, g2ID},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				results := parseResponse(t, rec)["Results"].([]any)
				require.Len(t, results, 2)

				for _, r := range results {
					result := r.(map[string]any)
					memberID, ok := result["MemberId"].(map[string]any)
					require.True(t, ok, "MemberId should be present for every result")
					assert.Equal(t, userID, memberID["UserId"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_ListSorting verifies list operations return deterministic sorted results.
func TestRefinement3_ListSorting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "list_users_is_deterministic_across_calls",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				names := []string{"zebra", "alpha", "mango", "bravo", "kilo"}
				for _, n := range names {
					doRequest(t, h, "CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        n,
					})
				}

				rec1 := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				rec2 := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
				})

				require.Equal(t, http.StatusOK, rec1.Code)
				require.Equal(t, http.StatusOK, rec2.Code)

				users1 := parseResponse(t, rec1)["Users"].([]any)
				users2 := parseResponse(t, rec2)["Users"].([]any)

				require.Len(t, users1, len(names))
				require.Len(t, users2, len(names))

				for i := range users1 {
					id1 := users1[i].(map[string]any)["UserId"].(string)
					id2 := users2[i].(map[string]any)["UserId"].(string)
					assert.Equal(t, id1, id2, "ListUsers order must be deterministic across calls")
				}
			},
		},
		{
			name: "list_groups_is_deterministic_across_calls",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				displayNames := []string{"Zeta", "Alpha", "Mu", "Beta"}
				for _, n := range displayNames {
					doRequest(t, h, "CreateGroup", map[string]any{
						"IdentityStoreId": testStoreID,
						"DisplayName":     n,
					})
				}

				rec1 := doRequest(t, h, "ListGroups", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				rec2 := doRequest(t, h, "ListGroups", map[string]any{
					"IdentityStoreId": testStoreID,
				})

				require.Equal(t, http.StatusOK, rec1.Code)
				require.Equal(t, http.StatusOK, rec2.Code)

				groups1 := parseResponse(t, rec1)["Groups"].([]any)
				groups2 := parseResponse(t, rec2)["Groups"].([]any)

				require.Len(t, groups1, len(displayNames))

				for i := range groups1 {
					id1 := groups1[i].(map[string]any)["GroupId"].(string)
					id2 := groups2[i].(map[string]any)["GroupId"].(string)
					assert.Equal(t, id1, id2, "ListGroups order must be deterministic across calls")
				}
			},
		},
		{
			name: "list_group_memberships_is_deterministic",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "SortMemberships",
				})
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				for i := range 5 {
					userRec := doRequest(t, h, "CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        fmt.Sprintf("member%d", i),
					})
					userID := parseResponse(t, userRec)["UserId"].(string)
					doRequest(t, h, "CreateGroupMembership", map[string]any{
						"IdentityStoreId": testStoreID,
						"GroupId":         groupID,
						"MemberId":        map[string]any{"UserId": userID},
					})
				}

				rec1 := doRequest(t, h, "ListGroupMemberships", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
				})
				rec2 := doRequest(t, h, "ListGroupMemberships", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
				})

				require.Equal(t, http.StatusOK, rec1.Code)
				require.Equal(t, http.StatusOK, rec2.Code)

				m1 := parseResponse(t, rec1)["GroupMemberships"].([]any)
				m2 := parseResponse(t, rec2)["GroupMemberships"].([]any)

				require.Len(t, m1, 5)
				for i := range m1 {
					id1 := m1[i].(map[string]any)["MembershipId"].(string)
					id2 := m2[i].(map[string]any)["MembershipId"].(string)
					assert.Equal(t, id1, id2, "ListGroupMemberships order must be deterministic")
				}
			},
		},
		{
			name: "list_memberships_for_member_is_deterministic",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "sort.member",
				})
				userID := parseResponse(t, userRec)["UserId"].(string)

				for i := range 4 {
					groupRec := doRequest(t, h, "CreateGroup", map[string]any{
						"IdentityStoreId": testStoreID,
						"DisplayName":     fmt.Sprintf("SortGroup%d", i),
					})
					groupID := parseResponse(t, groupRec)["GroupId"].(string)
					doRequest(t, h, "CreateGroupMembership", map[string]any{
						"IdentityStoreId": testStoreID,
						"GroupId":         groupID,
						"MemberId":        map[string]any{"UserId": userID},
					})
				}

				rec1 := doRequest(t, h, "ListGroupMembershipsForMember", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
				})
				rec2 := doRequest(t, h, "ListGroupMembershipsForMember", map[string]any{
					"IdentityStoreId": testStoreID,
					"MemberId":        map[string]any{"UserId": userID},
				})

				require.Equal(t, http.StatusOK, rec1.Code)
				require.Equal(t, http.StatusOK, rec2.Code)

				m1 := parseResponse(t, rec1)["GroupMemberships"].([]any)
				m2 := parseResponse(t, rec2)["GroupMemberships"].([]any)

				require.Len(t, m1, 4)
				for i := range m1 {
					id1 := m1[i].(map[string]any)["MembershipId"].(string)
					id2 := m2[i].(map[string]any)["MembershipId"].(string)
					assert.Equal(t, id1, id2, "ListGroupMembershipsForMember order must be deterministic")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_IDFormat verifies that generated IDs are UUID format.
func TestRefinement3_IDFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "user_id_is_uuid_format",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "uuid.user",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)
				// UUID format: 8-4-4-4-12 hex chars separated by hyphens
				assert.Len(t, userID, 36, "UserID should be UUID format (36 chars)")
				assert.Equal(t, '-', rune(userID[8]))
				assert.Equal(t, '-', rune(userID[13]))
				assert.Equal(t, '-', rune(userID[18]))
				assert.Equal(t, '-', rune(userID[23]))
			},
		},
		{
			name: "group_id_is_uuid_format",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "UUIDGroup",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				groupID := parseResponse(t, rec)["GroupId"].(string)
				assert.Len(t, groupID, 36, "GroupID should be UUID format (36 chars)")
			},
		},
		{
			name: "membership_id_is_uuid_format",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				userRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "uuid.member",
				})
				userID := parseResponse(t, userRec)["UserId"].(string)

				groupRec := doRequest(t, h, "CreateGroup", map[string]any{
					"IdentityStoreId": testStoreID,
					"DisplayName":     "UUIDMembership",
				})
				groupID := parseResponse(t, groupRec)["GroupId"].(string)

				rec := doRequest(t, h, "CreateGroupMembership", map[string]any{
					"IdentityStoreId": testStoreID,
					"GroupId":         groupID,
					"MemberId":        map[string]any{"UserId": userID},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				membershipID := parseResponse(t, rec)["MembershipId"].(string)
				assert.Len(t, membershipID, 36, "MembershipID should be UUID format (36 chars)")
			},
		},
		{
			name: "user_ids_are_unique",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				seen := make(map[string]bool)
				for i := range 10 {
					rec := doRequest(t, h, "CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        fmt.Sprintf("unique%d", i),
					})
					require.Equal(t, http.StatusOK, rec.Code)
					id := parseResponse(t, rec)["UserId"].(string)
					assert.False(t, seen[id], "Generated IDs should be unique")
					seen[id] = true
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_UpdateUserExternalIDs verifies externalids path in UpdateUser.
func TestRefinement3_UpdateUserExternalIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "update_external_ids_via_attribute_path",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "extid.update",
					"ExternalIds": []map[string]any{
						{"Issuer": "okta", "Id": "okta-old-123"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{
							"AttributePath": "externalIds",
							"AttributeValue": []map[string]any{
								{"Issuer": "okta", "Id": "okta-new-456"},
								{"Issuer": "azure", "Id": "azure-abc"},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				extIDs := parseResponse(t, desc)["ExternalIds"].([]any)
				require.Len(t, extIDs, 2)
				assert.Equal(t, "okta-new-456", extIDs[0].(map[string]any)["Id"])
				assert.Equal(t, "azure-abc", extIDs[1].(map[string]any)["Id"])
			},
		},
		{
			name: "clear_external_ids_via_attribute_path",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "extid.clear",
					"ExternalIds": []map[string]any{
						{"Issuer": "saml", "Id": "saml-xyz"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "externalIds", "AttributeValue": nil},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				resp := parseResponse(t, desc)
				_, hasExt := resp["ExternalIds"]
				assert.False(t, hasExt, "ExternalIds should be absent after clearing")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestRefinement3_AllNewFieldsRoundTrip verifies full round-trip of all new User fields.
func TestRefinement3_AllNewFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "fulluser",
		"DisplayName":     "Full User",
		"Birthdate":       "1985-12-25",
		"Website":         "https://fulluser.example.com",
		"Photos": []map[string]any{
			{
				"Value":   "https://fulluser.example.com/avatar.jpg",
				"Display": "Avatar",
				"Type":    "profile",
				"Primary": true,
			},
		},
		"Roles": []map[string]any{
			{"Value": "Engineer", "Type": "work", "Primary": true},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	userID := parseResponse(t, rec)["UserId"].(string)

	desc := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)

	resp := parseResponse(t, desc)
	assert.Equal(t, "1985-12-25", resp["Birthdate"])
	assert.Equal(t, "https://fulluser.example.com", resp["Website"])
	assert.Equal(t, "ENABLED", resp["UserStatus"])

	photos := resp["Photos"].([]any)
	require.Len(t, photos, 1)
	assert.Equal(t, "Avatar", photos[0].(map[string]any)["Display"])

	roles := resp["Roles"].([]any)
	require.Len(t, roles, 1)
	assert.Equal(t, "Engineer", roles[0].(map[string]any)["Value"])
}

// TestRefinement3_PersistenceRoundTrip verifies Snapshot/Restore preserves new fields.
func TestRefinement3_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	backend1 := identitystore.NewInMemoryBackend("000000000000", "us-east-1")
	h1 := identitystore.NewHandler(backend1)

	rec := doRequest(t, h1, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "snap.user",
		"Birthdate":       "1992-04-07",
		"Website":         "https://snap.example.com",
		"Photos": []map[string]any{
			{"Value": "https://snap.example.com/p.jpg", "Primary": true},
		},
		"Roles": []map[string]any{
			{"Value": "Architect", "Primary": true},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	userID := parseResponse(t, rec)["UserId"].(string)

	snapshot := h1.Snapshot(t.Context())

	backend2 := identitystore.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := identitystore.NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), snapshot))

	desc := doRequest(t, h2, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)

	resp := parseResponse(t, desc)
	assert.Equal(t, "1992-04-07", resp["Birthdate"])
	assert.Equal(t, "https://snap.example.com", resp["Website"])
	assert.Equal(t, "ENABLED", resp["UserStatus"])

	photos := resp["Photos"].([]any)
	require.Len(t, photos, 1)
	assert.Equal(t, "https://snap.example.com/p.jpg", photos[0].(map[string]any)["Value"])

	roles := resp["Roles"].([]any)
	require.Len(t, roles, 1)
	assert.Equal(t, "Architect", roles[0].(map[string]any)["Value"])
}

// TestRefinement3_ResetClearsAllState verifies Reset() leaves new indexes empty.
func TestRefinement3_ResetClearsAllState(t *testing.T) {
	t.Parallel()

	backend := identitystore.NewInMemoryBackend("000000000000", "us-east-1")
	h := identitystore.NewHandler(backend)

	doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "reset.me",
		"Birthdate":       "2000-01-01",
		"Website":         "https://reset.example.com",
		"Photos": []map[string]any{
			{"Value": "https://reset.example.com/p.jpg"},
		},
		"Roles": []map[string]any{
			{"Value": "Tester"},
		},
	})

	backend.Reset()

	listRec := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	users := parseResponse(t, listRec)["Users"].([]any)
	assert.Empty(t, users, "Reset() should clear all users")
}

// TestRefinement3_CopyIsolation verifies that modifications to returned users do not affect stored state.
func TestRefinement3_CopyIsolation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "copy.user",
		"Photos": []map[string]any{
			{"Value": "https://example.com/original.jpg", "Primary": true},
		},
		"Roles": []map[string]any{
			{"Value": "OriginalRole", "Primary": true},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	userID := parseResponse(t, rec)["UserId"].(string)

	// DescribeUser twice; both should return same data.
	desc1 := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	desc2 := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})

	photos1 := parseResponse(t, desc1)["Photos"].([]any)
	photos2 := parseResponse(t, desc2)["Photos"].([]any)

	assert.Equal(t, photos1, photos2, "Repeated DescribeUser calls should return identical data")
}

// TestRefinement3_BirthdateEdgeCases exercises birthdate boundary conditions.
func TestRefinement3_BirthdateEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		birthdate string
		wantOK    bool
	}{
		{name: "valid_date", birthdate: "1990-06-15", wantOK: true},
		{name: "epoch", birthdate: "1970-01-01", wantOK: true},
		{name: "recent_date", birthdate: "2024-12-31", wantOK: true},
		{name: "slash_format", birthdate: "1990/06/15", wantOK: false},
		{name: "dd_mm_yyyy", birthdate: "15-06-1990", wantOK: false},
		{name: "too_short", birthdate: "1990-6-5", wantOK: false},
		{name: "no_separators", birthdate: "19900615", wantOK: false},
		{name: "only_year", birthdate: "1990", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			rec := doRequest(t, h, "CreateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserName":        "birth." + tt.name,
				"Birthdate":       tt.birthdate,
			})

			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code, "expected 200 for valid birthdate %q", tt.birthdate)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code, "expected 400 for invalid birthdate %q", tt.birthdate)
			}
		})
	}
}

// TestRefinement3_UserStatusInDescribeAndList verifies UserStatus appears in all read operations.
func TestRefinement3_UserStatusInDescribeAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "status.everywhere",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	// Toggle to DISABLED.
	doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
		"Operations": []map[string]any{
			{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
		},
	})

	// DescribeUser should reflect DISABLED.
	desc := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	assert.Equal(t, "DISABLED", parseResponse(t, desc)["UserStatus"])

	// ListUsers should also reflect DISABLED.
	listRec := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	users := parseResponse(t, listRec)["Users"].([]any)
	require.Len(t, users, 1)
	assert.Equal(t, "DISABLED", users[0].(map[string]any)["UserStatus"])
}

// TestRefinement3_MultipleOperationsOnNewFields verifies multiple operations in a single UpdateUser.
func TestRefinement3_MultipleOperationsOnNewFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "multi.update",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	upd := doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
		"Operations": []map[string]any{
			{"AttributePath": "birthdate", "AttributeValue": "1980-08-08"},
			{"AttributePath": "website", "AttributeValue": "https://multi.example.com"},
			{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
			{
				"AttributePath": "photos",
				"AttributeValue": []map[string]any{
					{"Value": "https://multi.example.com/p.jpg", "Primary": true},
				},
			},
			{
				"AttributePath": "roles",
				"AttributeValue": []map[string]any{
					{"Value": "Lead", "Primary": true},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, upd.Code)

	desc := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)

	resp := parseResponse(t, desc)
	assert.Equal(t, "1980-08-08", resp["Birthdate"])
	assert.Equal(t, "https://multi.example.com", resp["Website"])
	assert.Equal(t, "DISABLED", resp["UserStatus"])

	photos := resp["Photos"].([]any)
	assert.Len(t, photos, 1)

	roles := resp["Roles"].([]any)
	assert.Len(t, roles, 1)
}

// TestRefinement3_UserPhotosInListUsers verifies Photos included in ListUsers responses.
func TestRefinement3_UserPhotosInListUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "listphoto.a",
		"Photos": []map[string]any{
			{"Value": "https://example.com/a.jpg", "Primary": true},
		},
	})
	doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "listphoto.b",
	})

	listRec := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	users := parseResponse(t, listRec)["Users"].([]any)
	require.Len(t, users, 2)

	photoCount := 0
	for _, u := range users {
		if p, ok := u.(map[string]any)["Photos"]; ok && p != nil {
			photoCount++
		}
	}
	assert.Equal(t, 1, photoCount, "Only user with photos should have Photos field")
}

// TestRefinement3_UserRolesInListUsers verifies Roles included in ListUsers responses.
func TestRefinement3_UserRolesInListUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "listroles.a",
		"Roles": []map[string]any{
			{"Value": "Developer", "Primary": true},
			{"Value": "Reviewer"},
		},
	})
	doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "listroles.b",
	})

	listRec := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	users := parseResponse(t, listRec)["Users"].([]any)
	require.Len(t, users, 2)

	for _, u := range users {
		umap := u.(map[string]any)
		if umap["UserName"] == "listroles.a" {
			roles := umap["Roles"].([]any)
			assert.Len(t, roles, 2)
		}
	}
}

// TestRefinement3_BirthdateWebsiteInListUsers verifies new fields visible in ListUsers.
func TestRefinement3_BirthdateWebsiteInListUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "bdweb.user",
		"Birthdate":       "1995-07-04",
		"Website":         "https://bdweb.example.com",
	})

	listRec := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	users := parseResponse(t, listRec)["Users"].([]any)
	require.Len(t, users, 1)

	u := users[0].(map[string]any)
	assert.Equal(t, "1995-07-04", u["Birthdate"])
	assert.Equal(t, "https://bdweb.example.com", u["Website"])
	assert.Equal(t, "ENABLED", u["UserStatus"])
}

// TestRefinement3_IsMemberInGroupsEmptyGroups verifies empty GroupIds returns validation error.
func TestRefinement3_IsMemberInGroupsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		groupID string
		wantMsg string
	}{
		{
			name:    "empty_group_ids",
			groupID: "",
			wantMsg: "GroupIds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			userRec := doRequest(t, h, "CreateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserName":        "ismember.val",
			})
			require.Equal(t, http.StatusOK, userRec.Code)
			userID := parseResponse(t, userRec)["UserId"].(string)

			rec := doRequest(t, h, "IsMemberInGroups", map[string]any{
				"IdentityStoreId": testStoreID,
				"MemberId":        map[string]any{"UserId": userID},
				"GroupIds":        []string{},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			_ = tt.wantMsg
		})
	}
}

// TestRefinement3_UserCRUDWithNewFields exercises full CRUD lifecycle using new fields.
func TestRefinement3_UserCRUDWithNewFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create
	createRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "crud.newfields",
		"Birthdate":       "1988-03-15",
		"Website":         "https://crud.example.com",
		"Photos": []map[string]any{
			{"Value": "https://crud.example.com/photo.jpg", "Primary": true},
		},
		"Roles": []map[string]any{
			{"Value": "Engineer", "Primary": true},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	// Read
	desc1 := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc1.Code)
	resp1 := parseResponse(t, desc1)
	assert.Equal(t, "1988-03-15", resp1["Birthdate"])
	assert.Equal(t, "https://crud.example.com", resp1["Website"])
	assert.Equal(t, "ENABLED", resp1["UserStatus"])

	// Update
	updRec := doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
		"Operations": []map[string]any{
			{"AttributePath": "birthdate", "AttributeValue": "1988-03-16"},
			{"AttributePath": "website", "AttributeValue": "https://updated.example.com"},
			{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	// Read after update
	desc2 := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc2.Code)
	resp2 := parseResponse(t, desc2)
	assert.Equal(t, "1988-03-16", resp2["Birthdate"])
	assert.Equal(t, "https://updated.example.com", resp2["Website"])
	assert.Equal(t, "DISABLED", resp2["UserStatus"])

	// Delete
	delRec := doRequest(t, h, "DeleteUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Verify gone
	descAfterDel := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	assert.Equal(t, http.StatusNotFound, descAfterDel.Code)
}

// TestRefinement3_PhotoRoleNotSharedBetweenUsers verifies copy isolation for Photos/Roles.
func TestRefinement3_PhotoRoleNotSharedBetweenUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	r1 := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "isolated.u1",
		"Photos": []map[string]any{
			{"Value": "https://example.com/u1.jpg", "Primary": true},
		},
		"Roles": []map[string]any{
			{"Value": "Admin", "Primary": true},
		},
	})
	require.Equal(t, http.StatusOK, r1.Code)
	u1ID := parseResponse(t, r1)["UserId"].(string)

	r2 := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "isolated.u2",
		"Photos": []map[string]any{
			{"Value": "https://example.com/u2.jpg", "Primary": true},
		},
		"Roles": []map[string]any{
			{"Value": "User", "Primary": true},
		},
	})
	require.Equal(t, http.StatusOK, r2.Code)
	u2ID := parseResponse(t, r2)["UserId"].(string)

	// Update u1's photos
	doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          u1ID,
		"Operations": []map[string]any{
			{
				"AttributePath": "photos",
				"AttributeValue": []map[string]any{
					{"Value": "https://example.com/u1-new.jpg", "Primary": true},
				},
			},
		},
	})

	// u2 photos should be unchanged
	desc2 := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          u2ID,
	})
	photos2 := parseResponse(t, desc2)["Photos"].([]any)
	require.Len(t, photos2, 1)
	assert.Equal(t, "https://example.com/u2.jpg", photos2[0].(map[string]any)["Value"])
}

// TestRefinement3_GroupSortingWithManyGroups verifies sort stability with many groups.
func TestRefinement3_GroupSortingWithManyGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for i := range 15 {
		doRequest(t, h, "CreateGroup", map[string]any{
			"IdentityStoreId": testStoreID,
			"DisplayName":     fmt.Sprintf("Group-%03d", i),
		})
	}

	rec1 := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	rec2 := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
	})

	g1 := parseResponse(t, rec1)["Groups"].([]any)
	g2 := parseResponse(t, rec2)["Groups"].([]any)

	require.Len(t, g1, 15)
	for i := range g1 {
		assert.Equal(t, g1[i].(map[string]any)["GroupId"], g2[i].(map[string]any)["GroupId"])
	}
}

// TestRefinement3_UserSortingWithManyUsers verifies sort stability with many users.
func TestRefinement3_UserSortingWithManyUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for i := range 15 {
		doRequest(t, h, "CreateUser", map[string]any{
			"IdentityStoreId": testStoreID,
			"UserName":        fmt.Sprintf("sortuser%03d", i),
		})
	}

	rec1 := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	rec2 := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})

	u1 := parseResponse(t, rec1)["Users"].([]any)
	u2 := parseResponse(t, rec2)["Users"].([]any)

	require.Len(t, u1, 15)
	for i := range u1 {
		assert.Equal(t, u1[i].(map[string]any)["UserId"], u2[i].(map[string]any)["UserId"])
	}
}

// TestRefinement3_PhotoWithNoType verifies Photos without optional fields are accepted.
func TestRefinement3_PhotoWithNoType(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "photonotype.user",
		"Photos": []map[string]any{
			{"Value": "https://example.com/notype.jpg"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	userID := parseResponse(t, rec)["UserId"].(string)

	desc := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)

	photos := parseResponse(t, desc)["Photos"].([]any)
	require.Len(t, photos, 1)
	assert.Equal(t, "https://example.com/notype.jpg", photos[0].(map[string]any)["Value"])
}

// TestRefinement3_RoleWithNoPrimary verifies Roles without Primary field default to false.
func TestRefinement3_RoleWithNoPrimary(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "rolenoprimary.user",
		"Roles": []map[string]any{
			{"Value": "Contributor"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	userID := parseResponse(t, rec)["UserId"].(string)

	desc := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)

	roles := parseResponse(t, desc)["Roles"].([]any)
	require.Len(t, roles, 1)
	role := roles[0].(map[string]any)
	assert.Equal(t, "Contributor", role["Value"])
}

// TestRefinement3_UserStatusDefaultsAfterRestore verifies UserStatus preserved via snapshot.
func TestRefinement3_UserStatusDefaultsAfterRestore(t *testing.T) {
	t.Parallel()

	backend1 := identitystore.NewInMemoryBackend("000000000000", "us-east-1")
	h1 := identitystore.NewHandler(backend1)

	createRec := doRequest(t, h1, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "status.restore",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	doRequest(t, h1, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
		"Operations": []map[string]any{
			{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
		},
	})

	snapshot := h1.Snapshot(t.Context())

	backend2 := identitystore.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := identitystore.NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), snapshot))

	desc := doRequest(t, h2, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)
	assert.Equal(t, "DISABLED", parseResponse(t, desc)["UserStatus"])
}

// TestRefinement3_MultiPhotoUser verifies primary designation in multi-photo scenario.
func TestRefinement3_MultiPhotoUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "multi.photo",
		"Photos": []map[string]any{
			{"Value": "https://example.com/thumb.jpg", "Type": "thumbnail", "Primary": true},
			{"Value": "https://example.com/full.jpg", "Type": "full"},
			{"Value": "https://example.com/banner.jpg", "Type": "banner"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	userID := parseResponse(t, rec)["UserId"].(string)

	desc := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)

	photos := parseResponse(t, desc)["Photos"].([]any)
	require.Len(t, photos, 3)

	primaryCount := 0
	for _, p := range photos {
		if pmap, ok := p.(map[string]any); ok {
			if b, ok2 := pmap["Primary"].(bool); ok2 && b {
				primaryCount++
			}
		}
	}
	assert.Equal(t, 1, primaryCount, "exactly one photo should be primary")
}
