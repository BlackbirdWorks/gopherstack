package identitystore_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUserPhotos verifies Photos are stored, returned, updated, and limited to 3.
func TestUserPhotos(t *testing.T) {
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

// TestUserPhotosInListUsers verifies Photos included in ListUsers responses.
func TestUserPhotosInListUsers(t *testing.T) {
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

// TestPhotoWithNoType verifies Photos without optional fields are accepted.
func TestPhotoWithNoType(t *testing.T) {
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

// TestMultiPhotoUser verifies primary designation in multi-photo scenario.
func TestMultiPhotoUser(t *testing.T) {
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

// TestUserRoles verifies Roles are stored, returned, and updated.
func TestUserRoles(t *testing.T) {
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

// TestUserRolesInListUsers verifies Roles included in ListUsers responses.
func TestUserRolesInListUsers(t *testing.T) {
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

// TestRoleWithNoPrimary verifies Roles without Primary field default to false.
func TestRoleWithNoPrimary(t *testing.T) {
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

// TestPhotoRoleNotSharedBetweenUsers verifies copy isolation for Photos/Roles.
func TestPhotoRoleNotSharedBetweenUsers(t *testing.T) {
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
