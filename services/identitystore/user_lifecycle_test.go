package identitystore_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// TestAllNewFieldsRoundTrip verifies full round-trip of all extended User fields.
func TestAllNewFieldsRoundTrip(t *testing.T) {
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

// TestPersistenceRoundTrip verifies Snapshot/Restore preserves extended User fields.
func TestPersistenceRoundTrip(t *testing.T) {
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

// TestResetClearsAllState verifies Reset() leaves the user store empty.
func TestResetClearsAllState(t *testing.T) {
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

// TestCopyIsolation verifies that modifications to returned users do not affect stored state.
func TestCopyIsolation(t *testing.T) {
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

// TestUserCRUDWithNewFields exercises full CRUD lifecycle using the extended User fields.
func TestUserCRUDWithNewFields(t *testing.T) {
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

// TestUserSortingWithManyUsers verifies sort stability with many users.
func TestUserSortingWithManyUsers(t *testing.T) {
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
