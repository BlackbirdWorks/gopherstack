package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/iam"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---- Backend unit tests ----

func TestInMemoryBackend_Users(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndGetUser", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		u, err := b.CreateUser("alice", "/")
		require.NoError(t, err)
		assert.Equal(t, "alice", u.UserName)
		assert.Equal(t, "/", u.Path)
		assert.NotEmpty(t, u.UserID)
		assert.Contains(t, u.Arn, "alice")

		got, err := b.GetUser("alice")
		require.NoError(t, err)
		assert.Equal(t, "alice", got.UserName)
	})

	t.Run("CreateUserDefaultPath", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		u, err := b.CreateUser("bob", "")
		require.NoError(t, err)
		assert.Equal(t, "/", u.Path)
	})

	t.Run("CreateUserAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateUser("alice", "/")
		require.NoError(t, err)
		_, err = b.CreateUser("alice", "/")
		require.ErrorIs(t, err, iam.ErrUserAlreadyExists)
	})

	t.Run("GetUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.GetUser("nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("DeleteUser", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		err := b.DeleteUser("alice")
		require.NoError(t, err)
		_, err = b.GetUser("alice")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("DeleteUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteUser("nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("ListUsers", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("bob", "/")
		_, _ = b.CreateUser("alice", "/")
		users, err := b.ListUsers()
		require.NoError(t, err)
		require.Len(t, users, 2)
		assert.Equal(t, "alice", users[0].UserName) // sorted
		assert.Equal(t, "bob", users[1].UserName)
	})

	t.Run("ListAllUsers", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("charlie", "/")
		_, _ = b.CreateUser("alice", "/")
		users := b.ListAllUsers()
		require.Len(t, users, 2)
		assert.Equal(t, "alice", users[0].UserName)
	})
}

func TestInMemoryBackend_Roles(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndGetRole", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		doc := `{"Version":"2012-10-17","Statement":[]}`
		r, err := b.CreateRole("MyRole", "/", doc)
		require.NoError(t, err)
		assert.Equal(t, "MyRole", r.RoleName)
		assert.Equal(t, doc, r.AssumeRolePolicyDocument)

		got, err := b.GetRole("MyRole")
		require.NoError(t, err)
		assert.Equal(t, "MyRole", got.RoleName)
	})

	t.Run("CreateRoleAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateRole("MyRole", "/", "")
		require.NoError(t, err)
		_, err = b.CreateRole("MyRole", "/", "")
		require.ErrorIs(t, err, iam.ErrRoleAlreadyExists)
	})

	t.Run("GetRoleNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.GetRole("nonexistent")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})

	t.Run("DeleteRole", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("MyRole", "/", "")
		err := b.DeleteRole("MyRole")
		require.NoError(t, err)
	})

	t.Run("DeleteRoleNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteRole("nonexistent")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})

	t.Run("ListRoles", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("ZRole", "/", "")
		_, _ = b.CreateRole("ARole", "/", "")
		roles, err := b.ListRoles()
		require.NoError(t, err)
		require.Len(t, roles, 2)
		assert.Equal(t, "ARole", roles[0].RoleName) // sorted
	})

	t.Run("ListAllRoles", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("RoleB", "/", "")
		_, _ = b.CreateRole("RoleA", "/", "")
		roles := b.ListAllRoles()
		require.Len(t, roles, 2)
		assert.Equal(t, "RoleA", roles[0].RoleName)
	})
}

func TestInMemoryBackend_Policies(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndListPolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		pol, err := b.CreatePolicy("MyPolicy", "/", `{"Version":"2012-10-17"}`)
		require.NoError(t, err)
		assert.Equal(t, "MyPolicy", pol.PolicyName)
		assert.NotEmpty(t, pol.Arn)

		policies, err := b.ListPolicies()
		require.NoError(t, err)
		require.Len(t, policies, 1)
	})

	t.Run("CreatePolicyAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreatePolicy("MyPolicy", "/", "")
		require.NoError(t, err)
		_, err = b.CreatePolicy("MyPolicy", "/", "")
		require.ErrorIs(t, err, iam.ErrPolicyAlreadyExists)
	})

	t.Run("DeletePolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		pol, err := b.CreatePolicy("MyPolicy", "/", "")
		require.NoError(t, err)
		err = b.DeletePolicy(pol.Arn)
		require.NoError(t, err)
		policies, _ := b.ListPolicies()
		assert.Empty(t, policies)
	})

	t.Run("DeletePolicyNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeletePolicy("arn:aws:iam::000000000000:policy/nonexistent")
		require.ErrorIs(t, err, iam.ErrPolicyNotFound)
	})

	t.Run("AttachUserPolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		err := b.AttachUserPolicy("alice", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.NoError(t, err)
	})

	t.Run("AttachUserPolicyUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.AttachUserPolicy("nonexistent", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("AttachRolePolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("MyRole", "/", "")
		err := b.AttachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.NoError(t, err)
	})

	t.Run("AttachRolePolicyRoleNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.AttachRolePolicy("nonexistent", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})

	t.Run("ListAllPolicies", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreatePolicy("ZPolicy", "/", "")
		_, _ = b.CreatePolicy("APolicy", "/", "")
		policies := b.ListAllPolicies()
		require.Len(t, policies, 2)
		assert.Equal(t, "APolicy", policies[0].PolicyName)
	})
}

func TestInMemoryBackend_Groups(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndDeleteGroup", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		g, err := b.CreateGroup("Admins", "/")
		require.NoError(t, err)
		assert.Equal(t, "Admins", g.GroupName)

		err = b.DeleteGroup("Admins")
		require.NoError(t, err)
	})

	t.Run("CreateGroupAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateGroup("Admins", "/")
		require.NoError(t, err)
		_, err = b.CreateGroup("Admins", "/")
		require.ErrorIs(t, err, iam.ErrGroupAlreadyExists)
	})

	t.Run("DeleteGroupNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteGroup("nonexistent")
		require.ErrorIs(t, err, iam.ErrGroupNotFound)
	})

	t.Run("AddUserToGroup", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateGroup("Admins", "/")
		_, _ = b.CreateUser("alice", "/")
		err := b.AddUserToGroup("Admins", "alice")
		require.NoError(t, err)
	})

	t.Run("AddUserToGroupGroupNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		err := b.AddUserToGroup("nonexistent", "alice")
		require.ErrorIs(t, err, iam.ErrGroupNotFound)
	})

	t.Run("AddUserToGroupUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateGroup("Admins", "/")
		err := b.AddUserToGroup("Admins", "nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("ListAllGroups", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateGroup("ZGroup", "/")
		_, _ = b.CreateGroup("AGroup", "/")
		groups := b.ListAllGroups()
		require.Len(t, groups, 2)
		assert.Equal(t, "AGroup", groups[0].GroupName)
	})
}

func TestInMemoryBackend_AccessKeys(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndListAccessKeys", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		ak, err := b.CreateAccessKey("alice")
		require.NoError(t, err)
		assert.Equal(t, "alice", ak.UserName)
		assert.Equal(t, "Active", ak.Status)
		assert.NotEmpty(t, ak.AccessKeyID)
		assert.NotEmpty(t, ak.SecretAccessKey)

		keys, err := b.ListAccessKeys("alice")
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.Equal(t, ak.AccessKeyID, keys[0].AccessKeyID)
	})

	t.Run("CreateAccessKeyUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateAccessKey("nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("DeleteAccessKey", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		ak, _ := b.CreateAccessKey("alice")
		err := b.DeleteAccessKey("alice", ak.AccessKeyID)
		require.NoError(t, err)
		keys, _ := b.ListAccessKeys("alice")
		assert.Empty(t, keys)
	})

	t.Run("DeleteAccessKeyNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		err := b.DeleteAccessKey("alice", "AKIANONEXISTENT")
		require.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
	})

	t.Run("ListAccessKeysUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.ListAccessKeys("nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("ListAllAccessKeys", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		_, _ = b.CreateAccessKey("alice")
		keys := b.ListAllAccessKeys()
		require.Len(t, keys, 1)
	})
}

func TestInMemoryBackend_InstanceProfiles(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndListInstanceProfiles", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		ip, err := b.CreateInstanceProfile("MyProfile", "/")
		require.NoError(t, err)
		assert.Equal(t, "MyProfile", ip.InstanceProfileName)
		assert.Contains(t, ip.Arn, "MyProfile")

		profiles, err := b.ListInstanceProfiles()
		require.NoError(t, err)
		require.Len(t, profiles, 1)
	})

	t.Run("CreateInstanceProfileAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateInstanceProfile("MyProfile", "/")
		require.NoError(t, err)
		_, err = b.CreateInstanceProfile("MyProfile", "/")
		require.ErrorIs(t, err, iam.ErrInstanceProfileAlreadyExists)
	})

	t.Run("DeleteInstanceProfile", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateInstanceProfile("MyProfile", "/")
		err := b.DeleteInstanceProfile("MyProfile")
		require.NoError(t, err)
		profiles, _ := b.ListInstanceProfiles()
		assert.Empty(t, profiles)
	})

	t.Run("DeleteInstanceProfileNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteInstanceProfile("nonexistent")
		require.ErrorIs(t, err, iam.ErrInstanceProfileNotFound)
	})

	t.Run("ListAllInstanceProfiles", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateInstanceProfile("ZProfile", "/")
		_, _ = b.CreateInstanceProfile("AProfile", "/")
		profiles := b.ListAllInstanceProfiles()
		require.Len(t, profiles, 2)
		assert.Equal(t, "AProfile", profiles[0].InstanceProfileName)
	})
}

// ---- Handler HTTP tests ----

// iamRequest creates a form-encoded IAM HTTP request.
func iamRequest(action string, params map[string]string) *http.Request {
	vals := url.Values{}
	vals.Set("Action", action)
	vals.Set("Version", "2010-05-08")
	for k, v := range params {
		vals.Set(k, v)
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(vals.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	return req
}

func newTestHandler(t *testing.T) (*iam.Handler, *iam.InMemoryBackend) {
	t.Helper()
	b := iam.NewInMemoryBackend()
	h := iam.NewHandler(b, logger.NewTestLogger())

	return h, b
}

func TestIAMHandler_Users(t *testing.T) {
	t.Parallel()

	t.Run("CreateUser", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreateUser", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Header().Get("Content-Type"), "text/xml")

		var resp iam.CreateUserResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "alice", resp.CreateUserResult.User.UserName)
	})

	t.Run("GetUser", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/")

		req := iamRequest("GetUser", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("GetUserNotFound", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("GetUser", map[string]string{"UserName": "nonexistent"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "NoSuchEntity", errResp.Error.Code)
	})

	t.Run("DeleteUser", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/")

		req := iamRequest("DeleteUser", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListUsers", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/")
		_, _ = b.CreateUser("bob", "/")

		req := iamRequest("ListUsers", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListUsersResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListUsersResult.Users, 2)
	})
}

func TestIAMHandler_Roles(t *testing.T) {
	t.Parallel()

	t.Run("CreateRole", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreateRole", map[string]string{
			"RoleName":                 "MyRole",
			"AssumeRolePolicyDocument": `{"Version":"2012-10-17"}`,
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreateRoleResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "MyRole", resp.CreateRoleResult.Role.RoleName)
	})

	t.Run("GetRole", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "")

		req := iamRequest("GetRole", map[string]string{"RoleName": "MyRole"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("DeleteRole", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "")

		req := iamRequest("DeleteRole", map[string]string{"RoleName": "MyRole"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListRoles", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("RoleA", "/", "")
		_, _ = b.CreateRole("RoleB", "/", "")

		req := iamRequest("ListRoles", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListRolesResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListRolesResult.Roles, 2)
	})
}

func TestIAMHandler_Policies(t *testing.T) {
	t.Parallel()

	t.Run("CreatePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreatePolicy", map[string]string{
			"PolicyName":     "MyPolicy",
			"PolicyDocument": `{"Version":"2012-10-17"}`,
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreatePolicyResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "MyPolicy", resp.CreatePolicyResult.Policy.PolicyName)
	})

	t.Run("DeletePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		pol, _ := b.CreatePolicy("MyPolicy", "/", "")

		req := iamRequest("DeletePolicy", map[string]string{"PolicyArn": pol.Arn})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListPolicies", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreatePolicy("PolicyA", "/", "")

		req := iamRequest("ListPolicies", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListPoliciesResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListPoliciesResult.Policies, 1)
	})

	t.Run("AttachUserPolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/")

		req := iamRequest("AttachUserPolicy", map[string]string{
			"UserName":  "alice",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("AttachRolePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "")

		req := iamRequest("AttachRolePolicy", map[string]string{
			"RoleName":  "MyRole",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestIAMHandler_Groups(t *testing.T) {
	t.Parallel()

	t.Run("CreateGroup", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreateGroup", map[string]string{"GroupName": "Admins"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreateGroupResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "Admins", resp.CreateGroupResult.Group.GroupName)
	})

	t.Run("DeleteGroup", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateGroup("Admins", "/")

		req := iamRequest("DeleteGroup", map[string]string{"GroupName": "Admins"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("AddUserToGroup", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateGroup("Admins", "/")
		_, _ = b.CreateUser("alice", "/")

		req := iamRequest("AddUserToGroup", map[string]string{
			"GroupName": "Admins",
			"UserName":  "alice",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestIAMHandler_AccessKeys(t *testing.T) {
	t.Parallel()

	t.Run("CreateAccessKey", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/")

		req := iamRequest("CreateAccessKey", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreateAccessKeyResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "alice", resp.CreateAccessKeyResult.AccessKey.UserName)
		assert.Equal(t, "Active", resp.CreateAccessKeyResult.AccessKey.Status)
	})

	t.Run("DeleteAccessKey", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/")
		ak, _ := b.CreateAccessKey("alice")

		req := iamRequest("DeleteAccessKey", map[string]string{
			"UserName":    "alice",
			"AccessKeyId": ak.AccessKeyID,
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListAccessKeys", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/")
		_, _ = b.CreateAccessKey("alice")

		req := iamRequest("ListAccessKeys", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListAccessKeysResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListAccessKeysResult.AccessKeyMetadata, 1)
	})
}

func TestIAMHandler_InstanceProfiles(t *testing.T) {
	t.Parallel()

	t.Run("CreateInstanceProfile", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreateInstanceProfile", map[string]string{"InstanceProfileName": "MyProfile"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreateInstanceProfileResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "MyProfile", resp.CreateInstanceProfileResult.InstanceProfile.InstanceProfileName)
	})

	t.Run("DeleteInstanceProfile", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateInstanceProfile("MyProfile", "/")

		req := iamRequest("DeleteInstanceProfile", map[string]string{"InstanceProfileName": "MyProfile"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListInstanceProfiles", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateInstanceProfile("ProfileA", "/")

		req := iamRequest("ListInstanceProfiles", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListInstanceProfilesResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListInstanceProfilesResult.InstanceProfiles, 1)
	})
}

func TestIAMHandler_Routing(t *testing.T) {
	t.Parallel()

	t.Run("MissingActionReturns400", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := httptest.NewRequest(http.MethodPost, "/",
			strings.NewReader("Version=2010-05-08"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("UnknownActionReturns400", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("NonExistentAction", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "InvalidAction", errResp.Error.Code)
	})

	t.Run("GetMethodReturnsOperationList", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "CreateUser")
	})

	t.Run("WrongMethodReturns405", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := httptest.NewRequest(http.MethodPut, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})

	t.Run("EntityAlreadyExistsError", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/")

		req := iamRequest("CreateUser", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "EntityAlreadyExists", errResp.Error.Code)
	})

	t.Run("RouteMatcher_MatchesIAMRequests", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		matcher := h.RouteMatcher()

		req := iamRequest("CreateUser", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.True(t, matcher(c))
	})

	t.Run("RouteMatcher_RejectsNonIAM", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		matcher := h.RouteMatcher()

		// JSON request (not form-encoded)
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Action":"ListUsers"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.False(t, matcher(c))
	})

	t.Run("RouteMatcher_RejectsGETRequests", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		matcher := h.RouteMatcher()

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.False(t, matcher(c))
	})

	t.Run("ExtractOperation", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreateUser", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.Equal(t, "CreateUser", h.ExtractOperation(c))
	})

	t.Run("ExtractResource", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreateUser", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.Equal(t, "alice", h.ExtractResource(c))
	})

	t.Run("MatchPriority", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		assert.Equal(t, 80, h.MatchPriority())
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		assert.Equal(t, "IAM", h.Name())
	})

	t.Run("GetSupportedOperations", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		ops := h.GetSupportedOperations()
		assert.Contains(t, ops, "CreateUser")
		assert.Contains(t, ops, "CreateRole")
		assert.Contains(t, ops, "CreatePolicy")
	})
}
