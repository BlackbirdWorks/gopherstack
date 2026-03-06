package iam_test

import (
	"encoding/xml"
	"errors"
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
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// errSimulated is a sentinel error used to exercise InternalFailure handling.
var errSimulated = errors.New("simulated internal error")

// errBackend wraps InMemoryBackend and overrides CreateUser to return a raw (non-sentinel) error.
// This is used to exercise the InternalFailure code path in handleError.
type errBackend struct {
	*iam.InMemoryBackend
}

func (e *errBackend) CreateUser(_, _ string) (*iam.User, error) {
	return nil, errSimulated
}

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
	h := iam.NewHandler(b)

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

// TestIAMProvider covers Provider.Name() and Provider.Init().
func TestIAMProvider(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		p := &iam.Provider{}
		assert.Equal(t, "IAM", p.Name())
	})

	t.Run("Init", func(t *testing.T) {
		t.Parallel()
		p := &iam.Provider{}
		appCtx := &service.AppContext{Logger: logger.NewTestLogger()}
		svc, err := p.Init(appCtx)
		require.NoError(t, err)
		require.NotNil(t, svc)
		h, ok := svc.(*iam.Handler)
		require.True(t, ok)
		assert.NotNil(t, h.Backend)
	})
}

// TestIAMHandler_ExtractEdgeCases covers remaining branches in ExtractOperation,
// ExtractResource, RouteMatcher, and Handler().
func TestIAMHandler_ExtractEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("ExtractOperation_NoAction", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		// Body has no Action param → returns "Unknown"
		req := httptest.NewRequest(http.MethodPost, "/",
			strings.NewReader("Version=2010-05-08"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.Equal(t, "Unknown", h.ExtractOperation(c))
	})

	t.Run("ExtractResource_NoMatchingKey", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		// ListUsers has no UserName/RoleName/etc → returns ""
		req := iamRequest("ListUsers", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.Empty(t, h.ExtractResource(c))
	})

	t.Run("RouteMatcher_FormEncodedMissingVersion", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)
		matcher := h.RouteMatcher()

		// Form-encoded POST but body doesn't contain the IAM version string
		req := httptest.NewRequest(http.MethodPost, "/",
			strings.NewReader("Action=ListUsers"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.False(t, matcher(c))
	})

	t.Run("Handler_GETNonRoot_Returns405", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		// GET at a non-root path falls through to the method-not-allowed branch
		req := httptest.NewRequest(http.MethodGet, "/some/path", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})
}

// TestIAMHandler_SortCoverage adds tests with 2+ items to exercise
// the comparison closures inside [sort.Slice] calls.
func TestIAMHandler_SortCoverage(t *testing.T) {
	t.Parallel()

	t.Run("ListPolicies_TwoItems", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreatePolicy("ZPolicy", "/", "")
		_, _ = b.CreatePolicy("APolicy", "/", "")
		policies, err := b.ListPolicies()
		require.NoError(t, err)
		require.Len(t, policies, 2)
		assert.Equal(t, "APolicy", policies[0].PolicyName)
	})

	t.Run("ListAccessKeys_TwoKeys", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		_, _ = b.CreateAccessKey("alice")
		_, _ = b.CreateAccessKey("alice")
		keys, err := b.ListAccessKeys("alice")
		require.NoError(t, err)
		require.Len(t, keys, 2)
	})

	t.Run("ListAllAccessKeys_TwoKeys", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/")
		_, _ = b.CreateAccessKey("alice")
		_, _ = b.CreateAccessKey("alice")
		keys := b.ListAllAccessKeys()
		require.Len(t, keys, 2)
	})

	t.Run("ListInstanceProfiles_TwoItems", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateInstanceProfile("ZProfile", "/")
		_, _ = b.CreateInstanceProfile("AProfile", "/")
		profiles, err := b.ListInstanceProfiles()
		require.NoError(t, err)
		require.Len(t, profiles, 2)
		assert.Equal(t, "AProfile", profiles[0].InstanceProfileName)
	})
}

func TestInMemoryBackend_DetachRolePolicy(t *testing.T) {
	t.Parallel()

	t.Run("DetachExistingPolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("MyRole", "/", "")
		_ = b.AttachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")
		err := b.DetachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.NoError(t, err)

		policies, err := b.ListAttachedRolePolicies("MyRole")
		require.NoError(t, err)
		assert.Empty(t, policies)
	})

	t.Run("DetachNonExistentPolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("MyRole", "/", "")
		err := b.DetachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/NoSuchPolicy")
		require.NoError(t, err)
	})

	t.Run("DetachRolePolicyRoleNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DetachRolePolicy("nonexistent", "arn:aws:iam::000000000000:policy/P")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})
}

func TestIAMHandler_DetachRolePolicy(t *testing.T) {
	t.Parallel()

	t.Run("DetachRolePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "")
		_ = b.AttachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")

		req := iamRequest("DetachRolePolicy", map[string]string{
			"RoleName":  "MyRole",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("DetachRolePolicyRoleNotFound", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("DetachRolePolicy", map[string]string{
			"RoleName":  "nonexistent",
			"PolicyArn": "arn:aws:iam::000000000000:policy/P",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "NoSuchEntity", errResp.Error.Code)
	})
}

func TestIAMHandler_TagAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setup            func(*iam.InMemoryBackend) string
		tagAction        string
		tagParams        func(id string) map[string]string
		wantTagResp      string
		listAction       string
		listParams       func(id string) map[string]string
		wantListContains []string
	}{
		{
			name:      "role",
			setup:     func(_ *iam.InMemoryBackend) string { return "MyRole" },
			tagAction: "TagRole",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"RoleName":            id,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "prod",
					"Tags.member.2.Key":   "team",
					"Tags.member.2.Value": "platform",
				}
			},
			wantTagResp:      "TagRoleResponse",
			listAction:       "ListRoleTags",
			listParams:       func(id string) map[string]string { return map[string]string{"RoleName": id} },
			wantListContains: []string{"env", "prod"},
		},
		{
			name:      "user",
			setup:     func(_ *iam.InMemoryBackend) string { return "alice" },
			tagAction: "TagUser",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"UserName":            id,
					"Tags.member.1.Key":   "dept",
					"Tags.member.1.Value": "engineering",
				}
			},
			wantTagResp:      "TagUserResponse",
			listAction:       "ListUserTags",
			listParams:       func(id string) map[string]string { return map[string]string{"UserName": id} },
			wantListContains: []string{"dept", "engineering"},
		},
		{
			name: "policy",
			setup: func(b *iam.InMemoryBackend) string {
				pol, _ := b.CreatePolicy("MyPolicy", "/", `{"Version":"2012-10-17"}`)

				return pol.Arn
			},
			tagAction: "TagPolicy",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"PolicyArn":           id,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "staging",
					"Tags.member.2.Key":   "owner",
					"Tags.member.2.Value": "platform",
				}
			},
			wantTagResp:      "TagPolicyResponse",
			listAction:       "ListPolicyTags",
			listParams:       func(id string) map[string]string { return map[string]string{"PolicyArn": id} },
			wantListContains: []string{"ListPolicyTagsResponse", "env", "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			h, b := newTestHandler(t)
			id := tt.setup(b)

			req := iamRequest(tt.tagAction, tt.tagParams(id))
			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantTagResp)

			req = iamRequest(tt.listAction, tt.listParams(id))
			rec = httptest.NewRecorder()
			err = h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)
			for _, want := range tt.wantListContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestIAMHandler_UntagAndVerify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*iam.InMemoryBackend) string
		tagAction      string
		tagParams      func(id string) map[string]string
		untagAction    string
		untagParams    func(id string) map[string]string
		wantUntagResp  string
		listAction     string
		listParams     func(id string) map[string]string
		wantListAbsent []string
	}{
		{
			name:      "role",
			setup:     func(_ *iam.InMemoryBackend) string { return "MyRole" },
			tagAction: "TagRole",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"RoleName":            id,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "prod",
				}
			},
			untagAction: "UntagRole",
			untagParams: func(id string) map[string]string {
				return map[string]string{"RoleName": id, "TagKeys.member.1": "env"}
			},
			wantUntagResp:  "UntagRoleResponse",
			listAction:     "ListRoleTags",
			listParams:     func(id string) map[string]string { return map[string]string{"RoleName": id} },
			wantListAbsent: []string{"env"},
		},
		{
			name:      "user",
			setup:     func(_ *iam.InMemoryBackend) string { return "alice" },
			tagAction: "TagUser",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"UserName":            id,
					"Tags.member.1.Key":   "dept",
					"Tags.member.1.Value": "engineering",
				}
			},
			untagAction: "UntagUser",
			untagParams: func(id string) map[string]string {
				return map[string]string{"UserName": id, "TagKeys.member.1": "dept"}
			},
			wantUntagResp:  "UntagUserResponse",
			listAction:     "ListUserTags",
			listParams:     func(id string) map[string]string { return map[string]string{"UserName": id} },
			wantListAbsent: []string{"dept"},
		},
		{
			name: "policy",
			setup: func(b *iam.InMemoryBackend) string {
				pol, _ := b.CreatePolicy("MyPolicy", "/", `{"Version":"2012-10-17"}`)

				return pol.Arn
			},
			tagAction: "TagPolicy",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"PolicyArn":           id,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "prod",
				}
			},
			untagAction: "UntagPolicy",
			untagParams: func(id string) map[string]string {
				return map[string]string{"PolicyArn": id, "TagKeys.member.1": "env"}
			},
			wantUntagResp:  "UntagPolicyResponse",
			listAction:     "ListPolicyTags",
			listParams:     func(id string) map[string]string { return map[string]string{"PolicyArn": id} },
			wantListAbsent: []string{"env"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			h, b := newTestHandler(t)
			id := tt.setup(b)

			req := iamRequest(tt.tagAction, tt.tagParams(id))
			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)

			req = iamRequest(tt.untagAction, tt.untagParams(id))
			rec = httptest.NewRecorder()
			err = h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantUntagResp)

			req = iamRequest(tt.listAction, tt.listParams(id))
			rec = httptest.NewRecorder()
			err = h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			for _, absent := range tt.wantListAbsent {
				assert.NotContains(t, rec.Body.String(), absent)
			}
		})
	}
}

// TestIAMHandler_InternalFailure tests the InternalFailure error code path
// by injecting a backend that returns a raw (non-sentinel) error.
func TestIAMHandler_InternalFailure(t *testing.T) {
	t.Parallel()

	e := echo.New()
	eb := &errBackend{iam.NewInMemoryBackend()}
	h := iam.NewHandler(eb)

	req := iamRequest("CreateUser", map[string]string{"UserName": "alice"})
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InternalFailure", errResp.Error.Code)
}

// TestIAMHandler_DispatchErrors covers the error branches in every dispatch case.
// These tests trigger "NoSuchEntity" / "EntityAlreadyExists" responses from the backend.
func TestIAMHandler_DispatchErrors(t *testing.T) {
	t.Parallel()

	// helper to assert the correct error code in the XML response
	assertErrorCode := func(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
		t.Helper()
		assert.Equal(t, wantStatus, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, wantCode, errResp.Error.Code)
	}

	for _, tc := range []struct {
		name       string
		action     string
		params     map[string]string
		setup      func(*iam.InMemoryBackend)
		wantCode   string
		wantStatus int
	}{
		{
			name:       "DeleteUser_NotFound",
			action:     "DeleteUser",
			params:     map[string]string{"UserName": "nobody"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "CreateRole_AlreadyExists",
			action: "CreateRole",
			params: map[string]string{"RoleName": "MyRole"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "")
			},
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GetRole_NotFound",
			action:     "GetRole",
			params:     map[string]string{"RoleName": "ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteRole_NotFound",
			action:     "DeleteRole",
			params:     map[string]string{"RoleName": "ghost"},
			setup:      func(_ *iam.InMemoryBackend) {},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "CreatePolicy_AlreadyExists",
			action: "CreatePolicy",
			params: map[string]string{"PolicyName": "MyPolicy"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy("MyPolicy", "/", "")
			},
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeletePolicy_NotFound",
			action:     "DeletePolicy",
			params:     map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AttachUserPolicy_UserNotFound",
			action:     "AttachUserPolicy",
			params:     map[string]string{"UserName": "nobody", "PolicyArn": "arn:aws:iam::000000000000:policy/P"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AttachRolePolicy_RoleNotFound",
			action:     "AttachRolePolicy",
			params:     map[string]string{"RoleName": "ghost", "PolicyArn": "arn:aws:iam::000000000000:policy/P"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "CreateGroup_AlreadyExists",
			action: "CreateGroup",
			params: map[string]string{"GroupName": "Admins"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteGroup_NotFound",
			action:     "DeleteGroup",
			params:     map[string]string{"GroupName": "ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AddUserToGroup_GroupNotFound",
			action:     "AddUserToGroup",
			params:     map[string]string{"GroupName": "ghost", "UserName": "alice"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "CreateAccessKey_UserNotFound",
			action:     "CreateAccessKey",
			params:     map[string]string{"UserName": "nobody"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "DeleteAccessKey_NotFound",
			action: "DeleteAccessKey",
			params: map[string]string{"UserName": "alice", "AccessKeyId": "AKIANONEXISTENT"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/")
			},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "ListAccessKeys_UserNotFound",
			action:     "ListAccessKeys",
			params:     map[string]string{"UserName": "nobody"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "CreateInstanceProfile_AlreadyExists",
			action: "CreateInstanceProfile",
			params: map[string]string{"InstanceProfileName": "MyProfile"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("MyProfile", "/")
			},
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteInstanceProfile_NotFound",
			action:     "DeleteInstanceProfile",
			params:     map[string]string{"InstanceProfileName": "ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "DeleteUser_DeleteConflict",
			action: "DeleteUser",
			params: map[string]string{"UserName": "alice"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/")
				pol, _ := b.CreatePolicy("StuckPolicy", "/", "")
				_ = b.AttachUserPolicy("alice", pol.Arn)
			},
			wantCode:   "DeleteConflict",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "DeleteRole_DeleteConflict",
			action: "DeleteRole",
			params: map[string]string{"RoleName": "MyRole"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "")
				pol, _ := b.CreatePolicy("RolePolicy", "/", "")
				_ = b.AttachRolePolicy("MyRole", pol.Arn)
			},
			wantCode:   "DeleteConflict",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "CreateRole_MalformedPolicyDocument",
			action: "CreateRole",
			params: map[string]string{
				"RoleName":                 "BadRole",
				"AssumeRolePolicyDocument": "not-json",
			},
			wantCode:   "MalformedPolicyDocument",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "CreatePolicy_MalformedPolicyDocument",
			action: "CreatePolicy",
			params: map[string]string{
				"PolicyName":     "BadPolicy",
				"PolicyDocument": "not-json",
			},
			wantCode:   "MalformedPolicyDocument",
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			h, b := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(b)
			}

			req := iamRequest(tc.action, tc.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assertErrorCode(t, rec, tc.wantStatus, tc.wantCode)
		})
	}
}
