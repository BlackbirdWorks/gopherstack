package dashboard_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	iambackend "github.com/blackbirdworks/gopherstack/iam"
	"github.com/blackbirdworks/gopherstack/internal/teststack"
)

// newIAMStack creates an integration stack with a real IAM backend wired in.
func newIAMStack(t *testing.T) (*teststack.Stack, *iambackend.InMemoryBackend) {
	t.Helper()

	stack := newStack(t)

	return stack, stack.IAMBackend
}

func TestIAMDashboard_Index(t *testing.T) {
	t.Parallel()

	t.Run("EmptyState", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/iam", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "IAM")
		assert.Contains(t, w.Body.String(), "No users")
	})

	t.Run("WithData", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		_, _ = iamBk.CreateUser("alice", "/")
		_, _ = iamBk.CreateRole("ECSTaskRole", "/", "")
		_, _ = iamBk.CreatePolicy("ReadOnlyPolicy", "/", "")
		_, _ = iamBk.CreateGroup("Admins", "/")

		req := httptest.NewRequest(http.MethodGet, "/dashboard/iam", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		assert.Contains(t, body, "alice")
		assert.Contains(t, body, "ECSTaskRole")
		assert.Contains(t, body, "ReadOnlyPolicy")
		assert.Contains(t, body, "Admins")
	})

	t.Run("NilIAMOps", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/iam", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "IAM")
	})
}

func TestIAMDashboard_CreateUser(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)

		form := url.Values{"userName": {"alice"}, "path": {"/"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/user",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/dashboard/iam", w.Header().Get("Hx-Redirect"))

		users := iamBk.ListAllUsers()
		require.Len(t, users, 1)
		assert.Equal(t, "alice", users[0].UserName)
	})

	t.Run("Duplicate_Returns500", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		_, _ = iamBk.CreateUser("alice", "/")

		form := url.Values{"userName": {"alice"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/user",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("NilIAMOps_Noop", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		form := url.Values{"userName": {"alice"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/user",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		// No IAM ops → still redirects OK
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestIAMDashboard_DeleteUser(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		_, _ = iamBk.CreateUser("alice", "/")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/user?name=alice", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/dashboard/iam", w.Header().Get("Hx-Redirect"))
		assert.Empty(t, iamBk.ListAllUsers())
	})

	t.Run("MissingName_Returns400", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/user", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("NotFound_Returns500", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/user?name=nobody", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestIAMDashboard_CreateRole(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)

		form := url.Values{
			"roleName":                 {"MyRole"},
			"path":                     {"/"},
			"assumeRolePolicyDocument": {`{"Version":"2012-10-17"}`},
		}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/role",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/dashboard/iam", w.Header().Get("Hx-Redirect"))
		assert.Len(t, iamBk.ListAllRoles(), 1)
	})

	t.Run("Duplicate_Returns500", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		_, _ = iamBk.CreateRole("MyRole", "/", "")

		form := url.Values{"roleName": {"MyRole"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/role",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestIAMDashboard_DeleteRole(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		_, _ = iamBk.CreateRole("MyRole", "/", "")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/role?name=MyRole", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, iamBk.ListAllRoles())
	})

	t.Run("MissingName_Returns400", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/role", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("NotFound_Returns500", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/role?name=ghost", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestIAMDashboard_CreatePolicy(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)

		form := url.Values{
			"policyName":     {"MyPolicy"},
			"path":           {"/"},
			"policyDocument": {`{"Version":"2012-10-17"}`},
		}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/policy",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/dashboard/iam", w.Header().Get("Hx-Redirect"))
		assert.Len(t, iamBk.ListAllPolicies(), 1)
	})

	t.Run("Duplicate_Returns500", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		_, _ = iamBk.CreatePolicy("MyPolicy", "/", "")

		form := url.Values{"policyName": {"MyPolicy"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/policy",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestIAMDashboard_DeletePolicy(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		pol, _ := iamBk.CreatePolicy("MyPolicy", "/", "")

		req := httptest.NewRequest(http.MethodDelete,
			"/dashboard/iam/policy?arn="+url.QueryEscape(pol.Arn), nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, iamBk.ListAllPolicies())
	})

	t.Run("MissingArn_Returns400", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/policy", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("NotFound_Returns500", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodDelete,
			"/dashboard/iam/policy?arn=arn:aws:iam::000000000000:policy/ghost", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestIAMDashboard_CreateGroup(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)

		form := url.Values{"groupName": {"Admins"}, "path": {"/"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/group",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/dashboard/iam", w.Header().Get("Hx-Redirect"))
		assert.Len(t, iamBk.ListAllGroups(), 1)
	})

	t.Run("Duplicate_Returns500", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		_, _ = iamBk.CreateGroup("Admins", "/")

		form := url.Values{"groupName": {"Admins"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/iam/group",
			strings.NewReader(form))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestIAMDashboard_DeleteGroup(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		stack, iamBk := newIAMStack(t)
		_, _ = iamBk.CreateGroup("Admins", "/")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/group?name=Admins", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, iamBk.ListAllGroups())
	})

	t.Run("MissingName_Returns400", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/group", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("NotFound_Returns500", func(t *testing.T) {
		t.Parallel()
		stack, _ := newIAMStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/iam/group?name=ghost", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
