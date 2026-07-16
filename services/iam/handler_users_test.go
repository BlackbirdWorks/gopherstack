package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TagUser_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	req := iamRequest("TagUser", map[string]string{
		"UserName":            "alice",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListUserTags", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "env")
	assert.Contains(t, rec2.Body.String(), "prod")

	req3 := iamRequest("UntagUser", map[string]string{
		"UserName":         "alice",
		"TagKeys.member.1": "env",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_UpdateUser(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	req := iamRequest("UpdateUser", map[string]string{
		"UserName":    "alice",
		"NewUserName": "alicia",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Old name should be gone.
	req2 := iamRequest("GetUser", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusNotFound, rec2.Code)

	// New name should exist.
	req3 := iamRequest("GetUser", map[string]string{"UserName": "alicia"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
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
		_, _ = b.CreateUser("alice", "/", "")

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
		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "NoSuchEntity", errResp.Error.Code)
	})

	t.Run("DeleteUser", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/", "")

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
		_, _ = b.CreateUser("alice", "/", "")
		_, _ = b.CreateUser("bob", "/", "")

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

func TestIAMHandler_ListUsers_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params      map[string]string
		name        string
		wantLen     int
		wantHasNext bool
	}{
		{
			name:        "all users",
			params:      nil,
			wantLen:     3,
			wantHasNext: false,
		},
		{
			name:        "first page MaxItems=2",
			params:      map[string]string{"MaxItems": "2"},
			wantLen:     2,
			wantHasNext: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			_, _ = b.CreateUser("alice", "/", "")
			_, _ = b.CreateUser("bob", "/", "")
			_, _ = b.CreateUser("charlie", "/", "")

			e := echo.New()
			req := iamRequest("ListUsers", tt.params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp iam.ListUsersResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ListUsersResult.Users, tt.wantLen)
			assert.Equal(t, tt.wantHasNext, resp.ListUsersResult.IsTruncated)
		})
	}
}
