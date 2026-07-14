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

func TestHandler_AttachGroupPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateGroup("Admins", "/")
	p, _ := b.CreatePolicy("P", "/", doc)

	req := iamRequest("AttachGroupPolicy", map[string]string{
		"GroupName": "Admins",
		"PolicyArn": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListAttachedGroupPolicies", map[string]string{"GroupName": "Admins"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), p.Arn)

	req3 := iamRequest("DetachGroupPolicy", map[string]string{
		"GroupName": "Admins",
		"PolicyArn": p.Arn,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_GroupMembership_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateGroup("Admins", "/")

	req := iamRequest("AddUserToGroup", map[string]string{
		"UserName":  "alice",
		"GroupName": "Admins",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetGroup", map[string]string{"GroupName": "Admins"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "alice")

	req3 := iamRequest("ListGroupsForUser", map[string]string{"UserName": "alice"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Contains(t, rec3.Body.String(), "Admins")

	req4 := iamRequest("RemoveUserFromGroup", map[string]string{
		"UserName":  "alice",
		"GroupName": "Admins",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestHandler_UpdateGroup(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateGroup("OldGroup", "/")

	req := iamRequest("UpdateGroup", map[string]string{
		"GroupName":    "OldGroup",
		"NewGroupName": "NewGroup",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
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
		_, _ = b.CreateUser("alice", "/", "")

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

func TestIAMHandler_ListGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params  map[string]string
		name    string
		wantLen int
	}{
		{
			name:    "all groups",
			params:  nil,
			wantLen: 2,
		},
		{
			name:    "first page MaxItems=1",
			params:  map[string]string{"MaxItems": "1"},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			_, _ = b.CreateGroup("Admins", "/")
			_, _ = b.CreateGroup("Devs", "/")

			e := echo.New()
			req := iamRequest("ListGroups", tt.params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp iam.ListGroupsResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ListGroupsResult.Groups, tt.wantLen)
		})
	}
}
