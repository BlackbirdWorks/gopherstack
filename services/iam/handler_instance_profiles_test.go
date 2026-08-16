package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// Test_ListInstanceProfilesForRole verifies ListInstanceProfilesForRole
// returns the real instance profiles containing the given role, instead of
// the previous hardcoded-empty stub response (which silently discarded the
// RoleName parameter and never touched backend state).
func Test_ListInstanceProfilesForRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, b *iam.InMemoryBackend)
		name        string
		roleName    string
		wantProfile string
		wantCode    int
		wantEmpty   bool
	}{
		{
			name: "role_with_one_profile",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("MyRole", "/", "{}", "")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("MyProfile", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "MyRole"))
			},
			roleName:    "MyRole",
			wantCode:    http.StatusOK,
			wantProfile: "MyProfile",
		},
		{
			name: "role_with_no_profiles",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("LonelyRole", "/", "{}", "")
				require.NoError(t, err)
			},
			roleName:  "LonelyRole",
			wantCode:  http.StatusOK,
			wantEmpty: true,
		},
		{
			name:     "role_not_found",
			setup:    func(_ *testing.T, _ *iam.InMemoryBackend) {},
			roleName: "ghost",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(t, b)

			req := iamRequest("ListInstanceProfilesForRole", map[string]string{"RoleName": tt.roleName})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantProfile != "" {
				assert.Contains(t, rec.Body.String(), tt.wantProfile)
			}

			if tt.wantEmpty {
				assert.Contains(t, rec.Body.String(), "<InstanceProfiles></InstanceProfiles>")
			}
		})
	}
}

// Test_GetAccountAuthorizationDetails_InstanceProfileList verifies that
// RoleDetail.InstanceProfileList reports every instance profile containing the
// role, matching the real IAM RoleDetail shape
// (https://docs.aws.amazon.com/IAM/latest/APIReference/API_RoleDetail.html).
// Previously RoleDetailXML had no InstanceProfileList field at all, so this
// element was always absent from the wire response regardless of backend state.
func Test_GetAccountAuthorizationDetails_InstanceProfileList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(t *testing.T, b *iam.InMemoryBackend)
		name              string
		roleName          string
		wantProfileNames  []string
		wantProfileRoleID bool
	}{
		{
			name: "role_with_one_profile",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("MyRole", "/", "{}", "")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("MyProfile", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "MyRole"))
			},
			roleName:          "MyRole",
			wantProfileNames:  []string{"MyProfile"},
			wantProfileRoleID: true,
		},
		{
			name: "role_with_two_profiles_sorted",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("SharedRole", "/", "{}", "")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("ZProfile", "/")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("AProfile", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddRoleToInstanceProfile("ZProfile", "SharedRole"))
				require.NoError(t, b.AddRoleToInstanceProfile("AProfile", "SharedRole"))
			},
			roleName:         "SharedRole",
			wantProfileNames: []string{"AProfile", "ZProfile"},
		},
		{
			name: "role_with_no_profiles",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("LonelyRole", "/", "{}", "")
				require.NoError(t, err)
			},
			roleName:         "LonelyRole",
			wantProfileNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(t, b)

			req := iamRequest("GetAccountAuthorizationDetails", nil)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp iam.GetAccountAuthorizationDetailsResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			var role *iam.RoleDetailXML
			for i := range resp.GetAccountAuthorizationDetailsResult.RoleDetailList {
				if resp.GetAccountAuthorizationDetailsResult.RoleDetailList[i].RoleName == tt.roleName {
					role = &resp.GetAccountAuthorizationDetailsResult.RoleDetailList[i]
				}
			}
			require.NotNil(t, role, "role %q must be present in RoleDetailList", tt.roleName)

			gotNames := make([]string, 0, len(role.InstanceProfileList))
			for _, ip := range role.InstanceProfileList {
				gotNames = append(gotNames, ip.InstanceProfileName)
			}
			assert.Equal(t, tt.wantProfileNames, gotNames)

			if tt.wantProfileRoleID {
				require.Len(t, role.InstanceProfileList, 1)
				require.Len(t, role.InstanceProfileList[0].Roles, 1,
					"instance profile must report the full nested role, not just its name")
				assert.Equal(t, tt.roleName, role.InstanceProfileList[0].Roles[0].RoleName)
				assert.NotEmpty(t, role.InstanceProfileList[0].Roles[0].RoleID,
					"nested role must be resolved to the full RoleXML, not a bare-name placeholder")
			}
		})
	}
}

func TestHandler_InstanceProfile_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	// CreateInstanceProfile.
	req := iamRequest("CreateInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "MyProfile")

	// GetInstanceProfile.
	req2 := iamRequest("GetInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	// AddRoleToInstanceProfile.
	req3 := iamRequest("AddRoleToInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
		"RoleName":            "MyRole",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// ListInstanceProfiles.
	req4 := iamRequest("ListInstanceProfiles", map[string]string{})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Contains(t, rec4.Body.String(), "MyProfile")

	// ListInstanceProfilesForRole.
	req5 := iamRequest("ListInstanceProfilesForRole", map[string]string{"RoleName": "MyRole"})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)
	assert.Contains(t, rec5.Body.String(), "MyProfile")

	// RemoveRoleFromInstanceProfile.
	req6 := iamRequest("RemoveRoleFromInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
		"RoleName":            "MyRole",
	})
	rec6 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req6, rec6)))
	assert.Equal(t, http.StatusOK, rec6.Code)

	// DeleteInstanceProfile.
	req7 := iamRequest("DeleteInstanceProfile", map[string]string{
		"InstanceProfileName": "MyProfile",
	})
	rec7 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req7, rec7)))
	assert.Equal(t, http.StatusOK, rec7.Code)
}

func TestHandler_InstanceProfile_OneRoleLimit(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("R1", "/", doc, "")
	_, _ = b.CreateRole("R2", "/", doc, "")
	_, _ = b.CreateInstanceProfile("IP", "/")
	_ = b.AddRoleToInstanceProfile("IP", "R1")

	req := iamRequest("AddRoleToInstanceProfile", map[string]string{
		"InstanceProfileName": "IP",
		"RoleName":            "R2",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	// Real AWS IAM returns 409 for LimitExceeded, not 400.
	assert.Equal(t, http.StatusConflict, rec.Code, "second role must fail")

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceeded", errResp.Error.Code)
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
