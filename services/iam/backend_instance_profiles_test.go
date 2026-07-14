package iam_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddRoleToInstanceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *iam.InMemoryBackend)
		name    string
		profile string
		role    string
	}{
		{
			name: "add_role_success",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
				_, _ = b.CreateRole("ec2-role", "/", "{}", "")
			},
			profile: "my-profile",
			role:    "ec2-role",
		},
		{
			name: "add_role_idempotent",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
				_, _ = b.CreateRole("ec2-role", "/", "{}", "")
				_ = b.AddRoleToInstanceProfile("my-profile", "ec2-role")
			},
			profile: "my-profile",
			role:    "ec2-role",
		},
		{
			name:    "instance_profile_not_found",
			setup:   func(_ *iam.InMemoryBackend) {},
			profile: "ghost-profile",
			role:    "some-role",
			wantErr: iam.ErrInstanceProfileNotFound,
		},
		{
			name: "role_not_found",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
			},
			profile: "my-profile",
			role:    "ghost-role",
			wantErr: iam.ErrRoleNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.AddRoleToInstanceProfile(tt.profile, tt.role)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestRemoveRoleFromInstanceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *iam.InMemoryBackend)
		name    string
		profile string
		role    string
	}{
		{
			name: "remove_existing_role",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
				_, _ = b.CreateRole("ec2-role", "/", "{}", "")
				_ = b.AddRoleToInstanceProfile("my-profile", "ec2-role")
			},
			profile: "my-profile",
			role:    "ec2-role",
		},
		{
			name: "remove_non_member_role_is_no_op",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
			},
			profile: "my-profile",
			role:    "ec2-role",
		},
		{
			name:    "profile_not_found",
			setup:   func(_ *iam.InMemoryBackend) {},
			profile: "ghost-profile",
			role:    "some-role",
			wantErr: iam.ErrInstanceProfileNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.RemoveRoleFromInstanceProfile(tt.profile, tt.role)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInstanceProfile_FullRoleDetailsInXML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, b *iam.InMemoryBackend)
		name        string
		wantContain string
	}{
		{
			name: "role_arn_present_in_response",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole(
					"ec2-role",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					"",
				)
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("web-profile", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddRoleToInstanceProfile("web-profile", "ec2-role"))
			},
			wantContain: "ec2-role",
		},
		{
			name: "list_profiles_includes_role_details",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("lambda-role", "/", `{}`, "")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("fn-profile", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddRoleToInstanceProfile("fn-profile", "lambda-role"))
			},
			wantContain: "lambda-role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(t, b)

			rec := httptest.NewRecorder()
			req := iamRequest("ListInstanceProfiles", nil)
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

// TestGetInstanceProfile_Backend tests GetInstanceProfile.
func TestGetInstanceProfile_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*iam.InMemoryBackend)
		ipName  string
		wantErr bool
	}{
		{
			name: "found",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("MyProfile", "/")
			},
			ipName: "MyProfile",
		},
		{
			name:    "not_found",
			setup:   func(_ *iam.InMemoryBackend) {},
			ipName:  "ghost",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			ip, err := b.GetInstanceProfile(tt.ipName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ipName, ip.InstanceProfileName)
		})
	}
}

func TestInstanceProfileID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	ip, err := b.CreateInstanceProfile("MyProfile", "/")
	require.NoError(t, err)

	// Instance profile IDs are AIPA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(ip.InstanceProfileID, "AIPA"),
		"instance profile ID must start with AIPA")
	assert.Len(t, ip.InstanceProfileID, 20, "instance profile ID must be 20 chars")
	assert.NotContains(t, ip.InstanceProfileID, "-", "instance profile ID must not contain dashes")

	for _, ch := range ip.InstanceProfileID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"instance profile ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestAddRoleToInstanceProfile_OnlyOneRole(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("R1", "/", doc, "")
	_, _ = b.CreateRole("R2", "/", doc, "")
	_, _ = b.CreateInstanceProfile("MyProfile", "/")

	require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "R1"))

	// Adding a second role must fail (AWS allows only one role per instance profile).
	err := b.AddRoleToInstanceProfile("MyProfile", "R2")
	require.Error(t, err, "adding second role to instance profile must fail")
}

func TestInstanceProfile_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	ip, err := b.CreateInstanceProfile("MyProfile", "/")
	require.NoError(t, err)

	require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "MyRole"))

	got, err := b.GetInstanceProfile("MyProfile")
	require.NoError(t, err)
	assert.Equal(t, ip.InstanceProfileName, got.InstanceProfileName)
	assert.Len(t, got.Roles, 1)
	assert.Equal(t, "MyRole", got.Roles[0])
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

		profiles, err := b.ListInstanceProfiles("", 0)
		require.NoError(t, err)
		require.Len(t, profiles.Data, 1)
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
		profiles, _ := b.ListInstanceProfiles("", 0)
		assert.Empty(t, profiles.Data)
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
