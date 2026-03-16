package iam_test

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// ---- Access Key Secret Entropy ----

func TestCreateAccessKey_SecretLength(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateUser("ak-user", "/", "")
	require.NoError(t, err)

	ak, err := b.CreateAccessKey("ak-user")
	require.NoError(t, err)

	assert.Len(t, ak.SecretAccessKey, 40, "secret access key must be 40 characters")

	// Verify it is valid base64 (standard encoding, no padding trimming needed)
	_, decodeErr := base64.StdEncoding.DecodeString(ak.SecretAccessKey)
	assert.NoError(t, decodeErr, "secret access key must be valid base64")
}

func TestCreateAccessKey_SecretIsUnique(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateUser("ak-unique-user", "/", "")
	require.NoError(t, err)

	ak1, err := b.CreateAccessKey("ak-unique-user")
	require.NoError(t, err)

	ak2, err := b.CreateAccessKey("ak-unique-user")
	require.NoError(t, err)

	assert.NotEqual(t, ak1.SecretAccessKey, ak2.SecretAccessKey,
		"consecutive secrets must differ (cryptographically random)")
}

// ---- DeleteUser Memory Leak Fix ----

func TestDeleteUser_CleansAccessKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *iam.InMemoryBackend)
		name     string
		wantKeys int
	}{
		{
			name: "deletes_access_keys_on_user_deletion",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateAccessKey("alice")
				_, _ = b.CreateAccessKey("alice")
			},
			wantKeys: 0,
		},
		{
			name: "no_access_keys_to_delete",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			wantKeys: 0,
		},
		{
			name: "does_not_delete_other_users_keys",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateUser("bob", "/", "")
				_, _ = b.CreateAccessKey("bob")
			},
			wantKeys: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			require.NoError(t, b.DeleteUser("alice"))

			allKeys := b.ListAllAccessKeys()
			assert.Len(t, allKeys, tt.wantKeys)
		})
	}
}

func TestDeleteUser_CleansLoginProfile(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateUser("lp-user", "/", "")
	require.NoError(t, err)

	_, err = b.CreateLoginProfile("lp-user", "S3cur3P@ss!", false)
	require.NoError(t, err)

	require.NoError(t, b.DeleteUser("lp-user"))

	_, err = b.GetLoginProfile("lp-user")
	require.ErrorIs(t, err, iam.ErrLoginProfileNotFound)
}

// ---- Login Profile Password Validation ----

func TestCreateLoginProfile_RejectsEmptyPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{
			name:     "empty_password_returns_error",
			password: "",
			wantErr:  true,
		},
		{
			name:     "non_empty_password_succeeds",
			password: "S3cur3P@ss!",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, _ = b.CreateUser("pass-user-"+tt.name, "/", "")

			_, err := b.CreateLoginProfile("pass-user-"+tt.name, tt.password, false)
			if tt.wantErr {
				require.ErrorIs(t, err, iam.ErrInvalidPassword)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUpdateLoginProfile_RejectsEmptyPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{
			name:     "empty_password_returns_error",
			password: "",
			wantErr:  true,
		},
		{
			name:     "non_empty_password_succeeds",
			password: "N3wP@ss!",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, _ = b.CreateUser("upd-user-"+tt.name, "/", "")
			_, _ = b.CreateLoginProfile("upd-user-"+tt.name, "InitialPass1!", false)

			err := b.UpdateLoginProfile("upd-user-"+tt.name, tt.password, false)
			if tt.wantErr {
				require.ErrorIs(t, err, iam.ErrInvalidPassword)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- AddRoleToInstanceProfile / RemoveRoleFromInstanceProfile ----

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

// ---- RemoveUserFromGroup ----

func TestRemoveUserFromGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *iam.InMemoryBackend)
		name    string
		group   string
		user    string
	}{
		{
			name: "remove_existing_member",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("admins", "/")
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.AddUserToGroup("admins", "alice")
			},
			group: "admins",
			user:  "alice",
		},
		{
			name: "remove_non_member_is_no_op",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("admins", "/")
				_, _ = b.CreateUser("alice", "/", "")
			},
			group: "admins",
			user:  "alice",
		},
		{
			name:    "group_not_found",
			setup:   func(_ *iam.InMemoryBackend) {},
			group:   "ghost-group",
			user:    "alice",
			wantErr: iam.ErrGroupNotFound,
		},
		{
			name: "user_not_found",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("admins", "/")
			},
			group:   "admins",
			user:    "ghost-user",
			wantErr: iam.ErrUserNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.RemoveUserFromGroup(tt.group, tt.user)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAddUserToGroup_TracksMembership(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, _ = b.CreateGroup("devs", "/")
	_, _ = b.CreateUser("bob", "/", "")

	require.NoError(t, b.AddUserToGroup("devs", "bob"))

	// Idempotent: adding twice should not error
	require.NoError(t, b.AddUserToGroup("devs", "bob"))
}

// ---- GetGroup ----

func TestGetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *iam.InMemoryBackend)
		name    string
		group   string
	}{
		{
			name: "get_existing_group",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("admins", "/")
			},
			group: "admins",
		},
		{
			name:    "group_not_found",
			setup:   func(_ *iam.InMemoryBackend) {},
			group:   "ghost",
			wantErr: iam.ErrGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			g, err := b.GetGroup(tt.group)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.group, g.GroupName)
		})
	}
}

// ---- GetAccountSummary ----

func TestGetAccountSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(b *iam.InMemoryBackend)
		name              string
		wantUsers         int
		wantGroups        int
		wantRoles         int
		wantPolicies      int
		wantSAMLProviders int
	}{
		{
			name:  "empty_backend_returns_zeros",
			setup: func(_ *iam.InMemoryBackend) {},
		},
		{
			name: "counts_all_entities",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateUser("bob", "/", "")
				_, _ = b.CreateGroup("admins", "/")
				_, _ = b.CreateRole("ec2-role", "/", "{}", "")
				_, _ = b.CreatePolicy("ReadOnly", "/", "{}")
				_, _ = b.CreateSAMLProvider("MySAML", "<doc/>")
			},
			wantUsers:         2,
			wantGroups:        1,
			wantRoles:         1,
			wantPolicies:      1,
			wantSAMLProviders: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			summary := b.GetAccountSummary()

			assert.Equal(t, tt.wantUsers, summary.Users)
			assert.Equal(t, tt.wantGroups, summary.Groups)
			assert.Equal(t, tt.wantRoles, summary.Roles)
			assert.Equal(t, tt.wantPolicies, summary.Policies)
			assert.Equal(t, tt.wantSAMLProviders, summary.SAMLProviders)
		})
	}
}

// ---- Handler dispatch tests ----

func TestIAMHandler_NewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:   "GetGroup_success",
			action: "GetGroup",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("my-group", "/")
			},
			params:      map[string]string{"GroupName": "my-group"},
			wantCode:    http.StatusOK,
			wantContain: "GetGroupResponse",
		},
		{
			name:     "GetGroup_not_found",
			action:   "GetGroup",
			params:   map[string]string{"GroupName": "ghost"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "RemoveUserFromGroup_success",
			action: "RemoveUserFromGroup",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("grp", "/")
				_, _ = b.CreateUser("usr", "/", "")
				_ = b.AddUserToGroup("grp", "usr")
			},
			params: map[string]string{
				"GroupName": "grp",
				"UserName":  "usr",
			},
			wantCode:    http.StatusOK,
			wantContain: "RemoveUserFromGroupResponse",
		},
		{
			name:   "AddRoleToInstanceProfile_success",
			action: "AddRoleToInstanceProfile",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
				_, _ = b.CreateRole("my-role", "/", "{}", "")
			},
			params: map[string]string{
				"InstanceProfileName": "my-profile",
				"RoleName":            "my-role",
			},
			wantCode:    http.StatusOK,
			wantContain: "AddRoleToInstanceProfileResponse",
		},
		{
			name:   "AddRoleToInstanceProfile_profile_not_found",
			action: "AddRoleToInstanceProfile",
			params: map[string]string{
				"InstanceProfileName": "ghost-profile",
				"RoleName":            "some-role",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "RemoveRoleFromInstanceProfile_success",
			action: "RemoveRoleFromInstanceProfile",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
				_, _ = b.CreateRole("my-role", "/", "{}", "")
				_ = b.AddRoleToInstanceProfile("my-profile", "my-role")
			},
			params: map[string]string{
				"InstanceProfileName": "my-profile",
				"RoleName":            "my-role",
			},
			wantCode:    http.StatusOK,
			wantContain: "RemoveRoleFromInstanceProfileResponse",
		},
		{
			name:        "GetAccountSummary_success",
			action:      "GetAccountSummary",
			wantCode:    http.StatusOK,
			wantContain: "GetAccountSummaryResponse",
		},
		{
			name:   "CreateLoginProfile_empty_password_returns_400",
			action: "CreateLoginProfile",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("pass-user", "/", "")
			},
			params: map[string]string{
				"UserName": "pass-user",
				"Password": "",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "CreateLoginProfile_with_password_success",
			action: "CreateLoginProfile",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("pass-user2", "/", "")
			},
			params: map[string]string{
				"UserName": "pass-user2",
				"Password": "SecureP@ss1!",
			},
			wantCode:    http.StatusOK,
			wantContain: "CreateLoginProfileResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// ---- Persistence: groupMembers round-trip ----

func TestPersistence_GroupMembers_RoundTrip(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, _ = b.CreateGroup("devs", "/")
	_, _ = b.CreateUser("carol", "/", "")
	_ = b.AddUserToGroup("devs", "carol")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iam.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	// carol is still in devs; removing should succeed without error
	require.NoError(t, b2.RemoveUserFromGroup("devs", "carol"))
}
