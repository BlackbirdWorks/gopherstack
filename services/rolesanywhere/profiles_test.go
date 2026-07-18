package rolesanywhere_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Profile CRUD ----

func TestCreateProfile_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	roleArns := []string{"arn:aws:iam::123456789012:role/MyRole"}

	p, err := b.CreateProfile(context.Background(), "my-profile", roleArns, nil, nil, nil, "", false)
	require.NoError(t, err)

	assert.NotEmpty(t, p.ProfileID)
	assert.Equal(t, "my-profile", p.Name)
	assert.True(t, p.Enabled)
	assert.Equal(t, roleArns, p.RoleArns)
	assert.Contains(t, p.ProfileArn, "arn:aws:rolesanywhere:")
	assert.Contains(t, p.ProfileArn, "profile")
}

func TestCreateProfile_DuplicateNameRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateProfile(context.Background(), "dup-profile", nil, nil, nil, nil, "", false)
	require.NoError(t, err)

	_, err = b.CreateProfile(context.Background(), "dup-profile", nil, nil, nil, nil, "", false)
	require.Error(t, err)
}

func TestCreateProfile_EmptyName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		wantErr     bool
	}{
		{"empty name returns validation error", "", true},
		{"non-empty name succeeds", "valid-profile", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateProfile(context.Background(), tt.profileName, nil, nil, nil, nil, "", false)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateProfile_DurationSeconds(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	dur := int32(3600)

	p, err := b.CreateProfile(context.Background(), "dur-profile", nil, nil, &dur, nil, "", false)
	require.NoError(t, err)
	require.NotNil(t, p.DurationSeconds)
	assert.Equal(t, int32(3600), *p.DurationSeconds)
}

func TestGetProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetProfile(context.Background(), "nonexistent-profile-id")
	require.Error(t, err)
}

func TestListProfiles_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateProfile(context.Background(), "profile-1", nil, nil, nil, nil, "", false)
	_, _ = b.CreateProfile(context.Background(), "profile-2", nil, nil, nil, nil, "", false)

	all, next, err := b.ListProfiles(context.Background(), "", 0)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Empty(t, next)
}

func TestDeleteProfile_RemovesEntry(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, err := b.CreateProfile(context.Background(), "del-profile", nil, nil, nil, nil, "", false)
	require.NoError(t, err)

	deleted, err := b.DeleteProfile(context.Background(), p.ProfileID)
	require.NoError(t, err)
	assert.Equal(t, p.ProfileID, deleted.ProfileID)

	_, err = b.GetProfile(context.Background(), p.ProfileID)
	require.Error(t, err)
}

func TestUpdateProfile_ChangesRoleArns(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, _ := b.CreateProfile(
		context.Background(),
		"upd-profile",
		[]string{"arn:aws:iam::123:role/OldRole"},
		nil,
		nil,
		nil,
		"",
		false,
	)

	newRoles := []string{"arn:aws:iam::123:role/NewRole"}
	updated, err := b.UpdateProfile(context.Background(), p.ProfileID, "", newRoles, nil, nil, "", nil)
	require.NoError(t, err)
	assert.Equal(t, newRoles, updated.RoleArns)
}

func TestUpdateProfile_AllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		requireIP         *bool
		dur               *int32
		name              string
		newName           string
		sessionPolicy     string
		roleArns          []string
		managedPolicyArns []string
	}{
		{
			name:              "update all fields",
			newName:           "updated-profile",
			roleArns:          []string{"arn:aws:iam::123:role/NewRole"},
			managedPolicyArns: []string{"arn:aws:iam::123:policy/Managed"},
			sessionPolicy:     `{"Version":"2012-10-17"}`,
			requireIP:         new(true),
			dur:               new(int32(7200)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			p, _ := b.CreateProfile(context.Background(),
				"base-profile",
				[]string{"arn:aws:iam::123:role/OldRole"},
				nil,
				nil,
				nil,
				"",
				false,
			)

			updated, err := b.UpdateProfile(context.Background(),
				p.ProfileID,
				tt.newName,
				tt.roleArns,
				tt.dur,
				tt.managedPolicyArns,
				tt.sessionPolicy,
				tt.requireIP,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.newName, updated.Name)
			assert.Equal(t, tt.roleArns, updated.RoleArns)
			assert.Equal(t, tt.managedPolicyArns, updated.ManagedPolicyArns)
			assert.Equal(t, tt.sessionPolicy, updated.SessionPolicy)
			require.NotNil(t, updated.DurationSeconds)
			assert.Equal(t, int32(7200), *updated.DurationSeconds)
			assert.True(t, updated.RequireInstanceProperties)
		})
	}
}

func TestEnableDisableProfile(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, _ := b.CreateProfile(context.Background(), "toggle-profile", nil, nil, nil, nil, "", false)
	assert.True(t, p.Enabled)

	disabled, err := b.DisableProfile(context.Background(), p.ProfileID)
	require.NoError(t, err)
	assert.False(t, disabled.Enabled)

	enabled, err := b.EnableProfile(context.Background(), p.ProfileID)
	require.NoError(t, err)
	assert.True(t, enabled.Enabled)
}

// ---- unused helpers, kept as pre-existing (see nolint) ----

func boolPtr(b bool) *bool { return new(b) } //nolint:unused // existing issue.

func int32Ptr(i int32) *int32 { return new(i) } //nolint:unused // existing issue.
