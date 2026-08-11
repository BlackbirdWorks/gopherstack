package rolesanywhere_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// ---- Profile CRUD ----

func TestCreateProfile_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	roleArns := []string{"arn:aws:iam::123456789012:role/MyRole"}

	p, err := b.CreateProfile(
		context.Background(),
		"my-profile",
		roleArns,
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)
	require.NoError(t, err)

	assert.NotEmpty(t, p.ProfileID)
	assert.Equal(t, "my-profile", p.Name)
	assert.True(t, p.Enabled)
	assert.Equal(t, roleArns, p.RoleArns)
	assert.Contains(t, p.ProfileArn, "arn:aws:rolesanywhere:")
	assert.Contains(t, p.ProfileArn, "profile")
}

// TestCreateProfile_DuplicateNameAllowed proves creating two profiles with
// the same name succeeds -- real AWS Roles Anywhere has no uniqueness
// constraint on profile names (CreateProfile only models
// ValidationException/AccessDeniedException; there is no ConflictException
// shape anywhere in the service), so the two resources are distinguished
// only by their generated IDs/ARNs.
func TestCreateProfile_DuplicateNameAllowed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	first, err := b.CreateProfile(
		context.Background(),
		"dup-profile",
		[]string{},
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)
	require.NoError(t, err)

	second, err := b.CreateProfile(
		context.Background(),
		"dup-profile",
		[]string{},
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)
	require.NoError(t, err)

	assert.NotEqual(t, first.ProfileID, second.ProfileID)
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
			_, err := b.CreateProfile(
				context.Background(),
				tt.profileName,
				[]string{},
				nil,
				nil,
				nil,
				"",
				false,
				nil,
				nil,
			)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCreateProfile_RoleArnsRequired proves that CreateProfile rejects a nil
// roleArns list with ValidationException, matching real AWS:
// CreateProfileInput.RoleArns is "This member is required" (aws-sdk-go-v2's
// validateOpCreateProfileInput checks v.RoleArns == nil) and botocore's
// CreateProfileRequest declares "roleArns" in its top-level "required" list.
// An explicitly empty (non-nil) list is still accepted: the RoleArnList shape
// itself declares min:0, so the requirement is presence, not non-emptiness.
func TestCreateProfile_RoleArnsRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roleArns []string
		wantErr  bool
	}{
		{name: "nil role arns rejected", roleArns: nil, wantErr: true},
		{name: "empty role arns accepted", roleArns: []string{}, wantErr: false},
		{
			name:     "populated role arns accepted",
			roleArns: []string{"arn:aws:iam::123456789012:role/MyRole"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateProfile(
				context.Background(),
				t.Name(),
				tt.roleArns,
				nil,
				nil,
				nil,
				"",
				false,
				nil,
				nil,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateProfile_DurationSeconds(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	dur := int32(3600)

	p, err := b.CreateProfile(
		context.Background(),
		"dur-profile",
		[]string{},
		nil,
		&dur,
		nil,
		"",
		false,
		nil,
		nil,
	)
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
	_, _ = b.CreateProfile(
		context.Background(),
		"profile-1",
		[]string{},
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)
	_, _ = b.CreateProfile(
		context.Background(),
		"profile-2",
		[]string{},
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)

	all, next, err := b.ListProfiles(context.Background(), "", 0)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Empty(t, next)
}

func TestDeleteProfile_RemovesEntry(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, err := b.CreateProfile(
		context.Background(),
		"del-profile",
		[]string{},
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)
	require.NoError(t, err)

	deleted, err := b.DeleteProfile(context.Background(), p.ProfileID)
	require.NoError(t, err)
	assert.Equal(t, p.ProfileID, deleted.ProfileID)

	_, err = b.GetProfile(context.Background(), p.ProfileID)
	require.Error(t, err)
}

// TestDeleteProfile_CascadesAttributeMappingsAndTags proves that deleting a
// profile removes its attribute mappings and tags too (both live in
// separate ID/ARN-keyed maps, not on the Profile struct itself), so no ghost
// rows survive the profile they belonged to and a recreated profile (which
// gets a fresh generated ID/ARN) never inherits stale data.
func TestDeleteProfile_CascadesAttributeMappingsAndTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, err := b.CreateProfile(
		context.Background(),
		"cascade-profile",
		[]string{},
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.PutAttributeMapping(
		context.Background(),
		p.ProfileID,
		"x509Subject",
		[]rolesanywhere.MappingRule{{Specifier: "CN"}},
	)
	require.NoError(t, err)
	require.NoError(t, b.TagResource(context.Background(), p.ProfileArn,
		[]rolesanywhere.TagEntry{{Key: "env", Value: "prod"}}))

	_, err = b.DeleteProfile(context.Background(), p.ProfileID)
	require.NoError(t, err)

	mappings := b.GetAttributeMappings(context.Background(), p.ProfileID)
	assert.Empty(t, mappings, "attribute mappings must not survive profile deletion")

	tags, err := b.ListTagsForResource(context.Background(), p.ProfileArn)
	require.Error(
		t,
		err,
		"ListTagsForResource must report ResourceNotFoundException for the deleted profile's ARN",
	)
	assert.Empty(t, tags)
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
		nil,
		nil,
	)

	newRoles := []string{"arn:aws:iam::123:role/NewRole"}
	updated, err := b.UpdateProfile(
		context.Background(),
		p.ProfileID,
		"",
		newRoles,
		nil,
		nil,
		"",
		nil,
		nil,
	)
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
				nil,
				nil,
			)

			updated, err := b.UpdateProfile(context.Background(),
				p.ProfileID,
				tt.newName,
				tt.roleArns,
				tt.dur,
				tt.managedPolicyArns,
				tt.sessionPolicy,
				tt.requireIP,
				nil,
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
	p, _ := b.CreateProfile(
		context.Background(),
		"toggle-profile",
		[]string{},
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)
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
