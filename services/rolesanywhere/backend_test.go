package rolesanywhere_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

func newBackend(t *testing.T) *rolesanywhere.InMemoryBackend {
	t.Helper()

	return rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")
}

// ---- Trust Anchor CRUD ----

func TestCreateTrustAnchor_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	source := rolesanywhere.TrustAnchorSource{
		SourceType: "AWS_ACM_PCA",
		SourceData: map[string]string{"acmPcaArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/abc"},
	}

	ta, err := b.CreateTrustAnchor("my-anchor", source, nil)
	require.NoError(t, err)

	assert.NotEmpty(t, ta.TrustAnchorID)
	assert.Equal(t, "my-anchor", ta.Name)
	assert.True(t, ta.Enabled)
	assert.Contains(t, ta.TrustAnchorArn, "arn:aws:rolesanywhere:")
	assert.Contains(t, ta.TrustAnchorArn, "trust-anchor")
}

func TestCreateTrustAnchor_DuplicateNameRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	source := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}

	_, err := b.CreateTrustAnchor("dup-anchor", source, nil)
	require.NoError(t, err)

	_, err = b.CreateTrustAnchor("dup-anchor", source, nil)
	require.Error(t, err)
}

func TestGetTrustAnchor_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetTrustAnchor("nonexistent-id")
	require.Error(t, err)
}

func TestListTrustAnchors_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	_, _ = b.CreateTrustAnchor("anchor-1", src, nil)
	_, _ = b.CreateTrustAnchor("anchor-2", src, nil)

	all, next, err := b.ListTrustAnchors("", 0)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Empty(t, next)
}

func TestDeleteTrustAnchor_RemovesEntry(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, err := b.CreateTrustAnchor("del-anchor", src, nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteTrustAnchor(ta.TrustAnchorID))

	_, err = b.GetTrustAnchor(ta.TrustAnchorID)
	require.Error(t, err)
}

func TestUpdateTrustAnchor_ChangesName(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, _ := b.CreateTrustAnchor("orig-anchor", src, nil)

	updated, err := b.UpdateTrustAnchor(ta.TrustAnchorID, "renamed-anchor", nil)
	require.NoError(t, err)
	assert.Equal(t, "renamed-anchor", updated.Name)
}

func TestEnableDisableTrustAnchor(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, _ := b.CreateTrustAnchor("toggle-anchor", src, nil)
	assert.True(t, ta.Enabled)

	disabled, err := b.DisableTrustAnchor(ta.TrustAnchorID)
	require.NoError(t, err)
	assert.False(t, disabled.Enabled)

	enabled, err := b.EnableTrustAnchor(ta.TrustAnchorID)
	require.NoError(t, err)
	assert.True(t, enabled.Enabled)
}

// ---- Profile CRUD ----

func TestCreateProfile_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	roleArns := []string{"arn:aws:iam::123456789012:role/MyRole"}

	p, err := b.CreateProfile("my-profile", roleArns, nil, nil, nil, "", false)
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
	_, err := b.CreateProfile("dup-profile", nil, nil, nil, nil, "", false)
	require.NoError(t, err)

	_, err = b.CreateProfile("dup-profile", nil, nil, nil, nil, "", false)
	require.Error(t, err)
}

func TestGetProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetProfile("nonexistent-profile-id")
	require.Error(t, err)
}

func TestListProfiles_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateProfile("profile-1", nil, nil, nil, nil, "", false)
	_, _ = b.CreateProfile("profile-2", nil, nil, nil, nil, "", false)

	all, next, err := b.ListProfiles("", 0)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Empty(t, next)
}

func TestDeleteProfile_RemovesEntry(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, err := b.CreateProfile("del-profile", nil, nil, nil, nil, "", false)
	require.NoError(t, err)

	require.NoError(t, b.DeleteProfile(p.ProfileID))

	_, err = b.GetProfile(p.ProfileID)
	require.Error(t, err)
}

func TestUpdateProfile_ChangesRoleArns(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, _ := b.CreateProfile("upd-profile", []string{"arn:aws:iam::123:role/OldRole"}, nil, nil, nil, "", false)

	newRoles := []string{"arn:aws:iam::123:role/NewRole"}
	updated, err := b.UpdateProfile(p.ProfileID, "", newRoles, nil, nil, "", nil)
	require.NoError(t, err)
	assert.Equal(t, newRoles, updated.RoleArns)
}

func TestEnableDisableProfile(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, _ := b.CreateProfile("toggle-profile", nil, nil, nil, nil, "", false)
	assert.True(t, p.Enabled)

	disabled, err := b.DisableProfile(p.ProfileID)
	require.NoError(t, err)
	assert.False(t, disabled.Enabled)

	enabled, err := b.EnableProfile(p.ProfileID)
	require.NoError(t, err)
	assert.True(t, enabled.Enabled)
}

// ---- Tags ----

func TestTagResource_Roundtrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resARN := "arn:aws:rolesanywhere:us-east-1:000000000000:trust-anchor/some-id"

	tags := []rolesanywhere.TagEntry{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "security"},
	}

	require.NoError(t, b.TagResource(resARN, tags))

	got, err := b.ListTagsForResource(resARN)
	require.NoError(t, err)
	assert.Len(t, got, 2)

	found := make(map[string]string)
	for _, t := range got {
		found[t.Key] = t.Value
	}

	assert.Equal(t, "prod", found["env"])
	assert.Equal(t, "security", found["team"])
}

func TestUntagResource_RemovesTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resARN := "arn:aws:rolesanywhere:us-east-1:000000000000:trust-anchor/untag-id"

	_ = b.TagResource(resARN, []rolesanywhere.TagEntry{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}})
	_ = b.UntagResource(resARN, []string{"a"})

	got, _ := b.ListTagsForResource(resARN)
	assert.Len(t, got, 1)
	assert.Equal(t, "b", got[0].Key)
}

// ---- Profile with DurationSeconds ----

func TestCreateProfile_DurationSeconds(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	dur := int32(3600)

	p, err := b.CreateProfile("dur-profile", nil, nil, &dur, nil, "", false)
	require.NoError(t, err)
	require.NotNil(t, p.DurationSeconds)
	assert.Equal(t, int32(3600), *p.DurationSeconds)
}

// ---- Reset ----

func TestReset_ClearsState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	_, _ = b.CreateTrustAnchor("reset-anchor", src, nil)
	_, _ = b.CreateProfile("reset-profile", nil, nil, nil, nil, "", false)

	b.Reset()

	anchors, _, _ := b.ListTrustAnchors("", 0)
	assert.Empty(t, anchors)

	profiles, _, _ := b.ListProfiles("", 0)
	assert.Empty(t, profiles)
}
