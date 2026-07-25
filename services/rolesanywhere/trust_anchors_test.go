package rolesanywhere_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// ---- Trust Anchor CRUD ----

func TestCreateTrustAnchor_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	source := rolesanywhere.TrustAnchorSource{
		SourceType: "AWS_ACM_PCA",
		SourceData: map[string]string{"acmPcaArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/abc"},
	}

	ta, err := b.CreateTrustAnchor(context.Background(), "my-anchor", source, nil, nil, nil)
	require.NoError(t, err)

	assert.NotEmpty(t, ta.TrustAnchorID)
	assert.Equal(t, "my-anchor", ta.Name)
	assert.True(t, ta.Enabled)
	assert.Contains(t, ta.TrustAnchorArn, "arn:aws:rolesanywhere:")
	assert.Contains(t, ta.TrustAnchorArn, "trust-anchor")
}

// TestCreateTrustAnchor_NotificationSettingsAtCreate proves that
// notificationSettings passed to CreateTrustAnchor are applied immediately,
// visible via GetNotificationSettings without a separate
// PutNotificationSettings call. Real AWS's CreateTrustAnchorInput carries a
// notificationSettings field for exactly this ("A list of notification
// settings to be associated to the trust anchor"); a prior version of
// CreateTrustAnchor silently dropped it.
func TestCreateTrustAnchor_NotificationSettingsAtCreate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	source := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	threshold := int32(30)

	ta, err := b.CreateTrustAnchor(context.Background(), "notif-at-create", source, nil, nil,
		[]rolesanywhere.NotificationSetting{
			{Event: "CA_CERTIFICATE_EXPIRY", Threshold: &threshold, Enabled: true},
		},
	)
	require.NoError(t, err)

	settings := b.GetNotificationSettings(context.Background(), ta.TrustAnchorID)
	require.Len(t, settings, 1)
	assert.Equal(t, "CA_CERTIFICATE_EXPIRY", settings[0].Event)
	require.NotNil(t, settings[0].Threshold)
	assert.Equal(t, int32(30), *settings[0].Threshold)
}

// TestCreateTrustAnchor_EmptySourceType proves that an empty (zero-value)
// Source is rejected with ValidationException -- real AWS's
// CreateTrustAnchorInput requires Source.
func TestCreateTrustAnchor_EmptySourceType(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateTrustAnchor(
		context.Background(), "no-source-anchor", rolesanywhere.TrustAnchorSource{}, nil, nil, nil,
	)
	require.Error(t, err)
}

// TestCreateTrustAnchor_DuplicateNameAllowed proves creating two trust
// anchors with the same name succeeds -- real AWS Roles Anywhere has no
// uniqueness constraint on trust anchor names (CreateTrustAnchor only models
// ValidationException/AccessDeniedException; there is no ConflictException
// shape anywhere in the service), so the two resources are distinguished
// only by their generated IDs/ARNs.
func TestCreateTrustAnchor_DuplicateNameAllowed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	source := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}

	first, err := b.CreateTrustAnchor(context.Background(), "dup-anchor", source, nil, nil, nil)
	require.NoError(t, err)

	second, err := b.CreateTrustAnchor(context.Background(), "dup-anchor", source, nil, nil, nil)
	require.NoError(t, err)

	assert.NotEqual(t, first.TrustAnchorID, second.TrustAnchorID)
}

func TestCreateTrustAnchor_EmptyName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		taName  string
		wantErr bool
	}{
		{"empty name returns validation error", "", true},
		{"non-empty name succeeds", "valid-name", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
			_, err := b.CreateTrustAnchor(context.Background(), tt.taName, src, nil, nil, nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetTrustAnchor_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetTrustAnchor(context.Background(), "nonexistent-id")
	require.Error(t, err)
}

func TestListTrustAnchors_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	_, _ = b.CreateTrustAnchor(context.Background(), "anchor-1", src, nil, nil, nil)
	_, _ = b.CreateTrustAnchor(context.Background(), "anchor-2", src, nil, nil, nil)

	all, next, err := b.ListTrustAnchors(context.Background(), "", 0)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Empty(t, next)
}

// TestListTrustAnchors_TokenWalk verifies that pagination emits a working
// NextToken and that walking it visits every item exactly once (no
// duplicates, no skips) -- the previous nextTokenFromSlice always returned "".
func TestListTrustAnchors_TokenWalk(t *testing.T) {
	t.Parallel()

	b := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")

	const total = 5
	for i := range total {
		_, err := b.CreateTrustAnchor(context.Background(),
			"anchor-"+string(rune('a'+i)),
			rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"},
			nil,
			nil,
			nil,
		)
		require.NoError(t, err)
	}

	seen := make(map[string]int)
	token := ""

	for range total + 2 {
		items, next, err := b.ListTrustAnchors(context.Background(), token, 2)
		require.NoError(t, err)

		for _, ta := range items {
			seen[ta.TrustAnchorID]++
		}

		if next == "" {
			break
		}

		token = next
	}

	assert.Len(t, seen, total, "every trust anchor must be returned exactly once")
	for id, count := range seen {
		assert.Equalf(t, 1, count, "trust anchor %s returned %d times", id, count)
	}
}

func TestDeleteTrustAnchor_RemovesEntry(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, err := b.CreateTrustAnchor(context.Background(), "del-anchor", src, nil, nil, nil)
	require.NoError(t, err)

	deleted, err := b.DeleteTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.NoError(t, err)
	assert.Equal(t, ta.TrustAnchorID, deleted.TrustAnchorID)

	_, err = b.GetTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.Error(t, err)
}

// TestDeleteTrustAnchor_CascadesNotificationSettingsAndTags proves that
// deleting a trust anchor removes its notification settings and tags too
// (both live in separate ID/ARN-keyed maps, not on the TrustAnchor struct
// itself), so no ghost rows survive the trust anchor they belonged to.
func TestDeleteTrustAnchor_CascadesNotificationSettingsAndTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, err := b.CreateTrustAnchor(context.Background(), "cascade-notif-anchor", src, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.PutNotificationSettings(context.Background(), ta.TrustAnchorID, []rolesanywhere.NotificationSetting{
		{Event: "CA_CERTIFICATE_EXPIRY", Enabled: true},
	})
	require.NoError(t, err)
	require.NoError(t, b.TagResource(context.Background(), ta.TrustAnchorArn,
		[]rolesanywhere.TagEntry{{Key: "env", Value: "prod"}}))

	_, err = b.DeleteTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.NoError(t, err)

	settings := b.GetNotificationSettings(context.Background(), ta.TrustAnchorID)
	assert.Empty(t, settings, "notification settings must not survive trust anchor deletion")

	tags, err := b.ListTagsForResource(context.Background(), ta.TrustAnchorArn)
	require.Error(t, err,
		"ListTagsForResource must report ResourceNotFoundException for the deleted trust anchor's ARN")
	assert.Empty(t, tags)
}

func TestUpdateTrustAnchor_ChangesName(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, _ := b.CreateTrustAnchor(context.Background(), "orig-anchor", src, nil, nil, nil)

	updated, err := b.UpdateTrustAnchor(context.Background(), ta.TrustAnchorID, "renamed-anchor", nil)
	require.NoError(t, err)
	assert.Equal(t, "renamed-anchor", updated.Name)
}

func TestEnableDisableTrustAnchor(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, _ := b.CreateTrustAnchor(context.Background(), "toggle-anchor", src, nil, nil, nil)
	assert.True(t, ta.Enabled)

	disabled, err := b.DisableTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.NoError(t, err)
	assert.False(t, disabled.Enabled)

	enabled, err := b.EnableTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.NoError(t, err)
	assert.True(t, enabled.Enabled)
}
