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

	ta, err := b.CreateTrustAnchor(context.Background(), "my-anchor", source, nil, nil)
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

	_, err := b.CreateTrustAnchor(context.Background(), "dup-anchor", source, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateTrustAnchor(context.Background(), "dup-anchor", source, nil, nil)
	require.Error(t, err)
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
			_, err := b.CreateTrustAnchor(context.Background(), tt.taName, src, nil, nil)
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
	_, _ = b.CreateTrustAnchor(context.Background(), "anchor-1", src, nil, nil)
	_, _ = b.CreateTrustAnchor(context.Background(), "anchor-2", src, nil, nil)

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
	ta, err := b.CreateTrustAnchor(context.Background(), "del-anchor", src, nil, nil)
	require.NoError(t, err)

	deleted, err := b.DeleteTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.NoError(t, err)
	assert.Equal(t, ta.TrustAnchorID, deleted.TrustAnchorID)

	_, err = b.GetTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.Error(t, err)
}

func TestUpdateTrustAnchor_ChangesName(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, _ := b.CreateTrustAnchor(context.Background(), "orig-anchor", src, nil, nil)

	updated, err := b.UpdateTrustAnchor(context.Background(), ta.TrustAnchorID, "renamed-anchor", nil)
	require.NoError(t, err)
	assert.Equal(t, "renamed-anchor", updated.Name)
}

func TestEnableDisableTrustAnchor(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	ta, _ := b.CreateTrustAnchor(context.Background(), "toggle-anchor", src, nil, nil)
	assert.True(t, ta.Enabled)

	disabled, err := b.DisableTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.NoError(t, err)
	assert.False(t, disabled.Enabled)

	enabled, err := b.EnableTrustAnchor(context.Background(), ta.TrustAnchorID)
	require.NoError(t, err)
	assert.True(t, enabled.Enabled)
}
