package rolesanywhere_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

func newBackend(t *testing.T) *rolesanywhere.InMemoryBackend {
	t.Helper()

	return rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")
}

// ---- Reset ----

func TestReset_ClearsState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
	_, _ = b.CreateTrustAnchor(context.Background(), "reset-anchor", src, nil, nil, nil)
	_, _ = b.CreateProfile(
		context.Background(),
		"reset-profile",
		[]string{},
		nil,
		nil,
		nil,
		"",
		false,
		nil,
		nil,
	)

	b.Reset()

	anchors, _, _ := b.ListTrustAnchors(context.Background(), "", 0)
	assert.Empty(t, anchors)

	profiles, _, _ := b.ListProfiles(context.Background(), "", 0)
	assert.Empty(t, profiles)
}

// ---- Region / AccountID ----

func TestRegionAccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{"us-east-1", "111111111111", "us-east-1"},
		{"eu-west-1", "222222222222", "eu-west-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rolesanywhere.NewInMemoryBackend(tt.accountID, tt.region)
			assert.Equal(t, tt.region, b.Region())
			assert.Equal(t, tt.accountID, b.AccountID())
		})
	}
}

// ---- Snapshot / Restore smoke tests (see persistence_test.go for the
// thorough round-trip coverage of every table/map) ----

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		anchorName        string
		profileName       string
		expectAnchorCount int
		expectProfileCnt  int
	}{
		{
			name:              "snapshot and restore preserves state",
			anchorName:        "snap-anchor",
			profileName:       "snap-profile",
			expectAnchorCount: 1,
			expectProfileCnt:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
			_, err := b.CreateTrustAnchor(context.Background(), tt.anchorName, src, nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateProfile(
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
			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			assert.NotEmpty(t, snap)

			// Restore into a fresh backend.
			b2 := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			anchors, _, err := b2.ListTrustAnchors(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Len(t, anchors, tt.expectAnchorCount)

			profiles, _, err := b2.ListProfiles(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Len(t, profiles, tt.expectProfileCnt)
		})
	}
}

func TestRestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{"invalid json returns error", []byte(`{invalid`), true},
		{"empty object restores cleanly", []byte(`{}`), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.Restore(t.Context(), tt.data)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
