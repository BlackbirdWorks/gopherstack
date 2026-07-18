package appmesh_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// ─── Backend AccountID / Region / Reset ───

func TestBackend_AccountIDRegionReset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{"standard", "123456789012", "us-west-2"},
		{"empty strings", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := appmesh.NewInMemoryBackend(tt.accountID, tt.region)
			assert.Equal(t, tt.accountID, b.AccountID())
			assert.Equal(t, tt.region, b.Region())
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()
	b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")

	// Populate the backend with resources.
	_, err := b.CreateMesh("mesh1", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateVirtualNode("mesh1", "vn1", nil, nil)
	require.NoError(t, err)

	// Reset should clear everything.
	b.Reset()

	meshes, _, err := b.ListMeshes(100, "")
	require.NoError(t, err)
	assert.Empty(t, meshes)
}

// ─── ErrIs ───

func TestErrIs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err      error
		sentinel error
		name     string
		want     bool
	}{
		{
			name:     "matching sentinel",
			err:      appmesh.ErrMeshNotFound,
			sentinel: appmesh.ErrMeshNotFound,
			want:     true,
		},
		{
			name:     "non-matching sentinel",
			err:      appmesh.ErrMeshNotFound,
			sentinel: appmesh.ErrVirtualNodeNotFound,
			want:     false,
		},
		{
			name:     "wrapped error matches",
			err:      fmt.Errorf("wrap: %w", appmesh.ErrMeshNotFound),
			sentinel: appmesh.ErrMeshNotFound,
			want:     true,
		},
		{name: "nil error", err: nil, sentinel: appmesh.ErrMeshNotFound, want: false},
		{
			name:     "unrelated error",
			err:      errors.New("other"), //nolint:err113 // existing issue.
			sentinel: appmesh.ErrMeshNotFound,
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := appmesh.ErrIs(tt.err, tt.sentinel)
			assert.Equal(t, tt.want, got)
		})
	}
}
