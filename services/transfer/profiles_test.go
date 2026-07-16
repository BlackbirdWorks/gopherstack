package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestProfileCountExport verifies ProfileCount export.
func TestProfileCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	assert.Equal(t, 0, transfer.ProfileCount(b))

	_, err := b.CreateProfile("LOCAL", "as2id", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.ProfileCount(b))
}

// TestProfileTypeValidation verifies CreateProfile rejects invalid types.
func TestProfileTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		profileType string
		name        string
		wantErr     bool
	}{
		{name: "local valid", profileType: "LOCAL", wantErr: false},
		{name: "partner valid", profileType: "PARTNER", wantErr: false},
		{name: "empty invalid", profileType: "", wantErr: true},
		{name: "lowercase invalid", profileType: "local", wantErr: true},
		{name: "unknown invalid", profileType: "UNKNOWN", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			_, err := b.CreateProfile(tt.profileType, "as2id", nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAddProfileInternal verifies the AddProfileInternal seed helper.
func TestAddProfileInternal(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	b.AddProfileInternal("p-test123", "LOCAL")

	assert.Equal(t, 1, transfer.ProfileCount(b))
}

func TestCreateProfile(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	p, err := b.CreateProfile("LOCAL", "AS2-id", map[string]string{"env": "test"})
	require.NoError(t, err)
	assert.NotEmpty(t, p.ProfileID)
	assert.Equal(t, "LOCAL", p.ProfileType)
	assert.Equal(t, "AS2-id", p.As2ID)
}
