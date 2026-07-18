package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestAccessCountExport verifies AccessCount export.
func TestAccessCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 0, transfer.AccessCount(b))

	_, err = b.CreateAccess(s.ServerID, "S-1-5-21-1", "", "", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.AccessCount(b))
}

// TestCreateAccessDuplicateExternalID verifies ResourceExistsException.
func TestCreateAccessDuplicateExternalID(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAccess(s.ServerID, "S-1-5-21-9999", "", "", nil)
	require.NoError(t, err)

	// Second CreateAccess with same ExternalId should fail.
	_, err = b.CreateAccess(s.ServerID, "S-1-5-21-9999", "", "", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrConflict)
	require.ErrorIs(t, err, transfer.ErrAccessAlreadyExists)
}

func TestCreateAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget  error
		tags       map[string]string
		name       string
		serverID   string
		externalID string
		role       string
		homeDir    string
		wantErr    bool
	}{
		{
			name:       "success",
			externalID: "S-1-5-21-1234",
			role:       "arn:aws:iam::123456789012:role/role",
			homeDir:    "/home",
		},
		{
			name:       "server not found",
			serverID:   "s-doesnotexist",
			externalID: "S-1-5-21-1234",
			wantErr:    true,
			errTarget:  awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			serverID := tt.serverID
			if serverID == "" {
				s, err := b.CreateServer(nil, nil)
				require.NoError(t, err)
				serverID = s.ServerID
			}

			a, err := b.CreateAccess(serverID, tt.externalID, tt.role, tt.homeDir, tt.tags)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.externalID, a.ExternalID)
			assert.Equal(t, serverID, a.ServerID)
			assert.Equal(t, tt.role, a.Role)
			assert.Equal(t, tt.homeDir, a.HomeDir)
		})
	}
}

func TestDeleteAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		serverID  string
		wantErr   bool
	}{
		{
			name: "success",
		},
		{
			name:      "server not found",
			serverID:  "s-doesnotexist",
			wantErr:   true,
			errTarget: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			serverID := tt.serverID
			externalID := "S-1-5-21-1234"

			if serverID == "" {
				s, err := b.CreateServer(nil, nil)
				require.NoError(t, err)
				serverID = s.ServerID
				_, err = b.CreateAccess(serverID, externalID, "", "", nil)
				require.NoError(t, err)
			}

			err := b.DeleteAccess(serverID, externalID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDeleteAccess_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	err = b.DeleteAccess(s.ServerID, "S-missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}
