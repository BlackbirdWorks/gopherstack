package transfer_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
)

func newTestBackend(t *testing.T) *transfer.InMemoryBackend {
	t.Helper()

	return transfer.NewInMemoryBackend(t.Context(), testAccountID, testRegion)
}

func TestCreateServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags      map[string]string
		name      string
		protocols []string
		wantErr   bool
	}{
		{
			name:      "default SFTP",
			protocols: nil,
		},
		{
			name:      "explicit SFTP",
			protocols: []string{"SFTP"},
		},
		{
			name:      "SFTP and FTPS",
			protocols: []string{"SFTP", "FTPS"},
			tags:      map[string]string{"env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			s, err := b.CreateServer(tt.protocols, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, s.ServerID)
			// AWS creates servers OFFLINE; StartServer is required to bring them ONLINE.
			assert.Equal(t, "OFFLINE", s.State)

			if len(tt.protocols) == 0 {
				assert.Equal(t, []string{"SFTP"}, s.Protocols)
			} else {
				assert.Equal(t, tt.protocols, s.Protocols)
			}
		})
	}
}

func TestDescribeServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		serverID string
		wantErr  bool
	}{
		{
			name: "found",
		},
		{
			name:     "not found",
			serverID: "s-doesnotexist",
			wantErr:  true,
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

			got, err := b.DescribeServer(serverID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, serverID, got.ServerID)
			// AWS creates servers OFFLINE; StartServer is required to bring them ONLINE.
			assert.Equal(t, "OFFLINE", got.State)
		})
	}
}

func TestListServers(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateServer([]string{"SFTP", "FTPS"}, nil)
	require.NoError(t, err)

	servers := b.ListServers()
	assert.Len(t, servers, 2)
}

func TestDeleteServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		serverID string
		wantErr  bool
	}{
		{
			name: "success",
		},
		{
			name:     "not found",
			serverID: "s-doesnotexist",
			wantErr:  true,
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
				// AWS requires the server to be OFFLINE before deletion.
				require.NoError(t, b.StopServer(serverID))
				for range 30 {
					got, _ := b.DescribeServer(serverID)
					if got != nil && got.State == "OFFLINE" {
						break
					}
					time.Sleep(15 * time.Millisecond)
				}
			}

			err := b.DeleteServer(serverID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)

			_, err = b.DescribeServer(serverID)
			require.Error(t, err)
		})
	}
}

func TestStartStopServer(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	// Stop — state becomes STOPPING then asynchronously OFFLINE.
	require.NoError(t, b.StopServer(s.ServerID))
	// Poll until OFFLINE (async transition takes ~100ms).
	var got *transfer.Server
	for range 20 {
		got, err = b.DescribeServer(s.ServerID)
		require.NoError(t, err)
		if got.State == "OFFLINE" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(t, "OFFLINE", got.State)

	// Start — state becomes STARTING then asynchronously ONLINE.
	require.NoError(t, b.StartServer(s.ServerID))
	for range 20 {
		got, err = b.DescribeServer(s.ServerID)
		require.NoError(t, err)
		if got.State == "ONLINE" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(t, "ONLINE", got.State)
}

func TestStartStopServer_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	require.Error(t, b.StartServer("s-missing"))
	require.Error(t, b.StopServer("s-missing"))
}

func TestUpdateServer(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdateServer(s.ServerID, []string{"SFTP", "FTPS"})
	require.NoError(t, err)
	assert.Equal(t, []string{"SFTP", "FTPS"}, updated.Protocols)
}

// TestServerCountExport verifies ServerCount export.
func TestServerCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	assert.Equal(t, 0, transfer.ServerCount(b))

	_, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.ServerCount(b))
}

// TestDeleteServerCascade verifies DeleteServer removes accesses and agreements.
func TestDeleteServerCascade(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	_, err = b.CreateAccess(s.ServerID, "S-1-5-21-1234", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateAgreement(s.ServerID, "desc", "p-local", "p-partner", "/base", "arn:role", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.UserCount(b))
	assert.Equal(t, 1, transfer.AccessCount(b))
	assert.Equal(t, 1, transfer.AgreementCount(b))

	// AWS requires server to be OFFLINE before deletion.
	require.NoError(t, b.StopServer(s.ServerID))
	for range 30 {
		got, _ := b.DescribeServer(s.ServerID)
		if got != nil && got.State == "OFFLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	require.NoError(t, b.DeleteServer(s.ServerID))

	assert.Equal(t, 0, transfer.ServerCount(b))
	assert.Equal(t, 0, transfer.UserCount(b))
	assert.Equal(t, 0, transfer.AccessCount(b))
	assert.Equal(t, 0, transfer.AgreementCount(b))
}

// TestAddServerInternal verifies the AddServerInternal seed helper.
func TestAddServerInternal(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	b.AddServerInternal("s-test123")

	assert.Equal(t, 1, transfer.ServerCount(b))

	s, err := b.DescribeServer("s-test123")
	require.NoError(t, err)
	assert.Equal(t, "ONLINE", s.State)
}

// TestDeleteServerOnlineReturnsError verifies the backend-level
// ErrServerOnline sentinel.
func TestDeleteServerOnlineReturnsError(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	// Servers are created OFFLINE; start it so it is ONLINE before delete is attempted.
	require.NoError(t, b.StartServer(s.ServerID))

	for range 30 {
		got, derr := b.DescribeServer(s.ServerID)
		require.NoError(t, derr)
		if got.State == "ONLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	err = b.DeleteServer(s.ServerID)
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrConflict)
	require.ErrorIs(t, err, transfer.ErrServerOnline)
}

// TestDeleteServerOfflineSucceeds verifies that stopping then deleting works.
func TestDeleteServerOfflineSucceeds(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.StopServer(s.ServerID))
	// Wait for async OFFLINE transition.
	for range 30 {
		got, _ := b.DescribeServer(s.ServerID)
		if got != nil && got.State == "OFFLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	require.NoError(t, b.DeleteServer(s.ServerID))
	assert.Equal(t, 0, transfer.ServerCount(b))
}

// TestDeleteServerAlsoDeletesSSHKeys verifies that SSH keys are removed
// when a user is deleted, and transitively when a server is deleted.
func TestDeleteServerAlsoDeletesSSHKeys(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	_, err = b.ImportSSHPublicKey(
		s.ServerID, "alice",
		"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test@example",
	)
	require.NoError(t, err)
	assert.Equal(t, 1, transfer.SSHPublicKeyCount(b))

	// Stop and delete server.
	require.NoError(t, b.StopServer(s.ServerID))
	for range 30 {
		got, _ := b.DescribeServer(s.ServerID)
		if got != nil && got.State == "OFFLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	require.NoError(t, b.DeleteServer(s.ServerID))

	assert.Equal(t, 0, transfer.SSHPublicKeyCount(b))
}

// TestStartServerIdempotent verifies starting an ONLINE server is a no-op.
func TestStartServerIdempotent(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)
	// AWS creates servers OFFLINE; bring it ONLINE first.
	assert.Equal(t, "OFFLINE", s.State)
	require.NoError(t, b.StartServer(s.ServerID))

	for range 30 {
		got, derr := b.DescribeServer(s.ServerID)
		require.NoError(t, derr)
		if got.State == "ONLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Starting an already-ONLINE server should not error.
	require.NoError(t, b.StartServer(s.ServerID))

	got, err := b.DescribeServer(s.ServerID)
	require.NoError(t, err)
	assert.Equal(t, "ONLINE", got.State)
}

// TestStopServerIdempotent verifies stopping an OFFLINE server is a no-op.
func TestStopServerIdempotent(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.StopServer(s.ServerID))
	for range 30 {
		got, _ := b.DescribeServer(s.ServerID)
		if got != nil && got.State == "OFFLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Stopping an already-OFFLINE server should not error.
	require.NoError(t, b.StopServer(s.ServerID))

	got, err := b.DescribeServer(s.ServerID)
	require.NoError(t, err)
	assert.Equal(t, "OFFLINE", got.State)
}

// TestListServersSortedByServerID verifies ListServers returns servers
// sorted by ServerId in ascending order (deterministic).
func TestListServersSortedByServerID(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

	// Create multiple servers and collect their IDs.
	for range 5 {
		_, err := b.CreateServer(nil, nil)
		require.NoError(t, err)
	}

	servers := b.ListServers()
	require.Len(t, servers, 5)

	for i := 1; i < len(servers); i++ {
		assert.LessOrEqual(t, servers[i-1].ServerID, servers[i].ServerID,
			"servers not sorted at index %d", i)
	}
}

// TestCreateServerDefaultsIdentityProviderType verifies that
// IdentityProviderType defaults to SERVICE_MANAGED when not provided.
func TestCreateServerDefaultsIdentityProviderType(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "SERVICE_MANAGED", s.IdentityProviderType)
}

// TestCreateServerDefaultsEndpointType verifies EndpointType defaults to PUBLIC.
func TestCreateServerDefaultsEndpointType(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "PUBLIC", s.EndpointType)
}

// TestCreateServerDefaultsDomain verifies Domain defaults to S3.
func TestCreateServerDefaultsDomain(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "S3", s.Domain)
}

// TestServerUserCountMethod verifies the new ServerUserCount method.
func TestServerUserCountMethod(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 0, b.ServerUserCount(s.ServerID))

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, b.ServerUserCount(s.ServerID))

	// Non-existent server returns 0.
	assert.Equal(t, 0, b.ServerUserCount("s-doesnotexist"))
}
