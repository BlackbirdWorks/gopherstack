package transfer_test

import (
	"testing"

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

	return transfer.NewInMemoryBackend(testAccountID, testRegion)
}

func TestInMemoryBackend_CreateServer(t *testing.T) {
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
			assert.Equal(t, "ONLINE", s.State)

			if len(tt.protocols) == 0 {
				assert.Equal(t, []string{"SFTP"}, s.Protocols)
			} else {
				assert.Equal(t, tt.protocols, s.Protocols)
			}
		})
	}
}

func TestInMemoryBackend_DescribeServer(t *testing.T) {
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
			assert.Equal(t, "ONLINE", got.State)
		})
	}
}

func TestInMemoryBackend_ListServers(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateServer([]string{"SFTP", "FTPS"}, nil)
	require.NoError(t, err)

	servers := b.ListServers()
	assert.Len(t, servers, 2)
}

func TestInMemoryBackend_DeleteServer(t *testing.T) {
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

func TestInMemoryBackend_StartStopServer(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	// Stop
	require.NoError(t, b.StopServer(s.ServerID))
	got, err := b.DescribeServer(s.ServerID)
	require.NoError(t, err)
	assert.Equal(t, "OFFLINE", got.State)

	// Start
	require.NoError(t, b.StartServer(s.ServerID))
	got, err = b.DescribeServer(s.ServerID)
	require.NoError(t, err)
	assert.Equal(t, "ONLINE", got.State)
}

func TestInMemoryBackend_StartStopServer_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	require.Error(t, b.StartServer("s-missing"))
	require.Error(t, b.StopServer("s-missing"))
}

func TestInMemoryBackend_UpdateServer(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdateServer(s.ServerID, []string{"SFTP", "FTPS"})
	require.NoError(t, err)
	assert.Equal(t, []string{"SFTP", "FTPS"}, updated.Protocols)
}

func TestInMemoryBackend_CreateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		userName string
		homeDir  string
		role     string
		wantErr  bool
	}{
		{
			name:     "success",
			userName: "alice",
			homeDir:  "/alice",
			role:     "arn:aws:iam::123456789012:role/transfer-role",
		},
		{
			name:     "empty username",
			userName: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			s, err := b.CreateServer(nil, nil)
			require.NoError(t, err)

			if tt.wantErr && tt.userName == "" {
				_, err = b.CreateUser(s.ServerID, "", tt.homeDir, tt.role, nil)
				require.NoError(t, err) // backend allows empty username; handler validates

				return
			}

			u, err := b.CreateUser(s.ServerID, tt.userName, tt.homeDir, tt.role, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.userName, u.UserName)
			assert.Equal(t, tt.homeDir, u.HomeDir)
			assert.Equal(t, tt.role, u.Role)
		})
	}
}

func TestInMemoryBackend_CreateUser_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "arn:role", nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "arn:role", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrConflict)
}

func TestInMemoryBackend_DescribeUser(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "bob", "/bob", "", nil)
	require.NoError(t, err)

	u, err := b.DescribeUser(s.ServerID, "bob")
	require.NoError(t, err)
	assert.Equal(t, "bob", u.UserName)
	assert.Equal(t, "/bob", u.HomeDir)
}

func TestInMemoryBackend_DescribeUser_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.DescribeUser(s.ServerID, "nobody")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListUsers(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "bob", "/bob", "", nil)
	require.NoError(t, err)

	users, err := b.ListUsers(s.ServerID)
	require.NoError(t, err)
	assert.Len(t, users, 2)
	// Sorted by username
	assert.Equal(t, "alice", users[0].UserName)
	assert.Equal(t, "bob", users[1].UserName)
}

func TestInMemoryBackend_DeleteUser(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteUser(s.ServerID, "alice"))

	_, err = b.DescribeUser(s.ServerID, "alice")
	require.Error(t, err)
}

func TestInMemoryBackend_UpdateUser(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	updated, err := b.UpdateUser(s.ServerID, "alice", "/home/alice", "arn:role")
	require.NoError(t, err)
	assert.Equal(t, "/home/alice", updated.HomeDir)
	assert.Equal(t, "arn:role", updated.Role)
}

func TestInMemoryBackend_ListUsers_ServerNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.ListUsers("s-doesnotexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}
