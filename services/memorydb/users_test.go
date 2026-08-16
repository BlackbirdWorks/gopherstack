package memorydb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func TestBackend_User_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		userName string
		wantErr  bool
	}{
		{
			name:     "create_and_describe",
			userName: "test-user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateUserRequest{
				UserName:     tt.userName,
				AccessString: "on ~* &* +@all",
				AuthenticationMode: memorydb.ExportedAuthModeReq{
					Type:      "password",
					Passwords: []string{"mypassword"},
				},
			}

			u, err := b.CreateUser(context.Background(), req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.userName, u.Name)
			assert.NotEmpty(t, u.ARN)

			users, err := b.DescribeUsers(context.Background(), tt.userName)
			require.NoError(t, err)
			require.Len(t, users, 1)

			_, err = b.DeleteUser(context.Background(), tt.userName)
			require.NoError(t, err)
		})
	}
}
