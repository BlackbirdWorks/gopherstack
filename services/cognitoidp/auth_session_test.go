package cognitoidp_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalSignOut_RevokesAccessToken(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "omar")

	err := b.GlobalSignOut(tokens.AccessToken)
	require.NoError(t, err)

	_, err = b.GetUser(tokens.AccessToken)
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized, "access token must be invalid after GlobalSignOut")
}

func TestInMemoryBackend_InitiateAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) (clientID, username, password string)
		name      string
		authFlow  string
		wantErr   bool
	}{
		{
			name:     "success",
			authFlow: "USER_PASSWORD_AUTH",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				u, _ := b.SignUp(client.ClientID, "dave", "Password123!", nil)
				_ = b.ConfirmSignUp(client.ClientID, "dave", u.ConfirmCode)

				return client.ClientID, "dave", "Password123!"
			},
		},
		{
			name:     "wrong_password",
			authFlow: "USER_PASSWORD_AUTH",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				u, _ := b.SignUp(client.ClientID, "dave", "Password123!", nil)
				_ = b.ConfirmSignUp(client.ClientID, "dave", u.ConfirmCode)

				return client.ClientID, "dave", "WrongPassword!"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrNotAuthorized,
		},
		{
			name:     "unconfirmed_user",
			authFlow: "USER_PASSWORD_AUTH",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				_, _ = b.SignUp(client.ClientID, "eve", "Password123!", nil)

				return client.ClientID, "eve", "Password123!"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserNotConfirmed,
		},
		{
			name:     "unsupported_auth_flow",
			authFlow: "REFRESH_TOKEN_AUTH",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				u, _ := b.SignUp(client.ClientID, "frank", "Password123!", nil)
				_ = b.ConfirmSignUp(client.ClientID, "frank", u.ConfirmCode)

				return client.ClientID, "frank", "Password123!"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrInvalidUserPoolConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			clientID, username, password := tt.setup(b)

			tokens, err := b.InitiateAuth(clientID, tt.authFlow, username, password)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, tokens.Tokens)
			assert.NotEmpty(t, tokens.Tokens.AccessToken)
			assert.NotEmpty(t, tokens.Tokens.IDToken)
			assert.NotEmpty(t, tokens.Tokens.RefreshToken)
			assert.Equal(t, int32(3600), tokens.Tokens.ExpiresIn)
		})
	}
}

func TestInMemoryBackend_AdminInitiateAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) (poolID, clientID, username, password string)
		name      string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				_, _ = b.AdminCreateUser(pool.ID, "grace", "Temp123!", nil)
				_ = b.AdminSetUserPassword(pool.ID, "grace", "Password123!", true)

				return pool.ID, client.ClientID, "grace", "Password123!"
			},
		},
		{
			name: "wrong_password",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				_, _ = b.AdminCreateUser(pool.ID, "henry", "Temp123!", nil)
				_ = b.AdminSetUserPassword(pool.ID, "henry", "Password123!", true)

				return pool.ID, client.ClientID, "henry", "Wrong!"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrNotAuthorized,
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.InMemoryBackend) (string, string, string, string) {
				return "us-east-1_nonexistent", "clientXYZ", "user", "pass"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
		{
			name: "client_not_found",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string, string) {
				pool, _ := b.CreateUserPool("p")

				return pool.ID, "invalid-client-id", "user", "pass"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
		},
		{
			name: "client_wrong_pool",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string, string) {
				pool1, _ := b.CreateUserPool("pool1")
				pool2, _ := b.CreateUserPool("pool2")
				client2, _ := b.CreateUserPoolClient(pool2.ID, "c2")

				return pool1.ID, client2.ClientID, "user", "pass"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID, clientID, username, password := tt.setup(b)

			tokens, err := b.AdminInitiateAuth(poolID, clientID, "USER_PASSWORD_AUTH", username, password)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, tokens.Tokens)
			assert.NotEmpty(t, tokens.Tokens.AccessToken)
		})
	}
}

func TestInMemoryBackend_AdminResetUserPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) (string, string)
		name      string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				p, _ := b.CreateUserPool("pool")
				u, _ := b.AdminCreateUser(p.ID, "alice", "TempPass1!", nil)
				_ = b.AdminSetUserPassword(p.ID, u.Username, "Perm1Pass!", true)

				return p.ID, u.Username
			},
		},
		{
			name: "user_not_found",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				p, _ := b.CreateUserPool("pool")

				return p.ID, "nobody"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID, username := tt.setup(b)

			err := b.AdminResetUserPassword(poolID, username)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)

			u, getErr := b.AdminGetUser(poolID, username)
			require.NoError(t, getErr)
			assert.Equal(t, cognitoidp.UserStatusForceChangePassword, u.Status)
		})
	}
}

func TestInMemoryBackend_DeleteRefreshTokensForClientCleansUserIndex(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	p, _ := b.CreateUserPool("pool")
	c, _ := b.CreateUserPoolClient(p.ID, "client")
	_, _ = b.AdminCreateUser(p.ID, "alice", "TempPass1!", nil)

	// Authenticate to create a refresh token.
	_ = b.AdminSetUserPassword(p.ID, "alice", "Perm1Pass!", true)
	auth, err := b.AdminInitiateAuth(p.ID, c.ClientID, "ADMIN_USER_PASSWORD_AUTH", "alice", "Perm1Pass!")
	require.NoError(t, err)
	require.NotNil(t, auth.Tokens)
	require.NotEmpty(t, auth.Tokens.RefreshToken)

	assert.Equal(t, 1, b.RefreshTokenCount())

	// Delete the client; should clean up both refreshTokensByClient and refreshTokensByUser.
	require.NoError(t, b.DeleteUserPoolClient(p.ID, c.ClientID))

	assert.Equal(t, 0, b.RefreshTokenCount())
}

// TestBackend_AdminUserGlobalSignOut covers the backend's AdminUserGlobalSignOut,
// including pool-not-found and user-not-found error paths.
func TestBackend_AdminUserGlobalSignOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget    error
		name         string
		wantErr      bool
		poolNotFound bool
		userNotFound bool
	}{
		{name: "success"},
		{
			name:         "pool_not_found",
			wantErr:      true,
			errTarget:    cognitoidp.ErrUserPoolNotFound,
			poolNotFound: true,
		},
		{
			name:         "user_not_found",
			wantErr:      true,
			errTarget:    cognitoidp.ErrUserNotFound,
			userNotFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("signout-pool")
			require.NoError(t, err)

			if tt.poolNotFound {
				err = b.AdminUserGlobalSignOut("bad-pool", "any-user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			if tt.userNotFound {
				err = b.AdminUserGlobalSignOut(pool.ID, "no-such-user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.AdminCreateUser(pool.ID, "signout-user", "TempPass1!", map[string]string{})
			require.NoError(t, err)
			require.NoError(t, b.AdminSetUserPassword(pool.ID, "signout-user", "PermPass1!", true))

			client, err := b.CreateUserPoolClient(pool.ID, "sc")
			require.NoError(t, err)

			result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "signout-user", "PermPass1!")
			require.NoError(t, err)
			require.NotNil(t, result.Tokens)

			// Before sign-out refresh token count > 0.
			assert.Positive(t, b.RefreshTokenCount())

			err = b.AdminUserGlobalSignOut(pool.ID, "signout-user")
			require.NoError(t, err)

			// After sign-out refresh tokens are gone.
			assert.Equal(t, 0, b.RefreshTokenCount())
		})
	}
}

func TestGlobalSignOut_RejectsIDToken(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "sigouter")

	err := b.GlobalSignOut(tokens.IDToken)
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)

	// The access token must still work.
	err = b.GlobalSignOut(tokens.AccessToken)
	require.NoError(t, err)
}
