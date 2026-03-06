package cognitoidp_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func newTestBackend() *cognitoidp.InMemoryBackend {
	return cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
}

func TestInMemoryBackend_CreateUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		poolName  string
		wantErr   bool
	}{
		{
			name:     "success",
			poolName: "my-pool",
		},
		{
			name:      "duplicate_name",
			poolName:  "my-pool",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolAlreadyExists,
		},
	}

	backend := newTestBackend()

	// Create the first pool for the duplicate test.
	_, err := backend.CreateUserPool("my-pool")
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "duplicate_name" {
				// Pre-create pool to trigger duplicate.
				_, setupErr := b.CreateUserPool("my-pool")
				require.NoError(t, setupErr)
			}

			pool, createErr := b.CreateUserPool(tt.poolName)

			if tt.wantErr {
				require.Error(t, createErr)
				assert.ErrorIs(t, createErr, tt.errTarget)

				return
			}

			require.NoError(t, createErr)
			assert.NotEmpty(t, pool.ID)
			assert.Equal(t, tt.poolName, pool.Name)
			assert.NotEmpty(t, pool.ARN)
		})
	}
}

func TestInMemoryBackend_DescribeUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget  error
		name       string
		userPoolID string
		wantErr    bool
	}{
		{
			name:    "success",
			wantErr: false,
		},
		{
			name:       "not_found",
			userPoolID: "us-east-1_nonexistent",
			wantErr:    true,
			errTarget:  cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, setupErr := b.CreateUserPool("test-pool")
			require.NoError(t, setupErr)

			poolID := pool.ID
			if tt.userPoolID != "" {
				poolID = tt.userPoolID
			}

			got, err := b.DescribeUserPool(poolID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, poolID, got.ID)
		})
	}
}

func TestInMemoryBackend_ListUserPools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		numPools int
	}{
		{
			name:     "empty",
			numPools: 0,
		},
		{
			name:     "multiple_pools",
			numPools: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for i := range tt.numPools {
				_, err := b.CreateUserPool("pool-" + strconv.Itoa(i))
				require.NoError(t, err)
			}

			pools := b.ListUserPools()
			assert.Len(t, pools, tt.numPools)
		})
	}
}

func TestInMemoryBackend_CreateUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget  error
		poolIDFunc func(b *cognitoidp.InMemoryBackend) string
		name       string
		clientName string
		wantErr    bool
	}{
		{
			name:       "success",
			clientName: "my-client",
			poolIDFunc: func(b *cognitoidp.InMemoryBackend) string {
				pool, err := b.CreateUserPool("test-pool")
				if err != nil {
					return ""
				}

				return pool.ID
			},
		},
		{
			name:       "pool_not_found",
			clientName: "my-client",
			poolIDFunc: func(_ *cognitoidp.InMemoryBackend) string {
				return "us-east-1_nonexistent"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID := tt.poolIDFunc(b)

			client, err := b.CreateUserPoolClient(poolID, tt.clientName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, client.ClientID)
			assert.Equal(t, tt.clientName, client.ClientName)
			assert.Equal(t, poolID, client.UserPoolID)
		})
	}
}

func TestInMemoryBackend_DescribeUserPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
		badPoolID bool
	}{
		{
			name: "success",
		},
		{
			name:      "not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
		},
		{
			name:      "wrong_pool",
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
			badPoolID: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("test-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "test-client")
			require.NoError(t, err)

			clientID := client.ClientID
			poolID := pool.ID

			if tt.name == "not_found" {
				clientID = "nonexistent"
			}

			if tt.badPoolID {
				poolID = "us-east-1_wrong"
			}

			got, descErr := b.DescribeUserPoolClient(poolID, clientID)

			if tt.wantErr {
				require.Error(t, descErr)
				assert.ErrorIs(t, descErr, tt.errTarget)

				return
			}

			require.NoError(t, descErr)
			assert.Equal(t, client.ClientID, got.ClientID)
		})
	}
}

func TestInMemoryBackend_SignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) string
		name      string
		username  string
		password  string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")

				return client.ClientID
			},
			username: "alice",
			password: "Password123!",
		},
		{
			name: "duplicate_user",
			setup: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				_, _ = b.SignUp(client.ClientID, "alice", "Password123!", nil)

				return client.ClientID
			},
			username:  "alice",
			password:  "Password123!",
			wantErr:   true,
			errTarget: cognitoidp.ErrUsernameExists,
		},
		{
			name: "client_not_found",
			setup: func(_ *cognitoidp.InMemoryBackend) string {
				return "nonexistent-client"
			},
			username:  "alice",
			password:  "Password123!",
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			clientID := tt.setup(b)

			user, err := b.SignUp(clientID, tt.username, tt.password, map[string]string{"email": "alice@example.com"})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.username, user.Username)
			assert.Equal(t, cognitoidp.UserStatusUnconfirmed, user.Status)
			assert.NotEmpty(t, user.Sub)
		})
	}
}

func TestInMemoryBackend_ConfirmSignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget        error
		setup            func(b *cognitoidp.InMemoryBackend) string
		name             string
		username         string
		confirmationCode string
		wantErr          bool
	}{
		{
			name: "success",
			setup: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				_, _ = b.SignUp(client.ClientID, "bob", "Password123!", nil)

				return client.ClientID
			},
			username:         "bob",
			confirmationCode: "123456",
		},
		{
			name: "user_not_found",
			setup: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")

				return client.ClientID
			},
			username:         "nobody",
			confirmationCode: "123456",
			wantErr:          true,
			errTarget:        cognitoidp.ErrUserNotFound,
		},
		{
			name: "empty_code",
			setup: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				_, _ = b.SignUp(client.ClientID, "carol", "Password123!", nil)

				return client.ClientID
			},
			username:         "carol",
			confirmationCode: "",
			wantErr:          true,
			errTarget:        cognitoidp.ErrCodeMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			clientID := tt.setup(b)

			err := b.ConfirmSignUp(clientID, tt.username, tt.confirmationCode)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
		})
	}
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
				_, _ = b.SignUp(client.ClientID, "dave", "Password123!", nil)
				_ = b.ConfirmSignUp(client.ClientID, "dave", "code")

				return client.ClientID, "dave", "Password123!"
			},
		},
		{
			name:     "wrong_password",
			authFlow: "USER_PASSWORD_AUTH",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				_, _ = b.SignUp(client.ClientID, "dave", "Password123!", nil)
				_ = b.ConfirmSignUp(client.ClientID, "dave", "code")

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
				_, _ = b.SignUp(client.ClientID, "frank", "Password123!", nil)
				_ = b.ConfirmSignUp(client.ClientID, "frank", "code")

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
			assert.NotEmpty(t, tokens.AccessToken)
			assert.NotEmpty(t, tokens.IDToken)
			assert.NotEmpty(t, tokens.RefreshToken)
			assert.Equal(t, int32(3600), tokens.ExpiresIn)
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
			assert.NotEmpty(t, tokens.AccessToken)
		})
	}
}

func TestInMemoryBackend_AdminCreateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) string
		name      string
		username  string
		password  string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")

				return pool.ID
			},
			username: "iris",
			password: "Temp123!",
		},
		{
			name: "duplicate_user",
			setup: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")
				_, _ = b.AdminCreateUser(pool.ID, "iris", "Temp123!", nil)

				return pool.ID
			},
			username:  "iris",
			password:  "Temp123!",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserAlreadyExists,
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.InMemoryBackend) string {
				return "us-east-1_nonexistent"
			},
			username:  "iris",
			password:  "Temp123!",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID := tt.setup(b)

			user, err := b.AdminCreateUser(
				poolID,
				tt.username,
				tt.password,
				map[string]string{"email": "test@example.com"},
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.username, user.Username)
			assert.Equal(t, cognitoidp.UserStatusForceChangePassword, user.Status)
		})
	}
}

func TestInMemoryBackend_AdminSetUserPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) (poolID, username string)
		name      string
		password  string
		permanent bool
		wantErr   bool
	}{
		{
			name: "permanent_password",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				pool, _ := b.CreateUserPool("p")
				_, _ = b.AdminCreateUser(pool.ID, "jack", "Temp!", nil)

				return pool.ID, "jack"
			},
			password:  "NewPass123!",
			permanent: true,
		},
		{
			name: "temporary_password",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				pool, _ := b.CreateUserPool("p")
				_, _ = b.AdminCreateUser(pool.ID, "kate", "Temp!", nil)

				return pool.ID, "kate"
			},
			password:  "NewTemp123!",
			permanent: false,
		},
		{
			name: "user_not_found",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				pool, _ := b.CreateUserPool("p")

				return pool.ID, "nobody"
			},
			password:  "Pass123!",
			permanent: true,
			wantErr:   true,
			errTarget: cognitoidp.ErrUserNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID, username := tt.setup(b)

			err := b.AdminSetUserPassword(poolID, username, tt.password, tt.permanent)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)

			if tt.permanent {
				user, getUserErr := b.AdminGetUser(poolID, username)
				require.NoError(t, getUserErr)
				assert.Equal(t, cognitoidp.UserStatusConfirmed, user.Status)
			}
		})
	}
}

func TestInMemoryBackend_AdminGetUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) (poolID, username string)
		name      string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				pool, _ := b.CreateUserPool("p")
				_, _ = b.AdminCreateUser(pool.ID, "lena", "Temp!", nil)

				return pool.ID, "lena"
			},
		},
		{
			name: "not_found",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				pool, _ := b.CreateUserPool("p")

				return pool.ID, "nobody"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserNotFound,
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.InMemoryBackend) (string, string) {
				return "us-east-1_nonexistent", "user"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID, username := tt.setup(b)

			user, err := b.AdminGetUser(poolID, username)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, username, user.Username)
		})
	}
}

func TestInMemoryBackend_GetUserPoolJWKS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget  error
		userPoolID func(b *cognitoidp.InMemoryBackend) string
		name       string
		wantErr    bool
	}{
		{
			name: "success",
			userPoolID: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")

				return pool.ID
			},
		},
		{
			name: "pool_not_found",
			userPoolID: func(_ *cognitoidp.InMemoryBackend) string {
				return "us-east-1_nonexistent"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID := tt.userPoolID(b)

			jwks, err := b.GetUserPoolJWKS(poolID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			require.Len(t, jwks.Keys, 1)
			assert.Equal(t, "RSA", jwks.Keys[0].Kty)
			assert.Equal(t, "RS256", jwks.Keys[0].Alg)
			assert.Equal(t, "sig", jwks.Keys[0].Use)
			assert.NotEmpty(t, jwks.Keys[0].N)
			assert.NotEmpty(t, jwks.Keys[0].E)
		})
	}
}
