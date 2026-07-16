package cognitoidp_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExplicitAuthFlows_Enforcement(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("authflow-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "restricted-client", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_USER_PASSWORD_AUTH"},
	})
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "dave", "Pass1234!", nil)
	require.NoError(t, err)
	err = b.ConfirmSignUp(client.ClientID, "dave", user.ConfirmCode)
	require.NoError(t, err)

	_, err = b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "dave", "Pass1234!")
	require.NoError(t, err)

	_, err = b.InitiateAuth(client.ClientID, "USER_SRP_AUTH", "dave", "Pass1234!")
	require.ErrorIs(t, err, cognitoidp.ErrInvalidUserPoolConfig)
}

func TestSignUpWithValidation_AutoVerify(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("auto-verify-pool", cognitoidp.UserPoolOptions{
		AutoVerifiedAttributes: []string{"email"},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "av-client")
	require.NoError(t, err)

	user, err := b.SignUpWithValidation(client.ClientID, "frank", "Pass1234!",
		map[string]string{"email": "frank@example.com"})
	require.NoError(t, err)
	assert.Equal(t, cognitoidp.UserStatusConfirmed, user.Status)
	assert.Empty(t, user.ConfirmCode)
	assert.Equal(t, "true", user.Attributes["email_verified"])
}

func TestSignUpWithValidation_RequiresCode_WhenEmailMissing(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("auto-verify-pool2", cognitoidp.UserPoolOptions{
		AutoVerifiedAttributes: []string{"email"},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "av-client2")
	require.NoError(t, err)

	user, err := b.SignUpWithValidation(client.ClientID, "greta", "Pass1234!", nil)
	require.NoError(t, err)
	assert.Equal(t, cognitoidp.UserStatusUnconfirmed, user.Status)
	assert.NotEmpty(t, user.ConfirmCode)
}

func TestAdminCreateUser_ForceChangePassword(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("fcp-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "fcp-client")
	require.NoError(t, err)

	_, err = b.AdminCreateUser(pool.ID, "hank", "Temp1234!", nil)
	require.NoError(t, err)

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "hank", "Temp1234!")
	require.NoError(t, err)
	assert.Equal(t, "NEW_PASSWORD_REQUIRED", result.ChallengeName)
	assert.NotEmpty(t, result.MFASession)
}

func TestRespondToNewPasswordRequired_IssuesTokens(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("fcp-pool2")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "fcp-client2")
	require.NoError(t, err)

	_, err = b.AdminCreateUser(pool.ID, "ivan", "Temp1234!", nil)
	require.NoError(t, err)

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "ivan", "Temp1234!")
	require.NoError(t, err)
	require.Equal(t, "NEW_PASSWORD_REQUIRED", result.ChallengeName)

	tokens, err := b.RespondToNewPasswordRequired(client.ClientID, result.MFASession, "NewPass1234!")
	require.NoError(t, err)
	assert.NotEmpty(t, tokens.IDToken)
	assert.NotEmpty(t, tokens.AccessToken)
}

func TestGlobalSignOut_RevokesAccessToken(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "omar")

	err := b.GlobalSignOut(tokens.AccessToken)
	require.NoError(t, err)

	_, err = b.GetUser(tokens.AccessToken)
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized, "access token must be invalid after GlobalSignOut")
}

func TestSignUpWithValidation_PasswordPolicyEnforced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("pp-enforce-pool", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{
			MinimumLength:    10,
			RequireUppercase: true,
			RequireNumbers:   true,
		},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "pp-client")
	require.NoError(t, err)

	_, err = b.SignUpWithValidation(client.ClientID, "pat", "short", nil)
	require.ErrorIs(t, err, cognitoidp.ErrInvalidPassword)

	_, err = b.SignUpWithValidation(client.ClientID, "pat2", "alllowercase1", nil)
	require.ErrorIs(t, err, cognitoidp.ErrInvalidPassword)

	_, err = b.SignUpWithValidation(client.ClientID, "pat3", "LongEnough1234", nil)
	require.NoError(t, err)
}

func TestChangePassword_PolicyEnforced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("cp-pool", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{
			MinimumLength:    8,
			RequireUppercase: true,
		},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "cp-client")
	require.NoError(t, err)

	user, err := b.SignUpWithValidation(client.ClientID, "quinn", "Pass1234",
		map[string]string{"email": "q@x.com"})
	require.NoError(t, err)
	err = b.AdminConfirmSignUp(pool.ID, user.Username)
	require.NoError(t, err)

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "quinn", "Pass1234")
	require.NoError(t, err)

	err = b.ChangePassword(result.Tokens.AccessToken, "Pass1234", "toolow")
	require.ErrorIs(t, err, cognitoidp.ErrInvalidPassword)

	err = b.ChangePassword(result.Tokens.AccessToken, "Pass1234", "NewPass1234")
	require.NoError(t, err)
}

func TestConfirmForgotPassword_PolicyEnforced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("cfp-pool", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{
			MinimumLength:    8,
			RequireUppercase: true,
		},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "cfp-client")
	require.NoError(t, err)

	user, err := b.SignUpWithValidation(client.ClientID, "rachel", "Pass1234",
		map[string]string{"email": "r@x.com"})
	require.NoError(t, err)
	err = b.AdminConfirmSignUp(pool.ID, user.Username)
	require.NoError(t, err)

	code, err := b.ForgotPassword(client.ClientID, "rachel")
	require.NoError(t, err)

	err = b.ConfirmForgotPassword(client.ClientID, "rachel", code, "toolow")
	require.ErrorIs(t, err, cognitoidp.ErrInvalidPassword)

	err = b.ConfirmForgotPassword(client.ClientID, "rachel", code, "NewPass1234")
	require.NoError(t, err)
}

func TestConfirmSignUp_ExpiredCode_Rejected(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	poolID := b.UserPoolID(client.ClientID)

	user, err := b.SignUp(client.ClientID, "wendy", "Pass1234!",
		map[string]string{"email": "w@x.com"})
	require.NoError(t, err)

	b.ExpireConfirmCodeForTest(poolID, user.Username)

	err = b.ConfirmSignUp(client.ClientID, user.Username, user.ConfirmCode)
	require.ErrorIs(t, err, cognitoidp.ErrExpiredCode)
}

func TestSecretHash_InitiateAuth_Valid(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("sh-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "sh-client", cognitoidp.UserPoolClientOptions{
		GenerateSecret: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, client.ClientSecret)

	user, err := b.SignUp(client.ClientID, "alice", "Pass1234!", nil)
	require.NoError(t, err)
	err = b.ConfirmSignUp(client.ClientID, "alice", user.ConfirmCode)
	require.NoError(t, err)

	validHash := computeSecretHash(client.ClientID, "alice", client.ClientSecret)
	err = b.ValidateSecretHash(client.ClientID, "alice", validHash)
	require.NoError(t, err)
}

// TestSecretHash_Validation table-drives the ValidateSecretHash matrix
// previously spread across four near-identical single-scenario tests
// (invalid hash, secret required, secret forbidden, empty accepted).
func TestSecretHash_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *cognitoidp.InMemoryBackend) *cognitoidp.UserPoolClient
		errTarget error
		name      string
		hash      string
		wantErr   bool
	}{
		{
			name: "wrong_hash_rejected",
			setup: func(b *cognitoidp.InMemoryBackend) *cognitoidp.UserPoolClient {
				pool, err := b.CreateUserPoolWithOpts("sh-fail-pool", cognitoidp.UserPoolOptions{})
				require.NoError(t, err)

				client, err := b.CreateUserPoolClientWithOpts(
					pool.ID,
					"sh-fail-client",
					cognitoidp.UserPoolClientOptions{
						GenerateSecret: true,
					},
				)
				require.NoError(t, err)

				return client
			},
			hash:      "wronghash",
			wantErr:   true,
			errTarget: cognitoidp.ErrNotAuthorized,
		},
		{
			name: "required_when_client_has_secret",
			setup: func(b *cognitoidp.InMemoryBackend) *cognitoidp.UserPoolClient {
				pool, err := b.CreateUserPoolWithOpts("sh-req-pool", cognitoidp.UserPoolOptions{})
				require.NoError(t, err)

				client, err := b.CreateUserPoolClientWithOpts(
					pool.ID,
					"sh-req-client",
					cognitoidp.UserPoolClientOptions{
						GenerateSecret: true,
					},
				)
				require.NoError(t, err)

				return client
			},
			hash:      "",
			wantErr:   true,
			errTarget: cognitoidp.ErrInvalidParameter,
		},
		{
			name: "forbidden_when_client_has_no_secret",
			setup: func(b *cognitoidp.InMemoryBackend) *cognitoidp.UserPoolClient {
				pool, err := b.CreateUserPoolWithOpts("sh-forbid-pool", cognitoidp.UserPoolOptions{})
				require.NoError(t, err)

				client, err := b.CreateUserPoolClientWithOpts(
					pool.ID,
					"sh-forbid-client",
					cognitoidp.UserPoolClientOptions{},
				)
				require.NoError(t, err)

				return client
			},
			hash:      "somehash",
			wantErr:   true,
			errTarget: cognitoidp.ErrInvalidParameter,
		},
		{
			name: "accepts_empty_when_no_secret",
			setup: func(b *cognitoidp.InMemoryBackend) *cognitoidp.UserPoolClient {
				pool, err := b.CreateUserPoolWithOpts("sh-ok-pool", cognitoidp.UserPoolOptions{})
				require.NoError(t, err)

				client, err := b.CreateUserPoolClientWithOpts(
					pool.ID,
					"sh-ok-client",
					cognitoidp.UserPoolClientOptions{},
				)
				require.NoError(t, err)

				return client
			},
			hash: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			client := tt.setup(b)

			err := b.ValidateSecretHash(client.ClientID, "alice", tt.hash)
			if tt.wantErr {
				require.ErrorIs(t, err, tt.errTarget)
			} else {
				require.NoError(t, err)
			}
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
				u, _ := b.SignUp(client.ClientID, "bob", "Password123!", nil)

				_ = b.ConfirmSignUp(client.ClientID, "bob", u.ConfirmCode)

				return client.ClientID
			},
			username:         "bob",
			confirmationCode: "irrelevant-the-setup-already-confirmed",
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

// TestHandler_AdminUserGlobalSignOut_Via_HTTP covers the HTTP handler for AdminUserGlobalSignOut.
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

func TestBackend_ResendConfirmationCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget        error
		name             string
		wantErr          bool
		clientBad        bool
		poolBad          bool
		userBad          bool
		alreadyConfirmed bool
	}{
		{name: "success"},
		{
			name:      "client_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrClientNotFound,
			clientBad: true,
		},
		{
			name:      "user_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserNotFound,
			userBad:   true,
		},
		{
			name:             "already_confirmed",
			wantErr:          true,
			errTarget:        cognitoidp.ErrInvalidParameter,
			alreadyConfirmed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("resend-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "resend-client")
			require.NoError(t, err)

			if tt.clientBad {
				_, err = b.ResendConfirmationCode("bad-client-id", "user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			if tt.userBad {
				_, err = b.ResendConfirmationCode(client.ClientID, "no-user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			user, err := b.SignUp(client.ClientID, "resend-user", "Pass1234!", map[string]string{})
			require.NoError(t, err)

			if tt.alreadyConfirmed {
				require.NoError(t, b.ConfirmSignUp(client.ClientID, "resend-user", user.ConfirmCode))

				_, err = b.ResendConfirmationCode(client.ClientID, "resend-user")
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			newCode, err := b.ResendConfirmationCode(client.ClientID, "resend-user")
			require.NoError(t, err)
			assert.Len(t, newCode, 6)

			// New code must work to confirm.
			err = b.ConfirmSignUp(client.ClientID, "resend-user", newCode)
			require.NoError(t, err)
		})
	}
}

func TestBackend_ConfirmCodeExpirySet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "signup_sets_expiry", op: "signup"},
		{name: "forgot_password_sets_expiry", op: "forgot"},
		{name: "resend_sets_expiry", op: "resend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("expiry-pool")
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "expiry-client")
			require.NoError(t, err)

			user, err := b.SignUp(client.ClientID, "expiry-user", "Pass1234!", map[string]string{})
			require.NoError(t, err)

			// ConfirmCodeExpiresAt must be non-zero after SignUp.
			assert.False(t, user.ConfirmCodeExpiresAt.IsZero(), "ConfirmCodeExpiresAt must be set after SignUp")

			if tt.op == "signup" {
				return
			}

			// Confirm the user.
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "expiry-user", user.ConfirmCode))

			if tt.op == "forgot" {
				code, forgotErr := b.ForgotPassword(client.ClientID, "expiry-user")
				require.NoError(t, forgotErr)
				require.NotEmpty(t, code)

				// Now expire the code artificially and confirm the expiry check fires.
				b.ExpireConfirmCodeForTest(pool.ID, "expiry-user")

				confirmErr := b.ConfirmForgotPassword(client.ClientID, "expiry-user", code, "NewPass1234!")
				require.Error(t, confirmErr)
				assert.ErrorIs(t, confirmErr, cognitoidp.ErrExpiredCode)

				return
			}

			// resend case: re-sign up a fresh user since we confirmed above.
			user2, err := b.SignUp(client.ClientID, "expiry-user2", "Pass1234!", map[string]string{})
			require.NoError(t, err)
			assert.False(t, user2.ConfirmCodeExpiresAt.IsZero())

			newCode, err := b.ResendConfirmationCode(client.ClientID, "expiry-user2")
			require.NoError(t, err)
			require.NotEmpty(t, newCode)

			// Expire the code and check that confirmation fails.
			b.ExpireConfirmCodeForTest(pool.ID, "expiry-user2")

			err = b.ConfirmSignUp(client.ClientID, "expiry-user2", newCode)
			require.Error(t, err)
			assert.ErrorIs(t, err, cognitoidp.ErrExpiredCode)
		})
	}
}

func TestBackend_SignUpWithValidation_PasswordPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{name: "valid", password: "ValidPass123!"},
		{name: "too_short", password: "Abc1!", wantErr: true},
		{name: "no_uppercase", password: "lowercase1!", wantErr: true},
		{name: "no_lowercase", password: "UPPERCASE1!", wantErr: true},
		{name: "no_number", password: "NoNumber!", wantErr: true},
		{name: "no_symbol", password: "NoSymbol123", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("policy-pool")
			require.NoError(t, err)

			require.NoError(t, b.UpdateUserPoolWithOpts(pool.ID, "", cognitoidp.UserPoolOptions{
				PasswordPolicy: &cognitoidp.PasswordPolicy{
					MinimumLength:    8,
					RequireUppercase: true,
					RequireLowercase: true,
					RequireNumbers:   true,
					RequireSymbols:   true,
				},
			}))

			client, err := b.CreateUserPoolClientWithOpts(pool.ID, "pc", cognitoidp.UserPoolClientOptions{})
			require.NoError(t, err)

			_, signUpErr := b.SignUpWithValidation(client.ClientID, "test-user", tt.password, map[string]string{})

			if tt.wantErr {
				require.Error(t, signUpErr)
			} else {
				require.NoError(t, signUpErr)
			}
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

// TestParity_ConfirmSignUp_EmptyStoredCode verifies that an unconfirmed user with
// no stored confirmation code cannot be confirmed by an arbitrary code, while
// re-confirming an already-confirmed user remains idempotent.
func TestConfirmSignUp_EmptyStoredCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *cognitoidp.InMemoryBackend) (clientID, username, code string)
		errTarget error
		name      string
		wantErr   bool
	}{
		{
			name: "unconfirmed_empty_stored_code_rejected",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				_, _ = b.SignUp(client.ClientID, "eve", "Password123!", nil)
				// Clear the stored confirm code to simulate "no code stored".
				b.ClearConfirmCodeForTest(pool.ID, "eve")

				return client.ClientID, "eve", "999999"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrCodeMismatch,
		},
		{
			name: "already_confirmed_idempotent",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				u, _ := b.SignUp(client.ClientID, "frank", "Password123!", nil)
				_ = b.ConfirmSignUp(client.ClientID, "frank", u.ConfirmCode)

				return client.ClientID, "frank", "irrelevant"
			},
			wantErr: false,
		},
		{
			name: "valid_code_confirms",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string, string) {
				pool, _ := b.CreateUserPool("p")
				client, _ := b.CreateUserPoolClient(pool.ID, "c")
				u, _ := b.SignUp(client.ClientID, "grace", "Password123!", nil)

				return client.ClientID, "grace", u.ConfirmCode
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			clientID, username, code := tt.setup(b)

			err := b.ConfirmSignUp(clientID, username, code)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
		})
	}
}
