package cognitoidp_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestConfirmSignUp_EmptyStoredCode verifies that an unconfirmed user with
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
