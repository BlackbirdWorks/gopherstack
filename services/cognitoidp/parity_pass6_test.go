package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestParity_GetUser_RejectsIDToken verifies that access-token operations reject
// an ID token presented in place of an access token (token_use enforcement).
func TestParity_GetUser_RejectsIDToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		useID     bool
		wantErr   bool
		errTarget error
	}{
		{name: "access_token_accepted", useID: false, wantErr: false},
		{name: "id_token_rejected", useID: true, wantErr: true, errTarget: cognitoidp.ErrNotAuthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, client := setupTestPoolAndClient(t)
			tokens := signUpConfirmAndLogin(t, b, client.ClientID, "tokuser")

			tok := tokens.AccessToken
			if tt.useID {
				tok = tokens.IDToken
			}

			_, err := b.GetUser(tok)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestParity_GlobalSignOut_RejectsIDToken confirms GlobalSignOut (an access-token
// op) also rejects an ID token.
func TestParity_GlobalSignOut_RejectsIDToken(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "sigouter")

	err := b.GlobalSignOut(tokens.IDToken)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)

	// The access token must still work.
	err = b.GlobalSignOut(tokens.AccessToken)
	require.NoError(t, err)
}

// TestParity_RefreshToken_PreservesAuthTime verifies that REFRESH_TOKEN_AUTH
// preserves the original auth_time rather than resetting it on each refresh.
func TestParity_RefreshToken_PreservesAuthTime(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "authtimer")

	origClaims := decodeJWTPayload(t, tokens.AccessToken)
	origAuthTime, ok := origClaims["auth_time"].(float64)
	require.True(t, ok, "original access token must carry auth_time")

	refreshed, err := b.InitiateAuthRefreshToken(client.ClientID, tokens.RefreshToken)
	require.NoError(t, err)

	newClaims := decodeJWTPayload(t, refreshed.AccessToken)
	newAuthTime, ok := newClaims["auth_time"].(float64)
	require.True(t, ok, "refreshed access token must carry auth_time")

	assert.Equal(t, origAuthTime, newAuthTime,
		"auth_time must be preserved across refresh, not reset")
}

// TestParity_ConfirmSignUp_EmptyStoredCode verifies that an unconfirmed user with
// no stored confirmation code cannot be confirmed by an arbitrary code, while
// re-confirming an already-confirmed user remains idempotent.
func TestParity_ConfirmSignUp_EmptyStoredCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *cognitoidp.InMemoryBackend) (clientID, username, code string)
		wantErr   bool
		errTarget error
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
