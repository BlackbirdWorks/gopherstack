package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestSignInIdentifierResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs    map[string]string
		name     string
		login    string
		settings cognitoidp.PoolSettings
		wantOK   bool
	}{
		{name: "exact username", login: "Alice", wantOK: true},
		{name: "case differs, sensitive by default", login: "ALICE", wantOK: false},
		{
			name:     "case differs, insensitive pool",
			settings: cognitoidp.PoolSettings{UsernameConfiguration: map[string]any{"CaseSensitive": false}},
			login:    "ALICE", wantOK: true,
		},
		{
			name:     "case differs, explicitly sensitive pool",
			settings: cognitoidp.PoolSettings{UsernameConfiguration: map[string]any{"CaseSensitive": true}},
			login:    "alice", wantOK: false,
		},
		{
			name:     "email as username attribute",
			settings: cognitoidp.PoolSettings{UsernameAttributes: []string{"email"}},
			attrs:    map[string]string{"email": "alice@example.com"},
			login:    "alice@example.com", wantOK: true,
		},
		{
			name:     "unverified email alias is not a login",
			settings: cognitoidp.PoolSettings{AliasAttributes: []string{"email"}},
			attrs:    map[string]string{"email": "alice@example.com"},
			login:    "alice@example.com", wantOK: false,
		},
		{
			name:     "verified email alias",
			settings: cognitoidp.PoolSettings{AliasAttributes: []string{"email"}},
			attrs:    map[string]string{"email": "alice@example.com", "email_verified": "true"},
			login:    "alice@example.com", wantOK: true,
		},
		{
			name:     "preferred username alias",
			settings: cognitoidp.PoolSettings{AliasAttributes: []string{"preferred_username"}},
			attrs:    map[string]string{"preferred_username": "ally"},
			login:    "ally", wantOK: true,
		},
		{
			name:     "alias not enabled for the attribute",
			settings: cognitoidp.PoolSettings{AliasAttributes: []string{"preferred_username"}},
			attrs:    map[string]string{"email": "alice@example.com", "email_verified": "true"},
			login:    "alice@example.com", wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, err := b.CreateUserPoolWithOpts("resolve", cognitoidp.UserPoolOptions{Settings: tt.settings})
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "resolve-client")
			require.NoError(t, err)

			_, err = b.AdminCreateUser(pool.ID, "Alice", "Temp1234!", tt.attrs)
			require.NoError(t, err)
			require.NoError(t, b.AdminSetUserPassword(pool.ID, "Alice", lambdaTestPassword, true))

			result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", tt.login, lambdaTestPassword)

			if !tt.wantOK {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result.Tokens)

			code, err := b.ForgotPassword(client.ClientID, tt.login)
			require.NoError(t, err)
			require.NoError(t, b.ConfirmForgotPassword(client.ClientID, tt.login, code, "NewPass1234!"))
		})
	}
}

func TestCaseInsensitivePoolRejectsDuplicateUsername(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		caseSensitive bool
		wantErr       bool
	}{
		{name: "insensitive", caseSensitive: false, wantErr: true},
		{name: "sensitive", caseSensitive: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, err := b.CreateUserPoolWithOpts("dup", cognitoidp.UserPoolOptions{Settings: cognitoidp.PoolSettings{
				UsernameConfiguration: map[string]any{"CaseSensitive": tt.caseSensitive},
			}})
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "dup-client")
			require.NoError(t, err)

			_, err = b.SignUp(client.ClientID, "Bob", lambdaTestPassword, nil)
			require.NoError(t, err)

			_, err = b.SignUp(client.ClientID, "bob", lambdaTestPassword, nil)

			if tt.wantErr {
				require.ErrorIs(t, err, cognitoidp.ErrUsernameExists)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
