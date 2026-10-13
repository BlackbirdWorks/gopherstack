package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestConfirmSignUp_ForceAliasCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		aliasAttrs   []string
		force        bool
		wantAliasErr bool
	}{
		{name: "alias taken", aliasAttrs: []string{"email"}, wantAliasErr: true},
		{name: "alias taken, forced", aliasAttrs: []string{"email"}, force: true},
		{name: "email not an alias", aliasAttrs: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, err := b.CreateUserPoolWithOpts("alias", cognitoidp.UserPoolOptions{
				AutoVerifiedAttributes: []string{"email"},
				Settings:               cognitoidp.PoolSettings{AliasAttributes: tt.aliasAttrs},
			})
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "alias-client")
			require.NoError(t, err)

			attrs := map[string]string{"email": "bob@example.com"}

			first, err := b.SignUpWithValidation(client.ClientID, "first", lambdaTestPassword, attrs)
			require.NoError(t, err)
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "first", first.ConfirmCode))

			second, err := b.SignUpWithValidation(client.ClientID, "second", lambdaTestPassword, attrs)
			require.NoError(t, err)

			err = b.ConfirmSignUpWithOptions(client.ClientID, "second", second.ConfirmCode,
				cognitoidp.ConfirmSignUpOptions{ForceAliasCreation: tt.force})

			if tt.wantAliasErr {
				require.ErrorIs(t, err, cognitoidp.ErrAliasExists)

				return
			}

			require.NoError(t, err)

			got, err := b.AdminGetUser(pool.ID, "first")
			require.NoError(t, err)

			if tt.force {
				assert.Equal(t, "false", got.Attributes["email_verified"], "alias moves to the new user")

				signedIn, authErr := b.InitiateAuth(
					client.ClientID,
					"USER_PASSWORD_AUTH",
					"bob@example.com",
					lambdaTestPassword,
				)
				require.NoError(t, authErr)
				assert.NotNil(t, signedIn.Tokens)
			} else {
				assert.Equal(t, "true", got.Attributes["email_verified"])
			}
		})
	}
}
