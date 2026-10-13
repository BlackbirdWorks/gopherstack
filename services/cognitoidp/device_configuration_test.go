package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestConfirmDevice_HonorsDeviceConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg        map[string]any
		name       string
		wantStatus string
		wantPrompt bool
	}{
		{name: "no device tracking", wantStatus: cognitoidp.DeviceStatusNotRemembered},
		{
			name: "always remember", cfg: map[string]any{"ChallengeRequiredOnNewDevice": true},
			wantStatus: cognitoidp.DeviceStatusRemembered,
		},
		{
			name: "remember on prompt", cfg: map[string]any{"DeviceOnlyRememberedOnUserPrompt": true},
			wantStatus: cognitoidp.DeviceStatusNotRemembered, wantPrompt: true,
		},
		{
			name: "explicit no prompt", cfg: map[string]any{"DeviceOnlyRememberedOnUserPrompt": false},
			wantStatus: cognitoidp.DeviceStatusRemembered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, err := b.CreateUserPoolWithOpts("dev", cognitoidp.UserPoolOptions{
				Settings: cognitoidp.PoolSettings{DeviceConfiguration: tt.cfg},
			})
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "dev-client")
			require.NoError(t, err)

			user, err := b.SignUp(client.ClientID, "dev-user", lambdaTestPassword, nil)
			require.NoError(t, err)
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "dev-user", user.ConfirmCode))

			auth, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "dev-user", lambdaTestPassword)
			require.NoError(t, err)

			key, prompt, err := b.ConfirmDevice(auth.Tokens.AccessToken, "", "phone")
			require.NoError(t, err)
			assert.Equal(t, tt.wantPrompt, prompt)

			device, err := b.AdminGetDevice(pool.ID, "dev-user", key)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, device.Status)
		})
	}
}
