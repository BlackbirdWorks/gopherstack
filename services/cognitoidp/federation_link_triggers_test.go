package cognitoidp_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

var errBlocked = errors.New("blocked by policy")

func newLinkBackend(t *testing.T) (*cognitoidp.InMemoryBackend, string) {
	t.Helper()

	b := newTestBackend()

	pool, err := b.CreateUserPoolWithOpts("link", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	_, err = b.CreateIdentityProviderFull(pool.ID, "Google", "Google",
		map[string]string{"client_id": "x"}, map[string]string{"email": "email"}, nil)
	require.NoError(t, err)

	return b, pool.ID
}

func userNames(t *testing.T, b *cognitoidp.InMemoryBackend, poolID string) []string {
	t.Helper()

	users, err := b.ListUsers(poolID)
	require.NoError(t, err)

	names := make([]string, 0, len(users))
	for _, u := range users {
		names = append(names, u.Username)
	}

	return names
}

func TestAdminLinkAndDisableProvider(t *testing.T) {
	t.Parallel()

	b, poolID := newLinkBackend(t)

	_, err := b.AdminCreateUserFull(
		poolID,
		"native",
		"",
		map[string]string{"email": "n@example.com"},
		"SUPPRESS",
		nil,
		false,
	)
	require.NoError(t, err)

	fed, err := b.CreateFederatedUserForTest(poolID, "Google", "g-1")
	require.NoError(t, err)
	assert.Equal(t, "Google_g-1", fed)
	assert.ElementsMatch(t, []string{"native", "Google_g-1"}, userNames(t, b, poolID))

	require.ErrorIs(t, b.AdminLinkProviderForUser(poolID, "Google_g-1", "Google", "Cognito_Subject", "g-2"),
		cognitoidp.ErrInvalidParameter, "destination must be a native user")
	require.ErrorIs(t, b.AdminLinkProviderForUser(poolID, "native", "Nope", "Cognito_Subject", "g-1"),
		cognitoidp.ErrUserPoolNotFound)

	require.NoError(t, b.AdminLinkProviderForUser(poolID, "native", "Google", "Cognito_Subject", "g-1"))
	assert.Equal(t, []string{"native"}, userNames(t, b, poolID), "the standalone federated profile is merged away")

	again, err := b.CreateFederatedUserForTest(poolID, "Google", "g-1")
	require.NoError(t, err)
	assert.Equal(t, "native", again, "a linked identity signs in as the native user")

	native, err := b.AdminGetUser(poolID, "native")
	require.NoError(t, err)
	assert.Contains(t, native.Attributes["identities"], `"providerName":"Google"`)
	assert.Contains(t, native.Attributes["identities"], `"primary":"false"`)

	_, err = b.AdminCreateUserFull(poolID, "other", "", nil, "SUPPRESS", nil, false)
	require.NoError(t, err)
	require.ErrorIs(t, b.AdminLinkProviderForUser(poolID, "other", "Google", "Cognito_Subject", "g-1"),
		cognitoidp.ErrAliasExists)

	require.NoError(t, b.AdminDisableProviderForUser(poolID, "Google", "Cognito_Subject", "g-1"))

	native, err = b.AdminGetUser(poolID, "native")
	require.NoError(t, err)
	assert.Empty(t, native.Attributes["identities"])
	assert.Empty(t, native.LinkedProviders)

	require.ErrorIs(t, b.AdminDisableProviderForUser(poolID, "Google", "Cognito_Subject", "g-1"),
		cognitoidp.ErrUserNotFound)

	fresh, err := b.CreateFederatedUserForTest(poolID, "Google", "g-1")
	require.NoError(t, err)
	assert.Equal(t, "Google_g-1", fresh, "after disabling, the identity creates a fresh federated profile")

	require.NoError(t, b.AdminDisableProviderForUser(poolID, "Google", "Cognito_Subject", "g-1"))
	assert.NotContains(t, userNames(t, b, poolID), "Google_g-1", "disabling a standalone profile deletes it")
}

func TestFederatedSignInTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fail      string
		name      string
		wantCalls []string
		wantUser  bool
	}{
		{
			name:      "first sign in",
			wantCalls: []string{"PreSignUp_ExternalProvider", "PostConfirmation_ConfirmSignUp"},
			wantUser:  true,
		},
		{
			name:      "pre sign up rejects",
			fail:      "PreSignUp_ExternalProvider",
			wantCalls: []string{"PreSignUp_ExternalProvider"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &fakeInvoker{respond: func(_ string, ev map[string]any) (map[string]any, error) {
				if ev["triggerSource"] == tt.fail {
					return nil, errBlocked
				}

				return map[string]any{}, nil
			}}

			b := newTestBackend()
			b.SetLambdaTriggerInvoker(inv)

			arn := "arn:aws:lambda:us-east-1:000000000000:function:f"
			pool, err := b.CreateUserPoolWithOpts("trig", cognitoidp.UserPoolOptions{
				LambdaConfig: map[string]any{"PreSignUp": arn, "PostConfirmation": arn},
			})
			require.NoError(t, err)

			_, err = b.CreateIdentityProviderFull(pool.ID, "Google", "Google",
				map[string]string{"client_id": "x"}, map[string]string{"email": "email"}, nil)
			require.NoError(t, err)

			_, err = b.CreateFederatedUserForTest(pool.ID, "Google", "g-1")
			if tt.fail != "" {
				require.ErrorIs(t, err, cognitoidp.ErrUserLambdaValidation)
			} else {
				require.NoError(t, err)
			}

			var got []string

			inv.mu.Lock()
			for _, c := range inv.calls {
				got = append(got, c.event["triggerSource"].(string))
			}
			inv.mu.Unlock()

			assert.Equal(t, tt.wantCalls, got)
			assert.Equal(t, tt.wantUser, len(userNames(t, b, pool.ID)) == 1)

			if tt.wantUser {
				_, err = b.CreateFederatedUserForTest(pool.ID, "Google", "g-1")
				require.NoError(t, err)
				assert.Equal(t, 2, inv.callCount(), "returning users do not re-run sign-up triggers")
			}
		})
	}
}
