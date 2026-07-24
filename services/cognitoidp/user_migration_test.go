package cognitoidp_test

// user_migration_test.go exercises the UserMigration Lambda trigger added in
// user_migration.go: an unknown username on USER_PASSWORD_AUTH/ADMIN_USER_PASSWORD_AUTH
// invokes UserMigration (if configured), and a response with userAttributes creates and
// authenticates a brand-new user in one round trip.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func newUserMigrationTestPool(
	t *testing.T, inv cognitoidp.LambdaTriggerInvoker,
) (*cognitoidp.InMemoryBackend, *cognitoidp.UserPool, *cognitoidp.UserPoolClient) {
	t.Helper()

	b := newTestBackend()
	b.SetLambdaTriggerInvoker(inv)

	pool, err := b.CreateUserPoolWithOpts("user-migration-pool", cognitoidp.UserPoolOptions{
		LambdaConfig: map[string]any{
			"UserMigration": "arn:aws:lambda:us-east-1:000000000000:function:UserMigration",
		},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "user-migration-client", cognitoidp.UserPoolClientOptions{})
	require.NoError(t, err)

	return b, pool, client
}

func Test_UserMigration_UnknownUser_LambdaAccepts_CreatesAndAuthenticates(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: func(_ string, event map[string]any) (map[string]any, error) {
			req, _ := event["request"].(map[string]any)
			assert.Equal(t, lambdaTestPassword, req["password"], "the plaintext password must reach the Lambda")

			return map[string]any{
				"userAttributes":  map[string]any{"email": "legacy@x.com", "email_verified": "true"},
				"finalUserStatus": "CONFIRMED",
			}, nil
		},
	}
	b, _, client := newUserMigrationTestPool(t, inv)

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "legacy-user", lambdaTestPassword)
	require.NoError(t, err)
	require.NotNil(t, result.Tokens, "a Lambda that accepts the migration must complete sign-in with tokens")

	require.Equal(t, 1, inv.callCount())
	assert.Equal(t, "UserMigration_Authentication", inv.lastCall().event["triggerSource"])

	// The migrated user must now really exist for admin lookups.
	pool := client.UserPoolID
	user, err := b.AdminGetUser(pool, "legacy-user")
	require.NoError(t, err)
	assert.Equal(t, cognitoidp.UserStatusConfirmed, user.Status)
	assert.Equal(t, "legacy@x.com", user.Attributes["email"])
}

func Test_UserMigration_LambdaDeclines_FallsBackToUserNotFound(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: func(_ string, _ map[string]any) (map[string]any, error) {
			// No userAttributes: the Lambda declines to migrate this user (e.g. the
			// external system rejected the password).
			return map[string]any{}, nil
		},
	}
	b, _, client := newUserMigrationTestPool(t, inv)

	_, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "rejected-user", "whatever")
	require.ErrorIs(t, err, cognitoidp.ErrUserNotFound)
	require.Equal(t, 1, inv.callCount(), "the Lambda must still have been given the chance to migrate")
}

func Test_UserMigration_NotConfigured_FallsBackToUserNotFound(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t) // no SetLambdaTriggerInvoker call at all

	_, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "no-such-user", "whatever")
	require.ErrorIs(t, err, cognitoidp.ErrUserNotFound)
}

func Test_UserMigration_DoesNotFireForUSER_SRP_AUTH(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{}
	b, _, client := newUserMigrationTestPool(t, inv)

	// USER_SRP_AUTH never hands Cognito a plaintext password, so UserMigration cannot
	// apply -- the unknown-user error must surface unchanged, and the Lambda must never
	// be invoked at all.
	_, err := b.InitiateAuth(client.ClientID, "USER_SRP_AUTH", "no-such-user", "irrelevant")
	require.ErrorIs(t, err, cognitoidp.ErrUserNotFound)
	assert.Zero(t, inv.callCount())
}

func Test_UserMigration_PreventUserExistenceErrors_StillMasksWhenLambdaDeclines(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: func(_ string, _ map[string]any) (map[string]any, error) {
			return map[string]any{}, nil
		},
	}
	b := newTestBackend()
	b.SetLambdaTriggerInvoker(inv)

	pool, err := b.CreateUserPoolWithOpts("peu-migration-pool", cognitoidp.UserPoolOptions{
		LambdaConfig: map[string]any{
			"UserMigration": "arn:aws:lambda:us-east-1:000000000000:function:UserMigration",
		},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "peu-migration-client", cognitoidp.UserPoolClientOptions{
		PreventUserExistenceErrors: "ENABLED",
	})
	require.NoError(t, err)

	_, err = b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "no-such-user", "whatever")
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized, "masking must still apply after a declined migration")
}

func Test_UserMigration_FinalUserStatusResetRequired(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: func(_ string, _ map[string]any) (map[string]any, error) {
			return map[string]any{
				"userAttributes":  map[string]any{"email": "reset-me@x.com"},
				"finalUserStatus": "RESET_REQUIRED",
			}, nil
		},
	}
	b, pool, client := newUserMigrationTestPool(t, inv)

	// This first, migrating attempt must still succeed with tokens: AWS trusts the
	// Lambda's out-of-band validation of the password for exactly this one attempt.
	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "reset-me", lambdaTestPassword)
	require.NoError(t, err)
	require.NotNil(t, result.Tokens, "RESET_REQUIRED must not block the migrating attempt itself")

	// A second sign-in with the same password must now find the account gated behind
	// a password reset, since FinalUserStatus=RESET_REQUIRED marks the migrated
	// password untrusted for any *future* sign-in.
	user, err := b.AdminGetUser(pool.ID, "reset-me")
	require.NoError(t, err)
	assert.Equal(t, cognitoidp.UserStatusForceChangePassword, user.Status)

	challenge, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "reset-me", lambdaTestPassword)
	require.NoError(t, err)
	assert.Nil(t, challenge.Tokens)
	assert.Equal(t, "NEW_PASSWORD_REQUIRED", challenge.ChallengeName)
}

func Test_UserMigration_AdminInitiateAuth(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: func(_ string, _ map[string]any) (map[string]any, error) {
			return map[string]any{
				"userAttributes":  map[string]any{"email": "admin-migrated@x.com"},
				"finalUserStatus": "CONFIRMED",
			}, nil
		},
	}
	b, pool, client := newUserMigrationTestPool(t, inv)

	result, err := b.AdminInitiateAuth(
		pool.ID, client.ClientID, "ADMIN_USER_PASSWORD_AUTH", "admin-legacy-user", lambdaTestPassword,
	)
	require.NoError(t, err)
	require.NotNil(t, result.Tokens)
	require.Equal(t, 1, inv.callCount())
}
