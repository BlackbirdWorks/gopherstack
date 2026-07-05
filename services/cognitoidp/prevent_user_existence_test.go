package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// Test_InitiateAuth_PreventUserExistenceErrors proves that a non-admin InitiateAuth call for
// an unknown username reveals UserNotFoundException only when the app client's
// PreventUserExistenceErrors is "LEGACY" (the default when unset); "ENABLED" must mask that
// distinction behind the exact same NotAuthorizedException a wrong password produces, so a
// caller cannot enumerate valid usernames by inspecting the error.
func Test_InitiateAuth_PreventUserExistenceErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		wantErr     error
		name        string
		clientOpts  cognitoidp.UserPoolClientOptions
		wantSameMsg bool
	}{
		{
			name:       "default (unset) is LEGACY: reveals UserNotFoundException",
			clientOpts: cognitoidp.UserPoolClientOptions{},
			wantErr:    cognitoidp.ErrUserNotFound,
		},
		{
			name:       "explicit LEGACY reveals UserNotFoundException",
			clientOpts: cognitoidp.UserPoolClientOptions{PreventUserExistenceErrors: "LEGACY"},
			wantErr:    cognitoidp.ErrUserNotFound,
		},
		{
			name:        "ENABLED masks as NotAuthorizedException matching a wrong password",
			clientOpts:  cognitoidp.UserPoolClientOptions{PreventUserExistenceErrors: "ENABLED"},
			wantErr:     cognitoidp.ErrNotAuthorized,
			wantSameMsg: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPoolWithOpts("peu-pool-"+sanitizeTestName(tc.name), cognitoidp.UserPoolOptions{})
			require.NoError(t, err)

			client, err := b.CreateUserPoolClientWithOpts(pool.ID, "peu-client", tc.clientOpts)
			require.NoError(t, err)

			existingUsername := "realuser-" + sanitizeTestName(tc.name)
			_ = signUpConfirmAndLogin(t, b, client.ClientID, existingUsername)

			_, unknownErr := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "no-such-user", "Pass1234!")
			require.ErrorIs(t, unknownErr, tc.wantErr)

			if tc.wantSameMsg {
				_, wrongPasswordErr := b.InitiateAuth(
					client.ClientID, "USER_PASSWORD_AUTH", existingUsername, "wrong-password-not-real",
				)
				require.ErrorIs(t, wrongPasswordErr, cognitoidp.ErrNotAuthorized)
				assert.Equal(
					t, wrongPasswordErr.Error(), unknownErr.Error(),
					"ENABLED must make the unknown-user and wrong-password errors indistinguishable",
				)
			}
		})
	}
}

// Test_AdminInitiateAuth_AlwaysRevealsUserNotFound proves PreventUserExistenceErrors=ENABLED
// only affects the non-admin InitiateAuth path: AdminInitiateAuth callers already hold
// admin-level AWS credentials, so AWS does not mask UserNotFoundException there.
func Test_AdminInitiateAuth_AlwaysRevealsUserNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("peu-admin-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "peu-admin-client", cognitoidp.UserPoolClientOptions{
		PreventUserExistenceErrors: "ENABLED",
	})
	require.NoError(t, err)

	_, err = b.AdminInitiateAuth(pool.ID, client.ClientID, "ADMIN_USER_PASSWORD_AUTH", "no-such-admin-user", "x")
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidp.ErrUserNotFound)
}

// Test_CreateUserPoolClient_PreventUserExistenceErrors_Validation locks in AWS's enum
// validation: only "ENABLED" and "LEGACY" (or omitted, which defaults to "LEGACY") are legal.
func Test_CreateUserPoolClient_PreventUserExistenceErrors_Validation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("peu-validate-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	_, err = b.CreateUserPoolClientWithOpts(pool.ID, "bad-client", cognitoidp.UserPoolClientOptions{
		PreventUserExistenceErrors: "NOT_A_REAL_VALUE",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidp.ErrInvalidParameter)
}

// Test_UpdateUserPoolClient_PreventUserExistenceErrors proves the setting can be flipped
// after creation and that InitiateAuth's masking behavior follows the update.
func Test_UpdateUserPoolClient_PreventUserExistenceErrors(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("peu-update-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "peu-update-client", cognitoidp.UserPoolClientOptions{})
	require.NoError(t, err)

	_, err = b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "ghost", "Pass1234!")
	require.ErrorIs(t, err, cognitoidp.ErrUserNotFound, "new client defaults to LEGACY")

	updated, err := b.UpdateUserPoolClientWithOpts(pool.ID, client.ClientID, "", cognitoidp.UserPoolClientOptions{
		PreventUserExistenceErrors: "ENABLED",
	})
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", updated.PreventUserExistenceErrors)

	_, err = b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "ghost", "Pass1234!")
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized, "after enabling, unknown users must be masked")
}
