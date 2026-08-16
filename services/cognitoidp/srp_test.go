package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestAdminUserSRPAuth_TwoStepFlow(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("admin-srp-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "admin-srp-client", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_ADMIN_USER_SRP_AUTH"},
	})
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "admin-srp-user", "Pass1234!", nil)
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "admin-srp-user", user.ConfirmCode))

	srpClient := newSRPTestClient(t)

	result, err := b.AdminInitiateAuthSRP(
		pool.ID, client.ClientID, "ADMIN_USER_SRP_AUTH", "admin-srp-user", srpClient.srpA(),
	)
	require.NoError(t, err)
	assert.Equal(t, "PASSWORD_VERIFIER", result.ChallengeName)

	responses := srpClient.challengeResponses(t, pool.ID, "Pass1234!", result.ChallengeParameters)
	authResult, err := b.RespondToSRPChallenge(client.ClientID, result.MFASession, responses)
	require.NoError(t, err)
	require.NotNil(t, authResult.Tokens)
	assert.NotEmpty(t, authResult.Tokens.AccessToken)
}

// TestUserSRPAuth_ForceChangePassword_IssuesNewPasswordChallenge locks in that a
// successful SRP password-claim verification for a FORCE_CHANGE_PASSWORD user issues a
// NEW_PASSWORD_REQUIRED challenge rather than tokens -- the same gate authenticate()
// applies to USER_PASSWORD_AUTH. Before this pass RespondToSRPChallenge always issued
// tokens unconditionally, bypassing this check.
func TestUserSRPAuth_ForceChangePassword_IssuesNewPasswordChallenge(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("srp-fcp-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "srp-fcp-client", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_USER_SRP_AUTH"},
	})
	require.NoError(t, err)

	_, err = b.AdminCreateUser(pool.ID, "srp-fcp-user", "TempPass1!", nil)
	require.NoError(t, err)

	srpClient := newSRPTestClient(t)

	result, err := b.InitiateAuthSRP(client.ClientID, "USER_SRP_AUTH", "srp-fcp-user", srpClient.srpA())
	require.NoError(t, err)

	responses := srpClient.challengeResponses(t, pool.ID, "TempPass1!", result.ChallengeParameters)
	authResult, err := b.RespondToSRPChallenge(client.ClientID, result.MFASession, responses)
	require.NoError(t, err)
	require.Nil(t, authResult.Tokens)
	assert.Equal(t, "NEW_PASSWORD_REQUIRED", authResult.ChallengeName)
}

// TestUserSRPAuth_TamperedSignature_Rejected proves the server actually verifies the
// zero-knowledge password-claim signature rather than trusting any client-supplied
// value: a syntactically-valid but wrong signature must fail even though the secret
// block and timestamp are correct.
func TestUserSRPAuth_TamperedSignature_Rejected(t *testing.T) {
	t.Parallel()

	b, pool, client := setupTestPoolAndClient(t)

	user, err := b.SignUp(client.ClientID, "tamper-user", "Pass1234!", nil)
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "tamper-user", user.ConfirmCode))

	srpClient := newSRPTestClient(t)

	result, err := b.InitiateAuthSRP(client.ClientID, "USER_SRP_AUTH", "tamper-user", srpClient.srpA())
	require.NoError(t, err)

	responses := srpClient.challengeResponses(t, pool.ID, "Pass1234!", result.ChallengeParameters)
	responses["PASSWORD_CLAIM_SIGNATURE"] = "dGFtcGVyZWQtc2lnbmF0dXJl" // base64("tampered-signature")

	_, err = b.RespondToSRPChallenge(client.ClientID, result.MFASession, responses)
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)
}

// TestUserSRPAuth_PlaintextInitiateAuth_Rejected locks in that the legacy
// password-based InitiateAuth path refuses USER_SRP_AUTH/ADMIN_USER_SRP_AUTH outright
// instead of silently accepting a plaintext password for a flow whose entire point is
// never sending one -- InitiateAuthSRP/AdminInitiateAuthSRP are the only valid entry
// points for these two flows.
func TestUserSRPAuth_PlaintextInitiateAuth_Rejected(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)

	user, err := b.SignUp(client.ClientID, "plaintext-srp-user", "Pass1234!", nil)
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "plaintext-srp-user", user.ConfirmCode))

	_, err = b.InitiateAuth(client.ClientID, "USER_SRP_AUTH", "plaintext-srp-user", "Pass1234!")
	require.ErrorIs(t, err, cognitoidp.ErrInvalidUserPoolConfig)
}

// TestPersistence_SRPCredentialsSurviveSnapshot locks in that a Snapshot/Restore cycle
// does not silently drop a user's SRP salt/verifier -- USER_SRP_AUTH would otherwise be
// unusable for every pre-existing user after a service restart with persistence enabled.
func TestPersistence_SRPCredentialsSurviveSnapshot(t *testing.T) {
	t.Parallel()

	b, pool, client := setupTestPoolAndClient(t)

	user, err := b.SignUp(client.ClientID, "srp-persist-user", "Pass1234!", nil)
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "srp-persist-user", user.ConfirmCode))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	srpClient := newSRPTestClient(t)

	result, err := b2.InitiateAuthSRP(client.ClientID, "USER_SRP_AUTH", "srp-persist-user", srpClient.srpA())
	require.NoError(t, err, "SRP credentials must survive Snapshot/Restore")

	responses := srpClient.challengeResponses(t, pool.ID, "Pass1234!", result.ChallengeParameters)
	authResult, err := b2.RespondToSRPChallenge(client.ClientID, result.MFASession, responses)
	require.NoError(t, err)
	require.NotNil(t, authResult.Tokens)
}
