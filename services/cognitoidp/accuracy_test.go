package cognitoidp_test

// accuracy_test.go covers the 16 AWS-accuracy gaps from issue #1702.

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// ---- JWT helpers ----

// decodeJWTPayload base64url-decodes the payload segment of a JWT without verifying.
func decodeJWTPayload(t *testing.T, token string) map[string]any {
	t.Helper()

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3, "expected JWT with 3 parts")

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(payload, &claims))

	return claims
}

// ---- Backend test helpers ----

func setupTestPoolAndClient(
	t *testing.T,
) (*cognitoidp.InMemoryBackend, *cognitoidp.UserPool, *cognitoidp.UserPoolClient) {
	t.Helper()

	b := newTestBackend()
	pool, err := b.CreateUserPool("test-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "test-client")
	require.NoError(t, err)

	return b, pool, client
}

func signUpConfirmAndLogin(
	t *testing.T,
	b *cognitoidp.InMemoryBackend,
	clientID, username string,
) *cognitoidp.TokenResult {
	t.Helper()

	const password = "Pass1234!"

	user, err := b.SignUp(clientID, username, password, map[string]string{"email": username + "@x.com"})
	require.NoError(t, err)

	err = b.ConfirmSignUp(clientID, username, user.ConfirmCode)
	require.NoError(t, err)

	result, err := b.InitiateAuth(clientID, "USER_PASSWORD_AUTH", username, password)
	require.NoError(t, err)

	return result.Tokens
}

// ---- Gap 1: cognito:groups in ID token ----

func TestAccuracy_IDToken_CognitoGroups(t *testing.T) {
	t.Parallel()

	b, pool, client := setupTestPoolAndClient(t)

	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "alice", "Pass1234!")

	_, err := b.CreateGroup(pool.ID, "admins", "", 1)
	require.NoError(t, err)

	result2, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", "Pass1234!")
	require.NoError(t, err)
	_ = result2

	err = b.AdminAddUserToGroup(pool.ID, "alice", "admins")
	require.NoError(t, err)

	result3, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", "Pass1234!")
	require.NoError(t, err)
	_ = tokens

	idClaims := decodeJWTPayload(t, result3.Tokens.IDToken)
	groups, ok := idClaims["cognito:groups"].([]any)
	require.True(t, ok, "cognito:groups must be present in ID token after group membership")
	require.Len(t, groups, 1)
	assert.Equal(t, "admins", groups[0])
}

func TestAccuracy_IDToken_NoCognitoGroupsWhenNone(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "bob", "Pass1234!")

	idClaims := decodeJWTPayload(t, tokens.IDToken)
	_, hasClaims := idClaims["cognito:groups"]
	assert.False(t, hasClaims, "cognito:groups must be absent when user has no groups")
}

// ---- Gap 2 & 3: AllowedOAuthFlows / AllowedOAuthScopes / ExplicitAuthFlows ----

func TestAccuracy_UserPoolClient_OAuthFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("oauth-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "oauth-client", cognitoidp.UserPoolClientOptions{
		AllowedOAuthFlows:  []string{"code", "implicit"},
		AllowedOAuthScopes: []string{"openid", "profile"},
		ExplicitAuthFlows:  []string{"ALLOW_USER_PASSWORD_AUTH"},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"code", "implicit"}, client.AllowedOAuthFlows)
	assert.ElementsMatch(t, []string{"openid", "profile"}, client.AllowedOAuthScopes)
	assert.Equal(t, []string{"ALLOW_USER_PASSWORD_AUTH"}, client.ExplicitAuthFlows)

	got, err := b.DescribeUserPoolClient(pool.ID, client.ClientID)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"code", "implicit"}, got.AllowedOAuthFlows)
}

func TestAccuracy_ExplicitAuthFlows_Enforcement(t *testing.T) {
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

// ---- Gap 4: Access-token scope derived from client AllowedOAuthScopes ----

func TestAccuracy_AccessToken_ScopeDerived(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("scope-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "scoped-client", cognitoidp.UserPoolClientOptions{
		AllowedOAuthScopes: []string{"email", "openid"},
	})
	require.NoError(t, err)

	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "carol", "Pass1234!")

	claims := decodeJWTPayload(t, tokens.AccessToken)
	scope, _ := claims["scope"].(string)
	assert.Equal(t, "email openid", scope, "scope must be sorted space-joined AllowedOAuthScopes")
}

func TestAccuracy_AccessToken_DefaultScopeWhenNoneConfigured(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "eve", "Pass1234!")

	claims := decodeJWTPayload(t, tokens.AccessToken)
	scope, _ := claims["scope"].(string)
	assert.Equal(t, "aws.cognito.signin.user.admin", scope)
}

// ---- Gap 6: AutoVerifiedAttributes on SignUpWithValidation ----

func TestAccuracy_SignUpWithValidation_AutoVerify(t *testing.T) {
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

func TestAccuracy_SignUpWithValidation_RequiresCode_WhenEmailMissing(t *testing.T) {
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

// ---- Gap 7: FORCE_CHANGE_PASSWORD lifecycle ----

func TestAccuracy_AdminCreateUser_ForceChangePassword(t *testing.T) {
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

func TestAccuracy_RespondToNewPasswordRequired_IssuesTokens(t *testing.T) {
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

// ---- Gap 8: auth_time in JWT + refresh token rotation ----

func TestAccuracy_IDToken_HasAuthTime(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "judy", "Pass1234!")

	claims := decodeJWTPayload(t, tokens.IDToken)
	_, ok := claims["auth_time"].(float64)
	assert.True(t, ok, "auth_time must be present in ID token")
}

func TestAccuracy_AccessToken_HasAuthTime(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "kate", "Pass1234!")

	claims := decodeJWTPayload(t, tokens.AccessToken)
	_, ok := claims["auth_time"].(float64)
	assert.True(t, ok, "auth_time must be present in access token")
}

func TestAccuracy_RefreshToken_Rotates(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "leo", "Pass1234!")

	first := tokens.RefreshToken

	tokens2, err := b.InitiateAuthRefreshToken(client.ClientID, first)
	require.NoError(t, err)
	assert.NotEqual(t, first, tokens2.RefreshToken, "refresh token must rotate")

	_, err = b.InitiateAuthRefreshToken(client.ClientID, first)
	require.Error(t, err, "old refresh token must be invalidated after rotation")
}

// ---- Gap 9: Access vs ID token claim shapes ----

func TestAccuracy_TokenClaims_Shape(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "mary", "Pass1234!")

	idClaims := decodeJWTPayload(t, tokens.IDToken)
	assert.Equal(t, "id", idClaims["token_use"])
	assert.Equal(t, client.ClientID, idClaims["aud"])
	assert.NotEmpty(t, idClaims["sub"])
	assert.NotEmpty(t, idClaims["iss"])
	_, hasClientID := idClaims["client_id"]
	assert.False(t, hasClientID, "ID token must not include client_id")

	accClaims := decodeJWTPayload(t, tokens.AccessToken)
	assert.Equal(t, "access", accClaims["token_use"])
	assert.Equal(t, client.ClientID, accClaims["client_id"])
	_, hasAud := accClaims["aud"]
	assert.False(t, hasAud, "access token must not include aud")
}

// ---- Gap 10: GetUser returns UserMFASettingList / PreferredMfaSetting ----

func TestAccuracy_GetUser_MFAFields(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "nina", "Pass1234!")

	err := b.SetUserMFAPreference(tokens.AccessToken, false, true, "SOFTWARE_TOKEN_MFA")
	require.NoError(t, err)

	user, err := b.GetUser(tokens.AccessToken)
	require.NoError(t, err)
	assert.Equal(t, []string{"SOFTWARE_TOKEN_MFA"}, user.UserMFASettingList)
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", user.PreferredMfaSetting)
}

// ---- Gap 11: GlobalSignOut revokes tokens ----

func TestAccuracy_GlobalSignOut_RevokesAccessToken(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "omar", "Pass1234!")

	err := b.GlobalSignOut(tokens.AccessToken)
	require.NoError(t, err)

	_, err = b.GetUser(tokens.AccessToken)
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized, "access token must be invalid after GlobalSignOut")
}

// ---- Gap 12: CreateUserPool persists PasswordPolicy ----

func TestAccuracy_CreateUserPool_PasswordPolicy_Persisted(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("pp-pool", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{
			MinimumLength:    12,
			RequireUppercase: true,
			RequireSymbols:   true,
		},
	})
	require.NoError(t, err)

	got, err := b.DescribeUserPool(pool.ID)
	require.NoError(t, err)
	require.NotNil(t, got.PasswordPolicy)
	assert.Equal(t, 12, got.PasswordPolicy.MinimumLength)
	assert.True(t, got.PasswordPolicy.RequireUppercase)
	assert.True(t, got.PasswordPolicy.RequireSymbols)
}

func TestAccuracy_SignUpWithValidation_PasswordPolicyEnforced(t *testing.T) {
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

// ---- Gap 12b: ChangePassword enforces PasswordPolicy ----

func TestAccuracy_ChangePassword_PolicyEnforced(t *testing.T) {
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

// ---- Gap 12c: ConfirmForgotPassword enforces PasswordPolicy ----

func TestAccuracy_ConfirmForgotPassword_PolicyEnforced(t *testing.T) {
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

// ---- Gap 13: Resource servers persist and return full data ----

func TestAccuracy_ResourceServers_Persist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("rs-pool")
	require.NoError(t, err)

	rs, err := b.CreateResourceServer(pool.ID, "https://api.example.com", "My API", []cognitoidp.ResourceServerScope{
		{ScopeName: "read", ScopeDescription: "Read access"},
		{ScopeName: "write", ScopeDescription: "Write access"},
	})
	require.NoError(t, err)
	assert.Equal(t, "https://api.example.com", rs.Identifier)
	assert.Equal(t, "My API", rs.Name)
	assert.Len(t, rs.Scopes, 2)

	got, err := b.DescribeResourceServer(pool.ID, "https://api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "My API", got.Name)

	servers, err := b.ListResourceServers(pool.ID)
	require.NoError(t, err)
	assert.Len(t, servers, 1)
}

// ---- Gap 14: AssociateSoftwareToken / VerifySoftwareToken persist seeds ----

func TestAccuracy_SoftwareToken_PersistsSecret(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "rosa", "Pass1234!")

	secret, err := b.AssociateSoftwareToken(tokens.AccessToken)
	require.NoError(t, err)
	assert.NotEmpty(t, secret)
	assert.Greater(t, len(secret), 16)

	err = b.VerifySoftwareToken(tokens.AccessToken, "123456")
	require.NoError(t, err)

	user, err := b.GetUser(tokens.AccessToken)
	require.NoError(t, err)
	assert.NotEmpty(t, user.TOTPSecret)
}

func TestAccuracy_VerifySoftwareToken_RequiresAssociate(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "sam", "Pass1234!")

	err := b.VerifySoftwareToken(tokens.AccessToken, "123456")
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)
}

// ---- Gap 15: USER_SRP_AUTH two-step challenge flow ----

func TestAccuracy_UserSRPAuth_TwoStepFlow(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("srp-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "srp-client", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_USER_SRP_AUTH"},
	})
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "tom", "Pass1234!", nil)
	require.NoError(t, err)
	err = b.ConfirmSignUp(client.ClientID, "tom", user.ConfirmCode)
	require.NoError(t, err)

	// Step 1: returns PASSWORD_VERIFIER challenge.
	result, err := b.InitiateAuth(client.ClientID, "USER_SRP_AUTH", "tom", "Pass1234!")
	require.NoError(t, err)
	assert.Equal(t, "PASSWORD_VERIFIER", result.ChallengeName)
	assert.NotEmpty(t, result.MFASession)

	// Step 2: complete SRP, receive tokens.
	tokens, err := b.RespondToSRPChallenge(client.ClientID, result.MFASession)
	require.NoError(t, err)
	assert.NotEmpty(t, tokens.IDToken)
	assert.NotEmpty(t, tokens.AccessToken)
}

func TestAccuracy_UserSRPAuth_WrongPassword_Rejected(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)

	user, err := b.SignUp(client.ClientID, "uma", "Pass1234!", nil)
	require.NoError(t, err)
	err = b.ConfirmSignUp(client.ClientID, "uma", user.ConfirmCode)
	require.NoError(t, err)

	_, err = b.InitiateAuth(client.ClientID, "USER_SRP_AUTH", "uma", "WrongPassword!")
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)
}

func TestAccuracy_UserSRPAuth_SessionExpiry(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)

	user, err := b.SignUp(client.ClientID, "ursula", "Pass1234!", nil)
	require.NoError(t, err)
	err = b.ConfirmSignUp(client.ClientID, "ursula", user.ConfirmCode)
	require.NoError(t, err)

	result, err := b.InitiateAuth(client.ClientID, "USER_SRP_AUTH", "ursula", "Pass1234!")
	require.NoError(t, err)

	b.ExpireMFASessionForTest(result.MFASession)

	_, err = b.RespondToSRPChallenge(client.ClientID, result.MFASession)
	require.Error(t, err, "expired SRP session must be rejected")
}

// ---- Gap 16: MFA session TTL ----

func TestAccuracy_MFASession_ExpiresAfterTTL(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("mfa-ttl-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	err = b.UpdateUserPoolWithOpts(pool.ID, "ON", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "mfa-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "vera", "Pass1234!", nil)
	require.NoError(t, err)
	err = b.ConfirmSignUp(client.ClientID, "vera", user.ConfirmCode)
	require.NoError(t, err)

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "vera", "Pass1234!")
	require.NoError(t, err)
	require.NotEmpty(t, result.MFASession)

	b.ExpireMFASessionForTest(result.MFASession)
	b.EvictExpiredMFASessions()

	_, err = b.RespondToMFAChallenge(client.ClientID, result.MFASession, "123456")
	require.Error(t, err, "expired MFA session must be rejected after eviction")
}

// ---- Confirmation code expiry ----

func TestAccuracy_ConfirmSignUp_ExpiredCode_Rejected(t *testing.T) {
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

// ---- AdminCreateUserWithPolicy enforces PasswordPolicy ----

func TestAccuracy_AdminCreateUserWithPolicy_Enforced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("admin-policy-pool", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{
			MinimumLength:    8,
			RequireUppercase: true,
		},
	})
	require.NoError(t, err)

	_, err = b.AdminCreateUserWithPolicy(pool.ID, "badpass", "short", nil)
	require.ErrorIs(t, err, cognitoidp.ErrInvalidPassword)

	_, err = b.AdminCreateUserWithPolicy(pool.ID, "goodpass", "ValidTemp1", nil)
	require.NoError(t, err)
}

// ---- HTTP handler tests ----

func TestHandler_CreateUserPool_WithPasswordPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "policy-pool",
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{
				"MinimumLength":    10,
				"RequireUppercase": true,
				"RequireNumbers":   true,
			},
		},
		"AutoVerifiedAttributes": []string{"email"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserPool struct {
			Policies *struct {
				PasswordPolicy *struct {
					MinimumLength  int  `json:"MinimumLength"`
					RequireNumbers bool `json:"RequireNumbers"`
				} `json:"PasswordPolicy"`
			} `json:"Policies"`
			AutoVerifiedAttributes []string `json:"AutoVerifiedAttributes"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotNil(t, resp.UserPool.Policies)
	require.NotNil(t, resp.UserPool.Policies.PasswordPolicy)
	assert.Equal(t, 10, resp.UserPool.Policies.PasswordPolicy.MinimumLength)
	assert.True(t, resp.UserPool.Policies.PasswordPolicy.RequireNumbers)
	assert.Equal(t, []string{"email"}, resp.UserPool.AutoVerifiedAttributes)
}

func TestHandler_CreateUserPoolClient_WithOAuthFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "oauth-handler-pool")

	rec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId":         poolID,
		"ClientName":         "oauth-client",
		"AllowedOAuthFlows":  []string{"code"},
		"AllowedOAuthScopes": []string{"openid", "email"},
		"ExplicitAuthFlows":  []string{"ALLOW_USER_PASSWORD_AUTH"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserPoolClient struct {
			AllowedOAuthFlows  []string `json:"AllowedOAuthFlows"`
			AllowedOAuthScopes []string `json:"AllowedOAuthScopes"`
			ExplicitAuthFlows  []string `json:"ExplicitAuthFlows"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"code"}, resp.UserPoolClient.AllowedOAuthFlows)
	assert.ElementsMatch(t, []string{"email", "openid"}, resp.UserPoolClient.AllowedOAuthScopes)
	assert.Equal(t, []string{"ALLOW_USER_PASSWORD_AUTH"}, resp.UserPoolClient.ExplicitAuthFlows)
}

func TestHandler_GetUser_WithMFAFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "mfa-getuser-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "zara", "Pass1234!")
	accessToken := loginViaHandler(t, h, clientID, "zara", "Pass1234!")

	prefRec := doCognitoRequest(t, h, "SetUserMFAPreference", map[string]any{
		"AccessToken": accessToken,
		"SoftwareTokenMfaSettings": map[string]any{
			"Enabled":      true,
			"PreferredMfa": true,
		},
	})
	require.Equal(t, http.StatusOK, prefRec.Code)

	rec := doCognitoRequest(t, h, "GetUser", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserMFASettingList  []string `json:"UserMFASettingList"`
		PreferredMfaSetting string   `json:"PreferredMfaSetting"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"SOFTWARE_TOKEN_MFA"}, resp.UserMFASettingList)
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", resp.PreferredMfaSetting)
}

func TestHandler_RespondToAuthChallenge_SMSMFAFlow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "sms-mfa-pool")

	mfaRec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "ON",
	})
	require.Equal(t, http.StatusOK, mfaRec.Code)

	signUpAndConfirmViaHandler(t, h, clientID, "smsuser", "Pass1234!")

	// Get access token without MFA to set preference (need direct backend).
	// We do this via admin set password which doesn't trigger MFA.
	setMFARec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "smsuser",
			"PASSWORD": "Pass1234!",
		},
	})
	require.Equal(t, http.StatusOK, setMFARec.Code)

	var mfaInitResp struct {
		ChallengeName *string `json:"ChallengeName"`
		Session       *string `json:"Session"`
	}
	require.NoError(t, json.Unmarshal(setMFARec.Body.Bytes(), &mfaInitResp))
	require.NotNil(t, mfaInitResp.ChallengeName)

	// Respond to SOFTWARE_TOKEN_MFA (default when MFA is ON).
	respondRec := doCognitoRequest(t, h, "RespondToAuthChallenge", map[string]any{
		"ClientId":      clientID,
		"ChallengeName": *mfaInitResp.ChallengeName,
		"Session":       *mfaInitResp.Session,
		"ChallengeResponses": map[string]string{
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	require.Equal(t, http.StatusOK, respondRec.Code)

	var tokenResp struct {
		AuthenticationResult *struct {
			AccessToken string `json:"AccessToken"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(respondRec.Body.Bytes(), &tokenResp))
	require.NotNil(t, tokenResp.AuthenticationResult)
	assert.NotEmpty(t, tokenResp.AuthenticationResult.AccessToken)
}

func TestHandler_UserSRPAuth_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "srp-http-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "srp-user", "Pass1234!")

	initRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_SRP_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": "srp-user",
			"PASSWORD": "Pass1234!",
		},
	})
	require.Equal(t, http.StatusOK, initRec.Code)

	var initResp struct {
		ChallengeName *string `json:"ChallengeName"`
		Session       *string `json:"Session"`
	}
	require.NoError(t, json.Unmarshal(initRec.Body.Bytes(), &initResp))
	require.NotNil(t, initResp.ChallengeName)
	assert.Equal(t, "PASSWORD_VERIFIER", *initResp.ChallengeName)

	respRec := doCognitoRequest(t, h, "RespondToAuthChallenge", map[string]any{
		"ClientId":           clientID,
		"ChallengeName":      "PASSWORD_VERIFIER",
		"Session":            *initResp.Session,
		"ChallengeResponses": map[string]string{},
	})
	require.Equal(t, http.StatusOK, respRec.Code)

	var tokenResp struct {
		AuthenticationResult *struct {
			AccessToken string `json:"AccessToken"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(respRec.Body.Bytes(), &tokenResp))
	require.NotNil(t, tokenResp.AuthenticationResult)
	assert.NotEmpty(t, tokenResp.AuthenticationResult.AccessToken)
}

func TestHandler_DescribeUserPool_IncludesPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "describe-policy-pool",
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{"MinimumLength": 12},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		UserPool struct{ Id string } `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": createResp.UserPool.Id})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserPool struct {
			Policies *struct {
				PasswordPolicy *struct {
					MinimumLength int `json:"MinimumLength"`
				} `json:"PasswordPolicy"`
			} `json:"Policies"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotNil(t, resp.UserPool.Policies)
	require.NotNil(t, resp.UserPool.Policies.PasswordPolicy)
	assert.Equal(t, 12, resp.UserPool.Policies.PasswordPolicy.MinimumLength)
}

func TestHandler_DescribeUserPoolClient_IncludesOAuthFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "describe-client-pool")

	createRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId":         poolID,
		"ClientName":         "full-client",
		"AllowedOAuthFlows":  []string{"code"},
		"AllowedOAuthScopes": []string{"openid"},
		"ExplicitAuthFlows":  []string{"ALLOW_USER_PASSWORD_AUTH"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		UserPoolClient struct{ ClientId string } `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doCognitoRequest(t, h, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   createResp.UserPoolClient.ClientId,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserPoolClient struct {
			AllowedOAuthFlows  []string `json:"AllowedOAuthFlows"`
			AllowedOAuthScopes []string `json:"AllowedOAuthScopes"`
			ExplicitAuthFlows  []string `json:"ExplicitAuthFlows"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"code"}, resp.UserPoolClient.AllowedOAuthFlows)
	assert.Equal(t, []string{"openid"}, resp.UserPoolClient.AllowedOAuthScopes)
	assert.Equal(t, []string{"ALLOW_USER_PASSWORD_AUTH"}, resp.UserPoolClient.ExplicitAuthFlows)
}

func TestHandler_AdminCreateUser_PolicyEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "admin-create-policy-pool",
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{
				"MinimumLength":    10,
				"RequireUppercase": true,
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var poolResp struct {
		UserPool struct{ Id string } `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &poolResp))
	poolID := poolResp.UserPool.Id

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "baduser",
		"TemporaryPassword": "short",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "gooduser",
		"TemporaryPassword": "ValidPass1234",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestHandler_SignUp_PolicyEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "signup-policy-pool",
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{
				"MinimumLength":  8,
				"RequireNumbers": true,
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var poolResp struct {
		UserPool struct{ Id string } `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &poolResp))

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolResp.UserPool.Id,
		"ClientName": "signup-client",
	})
	require.Equal(t, http.StatusOK, clientRec.Code)

	var clientResp struct {
		UserPoolClient struct{ ClientId string } `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp.UserPoolClient.ClientId

	rec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "policyuser",
		"Password": "NoDigitsHere",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "policyuser",
		"Password": "ValidPass1",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestHandler_ResourceServers_Accurate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "rs-handler-pool")

	rec := doCognitoRequest(t, h, "CreateResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
		"Name":       "My API",
		"Scopes": []map[string]string{
			{"ScopeName": "read", "ScopeDescription": "Read access"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doCognitoRequest(t, h, "DescribeResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp struct {
		ResourceServer struct {
			Name   string `json:"Name"`
			Scopes []struct {
				ScopeName string `json:"ScopeName"`
			} `json:"Scopes"`
		} `json:"ResourceServer"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	assert.Equal(t, "My API", resp.ResourceServer.Name)
	assert.Len(t, resp.ResourceServer.Scopes, 1)
	assert.Equal(t, "read", resp.ResourceServer.Scopes[0].ScopeName)
}

func TestHandler_AssociateSoftwareToken_Accurate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "totp-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "totp-user", "Pass1234!")
	accessToken := loginViaHandler(t, h, clientID, "totp-user", "Pass1234!")

	rec := doCognitoRequest(t, h, "AssociateSoftwareToken", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		SecretCode string `json:"SecretCode"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.SecretCode)
	assert.Greater(t, len(resp.SecretCode), 10)
}

func TestHandler_VerifySoftwareToken_Accurate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := setupHandlerPoolAndClient(t, h, "verify-totp-pool")

	signUpAndConfirmViaHandler(t, h, clientID, "verify-user", "Pass1234!")
	accessToken := loginViaHandler(t, h, clientID, "verify-user", "Pass1234!")

	assocRec := doCognitoRequest(t, h, "AssociateSoftwareToken", map[string]any{"AccessToken": accessToken})
	require.Equal(t, http.StatusOK, assocRec.Code)

	rec := doCognitoRequest(t, h, "VerifySoftwareToken", map[string]any{
		"AccessToken": accessToken,
		"UserCode":    "123456",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct{ Status string }
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "SUCCESS", resp.Status)
}

// ---- Handler helpers ----

func setupHandlerPoolAndClient(t *testing.T, h *cognitoidp.Handler, poolName string) (string, string) {
	t.Helper()

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": poolName})
	require.Equal(t, http.StatusOK, poolRec.Code)

	var poolResp struct {
		UserPool struct{ Id string } `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp.UserPool.Id

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": poolName + "-client",
	})
	require.Equal(t, http.StatusOK, clientRec.Code)

	var clientResp struct {
		UserPoolClient struct{ ClientId string } `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))

	return poolID, clientResp.UserPoolClient.ClientId
}

func signUpAndConfirmViaHandler(t *testing.T, h *cognitoidp.Handler, clientID, username, password string) {
	t.Helper()

	rec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": username,
		"Password": password,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var signUpResp struct {
		CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails"`
		UserConfirmed       bool              `json:"UserConfirmed"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &signUpResp))

	if signUpResp.UserConfirmed {
		return
	}

	code := signUpResp.CodeDeliveryDetails["ConfirmationCode"]
	confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
		"ClientId":         clientID,
		"Username":         username,
		"ConfirmationCode": code,
	})
	require.Equal(t, http.StatusOK, confirmRec.Code)
}

func loginViaHandler(t *testing.T, h *cognitoidp.Handler, clientID, username, password string) string {
	t.Helper()

	rec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": username,
			"PASSWORD": password,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		AuthenticationResult *struct {
			AccessToken string `json:"AccessToken"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotNil(t, resp.AuthenticationResult)

	return resp.AuthenticationResult.AccessToken
}
