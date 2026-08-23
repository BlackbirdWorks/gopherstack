package cognitoidp_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestMFASetupSession_CompletesSignInWithoutAccessToken drives the documented MFA_SETUP
// continuation flow through a real SDK client: InitiateAuth returns an MFA_SETUP challenge
// and a Session for a user who signed in with MFA required but no MFA factor configured yet
// (api_op_InitiateAuth.go: "For users who are required to setup an MFA factor before they
// can sign in ... use the session returned in this challenge ... as an input to
// AssociateSoftwareToken. Then, use the session returned by VerifySoftwareToken as an input
// to RespondToAuthChallenge ... with challenge name MFA_SETUP to complete sign-in"). Neither
// AssociateSoftwareToken nor VerifySoftwareToken is given an AccessToken anywhere in this
// test -- the whole point is that none exists yet at this stage of sign-in (gopherstack-1b07).
func TestMFASetupSession_CompletesSignInWithoutAccessToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("mfa-setup-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.SetUserPoolMfaConfig(ctx, &cognitoidpsdk.SetUserPoolMfaConfigInput{
		UserPoolId:       aws.String(poolID),
		MfaConfiguration: types.UserPoolMfaTypeOn,
	})
	require.NoError(t, err)

	appClient, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("mfa-setup-client"),
	})
	require.NoError(t, err)
	clientID := aws.ToString(appClient.UserPoolClient.ClientId)

	const username = "mfa-setup-user"
	const password = "Pass1234!"

	_, err = client.SignUp(ctx, &cognitoidpsdk.SignUpInput{
		ClientId: aws.String(clientID),
		Username: aws.String(username),
		Password: aws.String(password),
	})
	require.NoError(t, err)

	_, err = client.AdminConfirmSignUp(ctx, &cognitoidpsdk.AdminConfirmSignUpInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(username),
	})
	require.NoError(t, err)

	initResp, err := client.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
		AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		ClientId: aws.String(clientID),
		AuthParameters: map[string]string{
			"USERNAME": username,
			"PASSWORD": password,
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypeMfaSetup, initResp.ChallengeName)
	require.NotNil(t, initResp.Session)
	require.Nil(t, initResp.AuthenticationResult, "sign-in must not complete before MFA setup")

	assocResp, err := client.AssociateSoftwareToken(ctx, &cognitoidpsdk.AssociateSoftwareTokenInput{
		Session: initResp.Session,
	})
	require.NoError(t, err, "AssociateSoftwareToken with Session and no AccessToken must succeed")
	require.NotNil(t, assocResp.SecretCode)
	assert.Greater(t, len(aws.ToString(assocResp.SecretCode)), 10)

	code, err := cognitoidp.GenerateTOTPCode(aws.ToString(assocResp.SecretCode), time.Now())
	require.NoError(t, err)

	verifyResp, err := client.VerifySoftwareToken(ctx, &cognitoidpsdk.VerifySoftwareTokenInput{
		Session:  assocResp.Session,
		UserCode: aws.String(code),
	})
	require.NoError(t, err, "VerifySoftwareToken with Session and no AccessToken must succeed")
	require.Equal(t, types.VerifySoftwareTokenResponseTypeSuccess, verifyResp.Status)
	require.NotNil(t, verifyResp.Session)

	respondResp, err := client.RespondToAuthChallenge(ctx, &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId:      aws.String(clientID),
		ChallengeName: types.ChallengeNameTypeMfaSetup,
		Session:       verifyResp.Session,
	})
	require.NoError(t, err)
	require.NotNil(t, respondResp.AuthenticationResult)
	assert.NotEmpty(t, aws.ToString(respondResp.AuthenticationResult.AccessToken))
}

// TestAssociateAndVerifySoftwareToken_AccessTokenPath is a regression guard: the
// pre-existing AccessToken path (an already-authenticated user enrolling TOTP after
// sign-in, e.g. from account settings) must keep working unchanged now that Session is
// also accepted as an alternate.
func TestAssociateAndVerifySoftwareToken_AccessTokenPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("mfa-access-token-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	appClient, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("mfa-access-token-client"),
	})
	require.NoError(t, err)
	clientID := aws.ToString(appClient.UserPoolClient.ClientId)

	const username = "mfa-access-token-user"
	const password = "Pass1234!"

	_, err = client.SignUp(ctx, &cognitoidpsdk.SignUpInput{
		ClientId: aws.String(clientID),
		Username: aws.String(username),
		Password: aws.String(password),
	})
	require.NoError(t, err)

	_, err = client.AdminConfirmSignUp(ctx, &cognitoidpsdk.AdminConfirmSignUpInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(username),
	})
	require.NoError(t, err)

	initResp, err := client.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
		AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		ClientId: aws.String(clientID),
		AuthParameters: map[string]string{
			"USERNAME": username,
			"PASSWORD": password,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, initResp.AuthenticationResult)
	accessToken := aws.ToString(initResp.AuthenticationResult.AccessToken)
	require.NotEmpty(t, accessToken)

	assocResp, err := client.AssociateSoftwareToken(ctx, &cognitoidpsdk.AssociateSoftwareTokenInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err)
	require.NotNil(t, assocResp.SecretCode)
	require.Nil(t, assocResp.Session, "AccessToken path must not echo back a Session")

	code, err := cognitoidp.GenerateTOTPCode(aws.ToString(assocResp.SecretCode), time.Now())
	require.NoError(t, err)

	verifyResp, err := client.VerifySoftwareToken(ctx, &cognitoidpsdk.VerifySoftwareTokenInput{
		AccessToken: aws.String(accessToken),
		UserCode:    aws.String(code),
	})
	require.NoError(t, err)
	require.Equal(t, types.VerifySoftwareTokenResponseTypeSuccess, verifyResp.Status)
	require.Nil(t, verifyResp.Session, "AccessToken path must not echo back a Session")
}
