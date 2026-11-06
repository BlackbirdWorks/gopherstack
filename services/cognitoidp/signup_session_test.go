package cognitoidp_test

import (
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var confirmationCode = regexp.MustCompile(`code is (\S+)`)

type signUpEnv struct {
	client   *cognitoidpsdk.Client
	sender   *recordingSender
	offset   *atomic.Int64
	clientID string
}

func newSignUpEnv(t *testing.T, flows ...types.ExplicitAuthFlowsType) *signUpEnv {
	t.Helper()

	h := newTestHandler(t)
	env := &signUpEnv{client: newTestCognitoIDPClient(t, h), sender: &recordingSender{}, offset: &atomic.Int64{}}

	h.Backend.SetMessageSender(env.sender)
	h.Backend.SetClock(func() time.Time { return time.Now().Add(time.Duration(env.offset.Load())) })

	pool, err := env.client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName:               aws.String("signup-pool"),
		AutoVerifiedAttributes: []types.VerifiedAttributeType{types.VerifiedAttributeTypeEmail},
		Policies: &types.UserPoolPolicyType{SignInPolicy: &types.SignInPolicyType{
			AllowedFirstAuthFactors: []types.AuthFactorType{types.AuthFactorTypePassword},
		}},
	})
	require.NoError(t, err)

	cl, err := env.client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: pool.UserPool.Id, ClientName: aws.String("signup-client"), ExplicitAuthFlows: flows,
	})
	require.NoError(t, err)

	env.clientID = aws.ToString(cl.UserPoolClient.ClientId)

	return env
}

func (e *signUpEnv) signUp(t *testing.T, username string) (*cognitoidpsdk.SignUpOutput, string) {
	t.Helper()

	out, err := e.client.SignUp(t.Context(), &cognitoidpsdk.SignUpInput{
		ClientId: aws.String(e.clientID), Username: aws.String(username), Password: aws.String("Passw0rd1!"),
		UserAttributes: []types.AttributeType{
			{Name: aws.String("email"), Value: aws.String(username + "@example.com")},
		},
	})
	require.NoError(t, err)

	e.sender.mu.Lock()
	defer e.sender.mu.Unlock()

	require.NotEmpty(t, e.sender.emails)

	return out, confirmationCode.FindStringSubmatch(e.sender.emails[len(e.sender.emails)-1].Text)[1]
}

func (e *signUpEnv) confirm(t *testing.T, username, code string, session *string) *cognitoidpsdk.ConfirmSignUpOutput {
	t.Helper()

	out, err := e.client.ConfirmSignUp(t.Context(), &cognitoidpsdk.ConfirmSignUpInput{
		ClientId: aws.String(
			e.clientID,
		), Username: aws.String(username), ConfirmationCode: aws.String(code), Session: session,
	})
	require.NoError(t, err)

	return out
}

func (e *signUpEnv) autoSignIn(
	t *testing.T,
	username string,
	session *string,
) (*cognitoidpsdk.InitiateAuthOutput, error) {
	t.Helper()

	return e.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(e.clientID), AuthFlow: types.AuthFlowTypeUserAuth, Session: session,
		AuthParameters: map[string]string{"USERNAME": username},
	})
}

func TestSignUpConfirmSignUpAutoSignIn(t *testing.T) {
	t.Parallel()

	userAuth := []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserAuth}

	tests := []struct {
		run  func(t *testing.T, e *signUpEnv)
		name string
	}{
		{name: "signs in with the confirm session", run: func(t *testing.T, e *signUpEnv) {
			t.Helper()

			up, code := e.signUp(t, "ann")
			require.NotEmpty(t, aws.ToString(up.Session))

			confirmed := e.confirm(t, "ann", code, up.Session)
			require.NotEmpty(t, aws.ToString(confirmed.Session))

			out, err := e.autoSignIn(t, "ann", confirmed.Session)
			require.NoError(t, err)
			require.NotNil(t, out.AuthenticationResult)
			assert.NotEmpty(t, aws.ToString(out.AuthenticationResult.AccessToken))

			_, err = e.autoSignIn(t, "ann", confirmed.Session)
			requireAPIErrorCode(t, err, "NotAuthorizedException")
		}},
		{name: "confirm without session returns none", run: func(t *testing.T, e *signUpEnv) {
			t.Helper()

			up, code := e.signUp(t, "bob")
			assert.NotEmpty(t, aws.ToString(up.Session))
			assert.Empty(t, aws.ToString(e.confirm(t, "bob", code, nil).Session))
		}},
		{name: "unconfirmed sign-up session cannot sign in", run: func(t *testing.T, e *signUpEnv) {
			t.Helper()

			up, _ := e.signUp(t, "cy")
			_, err := e.autoSignIn(t, "cy", up.Session)
			requireAPIErrorCode(t, err, "NotAuthorizedException")
		}},
		{name: "foreign session", run: func(t *testing.T, e *signUpEnv) {
			t.Helper()

			upA, codeA := e.signUp(t, "dee")
			_, codeB := e.signUp(t, "eve")
			sessA := e.confirm(t, "dee", codeA, upA.Session).Session
			e.confirm(t, "eve", codeB, nil)

			_, err := e.autoSignIn(t, "eve", sessA)
			requireAPIErrorCode(t, err, "NotAuthorizedException")
		}},
		{name: "session expires", run: func(t *testing.T, e *signUpEnv) {
			t.Helper()

			up, code := e.signUp(t, "fay")
			sess := e.confirm(t, "fay", code, up.Session).Session
			e.offset.Store(int64(10 * time.Minute))

			_, err := e.autoSignIn(t, "fay", sess)
			requireAPIErrorCode(t, err, "NotAuthorizedException")
		}},
		{name: "bogus session", run: func(t *testing.T, e *signUpEnv) {
			t.Helper()

			_, code := e.signUp(t, "gil")
			e.confirm(t, "gil", code, nil)

			_, err := e.autoSignIn(t, "gil", aws.String("nope"))
			requireAPIErrorCode(t, err, "NotAuthorizedException")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newSignUpEnv(t, userAuth...))
		})
	}
}

func TestSignUpWithoutUserAuthReturnsNoSession(t *testing.T) {
	t.Parallel()

	env := newSignUpEnv(t, types.ExplicitAuthFlowsTypeAllowUserPasswordAuth)
	up, _ := env.signUp(t, "hal")
	assert.Empty(t, aws.ToString(up.Session))
}

func TestSignUpSessionKeepsPoolMFA(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("signup-mfa"), MfaConfiguration: types.UserPoolMfaTypeOn,
		Policies: &types.UserPoolPolicyType{SignInPolicy: &types.SignInPolicyType{
			AllowedFirstAuthFactors: []types.AuthFactorType{types.AuthFactorTypePassword},
		}},
	})
	require.NoError(t, err)

	cl, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: pool.UserPool.Id, ClientName: aws.String("c"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserAuth},
	})
	require.NoError(t, err)

	cid := cl.UserPoolClient.ClientId
	up, err := client.SignUp(t.Context(), &cognitoidpsdk.SignUpInput{
		ClientId: cid, Username: aws.String("ivy"), Password: aws.String("Passw0rd1!"),
	})
	require.NoError(t, err)

	require.NoError(t, h.Backend.AdminConfirmSignUp(aws.ToString(pool.UserPool.Id), "ivy"))

	// the user was confirmed by an admin, so the confirm step is skipped: promote through ConfirmSignUp's backend hook
	sess := h.Backend.ConfirmSignUpSession(aws.ToString(cid), "ivy", aws.ToString(up.Session))
	require.NotEmpty(t, sess)

	out, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: cid, AuthFlow: types.AuthFlowTypeUserAuth, Session: aws.String(sess),
		AuthParameters: map[string]string{"USERNAME": "ivy"},
	})
	require.NoError(t, err)
	assert.Equal(t, types.ChallengeNameTypeMfaSetup, out.ChallengeName)
	assert.Nil(t, out.AuthenticationResult)
}
