package cognitoidp_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/document"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const (
	passkeyRPID     = "auth.example.com"
	passkeyUsername = "passkey-user"
	passkeyPassword = "Passw0rd1!"
)

type passkeyEnv struct {
	h           *cognitoidp.Handler
	client      *cognitoidpsdk.Client
	offset      *atomic.Int64
	poolID      string
	clientID    string
	accessToken string
}

type passkeyOpts struct {
	webauthnCfg *types.WebAuthnConfigurationType
	noFactor    bool
}

func requireAPIErrorCode(t *testing.T, err error, code string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, code, apiErr.ErrorCode())
}

// newPasskeyEnv builds a pool allowing PASSWORD and WEB_AUTHN first factors, a client with
// USER_AUTH + USER_PASSWORD_AUTH, and a signed-in user.
func newPasskeyEnv(t *testing.T, o passkeyOpts) *passkeyEnv {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	offset := &atomic.Int64{}

	h.Backend.SetClock(func() time.Time { return time.Now().Add(time.Duration(offset.Load())) })

	factors := []types.AuthFactorType{types.AuthFactorTypePassword}
	if !o.noFactor {
		factors = append(factors, types.AuthFactorTypeWebAuthn)
	}

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("passkey-pool"),
		Policies: &types.UserPoolPolicyType{SignInPolicy: &types.SignInPolicyType{AllowedFirstAuthFactors: factors}},
	})
	require.NoError(t, err)

	poolID := aws.ToString(pool.UserPool.Id)

	if o.webauthnCfg != nil {
		_, err = client.SetUserPoolMfaConfig(t.Context(), &cognitoidpsdk.SetUserPoolMfaConfigInput{
			UserPoolId: aws.String(poolID), WebAuthnConfiguration: o.webauthnCfg,
		})
		require.NoError(t, err)
	}

	cl, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("passkey-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{
			types.ExplicitAuthFlowsTypeAllowUserAuth, types.ExplicitAuthFlowsTypeAllowUserPasswordAuth,
		},
	})
	require.NoError(t, err)

	clientID := aws.ToString(cl.UserPoolClient.ClientId)

	_, err = client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String(passkeyUsername),
	})
	require.NoError(t, err)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String(passkeyUsername),
		Password: aws.String(passkeyPassword), Permanent: true,
	})
	require.NoError(t, err)

	env := &passkeyEnv{h: h, client: client, offset: offset, poolID: poolID, clientID: clientID}
	env.accessToken = env.passwordAccessToken(t)

	return env
}

// passwordAccessToken signs in with a password; with pool MFA on this ends at a challenge, not tokens.
func (e *passkeyEnv) passwordAccessToken(t *testing.T) string {
	t.Helper()

	out, err := e.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(e.clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": passkeyUsername, "PASSWORD": passkeyPassword},
	})
	require.NoError(t, err)

	if out.AuthenticationResult == nil {
		return ""
	}

	return aws.ToString(out.AuthenticationResult.AccessToken)
}

func (e *passkeyEnv) startRegistration(t *testing.T) map[string]any {
	t.Helper()

	out, err := e.client.StartWebAuthnRegistration(t.Context(), &cognitoidpsdk.StartWebAuthnRegistrationInput{
		AccessToken: aws.String(e.accessToken),
	})
	require.NoError(t, err)

	var opts map[string]any

	require.NoError(t, out.CredentialCreationOptions.UnmarshalSmithyDocument(&opts))

	return opts
}

func (e *passkeyEnv) completeRegistration(t *testing.T, credential map[string]any) error {
	t.Helper()

	_, err := e.client.CompleteWebAuthnRegistration(t.Context(), &cognitoidpsdk.CompleteWebAuthnRegistrationInput{
		AccessToken: aws.String(e.accessToken), Credential: document.NewLazyDocument(credential),
	})

	return err
}

func (e *passkeyEnv) register(t *testing.T, p *softPasskey) {
	t.Helper()

	require.NoError(t, e.completeRegistration(t, p.register(t, e.startRegistration(t))))
}

func (e *passkeyEnv) startPasskeySignIn(t *testing.T, viaSelect bool) *cognitoidpsdk.InitiateAuthOutput {
	t.Helper()

	params := map[string]string{"USERNAME": passkeyUsername}
	if !viaSelect {
		params["PREFERRED_CHALLENGE"] = "WEB_AUTHN"
	}

	out, err := e.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(e.clientID), AuthFlow: types.AuthFlowTypeUserAuth, AuthParameters: params,
	})
	require.NoError(t, err)

	return out
}

func (e *passkeyEnv) respondWebAuthn(
	t *testing.T, session *string, credential string,
) (*cognitoidpsdk.RespondToAuthChallengeOutput, error) {
	t.Helper()

	return e.client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId: aws.String(e.clientID), ChallengeName: types.ChallengeNameTypeWebAuthn, Session: session,
		ChallengeResponses: map[string]string{"USERNAME": passkeyUsername, "CREDENTIAL": credential},
	})
}

func defaultPasskeyCfg() *types.WebAuthnConfigurationType {
	return &types.WebAuthnConfigurationType{
		RelyingPartyId: aws.String(passkeyRPID), UserVerification: types.UserVerificationTypePreferred,
	}
}

func TestWebAuthnRegistrationAndPasskeySignIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		viaSelect bool
	}{
		{name: "select challenge", viaSelect: true},
		{name: "preferred challenge", viaSelect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: defaultPasskeyCfg()})
			pk := newSoftPasskey(t)
			env.register(t, pk)

			listed, err := env.client.ListWebAuthnCredentials(t.Context(), &cognitoidpsdk.ListWebAuthnCredentialsInput{
				AccessToken: aws.String(env.accessToken),
			})
			require.NoError(t, err)
			require.Len(t, listed.Credentials, 1)
			assert.Equal(t, b64u(pk.credID), aws.ToString(listed.Credentials[0].CredentialId))
			assert.Equal(t, passkeyRPID, aws.ToString(listed.Credentials[0].RelyingPartyId))
			assert.Equal(t, []string{"internal"}, listed.Credentials[0].AuthenticatorTransports)

			out := env.startPasskeySignIn(t, tt.viaSelect)

			if tt.viaSelect {
				require.Equal(t, types.ChallengeNameTypeSelectChallenge, out.ChallengeName)
				assert.ElementsMatch(t,
					[]types.ChallengeNameType{types.ChallengeNameTypePassword, types.ChallengeNameTypeWebAuthn},
					out.AvailableChallenges)

				sel, selErr := env.client.RespondToAuthChallenge(
					t.Context(),
					&cognitoidpsdk.RespondToAuthChallengeInput{
						ClientId: aws.String(env.clientID), ChallengeName: types.ChallengeNameTypeSelectChallenge,
						Session:            out.Session,
						ChallengeResponses: map[string]string{"USERNAME": passkeyUsername, "ANSWER": "WEB_AUTHN"},
					},
				)
				require.NoError(t, selErr)

				out = &cognitoidpsdk.InitiateAuthOutput{
					ChallengeName:       sel.ChallengeName,
					Session:             sel.Session,
					ChallengeParameters: sel.ChallengeParameters,
				}
			}

			require.Equal(t, types.ChallengeNameTypeWebAuthn, out.ChallengeName)
			assertion := pk.assert(t, out.ChallengeParameters["CREDENTIAL_REQUEST_OPTIONS"])

			done, err := env.respondWebAuthn(t, out.Session, assertion)
			require.NoError(t, err)
			require.NotNil(t, done.AuthenticationResult)
			assert.NotEmpty(t, aws.ToString(done.AuthenticationResult.AccessToken))
		})
	}
}

func TestWebAuthnSignInRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(t *testing.T, pk *softPasskey, intruder *softPasskey) *softPasskey
		name     string
		wantCode string
	}{
		{
			name:     "foreign key signature",
			wantCode: "NotAuthorizedException",
			mutate: func(_ *testing.T, pk, intruder *softPasskey) *softPasskey {
				intruder.credID, intruder.userHandle = pk.credID, pk.userHandle

				return intruder
			},
		},
		{
			name:     "cloned authenticator sign count",
			wantCode: "NotAuthorizedException",
			mutate: func(_ *testing.T, pk, _ *softPasskey) *softPasskey {
				pk.signCount = 0

				return pk
			},
		},
		{
			name:     "foreign origin",
			wantCode: "WebAuthnOriginNotAllowedException",
			mutate: func(_ *testing.T, pk, _ *softPasskey) *softPasskey {
				pk.origin = "https://evil.example.org"

				return pk
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: defaultPasskeyCfg()})
			pk := newSoftPasskey(t)
			env.register(t, pk)

			first := env.startPasskeySignIn(t, false)
			ok, err := env.respondWebAuthn(
				t,
				first.Session,
				pk.assert(t, first.ChallengeParameters["CREDENTIAL_REQUEST_OPTIONS"]),
			)
			require.NoError(t, err)
			require.NotNil(t, ok.AuthenticationResult)

			second := env.startPasskeySignIn(t, false)
			actor := tt.mutate(t, pk, newSoftPasskey(t))
			_, err = env.respondWebAuthn(
				t,
				second.Session,
				actor.assert(t, second.ChallengeParameters["CREDENTIAL_REQUEST_OPTIONS"]),
			)
			requireAPIErrorCode(t, err, tt.wantCode)

			_, err = env.respondWebAuthn(
				t,
				second.Session,
				pk.assert(t, second.ChallengeParameters["CREDENTIAL_REQUEST_OPTIONS"]),
			)
			requireAPIErrorCode(t, err, "NotAuthorizedException")
		})
	}
}

func TestWebAuthnRegistrationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "passkeys not enabled", run: func(t *testing.T) {
			t.Helper()

			env := newPasskeyEnv(t, passkeyOpts{noFactor: true})
			_, err := env.client.StartWebAuthnRegistration(t.Context(), &cognitoidpsdk.StartWebAuthnRegistrationInput{
				AccessToken: aws.String(env.accessToken),
			})
			requireAPIErrorCode(t, err, "WebAuthnNotEnabledException")
		}},
		{name: "relying party and domain missing", run: func(t *testing.T) {
			t.Helper()

			env := newPasskeyEnv(t, passkeyOpts{})
			_, err := env.client.StartWebAuthnRegistration(t.Context(), &cognitoidpsdk.StartWebAuthnRegistrationInput{
				AccessToken: aws.String(env.accessToken),
			})
			requireAPIErrorCode(t, err, "WebAuthnConfigurationMissingException")
		}},
		{name: "relying party defaults to pool domain", run: func(t *testing.T) {
			t.Helper()

			env := newPasskeyEnv(t, passkeyOpts{})
			_, err := env.client.CreateUserPoolDomain(t.Context(), &cognitoidpsdk.CreateUserPoolDomainInput{
				UserPoolId: aws.String(env.poolID), Domain: aws.String("passkey-dom"),
			})
			require.NoError(t, err)

			opts := env.startRegistration(t)
			assert.Equal(t, "passkey-dom.auth.us-east-1.amazoncognito.com", opts["rp"].(map[string]any)["id"])
		}},
		{name: "complete without start", run: func(t *testing.T) {
			t.Helper()

			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: defaultPasskeyCfg()})
			opts := env.startRegistration(t)
			cred := newSoftPasskey(t).register(t, opts)
			require.NoError(t, env.completeRegistration(t, cred))
			requireAPIErrorCode(t, env.completeRegistration(t, cred), "WebAuthnChallengeNotFoundException")
		}},
		{name: "challenge expires", run: func(t *testing.T) {
			t.Helper()

			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: defaultPasskeyCfg()})
			cred := newSoftPasskey(t).register(t, env.startRegistration(t))
			env.offset.Store(int64(10 * time.Minute))
			requireAPIErrorCode(t, env.completeRegistration(t, cred), "WebAuthnChallengeNotFoundException")
		}},
		{name: "origin outside relying party", run: func(t *testing.T) {
			t.Helper()

			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: defaultPasskeyCfg()})
			pk := newSoftPasskey(t)
			pk.origin = "https://evil.example.org"
			err := env.completeRegistration(t, pk.register(t, env.startRegistration(t)))
			requireAPIErrorCode(t, err, "WebAuthnOriginNotAllowedException")
		}},
		{name: "credential bound to other relying party", run: func(t *testing.T) {
			t.Helper()

			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: defaultPasskeyCfg()})
			opts := env.startRegistration(t)
			opts["rp"].(map[string]any)["id"] = "other.example.net"
			pk := newSoftPasskey(t)
			pk.origin = "https://" + passkeyRPID
			requireAPIErrorCode(
				t,
				env.completeRegistration(t, pk.register(t, opts)),
				"WebAuthnRelyingPartyMismatchException",
			)
		}},
		{name: "user verification required", run: func(t *testing.T) {
			t.Helper()

			cfg := defaultPasskeyCfg()
			cfg.UserVerification = types.UserVerificationTypeRequired
			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: cfg})
			pk := newSoftPasskey(t)
			pk.verified = false
			requireAPIErrorCode(t, env.completeRegistration(t, pk.register(t, env.startRegistration(t))),
				"WebAuthnCredentialNotSupportedException")
		}},
		{name: "same passkey twice", run: func(t *testing.T) {
			t.Helper()

			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: defaultPasskeyCfg()})
			pk := newSoftPasskey(t)
			env.register(t, pk)
			requireAPIErrorCode(t, env.completeRegistration(t, pk.register(t, env.startRegistration(t))),
				"InvalidParameterException")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestWebAuthnConfigurationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  *types.WebAuthnConfigurationType
		name string
		ok   bool
	}{
		{name: "valid", cfg: defaultPasskeyCfg(), ok: true},
		{name: "bad user verification", cfg: &types.WebAuthnConfigurationType{UserVerification: "sometimes"}},
		{name: "bad factor", cfg: &types.WebAuthnConfigurationType{FactorConfiguration: "TWO"}},
		{name: "bad relying party", cfg: &types.WebAuthnConfigurationType{RelyingPartyId: aws.String("not a domain!")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			pool, err := client.CreateUserPool(
				t.Context(),
				&cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")},
			)
			require.NoError(t, err)

			_, err = client.SetUserPoolMfaConfig(t.Context(), &cognitoidpsdk.SetUserPoolMfaConfigInput{
				UserPoolId: pool.UserPool.Id, WebAuthnConfiguration: tt.cfg,
			})
			if tt.ok {
				require.NoError(t, err)

				return
			}

			requireAPIErrorCode(t, err, "InvalidParameterException")
		})
	}
}

func TestWebAuthnPasskeySatisfiesMFA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		factor    types.WebAuthnFactorConfigurationType
		verified  bool
		wantToken bool
	}{
		{
			name:     "single factor still asks for mfa",
			factor:   types.WebAuthnFactorConfigurationTypeSingleFactor,
			verified: true,
		},
		{
			name: "multi factor with user verification", verified: true, wantToken: true,
			factor: types.WebAuthnFactorConfigurationTypeMultiFactorWithUserVerification,
		},
		{
			name: "multi factor without verification still asks for mfa", verified: false,
			factor: types.WebAuthnFactorConfigurationTypeMultiFactorWithUserVerification,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultPasskeyCfg()
			cfg.FactorConfiguration = tt.factor
			env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: cfg})
			pk := newSoftPasskey(t)
			env.register(t, pk)

			_, err := env.client.SetUserPoolMfaConfig(t.Context(), &cognitoidpsdk.SetUserPoolMfaConfigInput{
				UserPoolId: aws.String(env.poolID), MfaConfiguration: types.UserPoolMfaTypeOn,
				SoftwareTokenMfaConfiguration: &types.SoftwareTokenMfaConfigType{Enabled: true},
				WebAuthnConfiguration:         cfg,
			})
			require.NoError(t, err)

			pk.verified = tt.verified
			out := env.startPasskeySignIn(t, false)
			done, err := env.respondWebAuthn(
				t,
				out.Session,
				pk.assert(t, out.ChallengeParameters["CREDENTIAL_REQUEST_OPTIONS"]),
			)

			require.NoError(t, err)

			if tt.wantToken {
				require.NotNil(t, done.AuthenticationResult)

				return
			}

			assert.Equal(t, types.ChallengeNameTypeMfaSetup, done.ChallengeName)
			assert.Nil(t, done.AuthenticationResult)
		})
	}
}

func TestWebAuthnDeleteRemovesPasskeyFactor(t *testing.T) {
	t.Parallel()

	env := newPasskeyEnv(t, passkeyOpts{webauthnCfg: defaultPasskeyCfg()})
	pk := newSoftPasskey(t)
	env.register(t, pk)

	_, err := env.client.DeleteWebAuthnCredential(t.Context(), &cognitoidpsdk.DeleteWebAuthnCredentialInput{
		AccessToken: aws.String(env.accessToken), CredentialId: aws.String(b64u(pk.credID)),
	})
	require.NoError(t, err)

	out, err := env.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(env.clientID), AuthFlow: types.AuthFlowTypeUserAuth,
		AuthParameters: map[string]string{"USERNAME": passkeyUsername},
	})
	require.NoError(t, err)
	assert.Equal(t, []types.ChallengeNameType{types.ChallengeNameTypePassword}, out.AvailableChallenges)
}
