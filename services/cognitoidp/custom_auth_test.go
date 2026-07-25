package cognitoidp_test

// custom_auth_test.go exercises the CUSTOM_AUTH state machine added in custom_auth.go:
// DefineAuthChallenge / CreateAuthChallenge / VerifyAuthChallengeResponse, driven round
// by round exactly as AWS Cognito drives a real custom-auth Lambda chain. Each test
// wires a fakeInvoker (from lambda_triggers_test.go) that dispatches on
// event["triggerSource"] so a single invoker can stand in for all three Lambdas.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// newCustomAuthTestPool creates a pool+client with all three CUSTOM_AUTH Lambda
// triggers configured (and CUSTOM_AUTH allowed in ExplicitAuthFlows), wires inv, and
// creates+confirms a user named "custom-auth-user" with lambdaTestPassword.
func newCustomAuthTestPool(
	t *testing.T, inv cognitoidp.LambdaTriggerInvoker,
) (*cognitoidp.InMemoryBackend, *cognitoidp.UserPool, *cognitoidp.UserPoolClient) {
	t.Helper()

	b := newTestBackend()
	b.SetLambdaTriggerInvoker(inv)

	pool, err := b.CreateUserPoolWithOpts("custom-auth-pool", cognitoidp.UserPoolOptions{
		LambdaConfig: map[string]any{
			"DefineAuthChallenge":         "arn:aws:lambda:us-east-1:000000000000:function:DefineAuthChallenge",
			"CreateAuthChallenge":         "arn:aws:lambda:us-east-1:000000000000:function:CreateAuthChallenge",
			"VerifyAuthChallengeResponse": "arn:aws:lambda:us-east-1:000000000000:function:VerifyAuthChallengeResponse",
		},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "custom-auth-client", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_CUSTOM_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"},
	})
	require.NoError(t, err)

	user, err := b.SignUpWithValidation(client.ClientID, "custom-auth-user", lambdaTestPassword, map[string]string{
		"email": "custom-auth-user@x.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "custom-auth-user", user.ConfirmCode))

	return b, pool, client
}

// customAuthRespond routes a fakeInvoker call to one of three handler funcs based on
// event["triggerSource"], so a single fakeInvoker can drive an entire CUSTOM_AUTH
// round trip across all three Lambdas.
func customAuthRespond(
	onDefine, onCreate, onVerify func(event map[string]any) map[string]any,
) func(string, map[string]any) (map[string]any, error) {
	return func(_ string, event map[string]any) (map[string]any, error) {
		switch event["triggerSource"] {
		case "DefineAuthChallenge_Authentication":
			return onDefine(event), nil
		case "CreateAuthChallenge_Authentication":
			return onCreate(event), nil
		case "VerifyAuthChallengeResponse_Authentication":
			return onVerify(event), nil
		default:
			return event["response"].(map[string]any), nil //nolint:forcetypeassert // test-only fake
		}
	}
}

func eventRequest(t *testing.T, event map[string]any) map[string]any {
	t.Helper()

	req, ok := event["request"].(map[string]any)
	require.True(t, ok, "event must carry a request object")

	return req
}

func Test_CustomAuth_SingleRound_IssuesTokensImmediately(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: customAuthRespond(
			func(map[string]any) map[string]any {
				return map[string]any{"challengeName": "", "issueTokens": true, "failAuthentication": false}
			},
			func(map[string]any) map[string]any {
				t.Fatal("CreateAuthChallenge must not fire")

				return nil
			},
			func(map[string]any) map[string]any {
				t.Fatal("VerifyAuthChallengeResponse must not fire")

				return nil
			},
		),
	}
	b, _, client := newCustomAuthTestPool(t, inv)

	result, err := b.InitiateAuth(client.ClientID, "CUSTOM_AUTH", "custom-auth-user", "")
	require.NoError(t, err)
	require.NotNil(t, result.Tokens, "DefineAuthChallenge.issueTokens=true must issue tokens on round 1")
	assert.Empty(t, result.MFASession)
}

func Test_CustomAuth_SingleRound_FailsImmediately(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: customAuthRespond(
			func(map[string]any) map[string]any {
				return map[string]any{"challengeName": "", "issueTokens": false, "failAuthentication": true}
			},
			nil, nil,
		),
	}
	b, _, client := newCustomAuthTestPool(t, inv)

	_, err := b.InitiateAuth(client.ClientID, "CUSTOM_AUTH", "custom-auth-user", "")
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)
}

func Test_CustomAuth_ChallengeRoundTrip_CorrectAnswer(t *testing.T) {
	t.Parallel()

	var defineCalls, createCalls, verifyCalls int

	inv := &fakeInvoker{
		respond: customAuthRespond(
			func(event map[string]any) map[string]any {
				defineCalls++
				req := eventRequest(t, event)
				session, _ := req["session"].([]any)

				if len(session) == 0 {
					// Round 1: no history yet, present a challenge.
					return map[string]any{
						"challengeName": "CAPTCHA_CHALLENGE", "issueTokens": false, "failAuthentication": false,
					}
				}

				// Round 2: history shows the answer was correct -- issue tokens.
				return map[string]any{"challengeName": "", "issueTokens": true, "failAuthentication": false}
			},
			func(event map[string]any) map[string]any {
				createCalls++
				req := eventRequest(t, event)
				assert.Equal(t, "CAPTCHA_CHALLENGE", req["challengeName"])

				return map[string]any{
					"publicChallengeParameters":  map[string]any{"captcha": "2+2="},
					"privateChallengeParameters": map[string]any{"answer": "4"},
					"challengeMetadata":          "CAPTCHA_CHALLENGE",
				}
			},
			func(event map[string]any) map[string]any {
				verifyCalls++
				req := eventRequest(t, event)
				priv, _ := req["privateChallengeParameters"].(map[string]any)
				answer, _ := req["challengeAnswer"].(string)

				return map[string]any{"answerCorrect": priv["answer"] == answer}
			},
		),
	}
	b, _, client := newCustomAuthTestPool(t, inv)

	result, err := b.InitiateAuth(client.ClientID, "CUSTOM_AUTH", "custom-auth-user", "")
	require.NoError(t, err)
	require.NotEmpty(t, result.MFASession, "round 1 must present a challenge, not issue tokens")
	assert.Equal(t, "CUSTOM_CHALLENGE", result.ChallengeName, "wire ChallengeName is always the fixed CUSTOM_CHALLENGE")
	assert.Equal(t, "2+2=", result.ChallengeParameters["captcha"], "public params must reach the client")
	assert.NotContains(t, result.ChallengeParameters, "answer", "private params must never reach the client")

	final, err := b.RespondToCustomAuthChallenge(client.ClientID, result.MFASession, "4")
	require.NoError(t, err)
	require.NotNil(t, final.Tokens, "correct answer + DefineAuthChallenge issueTokens=true must issue tokens")

	assert.Equal(t, 2, defineCalls, "DefineAuthChallenge fires once per round (initial + post-verify)")
	assert.Equal(t, 1, createCalls)
	assert.Equal(t, 1, verifyCalls)
}

func Test_CustomAuth_WrongAnswer_LambdaDecidesToFail(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: customAuthRespond(
			func(event map[string]any) map[string]any {
				req := eventRequest(t, event)
				session, _ := req["session"].([]any)

				if len(session) == 0 {
					return map[string]any{
						"challengeName":      "CUSTOM_CHALLENGE",
						"issueTokens":        false,
						"failAuthentication": false,
					}
				}

				// A wrong answer landed in the history -- the Lambda decides to fail
				// outright (matching AWS: Cognito never auto-fails on a wrong answer,
				// the Lambda always makes the call).
				last, _ := session[len(session)-1].(map[string]any)
				if last["challengeResult"] == false {
					return map[string]any{"challengeName": "", "issueTokens": false, "failAuthentication": true}
				}

				return map[string]any{"challengeName": "", "issueTokens": true, "failAuthentication": false}
			},
			func(map[string]any) map[string]any {
				return map[string]any{
					"publicChallengeParameters":  map[string]any{},
					"privateChallengeParameters": map[string]any{"answer": "correct"},
					"challengeMetadata":          "",
				}
			},
			func(event map[string]any) map[string]any {
				req := eventRequest(t, event)
				priv, _ := req["privateChallengeParameters"].(map[string]any)
				answer, _ := req["challengeAnswer"].(string)

				return map[string]any{"answerCorrect": priv["answer"] == answer}
			},
		),
	}
	b, _, client := newCustomAuthTestPool(t, inv)

	result, err := b.InitiateAuth(client.ClientID, "CUSTOM_AUTH", "custom-auth-user", "")
	require.NoError(t, err)
	require.NotEmpty(t, result.MFASession)

	_, err = b.RespondToCustomAuthChallenge(client.ClientID, result.MFASession, "wrong-answer")
	require.ErrorIs(t, err, cognitoidp.ErrNotAuthorized)
}

func Test_CustomAuth_WrongAnswer_LambdaAllowsRetry(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: customAuthRespond(
			func(event map[string]any) map[string]any {
				req := eventRequest(t, event)
				session, _ := req["session"].([]any)

				for _, raw := range session {
					entry, _ := raw.(map[string]any)
					if entry["challengeResult"] == true {
						return map[string]any{"challengeName": "", "issueTokens": true, "failAuthentication": false}
					}
				}

				// No correct answer yet (including zero rounds so far): present the
				// challenge again, however many wrong tries there have been.
				return map[string]any{
					"challengeName":      "CUSTOM_CHALLENGE",
					"issueTokens":        false,
					"failAuthentication": false,
				}
			},
			func(map[string]any) map[string]any {
				return map[string]any{
					"publicChallengeParameters":  map[string]any{},
					"privateChallengeParameters": map[string]any{"answer": "correct"},
					"challengeMetadata":          "",
				}
			},
			func(event map[string]any) map[string]any {
				req := eventRequest(t, event)
				priv, _ := req["privateChallengeParameters"].(map[string]any)
				answer, _ := req["challengeAnswer"].(string)

				return map[string]any{"answerCorrect": priv["answer"] == answer}
			},
		),
	}
	b, _, client := newCustomAuthTestPool(t, inv)

	result, err := b.InitiateAuth(client.ClientID, "CUSTOM_AUTH", "custom-auth-user", "")
	require.NoError(t, err)

	// First (wrong) answer: Lambda offers another round rather than failing outright.
	retry, err := b.RespondToCustomAuthChallenge(client.ClientID, result.MFASession, "wrong-answer")
	require.NoError(t, err)
	require.NotEmpty(t, retry.MFASession, "a wrong answer must not automatically fail the attempt")
	assert.NotEqual(t, result.MFASession, retry.MFASession, "each round mints a fresh session")

	// Second (correct) answer completes the flow.
	final, err := b.RespondToCustomAuthChallenge(client.ClientID, retry.MFASession, "correct")
	require.NoError(t, err)
	require.NotNil(t, final.Tokens)
}

func Test_CustomAuth_RequiresDefineAuthChallengeConfigured(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("no-custom-auth-pool", cognitoidp.UserPoolOptions{})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "no-custom-auth-client", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_CUSTOM_AUTH"},
	})
	require.NoError(t, err)

	user, err := b.SignUpWithValidation(client.ClientID, "lonely-user", lambdaTestPassword, map[string]string{
		"email": "lonely@x.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "lonely-user", user.ConfirmCode))

	// No invoker wired at all, matching a pool that never configured Lambda triggers.
	_, err = b.InitiateAuth(client.ClientID, "CUSTOM_AUTH", "lonely-user", "")
	require.ErrorIs(t, err, cognitoidp.ErrInvalidUserPoolConfig)
}

func Test_CustomAuth_NotAllowedUnlessInExplicitAuthFlows(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{}
	b, _, client := newLambdaTestPool(t, "DefineAuthChallenge", inv) // ExplicitAuthFlows unset (all flows allowed)

	// Restrict the client to exclude CUSTOM_AUTH.
	_, err := b.UpdateUserPoolClientWithOpts(client.UserPoolID, client.ClientID, "", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_USER_PASSWORD_AUTH"},
	})
	require.NoError(t, err)

	user, err := b.SignUpWithValidation(client.ClientID, "restricted-user", lambdaTestPassword, map[string]string{
		"email": "restricted@x.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "restricted-user", user.ConfirmCode))

	_, err = b.InitiateAuth(client.ClientID, "CUSTOM_AUTH", "restricted-user", "")
	require.ErrorIs(t, err, cognitoidp.ErrInvalidUserPoolConfig)
}

func Test_CustomAuth_AdminInitiateAuthAndAdminRespond(t *testing.T) {
	t.Parallel()

	inv := &fakeInvoker{
		respond: customAuthRespond(
			func(event map[string]any) map[string]any {
				req := eventRequest(t, event)
				session, _ := req["session"].([]any)

				if len(session) == 0 {
					return map[string]any{
						"challengeName":      "CUSTOM_CHALLENGE",
						"issueTokens":        false,
						"failAuthentication": false,
					}
				}

				return map[string]any{"challengeName": "", "issueTokens": true, "failAuthentication": false}
			},
			func(map[string]any) map[string]any {
				return map[string]any{
					"publicChallengeParameters":  map[string]any{},
					"privateChallengeParameters": map[string]any{"answer": "42"},
					"challengeMetadata":          "",
				}
			},
			func(event map[string]any) map[string]any {
				req := eventRequest(t, event)
				priv, _ := req["privateChallengeParameters"].(map[string]any)
				answer, _ := req["challengeAnswer"].(string)

				return map[string]any{"answerCorrect": priv["answer"] == answer}
			},
		),
	}
	b, pool, client := newCustomAuthTestPool(t, inv)

	result, err := b.AdminInitiateAuth(pool.ID, client.ClientID, "CUSTOM_AUTH", "custom-auth-user", "")
	require.NoError(t, err)
	require.NotEmpty(t, result.MFASession)

	final, err := b.RespondToCustomAuthChallenge(client.ClientID, result.MFASession, "42")
	require.NoError(t, err)
	require.NotNil(t, final.Tokens)
}
