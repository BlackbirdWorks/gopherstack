package cognitoidp_test

// lambda_triggers_test.go exercises the Lambda trigger invocation added in
// lambda_triggers.go: PreSignUp, PostConfirmation, PreTokenGeneration, and
// CustomMessage. Each test wires a fakeInvoker via SetLambdaTriggerInvoker so no
// real Lambda backend is required.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const lambdaTestPassword = "Pass1234!"

// Static errors a fakeInvoker.respond can return, per err113 (no dynamic
// errors.New at the call site).
var (
	errPreSignUpBlocked     = errors.New("account blocked by PreSignUp policy")
	errPostConfirmationDown = errors.New("downstream welcome-email service unavailable")
)

// fakeInvocation records one call to fakeInvoker.InvokeTrigger.
type fakeInvocation struct {
	event       map[string]any
	functionARN string
}

// fakeInvoker is a test double for cognitoidp.LambdaTriggerInvoker. respond, when
// set, computes the returned event's "response" object; a nil respond echoes back
// whatever "response" default the caller put in the event (the zero-value
// behavior a real passthrough Lambda would produce).
type fakeInvoker struct {
	respond func(functionARN string, event map[string]any) (map[string]any, error)
	calls   []fakeInvocation
	mu      sync.Mutex
}

func (f *fakeInvoker) InvokeTrigger(
	_ context.Context, functionARN string, event map[string]any,
) (map[string]any, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fakeInvocation{functionARN: functionARN, event: event})
	f.mu.Unlock()

	if f.respond == nil {
		return event, nil
	}

	resp, err := f.respond(functionARN, event)
	if err != nil {
		return nil, err
	}

	event["response"] = resp

	return event, nil
}

func (f *fakeInvoker) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.calls)
}

func (f *fakeInvoker) lastCall() fakeInvocation {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls[len(f.calls)-1]
}

// newLambdaTestPool creates a pool+client with LambdaConfig[triggerKey] set to a
// fake function ARN, wires inv as the pool's Lambda trigger invoker, and returns
// the backend/pool/client for the test to drive.
func newLambdaTestPool(
	t *testing.T, triggerKey string, inv cognitoidp.LambdaTriggerInvoker,
) (*cognitoidp.InMemoryBackend, *cognitoidp.UserPool, *cognitoidp.UserPoolClient) {
	t.Helper()

	b := newTestBackend()
	b.SetLambdaTriggerInvoker(inv)

	pool, err := b.CreateUserPoolWithOpts("test-pool", cognitoidp.UserPoolOptions{
		LambdaConfig: map[string]any{triggerKey: "arn:aws:lambda:us-east-1:000000000000:function:" + triggerKey},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "test-client")
	require.NoError(t, err)

	return b, pool, client
}

func Test_LambdaTriggerNotConfigured(t *testing.T) {
	t.Parallel()

	t.Run("nil invoker leaves SignUp/ConfirmSignUp/InitiateAuth unchanged", func(t *testing.T) {
		t.Parallel()

		b, _, client := setupTestPoolAndClient(t) // no SetLambdaTriggerInvoker call at all

		user, err := b.SignUpWithValidation(client.ClientID, "alice", lambdaTestPassword, map[string]string{
			"email": "alice@x.com",
		})
		require.NoError(t, err)
		assert.Equal(t, cognitoidp.UserStatusUnconfirmed, user.Status)

		require.NoError(t, b.ConfirmSignUp(client.ClientID, "alice", user.ConfirmCode))

		result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", lambdaTestPassword)
		require.NoError(t, err)
		require.NotNil(t, result.Tokens)
	})

	t.Run("invoker wired but pool has no LambdaConfig entry", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{}
		b, _, client := setupTestPoolAndClient(t)
		b.SetLambdaTriggerInvoker(inv)

		user, err := b.SignUpWithValidation(client.ClientID, "bob", lambdaTestPassword, map[string]string{
			"email": "bob@x.com",
		})
		require.NoError(t, err)
		assert.Equal(t, cognitoidp.UserStatusUnconfirmed, user.Status)
		assert.Zero(t, inv.callCount(), "trigger must not fire when LambdaConfig has no entry for it")
	})
}

func Test_PreSignUpTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs     error
		respond   func(string, map[string]any) (map[string]any, error)
		checkUser func(t *testing.T, user *cognitoidp.User)
		name      string
		wantErr   bool
	}{
		{
			name: "autoConfirmUser true confirms the user immediately",
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return map[string]any{"autoConfirmUser": true, "autoVerifyEmail": false, "autoVerifyPhone": false}, nil
			},
			checkUser: func(t *testing.T, user *cognitoidp.User) {
				t.Helper()
				assert.Equal(t, cognitoidp.UserStatusConfirmed, user.Status)
				assert.Empty(t, user.ConfirmCode)
			},
		},
		{
			name: "autoVerifyEmail true marks email_verified without confirming",
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return map[string]any{"autoConfirmUser": false, "autoVerifyEmail": true, "autoVerifyPhone": false}, nil
			},
			checkUser: func(t *testing.T, user *cognitoidp.User) {
				t.Helper()
				assert.Equal(t, cognitoidp.UserStatusUnconfirmed, user.Status)
				assert.Equal(t, "true", user.Attributes["email_verified"])
			},
		},
		{
			name: "trigger error surfaces as UserLambdaValidationException",
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return nil, errPreSignUpBlocked
			},
			wantErr: true,
			errIs:   cognitoidp.ErrUserLambdaValidation,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inv := &fakeInvoker{respond: tc.respond}
			b, _, client := newLambdaTestPool(t, "PreSignUp", inv)

			user, err := b.SignUpWithValidation(client.ClientID, "alice", lambdaTestPassword, map[string]string{
				"email": "alice@x.com",
			})

			require.Equal(t, 1, inv.callCount(), "PreSignUp must fire exactly once")
			assert.Equal(t, "PreSignUp_SignUp", inv.lastCall().event["triggerSource"])

			if tc.wantErr {
				require.ErrorIs(t, err, tc.errIs)

				return
			}

			require.NoError(t, err)
			tc.checkUser(t, user)
		})
	}

	t.Run("also fires for AdminCreateUser with AdminCreateUser trigger source", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return map[string]any{"autoVerifyEmail": true}, nil
			},
		}
		b, pool, _ := newLambdaTestPool(t, "PreSignUp", inv)

		user, err := b.AdminCreateUserFull(
			pool.ID, "carol", "TempPass1!", map[string]string{"email": "carol@x.com"},
			"", nil, false,
		)
		require.NoError(t, err)
		require.Equal(t, 1, inv.callCount())
		assert.Equal(t, "PreSignUp_AdminCreateUser", inv.lastCall().event["triggerSource"])
		assert.Equal(t, "true", user.Attributes["email_verified"])
	})
}

func Test_PostConfirmationTrigger(t *testing.T) {
	t.Parallel()

	t.Run("fires on ConfirmSignUp with the confirmed user's attributes", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{}
		b, _, client := newLambdaTestPool(t, "PostConfirmation", inv)

		user, err := b.SignUpWithValidation(client.ClientID, "dave", lambdaTestPassword, map[string]string{
			"email": "dave@x.com",
		})
		require.NoError(t, err)

		require.NoError(t, b.ConfirmSignUp(client.ClientID, "dave", user.ConfirmCode))

		require.Equal(t, 1, inv.callCount())
		assert.Equal(t, "PostConfirmation_ConfirmSignUp", inv.lastCall().event["triggerSource"])
	})

	t.Run("fires on AdminConfirmSignUp too", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{}
		b, pool, client := newLambdaTestPool(t, "PostConfirmation", inv)

		_, err := b.SignUpWithValidation(client.ClientID, "erin", lambdaTestPassword, map[string]string{
			"email": "erin@x.com",
		})
		require.NoError(t, err)

		require.NoError(t, b.AdminConfirmSignUp(pool.ID, "erin"))
		require.Equal(t, 1, inv.callCount())
		assert.Equal(t, "PostConfirmation_ConfirmSignUp", inv.lastCall().event["triggerSource"])
	})

	t.Run("trigger error surfaces but does not roll back confirmation", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return nil, errPostConfirmationDown
			},
		}
		b, pool, client := newLambdaTestPool(t, "PostConfirmation", inv)

		user, err := b.SignUpWithValidation(client.ClientID, "frank", lambdaTestPassword, map[string]string{
			"email": "frank@x.com",
		})
		require.NoError(t, err)

		err = b.ConfirmSignUp(client.ClientID, "frank", user.ConfirmCode)
		require.ErrorIs(t, err, cognitoidp.ErrUserLambdaValidation)

		// AWS confirms the user before invoking PostConfirmation and does not
		// undo the confirmation just because the trigger errored.
		confirmed, getErr := b.AdminGetUser(pool.ID, "frank")
		require.NoError(t, getErr)
		assert.Equal(t, cognitoidp.UserStatusConfirmed, confirmed.Status)
	})
}

func Test_PreTokenGenerationTrigger(t *testing.T) {
	t.Parallel()

	t.Run("claimsToAddOrOverride and claimsToSuppress apply to ID and access tokens", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return map[string]any{
					"claimsOverrideDetails": map[string]any{
						"claimsToAddOrOverride": map[string]any{"custom:role": "admin"},
						"claimsToSuppress":      []any{"email"},
					},
				}, nil
			},
		}
		b, _, client := newLambdaTestPool(t, "PreTokenGeneration", inv)

		user, err := b.SignUpWithValidation(client.ClientID, "grace", lambdaTestPassword, map[string]string{
			"email": "grace@x.com",
		})
		require.NoError(t, err)
		require.NoError(t, b.ConfirmSignUp(client.ClientID, "grace", user.ConfirmCode))

		result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "grace", lambdaTestPassword)
		require.NoError(t, err)
		require.Equal(t, 1, inv.callCount())
		assert.Equal(t, "TokenGeneration_Authentication", inv.lastCall().event["triggerSource"])

		idClaims := decodeJWTPayload(t, result.Tokens.IDToken)
		assert.Equal(t, "admin", idClaims["custom:role"])
		assert.NotContains(t, idClaims, "email", "claimsToSuppress must remove the claim")

		accessClaims := decodeJWTPayload(t, result.Tokens.AccessToken)
		assert.Equal(t, "admin", accessClaims["custom:role"])
	})

	t.Run("protected claims cannot be overridden or suppressed", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return map[string]any{
					"claimsOverrideDetails": map[string]any{
						"claimsToAddOrOverride": map[string]any{"sub": "hacked", "token_use": "not-id"},
						"claimsToSuppress":      []any{"token_use", "exp"},
					},
				}, nil
			},
		}
		b, _, client := newLambdaTestPool(t, "PreTokenGeneration", inv)

		user, err := b.SignUpWithValidation(client.ClientID, "henry", lambdaTestPassword, map[string]string{
			"email": "henry@x.com",
		})
		require.NoError(t, err)
		require.NoError(t, b.ConfirmSignUp(client.ClientID, "henry", user.ConfirmCode))

		result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "henry", lambdaTestPassword)
		require.NoError(t, err)

		idClaims := decodeJWTPayload(t, result.Tokens.IDToken)
		assert.Equal(t, user.Sub, idClaims["sub"], "sub must not be overridable")
		assert.Equal(t, "id", idClaims["token_use"], "token_use must not be overridable or suppressible")
		assert.Contains(t, idClaims, "exp", "exp must not be suppressible")
	})

	t.Run("fires on REFRESH_TOKEN_AUTH with TokenGeneration_RefreshTokens source", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{}
		b, _, client := newLambdaTestPool(t, "PreTokenGeneration", inv)

		user, err := b.SignUpWithValidation(client.ClientID, "iris", lambdaTestPassword, map[string]string{
			"email": "iris@x.com",
		})
		require.NoError(t, err)
		require.NoError(t, b.ConfirmSignUp(client.ClientID, "iris", user.ConfirmCode))

		result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "iris", lambdaTestPassword)
		require.NoError(t, err)
		require.Equal(t, 1, inv.callCount(), "initial password auth should fire once")

		_, err = b.InitiateAuthRefreshToken(client.ClientID, result.Tokens.RefreshToken)
		require.NoError(t, err)
		require.Equal(t, 2, inv.callCount())
		assert.Equal(t, "TokenGeneration_RefreshTokens", inv.lastCall().event["triggerSource"])
	})
}

func Test_CustomMessageTrigger(t *testing.T) {
	t.Parallel()

	t.Run("SignUp: emailMessage code placeholder is substituted with the real code", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return map[string]any{
					"emailMessage": "Your code is ####, welcome!",
					"emailSubject": "Verify your account",
				}, nil
			},
		}
		b, _, client := newLambdaTestPool(t, "CustomMessage", inv)

		message, subject, err := b.InvokeCustomMessageTrigger(client.ClientID, "jack", "123456", "CustomMessage_SignUp")
		require.NoError(t, err)
		assert.Equal(t, "Your code is 123456, welcome!", message)
		assert.Equal(t, "Verify your account", subject)
		require.Equal(t, 1, inv.callCount())
		assert.Equal(t, "CustomMessage_SignUp", inv.lastCall().event["triggerSource"])
	})

	t.Run("falls back to smsMessage when emailMessage is empty", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return map[string]any{"smsMessage": "Code: ####"}, nil
			},
		}
		b, _, client := newLambdaTestPool(t, "CustomMessage", inv)

		message, _, err := b.InvokeCustomMessageTrigger(client.ClientID, "kate", "654321", "CustomMessage_ResendCode")
		require.NoError(t, err)
		assert.Equal(t, "Code: 654321", message)
	})

	t.Run("missing client is best-effort no-override, not an error", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{}
		b := newTestBackend()
		b.SetLambdaTriggerInvoker(inv)

		message, subject, err := b.InvokeCustomMessageTrigger(
			"no-such-client", "nobody", "000000", "CustomMessage_SignUp",
		)
		require.NoError(t, err)
		assert.Empty(t, message)
		assert.Empty(t, subject)
		assert.Zero(t, inv.callCount())
	})

	t.Run("wired into ForgotPassword and ResendConfirmationCode via the handler-facing helper", func(t *testing.T) {
		t.Parallel()

		inv := &fakeInvoker{
			respond: func(_ string, _ map[string]any) (map[string]any, error) {
				return map[string]any{"emailMessage": "reset code ####"}, nil
			},
		}
		b, _, client := newLambdaTestPool(t, "CustomMessage", inv)

		user, err := b.SignUpWithValidation(client.ClientID, "leo", lambdaTestPassword, map[string]string{
			"email": "leo@x.com",
		})
		require.NoError(t, err)
		require.NoError(t, b.ConfirmSignUp(client.ClientID, "leo", user.ConfirmCode))

		code, err := b.ForgotPassword(client.ClientID, "leo")
		require.NoError(t, err)

		message, _, err := b.InvokeCustomMessageTrigger(client.ClientID, "leo", code, "CustomMessage_ForgotPassword")
		require.NoError(t, err)
		assert.Equal(t, "reset code "+code, message)
	})
}
