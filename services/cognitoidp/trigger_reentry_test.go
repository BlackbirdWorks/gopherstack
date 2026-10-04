package cognitoidp_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const reentryGuard = 10 * time.Second

// reentrantInvoker calls back into the backend from inside a trigger.
type reentrantInvoker struct {
	call func() error
	err  chan error
}

func (r *reentrantInvoker) InvokeTrigger(
	_ context.Context, _ string, event map[string]any,
) (map[string]any, error) {
	r.err <- r.call()

	return event, nil
}

func Test_TriggerReentersBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		triggerKey string
	}{
		{name: "pre authentication", triggerKey: "PreAuthentication"},
		{name: "post authentication", triggerKey: "PostAuthentication"},
		{name: "pre token generation", triggerKey: "PreTokenGeneration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &reentrantInvoker{err: make(chan error, 8)}
			b, pool, client := newLambdaTestPool(t, tt.triggerKey, inv)

			user, err := b.SignUpWithValidation(client.ClientID, "alice", lambdaTestPassword,
				map[string]string{"email": "alice@x.com"})
			require.NoError(t, err)
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "alice", user.ConfirmCode))

			inv.call = func() error {
				_, getErr := b.AdminGetUser(pool.ID, "alice")

				return getErr
			}

			done := make(chan error, 1)

			go func() {
				_, authErr := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", lambdaTestPassword)
				done <- authErr
			}()

			select {
			case authErr := <-done:
				require.NoError(t, authErr)
			case <-time.After(reentryGuard):
				t.Fatal("InitiateAuth deadlocked: trigger re-entering the backend never returned")
			}

			assert.NoError(t, <-inv.err)
		})
	}
}

var _ cognitoidp.LambdaTriggerInvoker = (*reentrantInvoker)(nil)

func Test_TriggerReentryOrderingAndRevalidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		mutate  func(b *cognitoidp.InMemoryBackend, poolID, oldAccess string)
		name    string
	}{
		{
			name: "global sign out of older token keeps new token valid",
			mutate: func(b *cognitoidp.InMemoryBackend, _, oldAccess string) {
				_ = b.GlobalSignOut(oldAccess)
			},
		},
		{
			name: "user deleted mid auth",
			mutate: func(b *cognitoidp.InMemoryBackend, poolID, _ string) {
				_ = b.AdminDeleteUser(poolID, "alice")
			},
			wantErr: cognitoidp.ErrUserNotFound,
		},
		{
			name: "pool deleted mid auth",
			mutate: func(b *cognitoidp.InMemoryBackend, poolID, _ string) {
				_ = b.DeleteUserPool(poolID)
			},
			wantErr: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &reentrantInvoker{err: make(chan error, 8)}
			b, pool, client := newLambdaTestPool(t, "PreTokenGeneration", inv)

			user, err := b.SignUpWithValidation(client.ClientID, "alice", lambdaTestPassword,
				map[string]string{"email": "alice@x.com"})
			require.NoError(t, err)
			require.NoError(t, b.ConfirmSignUp(client.ClientID, "alice", user.ConfirmCode))

			inv.call = func() error { return nil }

			first, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", lambdaTestPassword)
			require.NoError(t, err)
			<-inv.err

			inv.call = func() error {
				tt.mutate(b, pool.ID, first.Tokens.AccessToken)

				return nil
			}

			second, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", lambdaTestPassword)
			<-inv.err

			inv.call = func() error {
				_, getErr := b.AdminGetUser(pool.ID, "alice")

				return getErr
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			_, err = b.GetUser(second.Tokens.AccessToken)
			require.NoError(t, err)

			_, err = b.GetUser(first.Tokens.AccessToken)
			require.Error(t, err)

			_, err = b.InitiateAuthRefreshToken(client.ClientID, second.Tokens.RefreshToken)
			assert.NoError(t, err)
		})
	}
}

const (
	srcPreSignUp      = "PreSignUp_SignUp"
	srcPreSignUpAdmin = "PreSignUp_AdminCreateUser"
	srcPostConfirm    = "PostConfirmation_ConfirmSignUp"
	srcCustomMessage  = "CustomMessage_SignUp"
	srcMigrationAuth  = "UserMigration_Authentication"
	srcMigrationForgt = "UserMigration_ForgotPassword"
	srcDefine         = "DefineAuthChallenge_Authentication"
	srcCreate         = "CreateAuthChallenge_Authentication"
	srcVerify         = "VerifyAuthChallengeResponse_Authentication"
)

// hookInvoker answers every trigger with a passing response and runs hook (if set)
// first, with no backend lock held.
type hookInvoker struct {
	hook func(source string)
	mu   sync.Mutex
}

func (h *hookInvoker) setHook(fn func(source string)) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.hook = fn
}

func (h *hookInvoker) InvokeTrigger(
	_ context.Context, _ string, event map[string]any,
) (map[string]any, error) {
	h.mu.Lock()
	hook := h.hook
	h.mu.Unlock()

	source, _ := event["triggerSource"].(string)
	if hook != nil {
		hook(source)
	}

	req, _ := event["request"].(map[string]any)

	switch source {
	case srcDefine:
		session, _ := req["session"].([]any)
		if len(session) == 0 {
			event["response"] = map[string]any{"challengeName": "CAPTCHA"}
		} else {
			event["response"] = map[string]any{"issueTokens": true}
		}
	case srcCreate:
		event["response"] = map[string]any{
			"publicChallengeParameters":  map[string]any{"q": "x"},
			"privateChallengeParameters": map[string]any{"a": "y"},
		}
	case srcVerify:
		event["response"] = map[string]any{"answerCorrect": true}
	case srcMigrationAuth, srcMigrationForgt:
		event["response"] = map[string]any{
			"userAttributes": map[string]any{"email": "migrated@x.com"},
		}
	}

	return event, nil
}

type unlockHarness struct {
	b       *cognitoidp.InMemoryBackend
	pool    *cognitoidp.UserPool
	client  *cognitoidp.UserPoolClient
	inv     *hookInvoker
	session string
}

func newUnlockHarness(t *testing.T) *unlockHarness {
	t.Helper()

	inv := &hookInvoker{}
	b := newTestBackend()
	b.SetLambdaTriggerInvoker(inv)

	fn := "arn:aws:lambda:us-east-1:000000000000:function:"
	pool, err := b.CreateUserPoolWithOpts("unlock-pool", cognitoidp.UserPoolOptions{
		LambdaConfig: map[string]any{
			"PreSignUp":                   fn + "PreSignUp",
			"PostConfirmation":            fn + "PostConfirmation",
			"CustomMessage":               fn + "CustomMessage",
			"UserMigration":               fn + "UserMigration",
			"DefineAuthChallenge":         fn + "Define",
			"CreateAuthChallenge":         fn + "Create",
			"VerifyAuthChallengeResponse": fn + "Verify",
		},
	})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "unlock-client", cognitoidp.UserPoolClientOptions{
		ExplicitAuthFlows: []string{"ALLOW_CUSTOM_AUTH", "ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"},
	})
	require.NoError(t, err)

	user, err := b.SignUpWithValidation(client.ClientID, "alice", lambdaTestPassword,
		map[string]string{"email": "alice@x.com"})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "alice", user.ConfirmCode))

	return &unlockHarness{b: b, pool: pool, client: client, inv: inv}
}

func (h *unlockHarness) signUp(name, email string) error {
	_, err := h.b.SignUpWithValidation(h.client.ClientID, name, lambdaTestPassword,
		map[string]string{"email": email})

	return err
}

func (h *unlockHarness) adminCreate(name, email string) error {
	_, err := h.b.AdminCreateUserFull(h.pool.ID, name, "", map[string]string{"email": email}, "", nil, false)

	return err
}

func (h *unlockHarness) customAuth() error {
	res, err := h.b.InitiateAuth(h.client.ClientID, "CUSTOM_AUTH", "alice", "")
	if err != nil {
		return err
	}

	h.session = res.MFASession

	_, err = h.b.RespondToCustomAuthChallenge(h.client.ClientID, res.MFASession, "y")

	return err
}

// runGuarded runs fn and fails the test if it does not return within reentryGuard.
func runGuarded(t *testing.T, fn func() error) error {
	t.Helper()

	done := make(chan error, 1)

	go func() { done <- fn() }()

	select {
	case err := <-done:
		return err
	case <-time.After(reentryGuard):
		t.Fatal("deadlocked: trigger re-entering the backend never returned")

		return nil
	}
}

func Test_TriggerReentersBackendUnlockedFlows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run    func(h *unlockHarness) error
		name   string
		source string
	}{
		{name: "pre sign up", source: srcPreSignUp, run: func(h *unlockHarness) error {
			return h.signUp("bob", "bob@x.com")
		}},
		{name: "pre sign up admin create", source: srcPreSignUpAdmin, run: func(h *unlockHarness) error {
			return h.adminCreate("bob", "bob@x.com")
		}},
		{name: "post confirmation", source: srcPostConfirm, run: func(h *unlockHarness) error {
			user, err := h.b.SignUpWithValidation(h.client.ClientID, "bob", lambdaTestPassword,
				map[string]string{"email": "bob@x.com"})
			if err != nil {
				return err
			}

			return h.b.ConfirmSignUp(h.client.ClientID, "bob", user.ConfirmCode)
		}},
		{name: "post confirmation admin", source: srcPostConfirm, run: func(h *unlockHarness) error {
			if err := h.signUp("bob", "bob@x.com"); err != nil {
				return err
			}

			return h.b.AdminConfirmSignUp(h.pool.ID, "bob")
		}},
		{name: "custom message", source: srcCustomMessage, run: func(h *unlockHarness) error {
			_, _, err := h.b.InvokeCustomMessageTrigger(h.client.ClientID, "alice", "123456", srcCustomMessage)

			return err
		}},
		{name: "user migration", source: srcMigrationAuth, run: func(h *unlockHarness) error {
			_, err := h.b.InitiateAuth(h.client.ClientID, "USER_PASSWORD_AUTH", "ghost", lambdaTestPassword)

			return err
		}},
		{name: "user migration forgot password", source: srcMigrationForgt, run: func(h *unlockHarness) error {
			_, err := h.b.ForgotPassword(h.client.ClientID, "ghost")

			return err
		}},
		{name: "define auth challenge", source: srcDefine, run: func(h *unlockHarness) error { return h.customAuth() }},
		{name: "create auth challenge", source: srcCreate, run: func(h *unlockHarness) error { return h.customAuth() }},
		{name: "verify auth challenge", source: srcVerify, run: func(h *unlockHarness) error { return h.customAuth() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newUnlockHarness(t)
			fired := make(chan struct{}, 8)

			h.inv.setHook(func(source string) {
				if source != tt.source {
					return
				}

				_ = h.b.AdminDeleteUser(h.pool.ID, "nobody")
				fired <- struct{}{}
			})

			require.NoError(t, runGuarded(t, func() error { return tt.run(h) }))
			assert.NotEmpty(t, fired)
		})
	}
}

func Test_TriggerRaceOutcomes(t *testing.T) {
	t.Parallel()

	deletePool := func(h *unlockHarness) error { return h.b.DeleteUserPool(h.pool.ID) }
	deleteAlice := func(h *unlockHarness) error { return h.b.AdminDeleteUser(h.pool.ID, "alice") }
	createBob := func(h *unlockHarness) error { return h.signUp("bob", "inner@x.com") }
	createGhost := func(h *unlockHarness) error { return h.signUp("ghost", "inner@x.com") }

	tests := []struct {
		run         func(h *unlockHarness) error
		mutate      func(h *unlockHarness) error
		wantErr     error
		wantHookErr error
		check       func(t *testing.T, h *unlockHarness)
		name        string
		source      string
	}{
		{
			name: "sign up loses to concurrent create", source: srcPreSignUp, mutate: createBob,
			run:     func(h *unlockHarness) error { return h.signUp("bob", "outer@x.com") },
			wantErr: cognitoidp.ErrUsernameExists,
			check:   requireInnerEmail("bob"),
		},
		{
			name: "sign up after pool deleted", source: srcPreSignUp, mutate: deletePool,
			run:     func(h *unlockHarness) error { return h.signUp("bob", "outer@x.com") },
			wantErr: cognitoidp.ErrUserPoolNotFound,
		},
		{
			name: "admin create loses to concurrent sign up", source: srcPreSignUpAdmin, mutate: createBob,
			run:     func(h *unlockHarness) error { return h.adminCreate("bob", "outer@x.com") },
			wantErr: cognitoidp.ErrUsernameExists,
			check:   requireInnerEmail("bob"),
		},
		{
			name: "migration loses to concurrent create", source: srcMigrationAuth, mutate: createGhost,
			run: func(h *unlockHarness) error {
				_, err := h.b.InitiateAuth(h.client.ClientID, "USER_PASSWORD_AUTH", "ghost", lambdaTestPassword)

				return err
			},
			wantErr: cognitoidp.ErrUsernameExists,
			check:   requireInnerEmail("ghost"),
		},
		{
			name: "migration after pool deleted", source: srcMigrationAuth, mutate: deletePool,
			run: func(h *unlockHarness) error {
				_, err := h.b.InitiateAuth(h.client.ClientID, "USER_PASSWORD_AUTH", "ghost", lambdaTestPassword)

				return err
			},
			wantErr: cognitoidp.ErrUserPoolNotFound,
		},
		{
			name: "forgot migration loses to concurrent create", source: srcMigrationForgt, mutate: createGhost,
			run: func(h *unlockHarness) error {
				_, err := h.b.ForgotPassword(h.client.ClientID, "ghost")

				return err
			},
			wantErr: cognitoidp.ErrUsernameExists,
			check:   requireInnerEmail("ghost"),
		},
		{
			name: "define after user deleted", source: srcDefine, mutate: deleteAlice,
			run:     func(h *unlockHarness) error { return h.customAuth() },
			wantErr: cognitoidp.ErrUserNotFound,
		},
		{
			name: "define after pool deleted", source: srcDefine, mutate: deletePool,
			run:     func(h *unlockHarness) error { return h.customAuth() },
			wantErr: cognitoidp.ErrUserPoolNotFound,
		},
		{
			name: "create after user deleted", source: srcCreate, mutate: deleteAlice,
			run:     func(h *unlockHarness) error { return h.customAuth() },
			wantErr: cognitoidp.ErrUserNotFound,
		},
		{
			name: "verify after user deleted", source: srcVerify, mutate: deleteAlice,
			run:     func(h *unlockHarness) error { return h.customAuth() },
			wantErr: cognitoidp.ErrUserNotFound,
		},
		{
			name: "verify after pool deleted", source: srcVerify, mutate: deletePool,
			run:     func(h *unlockHarness) error { return h.customAuth() },
			wantErr: cognitoidp.ErrUserPoolNotFound,
		},
		{
			name: "verify after user disabled", source: srcVerify,
			mutate:  func(h *unlockHarness) error { return h.b.AdminDisableUser(h.pool.ID, "alice") },
			run:     func(h *unlockHarness) error { return h.customAuth() },
			wantErr: cognitoidp.ErrNotAuthorized,
		},
		{
			name: "verify session replayed mid flight", source: srcVerify,
			mutate: func(h *unlockHarness) error {
				_, err := h.b.RespondToCustomAuthChallenge(h.client.ClientID, h.session, "y")

				return err
			},
			run:         func(h *unlockHarness) error { return h.customAuth() },
			wantHookErr: cognitoidp.ErrNotAuthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newUnlockHarness(t)

			var (
				fired   atomic.Bool
				hookErr error
			)

			h.inv.setHook(func(source string) {
				if source != tt.source {
					return
				}

				if fired.CompareAndSwap(false, true) {
					hookErr = tt.mutate(h)
				}
			})

			err := runGuarded(t, func() error { return tt.run(h) })

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			if tt.wantHookErr != nil {
				require.ErrorIs(t, hookErr, tt.wantHookErr)
			}

			if tt.check != nil {
				tt.check(t, h)
			}
		})
	}
}

func requireInnerEmail(username string) func(t *testing.T, h *unlockHarness) {
	return func(t *testing.T, h *unlockHarness) {
		t.Helper()

		user, err := h.b.AdminGetUser(h.pool.ID, username)
		require.NoError(t, err)
		assert.Equal(t, "inner@x.com", user.Attributes["email"])
	}
}

var _ cognitoidp.LambdaTriggerInvoker = (*hookInvoker)(nil)
