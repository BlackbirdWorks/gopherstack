package cognitoidp_test

import (
	"context"
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
