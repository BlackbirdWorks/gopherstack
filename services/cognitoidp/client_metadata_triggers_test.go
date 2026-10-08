package cognitoidp_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func metadataPool(t *testing.T, inv *fakeInvoker, keys ...string) (
	*cognitoidp.InMemoryBackend, *cognitoidp.UserPool, *cognitoidp.UserPoolClient,
) {
	t.Helper()

	b := newTestBackend()
	b.SetLambdaTriggerInvoker(inv)

	cfg := map[string]any{}
	for _, k := range keys {
		cfg[k] = "arn:aws:lambda:us-east-1:000000000000:function:" + k
	}

	pool, err := b.CreateUserPoolWithOpts("md-pool", cognitoidp.UserPoolOptions{LambdaConfig: cfg})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "md-client")
	require.NoError(t, err)

	return b, pool, client
}

func confirmedMetadataUser(t *testing.T, b *cognitoidp.InMemoryBackend, clientID string) {
	t.Helper()

	user, err := b.SignUpWithValidation(clientID, "md-user", lambdaTestPassword, map[string]string{"email": "md@x.com"})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(clientID, "md-user", user.ConfirmCode))
}

// metadataOf returns request.<field> of the latest event whose triggerSource is source.
func metadataOf(t *testing.T, inv *fakeInvoker, source, field string) map[string]any {
	t.Helper()

	inv.mu.Lock()
	defer inv.mu.Unlock()

	for _, v := range slices.Backward(inv.calls) {
		ev := v.event
		if ev["triggerSource"] != source {
			continue
		}

		req, _ := ev["request"].(map[string]any)
		got, _ := req[field].(map[string]any)

		return got
	}

	require.Failf(t, "trigger not fired", "no %s event", source)

	return nil
}

func TestClientMetadataReachesTriggers(t *testing.T) {
	t.Parallel()

	meta := map[string]string{"tenant": "acme"}
	want := map[string]any{"tenant": "acme"}

	tests := []struct {
		run    func(t *testing.T, b *cognitoidp.InMemoryBackend, pool *cognitoidp.UserPool, clientID string)
		name   string
		source string
		field  string
		keys   []string
	}{
		{
			name: "pre authentication validationData", source: "PreAuthentication_Authentication",
			field: "validationData", keys: []string{"PreAuthentication"},
			run: func(t *testing.T, b *cognitoidp.InMemoryBackend, _ *cognitoidp.UserPool, clientID string) {
				t.Helper()
				_, err := b.InitiateAuth(clientID, "USER_PASSWORD_AUTH", "md-user", lambdaTestPassword, meta)
				require.NoError(t, err)
			},
		},
		{
			name: "post authentication", source: "PostAuthentication_Authentication",
			field: "clientMetadata", keys: []string{"PostAuthentication"},
			run: func(t *testing.T, b *cognitoidp.InMemoryBackend, _ *cognitoidp.UserPool, clientID string) {
				t.Helper()
				_, err := b.InitiateAuth(clientID, "USER_PASSWORD_AUTH", "md-user", lambdaTestPassword, meta)
				require.NoError(t, err)
			},
		},
		{
			name: "pre token generation on sign in", source: "TokenGeneration_Authentication",
			field: "clientMetadata", keys: []string{"PreTokenGeneration"},
			run: func(t *testing.T, b *cognitoidp.InMemoryBackend, pool *cognitoidp.UserPool, clientID string) {
				t.Helper()
				_, err := b.AdminInitiateAuth(
					pool.ID,
					clientID,
					"ADMIN_USER_PASSWORD_AUTH",
					"md-user",
					lambdaTestPassword,
					meta,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "pre token generation on refresh", source: "TokenGeneration_RefreshTokens",
			field: "clientMetadata", keys: []string{"PreTokenGeneration"},
			run: func(t *testing.T, b *cognitoidp.InMemoryBackend, _ *cognitoidp.UserPool, clientID string) {
				t.Helper()
				auth, err := b.InitiateAuth(clientID, "USER_PASSWORD_AUTH", "md-user", lambdaTestPassword)
				require.NoError(t, err)
				_, err = b.GetTokensFromRefreshToken(clientID, auth.Tokens.RefreshToken, "", meta)
				require.NoError(t, err)
			},
		},
		{
			name: "post confirmation on confirm sign up", source: "PostConfirmation_ConfirmSignUp",
			field: "clientMetadata", keys: []string{"PostConfirmation"},
			run: func(t *testing.T, b *cognitoidp.InMemoryBackend, _ *cognitoidp.UserPool, clientID string) {
				t.Helper()
				user, err := b.SignUpWithValidation(
					clientID,
					"fresh",
					lambdaTestPassword,
					map[string]string{"email": "f@x.com"},
				)
				require.NoError(t, err)
				require.NoError(t, b.ConfirmSignUp(clientID, "fresh", user.ConfirmCode, meta))
			},
		},
		{
			name: "post confirmation on admin confirm", source: "PostConfirmation_ConfirmSignUp",
			field: "clientMetadata", keys: []string{"PostConfirmation"},
			run: func(t *testing.T, b *cognitoidp.InMemoryBackend, pool *cognitoidp.UserPool, clientID string) {
				t.Helper()
				_, err := b.SignUpWithValidation(
					clientID,
					"fresh",
					lambdaTestPassword,
					map[string]string{"email": "f@x.com"},
				)
				require.NoError(t, err)
				require.NoError(t, b.AdminConfirmSignUp(pool.ID, "fresh", meta))
			},
		},
		{
			name: "post confirmation on confirm forgot password", source: "PostConfirmation_ConfirmForgotPassword",
			field: "clientMetadata", keys: []string{"PostConfirmation"},
			run: func(t *testing.T, b *cognitoidp.InMemoryBackend, _ *cognitoidp.UserPool, clientID string) {
				t.Helper()
				code, err := b.ForgotPassword(clientID, "md-user", meta)
				require.NoError(t, err)
				require.NoError(t, b.ConfirmForgotPassword(clientID, "md-user", code, "NewPass1234!", meta))
			},
		},
		{
			name: "custom message", source: "CustomMessage_ForgotPassword",
			field: "clientMetadata", keys: []string{"CustomMessage"},
			run: func(t *testing.T, b *cognitoidp.InMemoryBackend, _ *cognitoidp.UserPool, clientID string) {
				t.Helper()
				_, _, err := b.InvokeCustomMessageTrigger(
					clientID,
					"md-user",
					"123456",
					"CustomMessage_ForgotPassword",
					meta,
				)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &fakeInvoker{}
			b, pool, client := metadataPool(t, inv, tt.keys...)
			confirmedMetadataUser(t, b, client.ClientID)

			tt.run(t, b, pool, client.ClientID)

			assert.Equal(t, want, metadataOf(t, inv, tt.source, tt.field))
		})
	}
}
