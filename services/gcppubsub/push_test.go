package gcppubsub_test

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json/v2"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const pushEndpoint = "https://hook.example.test/push"

func receivedMessage(attrs map[string]string) *pubsubpb.ReceivedMessage {
	return &pubsubpb.ReceivedMessage{Message: &pubsubpb.PubsubMessage{
		Data: []byte("payload"), MessageId: "42", Attributes: attrs,
		PublishTime: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
	}}
}

func publicKey(t *testing.T, b bk) (*rsa.PublicKey, string) {
	t.Helper()

	raw, err := b.PushTokenKeySet()
	require.NoError(t, err)

	var set struct {
		Keys []struct{ Kid, N, E, Alg string } `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(raw, &set, jsonx.V1()))
	require.Len(t, set.Keys, 1)

	n, err := base64.RawURLEncoding.DecodeString(set.Keys[0].N)
	require.NoError(t, err)
	e, err := base64.RawURLEncoding.DecodeString(set.Keys[0].E)
	require.NoError(t, err)

	assert.Equal(t, "RS256", set.Keys[0].Alg)

	return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}, set.Keys[0].Kid
}

func TestPushOIDCToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		email    string
		audience string
		wantAud  string
		resolve  bool
	}{
		{
			name:     "explicit audience",
			email:    "pusher@p.iam.gserviceaccount.com",
			audience: "my-aud",
			wantAud:  "my-aud",
			resolve:  true,
		},
		{name: "default audience is endpoint", email: "pusher@p.iam.gserviceaccount.com", wantAud: pushEndpoint},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBK(t, nil)
			if tt.resolve {
				b.SetServiceAccountResolver(func(string) (string, bool) { return "113774264463038321964", true })
			}

			cfg := &pubsubpb.PushConfig{
				PushEndpoint: pushEndpoint,
				AuthenticationMethod: &pubsubpb.PushConfig_OidcToken_{
					OidcToken: &pubsubpb.PushConfig_OidcToken{ServiceAccountEmail: tt.email, Audience: tt.audience},
				},
			}

			req, err := b.BuildPushRequest(t.Context(), "sub", pushEndpoint, cfg, receivedMessage(nil))
			require.NoError(t, err)

			raw, found := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
			require.True(t, found, "bearer token present")

			pub, kid := publicKey(t, b)
			claims := jwt.MapClaims{}
			tok, err := jwt.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return pub, nil },
				jwt.WithValidMethods([]string{"RS256"}), jwt.WithTimeFunc(b.clock.Now))
			require.NoError(t, err)

			assert.Equal(t, kid, tok.Header["kid"])
			assert.Equal(t, "https://accounts.google.com", claims["iss"])
			assert.Equal(t, tt.wantAud, claims["aud"])
			assert.Equal(t, tt.email, claims["email"])
			assert.Equal(t, true, claims["email_verified"])
			assert.Equal(t, claims["sub"], claims["azp"])
			assert.InDelta(t, float64(b.clock.Now().Add(time.Hour).Unix()), claims["exp"], 0)

			if tt.resolve {
				assert.Equal(t, "113774264463038321964", claims["sub"])
			} else {
				assert.Regexp(t, `^\d{21}$`, claims["sub"])
			}
		})
	}
}

func TestPushWithoutOIDCHasNoAuthorization(t *testing.T) {
	t.Parallel()

	b, _ := newBK(t, nil)
	req, err := b.BuildPushRequest(
		t.Context(),
		"sub",
		pushEndpoint,
		&pubsubpb.PushConfig{PushEndpoint: pushEndpoint},
		receivedMessage(nil),
	)
	require.NoError(t, err)
	assert.Empty(t, req.Header.Get("Authorization"))
}

func TestPushUnwrappedHeaders(t *testing.T) {
	t.Parallel()

	attrs := map[string]string{
		"Content-Type": "application/json", "custom": "v", "Authorization": "Bearer stolen",
		"Host": "evil", "bad key": "x", "x-goog-pubsub-message-id": "forged",
	}

	tests := []struct {
		want  map[string]string
		name  string
		none  []string
		write bool
	}{
		{
			name: "metadata", write: true,
			want: map[string]string{
				"Content-Type": "application/json", "Custom": "v", "X-Goog-Pubsub-Message-Id": "42",
				"X-Goog-Pubsub-Subscription-Name": "sub", "X-Goog-Pubsub-Publish-Time": "2026-01-01T00:00:00Z",
				"X-Goog-Pubsub-Ordering-Key": "",
			},
			none: []string{"Authorization"},
		},
		{name: "no metadata", none: []string{"Custom", "Content-Type", "X-Goog-Pubsub-Message-Id"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBK(t, nil)
			cfg := &pubsubpb.PushConfig{
				PushEndpoint: pushEndpoint,
				Wrapper: &pubsubpb.PushConfig_NoWrapper_{
					NoWrapper: &pubsubpb.PushConfig_NoWrapper{WriteMetadata: tt.write},
				},
			}

			req, err := b.BuildPushRequest(t.Context(), "sub", pushEndpoint, cfg, receivedMessage(attrs))
			require.NoError(t, err)

			for k, v := range tt.want {
				assert.Equal(t, v, req.Header.Get(k), k)
			}

			for _, k := range tt.none {
				assert.Empty(t, req.Header.Get(k), k)
			}

			assert.NotEqual(t, "evil", req.Host)
		})
	}
}

func TestPushDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	var target atomic.Int32

	dst := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { target.Add(1) }))
	t.Cleanup(dst.Close)

	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, dst.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(src.Close)

	b, _ := newBK(t, nil)
	ok := b.SendPush(t.Context(), "sub", src.URL, &pubsubpb.PushConfig{PushEndpoint: src.URL}, receivedMessage(nil))

	assert.False(t, ok, "a redirect is not an acknowledgment")
	assert.Zero(t, target.Load(), "redirect target must not be contacted")
}
