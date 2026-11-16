package azureredis_test

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func jwtWithExp(exp *int64) string {
	enc := base64.RawURLEncoding.EncodeToString
	payload := `{}`

	if exp != nil {
		payload = fmt.Sprintf(`{"exp":%d}`, *exp)
	}

	return enc([]byte(`{"alg":"none"}`)) + "." + enc([]byte(payload)) + ".sig"
}

func TestEntraTokenExpiry(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	expiring := start.Add(time.Minute).Unix()
	past := start.Add(-time.Minute).Unix()

	tests := []struct {
		exp     *int64
		name    string
		advance time.Duration
		wantErr bool
	}{
		{name: "before_expiry", exp: &expiring, advance: 30 * time.Second},
		{name: "at_expiry", exp: &expiring, advance: time.Minute, wantErr: true},
		{name: "after_expiry", exp: &expiring, advance: time.Hour, wantErr: true},
		{name: "no_exp_claim", exp: nil, advance: 24 * time.Hour},
		{name: "already_expired", exp: &past, advance: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBackend(t)

			var offset atomic.Int64

			b.SetClock(func() time.Time { return start.Add(time.Duration(offset.Load())) })
			b.SetTokenValidator(func(token string) (string, bool) {
				return entraTestOID, strings.Count(token, ".") == 2
			})

			ep := ensure(t, b, "c1", entraOn(basicSpec()))
			c := dialTLSUser(t, ep, entraTestOID, jwtWithExp(tt.exp))

			if tt.advance > 0 {
				require.NoError(t, c.Ping(t.Context()).Err())
			}

			offset.Store(int64(tt.advance))

			err := c.Set(t.Context(), "k", "v", 0).Err()
			if tt.wantErr {
				require.Error(t, err)

				if tt.advance > 0 {
					assert.Contains(t, err.Error(), "token expired")
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestEntraTokenSurvivesFailedReauth(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	exp := start.Add(time.Minute).Unix()

	tests := []struct {
		name string
		args []any
	}{
		{name: "wrong_key", args: []any{"AUTH", "default", "not-a-key"}},
		{name: "bad_token", args: []any{"AUTH", entraTestOID, "x.y"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBackend(t)

			var offset atomic.Int64

			b.SetClock(func() time.Time { return start.Add(time.Duration(offset.Load())) })
			b.SetTokenValidator(func(token string) (string, bool) {
				return entraTestOID, strings.Count(token, ".") == 2
			})

			ep := ensure(t, b, "c1", entraOn(basicSpec()))
			conn := dialTLSUser(t, ep, entraTestOID, jwtWithExp(&exp)).Conn()

			require.NoError(t, conn.Ping(t.Context()).Err())
			require.Error(t, conn.Do(t.Context(), tt.args...).Err())

			offset.Store(int64(time.Hour))

			err := conn.Set(t.Context(), "k", "v", 0).Err()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "token expired")
		})
	}
}

func TestEntraTokenMalformedExpFailsClosed(t *testing.T) {
	t.Parallel()

	enc := base64.RawURLEncoding.EncodeToString
	tests := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{name: "string_exp", payload: `{"exp":"soon"}`, wantErr: true},
		{name: "negative_exp", payload: `{"exp":-5}`, wantErr: true},
		{name: "huge_exp", payload: `{"exp":1e30}`, wantErr: true},
		{name: "not_json", payload: `nope`, wantErr: true},
		{name: "no_exp", payload: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBackend(t)
			b.SetTokenValidator(func(token string) (string, bool) {
				return entraTestOID, strings.Count(token, ".") == 2
			})

			ep := ensure(t, b, "c1", entraOn(basicSpec()))
			token := enc([]byte(`{"alg":"none"}`)) + "." + enc([]byte(tt.payload)) + ".sig"
			err := dialTLSUser(t, ep, entraTestOID, token).Ping(t.Context()).Err()

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
