package apigateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// policyInvoker allows only callers whose "auth" header (or token) is in allowed, for allowedARN.
type policyInvoker struct {
	allowed    map[string]bool
	allowedARN string
	calls      atomic.Int32
}

func (p *policyInvoker) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	p.calls.Add(1)

	var ev AuthorizerEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, http.StatusInternalServerError, err
	}

	who := ev.AuthorizationToken
	if who == "" {
		who = ev.Headers["auth"]
	}

	effect := "Deny"
	if p.allowed[who] {
		effect = "Allow"
	}

	res := p.allowedARN
	if res == "" {
		res = ev.MethodArn
	}

	body, _ := json.Marshal(AuthorizerResponse{
		PrincipalID: who,
		PolicyDocument: &PolicyDocument{Statement: []PolicyStatement{
			{Effect: effect, Action: "execute-api:Invoke", Resource: res},
		}},
	})

	return body, http.StatusOK, nil
}

type authCall struct {
	headers map[string]string
	path    string
	want    int
}

func runAuthCalls(t *testing.T, h *Handler, auth *Authorizer, calls []authCall) {
	t.Helper()

	cfg := &DeploymentConfig{Authorizers: map[string]*Authorizer{"auth1": auth}}

	for i, c := range calls {
		path := c.path
		if path == "" {
			path = "/a"
		}

		req := httptest.NewRequest(http.MethodGet, "/restapis/api1/prod/_user_request_"+path, nil)
		for k, v := range c.headers {
			req.Header.Set(k, v)
		}

		rec := httptest.NewRecorder()
		h.runAuthorizer(t.Context(), rec, req, "api1", "prod", cfg, "auth1")
		assert.Equal(t, c.want, rec.Code, "call %d", i)
	}
}

func TestRunAuthorizer_CacheKeyAndDecisions(t *testing.T) {
	t.Parallel()

	reqAuth := func(ttl int) *Authorizer {
		return &Authorizer{
			Type: "REQUEST", IdentitySource: "method.request.header.Auth",
			AuthorizerResultTTLInSeconds: ttl,
		}
	}
	tokAuth := func(ttl int, expr string) *Authorizer {
		return &Authorizer{
			Type: "TOKEN", IdentitySource: "method.request.header.Auth",
			IdentityValidationExpression: expr, AuthorizerResultTTLInSeconds: ttl,
		}
	}
	armA := "arn:aws:execute-api:us-east-1:000000000000:api1/prod/GET/a"

	tests := []struct {
		auth       *Authorizer
		name       string
		allowedARN string
		calls      []authCall
		wantInvoke int32
	}{
		{
			name: "request_callers_isolated_with_cache",
			auth: reqAuth(300),
			calls: []authCall{
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
				{headers: map[string]string{"Auth": "bob"}, want: http.StatusForbidden},
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
				{headers: map[string]string{"Auth": "bob"}, want: http.StatusForbidden},
			},
			wantInvoke: 2,
		},
		{
			name: "request_cached_deny_does_not_hide_other_identity_allow",
			auth: reqAuth(300),
			calls: []authCall{
				{headers: map[string]string{"Auth": "bob"}, want: http.StatusForbidden},
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
			},
			wantInvoke: 2,
		},
		{
			name: "token_callers_isolated_with_cache",
			auth: tokAuth(300, ""),
			calls: []authCall{
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
				{headers: map[string]string{"Auth": "bob"}, want: http.StatusForbidden},
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
			},
			wantInvoke: 2,
		},
		{
			name: "request_ttl_zero_invokes_every_time",
			auth: reqAuth(0),
			calls: []authCall{
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
			},
			wantInvoke: 3,
		},
		{
			name: "token_ttl_zero_invokes_every_time",
			auth: tokAuth(0, ""),
			calls: []authCall{
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
			},
			wantInvoke: 2,
		},
		{
			name:       "cached_allow_for_one_method_arn_does_not_allow_another",
			auth:       reqAuth(300),
			allowedARN: armA,
			calls: []authCall{
				{headers: map[string]string{"Auth": "alice"}, path: "/a", want: http.StatusOK},
				{headers: map[string]string{"Auth": "alice"}, path: "/b", want: http.StatusForbidden},
				{headers: map[string]string{"Auth": "alice"}, path: "/a", want: http.StatusOK},
			},
			wantInvoke: 1,
		},
		{
			name: "request_missing_identity_source_401_without_invoke",
			auth: reqAuth(300),
			calls: []authCall{
				{want: http.StatusUnauthorized},
			},
			wantInvoke: 0,
		},
		{
			name: "request_missing_identity_source_ttl_zero_invokes",
			auth: reqAuth(0),
			calls: []authCall{
				{want: http.StatusForbidden},
			},
			wantInvoke: 1,
		},
		{
			name: "token_missing_token_401_without_invoke",
			auth: tokAuth(300, ""),
			calls: []authCall{
				{want: http.StatusUnauthorized},
			},
			wantInvoke: 0,
		},
		{
			name: "token_missing_token_ttl_zero_401_without_invoke",
			auth: tokAuth(0, ""),
			calls: []authCall{
				{want: http.StatusUnauthorized},
			},
			wantInvoke: 0,
		},
		{
			name: "token_validation_mismatch_401_without_invoke",
			auth: tokAuth(300, "^Bearer [a-z]+$"),
			calls: []authCall{
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusUnauthorized},
			},
			wantInvoke: 0,
		},
		{
			name: "token_validation_match_invokes",
			auth: tokAuth(300, "^[a-z]+$"),
			calls: []authCall{
				{headers: map[string]string{"Auth": "alice"}, want: http.StatusOK},
			},
			wantInvoke: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &policyInvoker{allowed: map[string]bool{"alice": true}, allowedARN: tt.allowedARN}
			h := NewHandler(NewInMemoryBackend())
			h.SetLambdaInvoker(inv)

			runAuthCalls(t, h, tt.auth, tt.calls)
			assert.Equal(t, tt.wantInvoke, inv.calls.Load())
		})
	}
}

func TestRunAuthorizer_UnauthorizedBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth *Authorizer
		name string
	}{
		{name: "token", auth: &Authorizer{Type: "TOKEN", IdentitySource: "method.request.header.Auth"}},
		{name: "request", auth: &Authorizer{
			Type: "REQUEST", IdentitySource: "method.request.header.Auth", AuthorizerResultTTLInSeconds: 60,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryBackend())
			cfg := &DeploymentConfig{Authorizers: map[string]*Authorizer{"auth1": tt.auth}}
			req := httptest.NewRequest(http.MethodGet, "/restapis/api1/prod/_user_request_/a", nil)
			rec := httptest.NewRecorder()

			require.True(t, h.runAuthorizer(t.Context(), rec, req, "api1", "prod", cfg, "auth1"))
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.JSONEq(t, `{"message":"Unauthorized"}`, rec.Body.String())
		})
	}
}

func TestAuthorizerCache_TTLAndBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ttl      time.Duration
		wait     time.Duration
		wantHit  bool
		maxEntry int
	}{
		{name: "hit_before_expiry", ttl: time.Minute, wait: time.Minute - time.Second, wantHit: true, maxEntry: 8},
		{name: "miss_after_expiry", ttl: time.Minute, wait: time.Minute + time.Second, wantHit: false, maxEntry: 8},
		{name: "zero_ttl_never_cached", ttl: 0, wait: 0, wantHit: false, maxEntry: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				c := newAuthorizerCacheWithMaxEntries(tt.maxEntry)
				c.set("k", testAllowPolicy(), tt.ttl)
				time.Sleep(tt.wait)

				_, hit := c.get("k")
				assert.Equal(t, tt.wantHit, hit)
			})
		})
	}
}

func TestAuthorizerCache_EvictsExpiredBeforeLRU(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newAuthorizerCacheWithMaxEntries(3)
		c.set("old1", testAllowPolicy(), time.Second)
		c.set("old2", testAllowPolicy(), time.Second)
		c.set("live", testAllowPolicy(), time.Hour)
		time.Sleep(2 * time.Second)

		c.set("new", testAllowPolicy(), time.Hour)

		assert.Len(t, c.entries, 2)
		_, hit := c.get("live")
		assert.True(t, hit, "live entry must survive expired-entry eviction")
	})
}
