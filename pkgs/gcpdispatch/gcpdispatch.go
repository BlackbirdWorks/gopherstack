// Package gcpdispatch holds helpers shared by the GCP emulators that deliver scheduled HTTP requests.
package gcpdispatch

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"google.golang.org/genproto/googleapis/rpc/code"
)

const (
	tokenTTL = time.Hour
	issuer   = "https://accounts.google.com"
)

// HTTPToRPC maps an HTTP status code to the closest google.rpc.Code.
func HTTPToRPC(sc int) code.Code {
	switch sc {
	case http.StatusBadRequest:
		return code.Code_INVALID_ARGUMENT
	case http.StatusUnauthorized:
		return code.Code_UNAUTHENTICATED
	case http.StatusForbidden:
		return code.Code_PERMISSION_DENIED
	case http.StatusNotFound:
		return code.Code_NOT_FOUND
	case http.StatusConflict:
		return code.Code_ABORTED
	case http.StatusTooManyRequests:
		return code.Code_RESOURCE_EXHAUSTED
	case http.StatusInternalServerError:
		return code.Code_INTERNAL
	case http.StatusNotImplemented:
		return code.Code_UNIMPLEMENTED
	case http.StatusServiceUnavailable:
		return code.Code_UNAVAILABLE
	case http.StatusGatewayTimeout:
		return code.Code_DEADLINE_EXCEEDED
	}

	if sc >= http.StatusInternalServerError {
		return code.Code_UNAVAILABLE
	}

	return code.Code_UNKNOWN
}

// Backoff is the delay after the failures-th failed attempt: lo doubles doublings times, then grows
// linearly, capped at hi.
func Backoff(lo, hi time.Duration, doublings, failures int32) time.Duration {
	i := int(max(failures-1, 0))
	d := int(doublings)

	var mult float64
	if i <= d {
		mult = math.Ldexp(1, i)
	} else {
		mult = math.Ldexp(1, d) * float64(i-d+1)
	}

	return time.Duration(min(float64(lo)*mult, float64(hi)))
}

func b64(v any) string {
	raw, _ := json.Marshal(v)

	return base64.RawURLEncoding.EncodeToString(raw)
}

// OIDCToken mints an UNSIGNED JWT (alg none) marked as a gopherstack test token; no verifier will accept it as real.
func OIDCToken(email, audience, targetURL string, now time.Time) string {
	if audience == "" {
		audience = targetURL
	}

	claims := map[string]any{
		"iss": issuer, "aud": audience, "sub": email, "email": email,
		"email_verified": true, "iat": now.Unix(), "exp": now.Add(tokenTTL).Unix(),
		"gopherstack": "unsigned-test-token",
	}

	return b64(map[string]string{"alg": "none", "typ": "JWT"}) + "." + b64(claims) + "."
}

// OAuthToken returns an opaque placeholder access token tied to the service account and scope.
func OAuthToken(email, scope string) string {
	if scope == "" {
		scope = "https://www.googleapis.com/auth/cloud-platform"
	}

	return "gopherstack-unsigned-oauth." + base64.RawURLEncoding.EncodeToString([]byte(email+"|"+scope))
}
