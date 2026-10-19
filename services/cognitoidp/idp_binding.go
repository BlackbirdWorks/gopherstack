package cognitoidp

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	idpBindCookiePrefix = "gs_idp_"
	idpBindNameHashLen  = 16
)

func isHTTPSRequest(r *http.Request) bool {
	return r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == schemeHTTPS
}

func idpBindHash(state string) string {
	sum := sha256.Sum256([]byte(state))

	return hex.EncodeToString(sum[:])
}

func idpBindCookieName(hash string) string { return idpBindCookiePrefix + hash[:idpBindNameHashLen] }

// setIDPBinding ties state to the initiating browser (login-CSRF defence). The IdP may return via
// a cross-site POST, which SameSite=Lax cookies never accompany, so post bindings use SameSite=None;
// Secure on https; browsers reject that over plain http, so verifyIDPBinding tolerates its absence.
func setIDPBinding(c *echo.Context, state, callbackPath string, post bool) {
	r := c.Request()
	https := isHTTPSRequest(r)

	ck := &http.Cookie{ //nolint:gosec // Secure tracks the request scheme; plain http is local dev
		Name: idpBindCookieName(idpBindHash(state)), Value: idpBindHash(state), Path: callbackPath,
		MaxAge: int(idpStateTTL.Seconds()), HttpOnly: true, Secure: https, SameSite: http.SameSiteLaxMode,
	}
	if post && https {
		ck.SameSite = http.SameSiteNoneMode
	}

	http.SetCookie(c.Response(), ck)
}

// verifyIDPBinding checks the callback carries the cookie set by setIDPBinding for this state.
func verifyIDPBinding(r *http.Request, state string, post bool) bool {
	want := idpBindHash(state)

	ck, err := r.Cookie(idpBindCookieName(want))
	if err != nil {
		if post && !isHTTPSRequest(r) {
			logger.Load(r.Context()).WarnContext(r.Context(),
				"cognitoidp: federation callback has no browser-binding cookie; accepted over plain http")

			return true
		}

		return false
	}

	return subtle.ConstantTimeCompare([]byte(ck.Value), []byte(want)) == 1
}
