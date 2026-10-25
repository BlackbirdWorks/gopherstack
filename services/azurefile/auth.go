package azurefile

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

// authenticate accepts Shared Key, Shared Key Lite, SAS, bearer and anonymous
// requests. Signatures are only enforced when VerifyAuth is set, and only for
// the development account whose key is public.
func (h *Handler) authenticate(rc *reqCtx) *apiError {
	if !h.VerifyAuth {
		return nil
	}

	r := rc.r
	authz := r.Header.Get("Authorization")

	switch {
	case strings.HasPrefix(authz, "Bearer "):
		return nil
	case authz != "":
		return verifySharedKey(rc)
	case rc.q.Get("sig") != "":
		return verifySAS(rc)
	}

	return nil
}

func verifySharedKey(rc *reqCtx) *apiError {
	auth, ok := azureauth.ParseAuthorizationHeader(rc.r.Header.Get("Authorization"))
	if !ok {
		return authFailed("The Authorization header is malformed.")
	}

	if !strings.EqualFold(auth.Account, azureauth.DefaultAccountName) {
		return nil
	}

	valid, err := azureauth.VerifySharedKey(azureauth.DefaultAccountKey, signedView(rc))
	if err != nil || !valid {
		return authFailed(msgBadSignature)
	}

	return nil
}

// msgBadSignature is the message of every rejected signature or SAS.
const msgBadSignature = "Server failed to authenticate the request. " +
	"Make sure the value of Authorization header is formed correctly including the signature."

func authFailed(msg string) *apiError {
	return newErr(http.StatusForbidden, "AuthenticationFailed", msg)
}

// signedView returns the request as the client signed it. Path-style URLs
// repeat the account in the canonicalized resource (as for the storage
// emulator), which pkgs/azureauth would otherwise collapse.
func signedView(rc *reqCtx) *http.Request {
	if !rc.pathStyle {
		return rc.r
	}

	r := rc.r.Clone(rc.r.Context())
	u := *rc.r.URL
	prefix := "/" + rc.acct
	u.Path = prefix + u.Path

	if u.RawPath != "" {
		u.RawPath = prefix + u.RawPath
	}

	r.URL = &u

	return r
}
