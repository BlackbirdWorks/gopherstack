package azureauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
)

// ErrMalformedAuthorization is returned by [VerifySharedKey] when the
// request's Authorization header cannot be parsed as SharedKey or
// SharedKeyLite.
var ErrMalformedAuthorization = errors.New("azureauth: malformed Authorization header")

// SignSharedKey computes the SharedKey signature for r as account, using
// accountKey (base64-encoded, e.g. [DefaultAccountKey]). It does not modify r
// or set the Authorization header; callers that want a signed request do that
// themselves with the returned signature.
func SignSharedKey(r *http.Request, account, accountKey string) (string, error) {
	return sign(accountKey, StringToSign(r, account))
}

// SignSharedKeyLite computes the SharedKeyLite signature for r as account,
// using accountKey (base64-encoded).
func SignSharedKeyLite(r *http.Request, account, accountKey string) (string, error) {
	return sign(accountKey, StringToSignLite(r, account))
}

// sign returns base64(HMAC-SHA256(base64decode(accountKey), stringToSign)).
func sign(accountKey, stringToSign string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return "", fmt.Errorf("azureauth: decode account key: %w", err)
	}

	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(stringToSign))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

// VerifySharedKey recomputes the SharedKey or SharedKeyLite signature for r
// (dispatching on the scheme named in its Authorization header) using
// accountKey, and reports whether it matches the signature the client sent.
//
// This is an explicit, opt-in check: nothing in this package calls it
// implicitly, and [ParseAuthorizationHeader] never fails or reports a
// mismatch on its own — callers that want enforcement invoke VerifySharedKey
// themselves, mirroring services/s3's opt-in WithPresignValidation pattern.
// It returns ([ErrMalformedAuthorization]) wrapped in the error when the
// header can't be parsed at all.
func VerifySharedKey(accountKey string, r *http.Request) (bool, error) {
	auth, ok := ParseAuthorizationHeader(r.Header.Get("Authorization"))
	if !ok {
		return false, ErrMalformedAuthorization
	}

	var (
		expected string
		err      error
	)

	switch auth.Scheme {
	case SchemeSharedKeyLite:
		expected, err = SignSharedKeyLite(r, auth.Account, accountKey)
	default:
		expected, err = SignSharedKey(r, auth.Account, accountKey)
	}
	if err != nil {
		return false, err
	}

	return hmac.Equal([]byte(expected), []byte(auth.Signature)), nil
}
