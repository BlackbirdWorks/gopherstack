package ssooidc

import (
	"errors"
	"net/http"
)

// Exception names as the pinned ssooidc SDK deserializes them.
const (
	codeAccessDenied         = "AccessDeniedException"
	codeAuthorizationPending = "AuthorizationPendingException"
	codeExpiredToken         = "ExpiredTokenException"
	codeInvalidClient        = "InvalidClientException"
	codeInvalidClientMeta    = "InvalidClientMetadataException"
	codeInvalidGrant         = "InvalidGrantException"
	codeInvalidRedirectURI   = "InvalidRedirectUriException"
	codeInvalidRequest       = "InvalidRequestException"
	codeInvalidScope         = "InvalidScopeException"
	codeSlowDown             = "SlowDownException"
	codeUnauthorizedClient   = "UnauthorizedClientException"
	codeUnsupportedGrant     = "UnsupportedGrantTypeException"
)

// APIError is an OIDC protocol error: Code is the exception name, OAuth the RFC 6749 "error" value.
type APIError struct {
	Code    string
	OAuth   string
	Message string
	Status  int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func newError(status int, code, oauth, msg string) *APIError {
	return &APIError{Status: status, Code: code, OAuth: oauth, Message: msg}
}

func errInvalidRequest(msg string) *APIError {
	return newError(http.StatusBadRequest, codeInvalidRequest, "invalid_request", msg)
}

func errInvalidClient(msg string) *APIError {
	return newError(http.StatusUnauthorized, codeInvalidClient, "invalid_client", msg)
}

func errInvalidGrant(msg string) *APIError {
	return newError(http.StatusBadRequest, codeInvalidGrant, "invalid_grant", msg)
}

func errInvalidClientMetadata(msg string) *APIError {
	return newError(http.StatusBadRequest, codeInvalidClientMeta, "invalid_client_metadata", msg)
}

func errInvalidRedirectURI(msg string) *APIError {
	return newError(http.StatusBadRequest, codeInvalidRedirectURI, "invalid_redirect_uri", msg)
}

func errUnauthorizedClient(msg string) *APIError {
	return newError(http.StatusBadRequest, codeUnauthorizedClient, "unauthorized_client", msg)
}

func errUnsupportedGrant(msg string) *APIError {
	return newError(http.StatusBadRequest, codeUnsupportedGrant, "unsupported_grant_type", msg)
}

func errInvalidScope(msg string) *APIError {
	return newError(http.StatusBadRequest, codeInvalidScope, "invalid_scope", msg)
}

func errPending() *APIError {
	return newError(http.StatusBadRequest, codeAuthorizationPending, "authorization_pending",
		"The authorization request is still pending.")
}

func errSlowDown() *APIError {
	return newError(http.StatusBadRequest, codeSlowDown, "slow_down",
		"The client is polling faster than the allowed interval.")
}

func errExpired(msg string) *APIError {
	return newError(http.StatusBadRequest, codeExpiredToken, "expired_token", msg)
}

func errDenied(msg string) *APIError {
	return newError(http.StatusBadRequest, codeAccessDenied, "access_denied", msg)
}

// Sentinels for the approval endpoints and the portal's token lookup.
var (
	ErrUnknownUserCode = errors.New("unknown or expired user code")
	ErrUnknownUser     = errors.New("unknown user")
	ErrTokenNotFound   = errors.New("session token not found or invalid")
	ErrTokenExpired    = errors.New("session token expired")
	ErrNoResolver      = errors.New("no identity resolver is wired")
)
