package signin

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrValidation is returned for malformed or out-of-range input.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrNotFound is returned when the requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrConflict is returned when the request conflicts with existing state.
	ErrConflict = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrAccessDenied is returned when the caller or token may not perform the request.
	ErrAccessDenied = errors.New("AccessDeniedException")
)

const (
	statusForbidden    = http.StatusForbidden
	statusUnauthorized = http.StatusUnauthorized
)

// OAuth2ErrorCode values carried in the "error" member (types.OAuth2ErrorCode).
const (
	oauthTokenExpired       = "TOKEN_EXPIRED"
	oauthCredentialsChanged = "USER_CREDENTIALS_CHANGED"
	oauthInsufficientPerms  = "INSUFFICIENT_PERMISSIONS"
	oauthAuthcodeExpired    = "AUTHCODE_EXPIRED"
	oauthInvalidRequest     = "INVALID_REQUEST"
	oauthResourceNotFound   = "RESOURCE_NOT_FOUND"
	oauthConflict           = "CONFLICT"
)

// signinError carries the OAuth2ErrorCode and an optional status override.
type signinError struct {
	kind   error
	msg    string
	oauth  string
	status int
}

func (e *signinError) Error() string { return e.msg }

func (e *signinError) Is(target error) bool { return target == e.kind }

func validation(msg string) error {
	return &signinError{kind: ErrValidation, msg: msg, oauth: oauthInvalidRequest}
}

func notFound(msg string) error {
	return &signinError{kind: ErrNotFound, msg: msg, oauth: oauthResourceNotFound}
}

func conflict(msg string) error {
	return &signinError{kind: ErrConflict, msg: msg, oauth: oauthConflict}
}

func accessDenied(oauth string, status int, msg string) error {
	return &signinError{kind: ErrAccessDenied, msg: msg, oauth: oauth, status: status}
}

// errCodeLookup maps backend sentinels to wire codes (types/errors.go in service/signin).
//
//nolint:gochecknoglobals // static lookup table
var errCodeLookup = []struct {
	err    error
	code   string
	status int
}{
	{ErrValidation, "ValidationException", http.StatusBadRequest},
	{ErrNotFound, "ResourceNotFoundException", http.StatusNotFound},
	{ErrConflict, "ConflictException", http.StatusConflict},
	{ErrAccessDenied, "AccessDeniedException", http.StatusForbidden},
}

func classify(err error) (string, int) {
	for _, e := range errCodeLookup {
		if errors.Is(err, e.err) {
			if se, ok := errors.AsType[*signinError](err); ok && se.status != 0 {
				return e.code, se.status
			}

			return e.code, e.status
		}
	}

	return "InternalServerException", http.StatusInternalServerError
}

func oauthCode(err error) string {
	if se, ok := errors.AsType[*signinError](err); ok {
		return se.oauth
	}

	return "server_error"
}
