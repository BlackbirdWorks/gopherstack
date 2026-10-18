package networkfirewall

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors; each message starts with the AWS exception name.
var (
	ErrResourceNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrInvalidRequest        = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	ErrInvalidToken          = awserr.New("InvalidTokenException", awserr.ErrConflict)
	ErrInvalidOperation      = awserr.New("InvalidOperationException", awserr.ErrConflict)
	ErrInvalidResourcePolicy = awserr.New("InvalidResourcePolicyException", awserr.ErrInvalidParameter)
	ErrLimitExceeded         = awserr.New("LimitExceededException", awserr.ErrInvalidParameter)
	ErrUnsupportedOperation  = awserr.New("UnsupportedOperationException", awserr.ErrInvalidParameter)
	ErrThrottling            = awserr.New("ThrottlingException", awserr.ErrConflict)

	errUnknownAction = errors.New("UnknownOperationException")
)

type errEntry struct {
	err    error
	code   string
	status int
}

// errCodeLookup maps sentinels to the exception name and HTTP status the SDK expects.
var errCodeLookup = []errEntry{ //nolint:gochecknoglobals // static classification table
	{ErrResourceNotFound, "ResourceNotFoundException", http.StatusBadRequest},
	{ErrInvalidRequest, "InvalidRequestException", http.StatusBadRequest},
	{ErrInvalidToken, "InvalidTokenException", http.StatusBadRequest},
	{ErrInvalidOperation, "InvalidOperationException", http.StatusBadRequest},
	{ErrInvalidResourcePolicy, "InvalidResourcePolicyException", http.StatusBadRequest},
	{ErrLimitExceeded, "LimitExceededException", http.StatusBadRequest},
	{ErrUnsupportedOperation, "UnsupportedOperationException", http.StatusBadRequest},
	{ErrThrottling, "ThrottlingException", http.StatusBadRequest},
	{errUnknownAction, "UnknownOperationException", http.StatusBadRequest},
}

func classifyError(err error) (int, string) {
	for _, e := range errCodeLookup {
		if errors.Is(err, e.err) {
			return e.status, e.code
		}
	}

	return http.StatusInternalServerError, "InternalServerError"
}

func notFound(kind, name string) error {
	return fmt.Errorf("%w: %s %s not found", ErrResourceNotFound, kind, name)
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidRequest, fmt.Sprintf(format, args...))
}

func invalidOp(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidOperation, fmt.Sprintf(format, args...))
}

func fmtInvalidToken() error {
	return fmt.Errorf("%w: The token you provided is stale or is not valid for the resource", ErrInvalidToken)
}

func unsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupportedOperation, fmt.Sprintf(format, args...))
}

func invalidResourcePolicy(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidResourcePolicy, fmt.Sprintf(format, args...))
}
