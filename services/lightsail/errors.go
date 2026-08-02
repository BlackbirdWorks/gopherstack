package lightsail

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrNilAppContext is returned by Provider.Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// Sentinel errors matched via errors.Is by handler.go's handleError to pick
// the wire exception shape/HTTP status. All 8 shapes share the identical
// {Message, ErrorCodeOverride, Code, Docs, Tip} field set (PARITY.md
// "Errors" section) -- this backend only ever populates Message (it has no
// real per-error Docs URL or remediation Tip to report, and fabricating one
// would violate parity-principles.md), so apiError below carries only a
// message, same as directconnect's identically-thin apiError.
var (
	errAccessDenied     = errors.New("access denied")
	errAccountSetup     = errors.New("account setup in progress")
	errInvalidInput     = errors.New("invalid input")
	errNotFound         = errors.New("not found")
	errOperationFailure = errors.New("operation failure")
	errRegionSetup      = errors.New("region setup in progress")
	errServiceFault     = errors.New("service error")
	errUnauthenticated  = errors.New("unauthenticated")
)

// apiError carries the message for one of Lightsail's 8 wire exception
// shapes.
type apiError struct {
	cause   error
	message string
}

func (e *apiError) Error() string { return e.message }
func (e *apiError) Unwrap() error { return e.cause }

// validationError builds an InvalidInputException-shaped error -- the
// general-purpose validation-failure shape (no Reason/Fields structured
// breakdown, just Message, PARITY.md "Errors" section).
func validationError(msg string) error {
	return &apiError{cause: errInvalidInput, message: msg}
}

// notFoundError builds a NotFoundException-shaped "not found" error for the
// named resource kind/id.
func notFoundError(resourceKind, name string) error {
	return &apiError{cause: errNotFound, message: fmt.Sprintf("%s %s not found", resourceKind, name)}
}

// serviceError builds a ServiceException-shaped error -- the only server
// fault of the 8 shapes.
func serviceError(msg string) error {
	return &apiError{cause: errServiceFault, message: msg}
}

// errAccessDenied/errAccountSetup/errOperationFailure/errRegionSetup/
// errUnauthenticated (declared above) back classifyLightsailError's wire-shape
// classification for AccessDeniedException/AccountSetupInProgressException/
// OperationFailureException/RegionSetupInProgressException/
// UnauthenticatedException, even though no call site in this package
// currently constructs any of them -- confirmed by re-reading the SDK's own
// doc comments (aws-sdk-go-v2/service/lightsail/types/errors.go): each names
// a precondition this backend has no model for. AccessDeniedException and
// UnauthenticatedException both require a caller-identity/permission model
// this backend, like directconnect/mgn, does not simulate.
// AccountSetupInProgressException and RegionSetupInProgressException both
// require an account/region provisioning-state model (something like mgn's
// InitializeService/serviceinit.go) that this package has no equivalent
// of -- Lightsail resources here are created immediately, never gated on an
// account or opt-in-region setup step. OperationFailureException's own doc
// comment ("Lightsail throws this exception when an operation fails to
// execute") names no specific operation or precondition to hang a real
// trigger off of. Wiring any of the five would mean inventing a state
// machine or permission model this backend does not otherwise have, purely
// to exercise an otherwise-unused constructor -- exactly the fabrication
// parity-principles.md warns against. Documented here as a real, deliberate
// gap -- not silently missing -- matching mgn/errors.go's identical
// disclosure of its own unused errAccessDenied/errQuotaExceeded/
// errThrottling shapes.

// classifyLightsailError maps err to its HTTP status and wire exception
// type.
func classifyLightsailError(err error) (int, string) {
	switch {
	case errors.Is(err, errServiceFault):
		return http.StatusInternalServerError, "ServiceException"
	case errors.Is(err, errAccessDenied):
		return http.StatusForbidden, "AccessDeniedException"
	case errors.Is(err, errAccountSetup):
		return http.StatusBadRequest, "AccountSetupInProgressException"
	case errors.Is(err, errNotFound):
		return http.StatusBadRequest, "NotFoundException"
	case errors.Is(err, errOperationFailure):
		return http.StatusBadRequest, "OperationFailureException"
	case errors.Is(err, errRegionSetup):
		return http.StatusBadRequest, "RegionSetupInProgressException"
	case errors.Is(err, errUnauthenticated):
		return http.StatusUnauthorized, "UnauthenticatedException"
	default:
		return http.StatusBadRequest, "InvalidInputException"
	}
}
