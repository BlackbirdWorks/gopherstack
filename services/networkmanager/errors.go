package networkmanager

import (
	"errors"
	"fmt"
)

// ErrNilAppContext is returned by Provider.Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// sentinel errors, matched via errors.Is by handler.go's handleError to pick
// the wire error type/HTTP status. All 8 modeled exception shapes were
// confirmed by reading types/errors.go directly -- see PARITY.md.
// errAccessDenied/errQuotaExceeded/errThrottling have no constructor in this
// file: this backend has no permission model, no account-level resource-
// count quota model (no AWS-published default numbers to enforce without
// fabricating one), and no rate limiter to simulate, matching
// services/mgn/outposts/resiliencehub/grafana's identical documented gap.
// They still back classifyError's wire-shape classification so a future
// fault-injection layer has somewhere to route them.
var (
	errAccessDenied       = errors.New("access denied")
	errConflictSentinel   = errors.New("conflict")
	errCoreNetworkPolicy  = errors.New("core network policy error")
	errInternalServer     = errors.New("internal server error")
	errNotFoundSentinel   = errors.New("resource not found")
	errQuotaExceeded      = errors.New("service quota exceeded")
	errThrottling         = errors.New("throttling")
	errValidationSentinel = errors.New("validation error")
)

// apiError carries every field any of this service's exception shapes might
// need. Only the fields relevant to the concrete shape being rendered are
// populated by any one constructor below; handler.go's handleError reads
// only the fields that shape's JSON envelope defines.
type apiError struct {
	cause        error
	message      string
	resourceID   string
	resourceType string
	reason       string
	limitCode    string
	serviceCode  string
	policyErrors []CoreNetworkPolicyError
}

func (e *apiError) Error() string { return e.message }
func (e *apiError) Unwrap() error { return e.cause }

// conflictError builds a ConflictException-shaped error.
func conflictError(resourceType, resourceID, msg string) error {
	return &apiError{cause: errConflictSentinel, message: msg, resourceType: resourceType, resourceID: resourceID}
}

// coreNetworkPolicyError builds a CoreNetworkPolicyException-shaped error --
// only CreateCoreNetwork/PutCoreNetworkPolicy carry this shape (PARITY.md:
// the only two ops that accept a raw PolicyDocument directly from the
// caller).
func coreNetworkPolicyError(msg string, errs []CoreNetworkPolicyError) error {
	return &apiError{cause: errCoreNetworkPolicy, message: msg, policyErrors: errs}
}

// internalServerError builds an InternalServerException-shaped error.
func internalServerError(msg string) error {
	return &apiError{cause: errInternalServer, message: msg}
}

// notFoundError builds a ResourceNotFoundException-shaped error.
func notFoundError(resourceType, resourceID string) error {
	return &apiError{
		cause:        errNotFoundSentinel,
		message:      fmt.Sprintf("%s %s not found", resourceType, resourceID),
		resourceType: resourceType,
		resourceID:   resourceID,
	}
}

// validationError builds a ValidationException-shaped error with Reason
// defaulted to "FieldValidationFailed" (types.ValidationExceptionReason's
// UpperCamelCase wire values -- PARITY.md wire trap: a third distinct
// casing convention from mgn's lowerCamelCase and this service's own
// SCREAMING_SNAKE_CASE majority).
func validationError(msg string) error {
	return &apiError{cause: errValidationSentinel, message: msg, reason: validationReasonFieldValidationFailed}
}
