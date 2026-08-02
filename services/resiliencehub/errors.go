package resiliencehub

import (
	"errors"
	"fmt"
)

// ErrNilAppContext is returned by Provider.Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// sentinel errors, matched via errors.Is by handler.go's handleError to pick
// the wire error type/HTTP status. All 7 modeled exception shapes
// (AccessDeniedException, ConflictException, InternalServerException,
// ResourceNotFoundException, ServiceQuotaExceededException,
// ThrottlingException, ValidationException) were confirmed by reading every
// op's own deserializeOpError<Op> switch in the SDK's deserializers.go, not
// assumed from the shared type declarations -- see PARITY.md.
//
// errQuotaExceeded backs ServiceQuotaExceededException: a real, wire-accurate
// error type on several Create*/Start* ops' own error sets, but this backend
// has no account-level resource-count quota model (no AWS-published default
// quota numbers to enforce without fabricating one), matching
// services/outposts and services/grafana's identical treatment. See
// PARITY.md.
var (
	errNotFoundSentinel   = errors.New("resource not found")
	errConflictSentinel   = errors.New("conflict")
	errValidationSentinel = errors.New("validation error")
	errQuotaExceeded      = errors.New("service quota exceeded")
	errAccessDenied       = errors.New("access denied")
)

// apiError carries the fields needed to render one of Resilience Hub's wire
// error shapes. resourceType/resourceID back ConflictException's and
// ResourceNotFoundException's own ResourceId/ResourceType fields -- both
// exception shapes carry them (confirmed from types/errors.go), unlike
// services/outposts' NotFoundException (which carries neither).
type apiError struct {
	cause        error
	message      string
	resourceType string
	resourceID   string
}

func (e *apiError) Error() string { return e.message }
func (e *apiError) Unwrap() error { return e.cause }

// notFoundError builds a ResourceNotFoundException-shaped error.
func notFoundError(resourceType, resourceID string) error {
	return &apiError{
		cause:        errNotFoundSentinel,
		message:      fmt.Sprintf("%s %s not found", resourceType, resourceID),
		resourceType: resourceType,
		resourceID:   resourceID,
	}
}

// conflictError builds a ConflictException-shaped error with no wire
// ResourceId/ResourceType.
func conflictError(msg string) error {
	return &apiError{cause: errConflictSentinel, message: msg}
}

// conflictErrorWithResource builds a ConflictException-shaped error that
// also carries the wire ResourceId/ResourceType fields.
func conflictErrorWithResource(resourceType, resourceID, msg string) error {
	return &apiError{
		cause:        errConflictSentinel,
		message:      msg,
		resourceType: resourceType,
		resourceID:   resourceID,
	}
}

// validationError builds a ValidationException-shaped error.
func validationError(msg string) error {
	return &apiError{cause: errValidationSentinel, message: msg}
}
