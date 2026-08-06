package grafana

import (
	"errors"
	"fmt"
)

// ErrNilAppContext is returned by Provider.Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// sentinel errors, matched via errors.Is by handler.go's handleError to pick
// the wire error type/HTTP status. Distinct from pkgs/awserr's sentinels
// because this service's exact wire error types (ValidationException,
// ConflictException, ResourceNotFoundException -- see types/errors.go in
// the SDK) carry their own optional resourceId/resourceType payload (see
// apiError below), which a shared cross-service sentinel has no place for.
var (
	errNotFoundSentinel   = errors.New("resource not found")
	errConflictSentinel   = errors.New("conflict")
	errValidationSentinel = errors.New("validation error")
	errQuotaSentinel      = errors.New("service quota exceeded")
)

// apiError carries the fields needed to render one of Grafana's five
// modeled wire error shapes (ValidationException/ConflictException/
// ResourceNotFoundException/ServiceQuotaExceededException, plus the
// InternalServerException fallback -- see handler.go's handleError).
// AccessDeniedException and ThrottlingException have no organic trigger
// path (no auth model) but are reachable via pkgs/chaos fault injection,
// wired generically for every service -- see PARITY.md.
type apiError struct {
	cause        error
	message      string
	resourceType string
	resourceID   string
}

func (e *apiError) Error() string { return e.message }
func (e *apiError) Unwrap() error { return e.cause }

// notFoundError builds a ResourceNotFoundException-shaped error for a
// missing resourceType/resourceID pair (e.g. "workspace"/"g-1234").
func notFoundError(resourceType, resourceID string) error {
	return &apiError{
		cause:        errNotFoundSentinel,
		message:      fmt.Sprintf("%s %s not found", resourceType, resourceID),
		resourceType: resourceType,
		resourceID:   resourceID,
	}
}

// conflictError builds a ConflictException-shaped error.
func conflictError(resourceType, resourceID, msg string) error {
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

// quotaError builds a ServiceQuotaExceededException-shaped error.
func quotaError(msg string) error {
	return &apiError{cause: errQuotaSentinel, message: msg}
}

// notActiveError builds the ConflictException-shaped error CreateWorkspace's
// sibling mutation ops (UpdateWorkspace, UpdateWorkspaceConfiguration,
// AssociateLicense) all return when the workspace is mid-transition
// (CREATING/UPDATING/UPGRADING/VERSION_UPDATING/DELETING) and therefore not
// ACTIVE or DEGRADED.
func notActiveError(id, status string) error {
	return conflictError(resourceTypeWorkspace, id, "workspace is not in a state that can be updated: "+status)
}
