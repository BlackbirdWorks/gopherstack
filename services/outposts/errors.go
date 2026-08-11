package outposts

import (
	"errors"
	"fmt"
)

// ErrNilAppContext is returned by Provider.Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// sentinel errors, matched via errors.Is by handler.go's handleError to pick
// the wire error type/HTTP status. types.NotFoundException carries no
// resourceId/resourceType payload at all (confirmed from types/errors.go --
// unlike types.ConflictException, which carries both but as a CLOSED enum
// (types.ResourceType) that only supports OUTPOST/ORDER -- see consts.go).
//
// errQuotaExceeded backs ServiceQuotaExceededException, a real, wire-accurate
// error type declared on CreateOutpost/CreateSite/CreateOrder's own error
// sets. CreateSite/CreateOutpost enforce it against AWS's own published
// default quotas (docs.aws.amazon.com/outposts/latest/userguide/outposts-limits.html:
// 100 sites/Region, 10 Outposts/site -- see consts.go). CreateOrder has no
// published per-account order quota to enforce without fabricating a number,
// matching services/grafana's identical treatment of AccessDeniedException --
// see PARITY.md.
var (
	errNotFoundSentinel   = errors.New("resource not found")
	errConflictSentinel   = errors.New("conflict")
	errValidationSentinel = errors.New("validation error")
	errQuotaExceeded      = errors.New("service quota exceeded")
)

// apiError carries the fields needed to render one of Outposts' wire error
// shapes. resourceType/resourceID are only ever populated for
// ConflictException, and only when the conflicting resource is an Outpost or
// an Order -- types.ResourceType has no other members, so a Site/Quote
// conflict must leave both empty rather than fabricate an enum value the SDK
// does not declare (see consts.go's ResourceType constants).
type apiError struct {
	cause        error
	message      string
	resourceType string
	resourceID   string
}

func (e *apiError) Error() string { return e.message }
func (e *apiError) Unwrap() error { return e.cause }

// notFoundError builds a NotFoundException-shaped error. resourceType here
// is message text only (e.g. "site"), never the wire's ResourceType enum --
// see resourceOutpost et al in consts.go.
func notFoundError(resourceType, resourceID string) error {
	return &apiError{
		cause:   errNotFoundSentinel,
		message: fmt.Sprintf("%s %s not found", resourceType, resourceID),
	}
}

// conflictError builds a ConflictException-shaped error with no wire
// resourceType/resourceId (for Site/Quote/CapacityTask conflicts, where the
// closed ResourceType enum has no matching member).
func conflictError(msg string) error {
	return &apiError{cause: errConflictSentinel, message: msg}
}

// conflictErrorWithResource builds a ConflictException-shaped error that
// also carries the wire ResourceId/ResourceType fields -- wireResourceType
// must be one of consts.go's ResourceTypeOutpost/ResourceTypeOrder.
func conflictErrorWithResource(wireResourceType, resourceID, msg string) error {
	return &apiError{
		cause:        errConflictSentinel,
		message:      msg,
		resourceType: wireResourceType,
		resourceID:   resourceID,
	}
}

// validationError builds a ValidationException-shaped error.
func validationError(msg string) error {
	return &apiError{cause: errValidationSentinel, message: msg}
}

// quotaExceededError builds a ServiceQuotaExceededException-shaped error.
func quotaExceededError(msg string) error {
	return &apiError{cause: errQuotaExceeded, message: msg}
}
