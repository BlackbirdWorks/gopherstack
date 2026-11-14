package lambdaweb

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrQuotaExceeded maps to ServiceQuotaExceededException.
var ErrQuotaExceeded = errors.New("quota exceeded")

// ErrCodeUnavailable is returned when a revision's code package cannot be read.
var ErrCodeUnavailable = errors.New("code package unavailable")

// apiError carries the resource fields the SDK's modeled exceptions expose.
type apiError struct {
	cause        error
	msg          string
	resourceType string
	resourceID   string
}

func (e *apiError) Error() string { return e.msg }
func (e *apiError) Unwrap() error { return e.cause }

func notFoundError(resourceType, id string) error {
	return &apiError{
		cause: awserr.ErrNotFound, resourceType: resourceType, resourceID: id,
		msg: fmt.Sprintf("The %s %s was not found.", resourceType, id),
	}
}

func conflictError(resourceType, id, format string, args ...any) error {
	return &apiError{
		cause: awserr.ErrConflict, resourceType: resourceType, resourceID: id, msg: fmt.Sprintf(format, args...),
	}
}

func quotaError(resourceType, id, format string, args ...any) error {
	return &apiError{
		cause: ErrQuotaExceeded, resourceType: resourceType, resourceID: id, msg: fmt.Sprintf(format, args...),
	}
}

func validationError(format string, args ...any) error {
	return &apiError{cause: awserr.ErrInvalidParameter, msg: fmt.Sprintf(format, args...)}
}
