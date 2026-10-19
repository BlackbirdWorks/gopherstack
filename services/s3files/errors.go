package s3files

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a file system, mount target, access point or policy does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrConflict is returned when the request conflicts with the resource's current state.
	ErrConflict = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrValidation is returned for malformed or out-of-range input.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// conflictError carries the resourceId/resourceType members of ConflictException.
type conflictError struct {
	msg          string
	resourceID   string
	resourceType string
}

func (e *conflictError) Error() string { return e.msg }

func (e *conflictError) Is(target error) bool { return target == ErrConflict }

func conflict(resourceType, resourceID, msg string) error {
	return &conflictError{msg: msg, resourceID: resourceID, resourceType: resourceType}
}

func validation(msg string) error { return awserr.New(msg, ErrValidation) }

func notFound(msg string) error { return awserr.New(msg, ErrNotFound) }

// errCodeLookup maps backend sentinels to wire codes (types/errors.go in service/s3files).
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
}

func classify(err error) (string, int) {
	for _, e := range errCodeLookup {
		if errors.Is(err, e.err) {
			return e.code, e.status
		}
	}

	return "InternalServerException", http.StatusInternalServerError
}
