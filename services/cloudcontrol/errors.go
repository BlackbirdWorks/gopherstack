package cloudcontrol

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same identifier already exists.
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when a required field is missing or has an invalid value.
	// CloudControl's error model has no ValidationException shape at all: every
	// operation instead declares InvalidRequestException ("invalid input from the
	// user has generated a generic exception") as its generic input-validation error.
	// See the Errors section of e.g. the CreateResource API reference.
	ErrValidation = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	// ErrRequestTokenNotFound is returned when a RequestToken does not correspond to
	// any tracked resource operation request. GetResourceRequestStatus declares
	// RequestTokenNotFoundException as its ONLY error, and CancelResourceRequest
	// declares it alongside ConcurrentModificationException; real AWS never returns
	// ResourceNotFoundException for an unrecognized RequestToken.
	ErrRequestTokenNotFound = awserr.New("RequestTokenNotFoundException", awserr.ErrNotFound)
	// ErrConcurrentModification is returned by CancelResourceRequest when the target
	// request is not in a cancellable (PENDING/IN_PROGRESS) state. Confirmed against
	// the CancelResourceRequest API reference's Errors section:
	// ConcurrentModificationException, HTTP 500 -- "The resource is currently being
	// modified by another operation".
	ErrConcurrentModification = awserr.New("ConcurrentModificationException", awserr.ErrConflict)
)
