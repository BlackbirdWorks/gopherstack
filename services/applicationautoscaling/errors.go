package applicationautoscaling

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ObjectNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ValidationException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request parameter fails validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
