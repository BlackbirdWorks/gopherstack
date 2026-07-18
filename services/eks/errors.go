package eks

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when an EKS resource is not found.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when an EKS resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrConflict)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)
)
