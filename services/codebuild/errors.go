package codebuild

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
)
