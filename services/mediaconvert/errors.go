package mediaconvert

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)
