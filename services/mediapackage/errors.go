package mediapackage

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrNotFound is returned when a resource does not exist.
var ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)

// ErrConflict is returned for duplicate resource IDs.
var ErrConflict = awserr.New("UnprocessableEntityException", awserr.ErrAlreadyExists)

// ErrInvalidParameter is returned for invalid input.
var ErrInvalidParameter = awserr.New("UnprocessableEntityException", awserr.ErrInvalidParameter)
