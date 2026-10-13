package scheduler

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

func notFound(kind, name string) error {
	return fmt.Errorf("%w: %s", ErrNotFound, kind+" "+name+" does not exist.")
}

func alreadyExists(kind, name string) error {
	return fmt.Errorf("%w: %s", ErrAlreadyExists, kind+" "+name+" already exists.")
}
