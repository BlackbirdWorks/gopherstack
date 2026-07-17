package pipes

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrNotFound      = awserr.New("NotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	ErrConflict      = awserr.New("ConflictException", awserr.ErrConflict)
	ErrQuota         = awserr.New("ServiceQuotaExceededException", awserr.ErrConflict)
)
