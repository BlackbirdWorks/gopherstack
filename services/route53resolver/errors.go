package route53resolver

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrNotFound         = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists    = awserr.New("ResourceExistsException", awserr.ErrAlreadyExists)
	ErrValidation       = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	ErrInvalidParameter = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
)
