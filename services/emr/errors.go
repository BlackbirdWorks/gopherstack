package emr

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var ErrValidation = awserr.New(
	"ValidationException: required field is missing",
	awserr.ErrInvalidParameter,
)

var (
	ErrNotFound      = awserr.New("ClientException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
)

var errTerminationProtected = awserr.New(
	"ValidationException: cluster has termination protection enabled",
	awserr.ErrInvalidParameter,
)
