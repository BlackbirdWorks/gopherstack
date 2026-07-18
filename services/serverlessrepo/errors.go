package serverlessrepo

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrApplicationNotFound is returned when an application does not exist.
	ErrApplicationNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrApplicationAlreadyExists is returned when an application already exists.
	ErrApplicationAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTemplateNotFound is returned when a CloudFormation template does not exist.
	ErrTemplateNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrVersionAlreadyExists is returned when an application version already exists.
	ErrVersionAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrValidation is returned when a request contains an invalid or missing parameter.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)
