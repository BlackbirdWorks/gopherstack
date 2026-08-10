package mq

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request contains an invalid parameter.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrInUse is returned when a resource cannot be deleted because another
	// resource still references it.
	ErrInUse = awserr.New("ConflictException", awserr.ErrConflict)
)
