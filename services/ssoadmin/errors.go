package ssoadmin

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)

	ErrInstanceNotFound                = errors.New("ResourceNotFoundException")
	ErrPermissionSetNotFound           = errors.New("ResourceNotFoundException")
	ErrPermissionSetAlreadyExists      = errors.New("ConflictException")
	ErrPermissionSetHasAssignments     = errors.New("ConflictException")
	ErrAssignmentNotFound              = errors.New("ResourceNotFoundException")
	ErrRequestNotFound                 = errors.New("ResourceNotFoundException")
	ErrApplicationNotFound             = errors.New("ResourceNotFoundException")
	ErrApplicationAlreadyExists        = errors.New("ConflictException")
	ErrTrustedTokenIssuerNotFound      = errors.New("ResourceNotFoundException")
	ErrTrustedTokenIssuerAlreadyExists = errors.New("ConflictException")
	ErrAccessScopeNotFound             = errors.New("ResourceNotFoundException")
	ErrAuthMethodNotFound              = errors.New("ResourceNotFoundException")
	ErrGrantNotFound                   = errors.New("ResourceNotFoundException")
	ErrACAAlreadyExists                = errors.New("ConflictException")
	ErrPermissionsBoundaryNotFound     = errors.New("ResourceNotFoundException")
	// ErrServiceQuotaExceeded is returned when a resource would exceed the
	// 50-tags-per-resource limit. Real ssoadmin has no TooManyTagsException in
	// its error model (see types/errors.go) -- quota overruns map to
	// ServiceQuotaExceededException like every other ssoadmin limit.
	ErrServiceQuotaExceeded = errors.New("ServiceQuotaExceededException")
)
