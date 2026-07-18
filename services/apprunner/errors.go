package apprunner

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

const (
	// invalidRequestType, resourceNotFoundType, invalidStateType, and
	// internalServiceErrorType are the exact exception names the real App
	// Runner API returns (see aws-sdk-go-v2/service/apprunner/types/errors.go
	// -- App Runner has exactly five exception types and no
	// "InvalidParameterException" or bare "InternalServiceError"; those
	// names don't exist in the model and would deserialize as a generic,
	// untyped smithy API error on the client instead of the typed exception
	// struct callers match with errors.As).
	invalidRequestType       = "InvalidRequestException"
	resourceNotFoundType     = "ResourceNotFoundException"
	invalidStateType         = "InvalidStateException"
	internalServiceErrorType = "InternalServiceErrorException"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(resourceNotFoundType, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a service/connection/vpc-ingress-connection
	// name (or custom domain) is already in use. App Runner has no dedicated
	// "already exists" exception; unlike ResourceNotFoundException,
	// ServiceQuotaExceededException is only in the documented error set for
	// some Create* operations and not others (e.g. AssociateCustomDomain
	// doesn't accept it), so InvalidRequestException -- valid on every App
	// Runner operation -- is used to describe the conflict instead.
	ErrAlreadyExists = awserr.New(invalidRequestType, awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = awserr.New(invalidRequestType, awserr.ErrInvalidParameter)
	// ErrInvalidState is returned when a service is in an invalid state for the operation.
	ErrInvalidState = awserr.New(invalidStateType, awserr.ErrConflict)
)
