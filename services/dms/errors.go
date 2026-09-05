package dms

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a requested DMS resource cannot be found.
	ErrNotFound = awserr.New("ResourceNotFoundFault", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a DMS resource already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsFault", awserr.ErrConflict)
	// ErrInvalidState is returned when a DMS resource is in an invalid state for the requested operation.
	ErrInvalidState = awserr.New("InvalidResourceStateFault", awserr.ErrInvalidParameter)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrCollectorNotFound is returned by DeleteFleetAdvisorCollector, whose own
	// deserializeOpError models CollectorNotFoundFault rather than the
	// service-wide ResourceNotFoundFault (databasemigrationservice@v1.66.4
	// deserializers.go:2875-2913).
	ErrCollectorNotFound = awserr.New("CollectorNotFoundFault", awserr.ErrNotFound)
)

// errUnknownAction is returned when an unsupported DMS action is requested.
var errUnknownAction = errors.New("UnknownOperationException")
