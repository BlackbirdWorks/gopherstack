package appstream

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errInvalidParameter = "InvalidParameterCombinationException"
	errResourceExists   = "ResourceAlreadyExistsException"
	errFleetNotStopped  = "InvalidAccountStatusException"
	errResourceInUse    = "ResourceInUseException"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrFleetNotStopped is returned when a fleet state transition is invalid
	// (e.g. starting a running fleet, stopping a stopped fleet, deleting a running fleet).
	ErrFleetNotStopped = awserr.New(errFleetNotStopped, awserr.ErrConflict)
	// ErrResourceInUse is returned when a resource cannot be deleted because it is in use.
	ErrResourceInUse = awserr.New(errResourceInUse, awserr.ErrConflict)
)
