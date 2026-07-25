package vpclattice

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists with the same name.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrDependencyConflict is returned when a delete is rejected because a
	// dependent resource still references the target (e.g. a service that is
	// still associated with a service network, a service network that still
	// has service or VPC associations, or a target group still referenced by
	// a listener rule). Real AWS returns ConflictException for all of these,
	// same as ErrAlreadyExists, but is kept as a distinct sentinel so callers
	// can tell "duplicate name" apart from "in use" via errors.Is.
	ErrDependencyConflict = awserr.New("ConflictException", awserr.ErrConflict)
)
