package rolesanywhere

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrTrustAnchorNotFound is returned when a trust anchor does not exist.
	ErrTrustAnchorNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTrustAnchorAlreadyExists is returned when creating a duplicate trust anchor.
	ErrTrustAnchorAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrProfileNotFound is returned when a profile does not exist.
	ErrProfileNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProfileAlreadyExists is returned when creating a duplicate profile.
	ErrProfileAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrCrlNotFound is returned when a CRL does not exist.
	ErrCrlNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrCrlAlreadyExists is returned when creating a duplicate CRL.
	ErrCrlAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrSubjectNotFound is returned when a subject does not exist.
	ErrSubjectNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
