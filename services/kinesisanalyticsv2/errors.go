package kinesisanalyticsv2

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrConcurrentModification is returned when the application version does not match.
	ErrConcurrentModification = awserr.New(
		"ConcurrentModificationException",
		awserr.ErrInvalidParameter,
	)
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
)
