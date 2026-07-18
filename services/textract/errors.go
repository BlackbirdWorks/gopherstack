package textract

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrJobNotFound is returned when a document job is not found.
	ErrJobNotFound = awserr.New("InvalidJobIdException", awserr.ErrNotFound)
	// ErrAdapterNotFound is returned when an adapter is not found.
	ErrAdapterNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrAdapterVersionNotFound is returned when an adapter version is not found.
	ErrAdapterVersionNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
