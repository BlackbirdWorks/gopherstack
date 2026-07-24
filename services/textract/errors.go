package textract

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrJobNotFound is returned when a document job is not found.
	ErrJobNotFound = awserr.New("InvalidJobIdException", awserr.ErrNotFound)
	// ErrAdapterNotFound is returned when an adapter is not found. Real AWS
	// returns ResourceNotFoundException (not InvalidParameterException) for
	// GetAdapter/UpdateAdapter/DeleteAdapter/CreateAdapterVersion/
	// ListAdapterVersions/Tag*Resource on a nonexistent adapter -- verified
	// against every deserializeOpError<Op> switch in the SDK.
	ErrAdapterNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAdapterVersionNotFound is returned when an adapter version is not
	// found. See ErrAdapterNotFound's doc comment for the error-code source.
	ErrAdapterVersionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
