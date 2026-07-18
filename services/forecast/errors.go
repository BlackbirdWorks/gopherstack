package forecast

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested Forecast resource is absent.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a Forecast resource name already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when a Forecast request is invalid.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
	// ErrInvalidNextToken is returned when a List* NextToken cannot be decoded.
	// Real Amazon Forecast models InvalidNextTokenException on every List operation.
	ErrInvalidNextToken = awserr.New("InvalidNextTokenException", awserr.ErrInvalidParameter)
)
