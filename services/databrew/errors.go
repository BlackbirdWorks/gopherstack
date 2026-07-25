package databrew

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// AWS restjson1 exception type names, shared between the top-level
// handleError mapping and any op that needs to embed one directly in a
// partial-failure response (e.g. BatchDeleteRecipeVersion's
// []RecipeVersionErrorDetail).
const (
	errCodeResourceNotFound = "ResourceNotFoundException"
	errCodeValidation       = "ValidationException"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New(errCodeResourceNotFound, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New(errCodeValidation, awserr.ErrInvalidParameter)
)
