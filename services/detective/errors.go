package detective

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

const (
	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
	errValidation        = "ValidationException"
)

var (
	// ErrGraphNotFound is returned when a behavior graph does not exist.
	ErrGraphNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAlreadyHasGraph is returned when account already has a graph in region.
	ErrAlreadyHasGraph = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrMemberNotFound is returned when a member does not exist.
	ErrMemberNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
)
