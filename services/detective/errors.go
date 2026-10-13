package detective

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrGraphNotFound is returned when a behavior graph does not exist.
	ErrGraphNotFound = awserr.New("The request refers to a nonexistent resource", awserr.ErrNotFound)
	// ErrAlreadyHasGraph is returned when account already has a graph in region.
	ErrAlreadyHasGraph = awserr.New("The request attempted an invalid action", awserr.ErrAlreadyExists)
	// ErrMemberNotFound is returned when a member does not exist.
	ErrMemberNotFound = awserr.New("The request refers to a nonexistent resource", awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("The request parameters are invalid", awserr.ErrInvalidParameter)
)
