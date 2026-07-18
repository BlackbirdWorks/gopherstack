package dlm

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errInvalidRequest   = "InvalidRequestException"
)

var (
	// ErrPolicyNotFound is returned when a lifecycle policy does not exist.
	ErrPolicyNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInvalidRequest is returned on invalid input.
	ErrInvalidRequest = awserr.New(errInvalidRequest, awserr.ErrInvalidParameter)
)
