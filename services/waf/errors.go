package waf

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

const (
	errResourceNotFound = "WAFNonexistentItemException"
	errStaleData        = "WAFStaleDataException"
	errInvalidParameter = "WAFInvalidParameterException"
	errReferencedItem   = "WAFReferencedItemException"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrStaleToken is returned when the change token is stale.
	ErrStaleToken = awserr.New(errStaleData, awserr.ErrConflict)
	// ErrInvalidParameter is returned on invalid input.
	ErrInvalidParameter = awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	// ErrReferencedItem is returned when a resource is still referenced.
	ErrReferencedItem = awserr.New(errReferencedItem, awserr.ErrConflict)
)
