package waf

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

const (
	errResourceNotFound = "WAFNonexistentItemException"
	errStaleData        = "WAFStaleDataException"
	errInvalidParameter = "WAFInvalidParameterException"
	errReferencedItem   = "WAFReferencedItemException"
	errNonEmptyEntity   = "WAFNonEmptyEntityException"
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
	// ErrNonEmptyEntity is returned when a resource still contains child
	// entities (e.g. a WebACL that still has Rules, a Rule that still has
	// Predicates, a ByteMatchSet that still has ByteMatchTuples).
	ErrNonEmptyEntity = awserr.New(errNonEmptyEntity, awserr.ErrConflict)
)
