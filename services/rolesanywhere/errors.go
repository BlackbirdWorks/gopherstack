package rolesanywhere

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrTrustAnchorNotFound is returned when a trust anchor does not exist.
	ErrTrustAnchorNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProfileNotFound is returned when a profile does not exist.
	ErrProfileNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrCrlNotFound is returned when a CRL does not exist.
	ErrCrlNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrSubjectNotFound is returned when a subject does not exist.
	ErrSubjectNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrResourceNotFound is returned when TagResource/ListTagsForResource is
	// given a resourceArn that does not match any trust anchor, profile, or
	// CRL. Unlike the resource-specific NotFound sentinels above, callers of
	// tag operations address resources purely by ARN, with no dedicated
	// per-family lookup path.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrTooManyTags is returned when TagResource would push a resource's
	// total tag count past the service's 200-tag limit (the same limit the
	// real SDK's TagList shape enforces on any single tags list; see
	// aws-sdk-go-v2/service/rolesanywhere's service-2.json "TagList" shape,
	// max: 200).
	ErrTooManyTags = awserr.New("TooManyTagsException", awserr.ErrInvalidParameter)
)
