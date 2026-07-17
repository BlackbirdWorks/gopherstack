package elasticbeanstalk

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a requested resource does not exist. It is
	// used by name-based lookups (application/environment/version/template),
	// which AWS surfaces as the generic InvalidParameterValue sender error.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrResourceNotFound is returned specifically when a ListTagsForResource
	// or UpdateTagsForResource ResourceArn does not match any known resource.
	// Unlike ErrNotFound, AWS documents this ARN-lookup case as a distinct
	// modeled exception (ResourceNotFoundException, HTTP 400) rather than
	// the generic InvalidParameterValue used by name-based CRUD ops -- see
	// handleOpError's mapping table.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
	// ErrUnknownAction is returned when an unknown action is requested.
	ErrUnknownAction = awserr.New("UnknownOperationException", awserr.ErrInvalidParameter)
	// ErrInvalidParameter is returned when a required parameter is missing or invalid.
	ErrInvalidParameter = awserr.New("InvalidParameterValue", awserr.ErrInvalidParameter)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
