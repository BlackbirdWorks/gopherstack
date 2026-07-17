package sesv2

import "errors"

// Errors returned by the SES v2 backend.
var (
	ErrNotFound      = errors.New("NotFoundException")
	ErrAlreadyExists = errors.New("AlreadyExistsException")
	ErrInvalidInput  = errors.New("BadRequestException")
)

// Aliases for backward compatibility within the package.
var (
	ErrIdentityNotFound       = ErrNotFound
	ErrIdentityAlreadyExists  = ErrAlreadyExists
	ErrConfigSetNotFound      = ErrNotFound
	ErrConfigSetAlreadyExists = ErrAlreadyExists
	ErrInvalidParameter       = ErrInvalidInput
)
