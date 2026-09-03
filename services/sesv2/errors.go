package sesv2

import "errors"

// Errors returned by the SES v2 backend.
var (
	ErrNotFound      = errors.New("NotFoundException")
	ErrAlreadyExists = errors.New("AlreadyExistsException")
	ErrInvalidInput  = errors.New("BadRequestException")
	// ErrMailFromDomainNotVerified is returned by SendEmail (and its siblings
	// sharing checkFromIdentityLocked) when the From identity/domain isn't
	// verified for sending, matching SendEmail's own declared error model
	// (sesv2@v1.66.4 types/errors.go:220).
	ErrMailFromDomainNotVerified = errors.New("MailFromDomainNotVerifiedException")
)

// Aliases for backward compatibility within the package.
var (
	ErrIdentityNotFound       = ErrNotFound
	ErrIdentityAlreadyExists  = ErrAlreadyExists
	ErrConfigSetNotFound      = ErrNotFound
	ErrConfigSetAlreadyExists = ErrAlreadyExists
	ErrInvalidParameter       = ErrInvalidInput
)
