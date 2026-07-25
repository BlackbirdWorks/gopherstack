package acm

import "errors"

var (
	ErrCertNotFound      = errors.New("ResourceNotFoundException")
	ErrInvalidParameter  = errors.New("ValidationException")
	ErrNotEligible       = errors.New("RequestInProgressException")
	ErrRequestInProgress = errors.New("RequestInProgressException")
	ErrAlreadyRevoked    = errors.New("InvalidStateException")
	ErrInvalidState      = errors.New("InvalidStateException")
	ErrResourceInUse     = errors.New("ResourceInUseException")
	ErrConflict          = errors.New("ConflictException")
	// ErrInvalidArn is returned when a CertificateArn does not match the
	// expected ACM ARN shape (arn:<partition>:acm:<region>:<account>:certificate/<id>).
	// Real AWS returns InvalidArnException for malformed ARNs, distinct from
	// ResourceNotFoundException (well-formed ARN, no such resource).
	ErrInvalidArn = errors.New("InvalidArnException")
	// ErrLimitExceeded is returned when an ACM account/resource quota (e.g. the
	// per-certificate domain-name count) is exceeded.
	ErrLimitExceeded = errors.New("LimitExceededException")
	// ErrTooManyTags is returned when a tagging operation would exceed the
	// maximum of 50 tags per certificate.
	ErrTooManyTags = errors.New("TooManyTagsException")
	// ErrInvalidTag is returned when a tag key or value fails AWS tag
	// constraints (e.g. the reserved "aws:" prefix).
	ErrInvalidTag = errors.New("InvalidTagException")
	// ErrInvalidDomainValidationOptions is returned when the
	// DomainValidationOptions input to RequestCertificate references a domain
	// not in the request, or specifies a ValidationDomain that is not the
	// same as or a superdomain of its DomainName.
	ErrInvalidDomainValidationOptions = errors.New("InvalidDomainValidationOptionsException")
	errInvalidPEM                     = errors.New("failed to decode PEM block")
)

var errWeakKey = errors.New("RSA_1024 is not supported due to weak security")
