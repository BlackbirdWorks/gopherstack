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
	errInvalidPEM        = errors.New("failed to decode PEM block")
)

var errWeakKey = errors.New("RSA_1024 is not supported due to weak security")
