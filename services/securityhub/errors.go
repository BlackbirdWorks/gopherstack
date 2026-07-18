package securityhub

import "errors"

var (
	ErrHubNotEnabled    = errors.New("SecurityHub is not enabled")
	ErrHubAlreadyExists = errors.New("SecurityHub is already enabled")
	ErrNotFound         = errors.New("not found")
	ErrInvalidInput     = errors.New("invalid input")
	ErrAlreadyExists    = errors.New("resource already exists")
)
