package appsync

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("BadRequestException", awserr.ErrAlreadyExists)
	// ErrInvalidSchema is returned when the provided schema SDL is invalid.
	ErrInvalidSchema = errors.New("InvalidSchemaError")
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrUnsupportedJSCode is returned when EvaluateCode is given an APPSYNC_JS
	// construct the emulator's evaluator does not support. The request is
	// well-formed, but the code uses features beyond the documented patterns the
	// emulator faithfully evaluates (the emulator does not embed a JS engine).
	ErrUnsupportedJSCode = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)
