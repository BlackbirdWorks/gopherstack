package backup

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Wire error codes match the real AWS Backup service-2.json model exactly
// (verified against botocore's backup/2018-11-15 model): every one of
// Backup's client-fault exceptions (ResourceNotFoundException,
// AlreadyExistsException, InvalidParameterValueException,
// MissingParameterValueException, InvalidRequestException, ...) has no
// explicit httpStatusCode override in the model, so the restJson1 protocol
// default of HTTP 400 applies uniformly -- Backup does NOT use 404 for
// not-found or 409 for conflict the way many other REST-JSON services do.
// "ValidationException" is NOT a real AWS Backup error code (gopherstack
// invention, since deleted); the real generic client-fault codes are
// InvalidParameterValueException / MissingParameterValueException, chosen
// in handleError based on whether the message names a missing field.
var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrConflict)
	ErrValidation    = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)
	// ErrInvalidRequest is for state-conflict validation failures (e.g.
	// deleting a non-empty or locked vault) that real AWS Backup reports as
	// InvalidRequestException rather than InvalidParameterValueException.
	ErrInvalidRequest = awserr.New("InvalidRequestException", awserr.ErrConflict)
)

var (
	errProtectedResourceNotFound = errors.New("protected resource not found")
	errReportJobNotFound         = errors.New("report job not found")
	errScanJobNotFound           = errors.New("scan job not found")
	errLegalHoldNotFound         = errors.New("legal hold not found")
	errRecoveryPointNotFound     = errors.New("recovery point not found")
	errVaultNotFoundB1           = errors.New("vault not found")
	errBackupJobNotFound         = errors.New("backup job not found")
	errBackupPlanNotFoundB1      = errors.New("backup plan not found")
)

var errInvalidRequest = errors.New("invalid request")
