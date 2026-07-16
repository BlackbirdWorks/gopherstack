package backup

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

var (
	errProtectedResourceNotFound = errors.New("protected resource not found")
	errRestoreJobNotFound        = errors.New("restore job not found")
	errReportJobNotFound         = errors.New("report job not found")
	errScanJobNotFound           = errors.New("scan job not found")
	errLegalHoldNotFound         = errors.New("legal hold not found")
	errRecoveryPointNotFound     = errors.New("recovery point not found")
	errVaultNotFoundB1           = errors.New("vault not found")
	errBackupJobNotFound         = errors.New("backup job not found")
	errBackupPlanNotFoundB1      = errors.New("backup plan not found")
	errTieringConfigNotFound     = errors.New("tiering configuration not found for vault")
)

var errInvalidRequest = errors.New("invalid request")
