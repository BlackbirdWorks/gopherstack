package rdsdata

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrTransactionNotFound is returned when a transaction does not exist.
	ErrTransactionNotFound = awserr.New("TransactionNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

// errIsValidation reports whether err wraps ErrValidation.
func errIsValidation(err error) bool {
	return errors.Is(err, awserr.ErrInvalidParameter)
}
