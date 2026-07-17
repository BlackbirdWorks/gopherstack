package lakeformation

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const errCodeInvalidInput = "InvalidInputException"

// ErrValidation is returned when input validation fails.
var ErrValidation = errors.New("validation error")

// errTransactionCommitted indicates an operation that requires a transaction
// to not be committed yet (CancelTransaction, ExtendTransaction,
// DeleteObjectsOnCancel) was attempted on a transaction that already
// committed. It wraps awserr.ErrConflict so existing errors.Is(err,
// awserr.ErrConflict) checks keep matching; handler.go additionally checks
// errors.Is(err, errTransactionCommitted) first so this maps to the AWS
// TransactionCommittedException wire error instead of the
// TransactionCanceledException used for the "already aborted" conflict case.
// Per the real aws-sdk-go-v2 deserializers, CancelTransaction's valid error
// set includes TransactionCommittedException but NOT TransactionCanceledException
// -- the reverse of CommitTransaction's valid set -- so these must not share
// one error code.
var errTransactionCommitted = fmt.Errorf("transaction already committed: %w", awserr.ErrConflict)
