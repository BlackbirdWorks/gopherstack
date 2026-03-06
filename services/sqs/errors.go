package sqs

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors for SQS operations.
var (
	ErrQueueNotFound         = awserr.New("AWS.SimpleQueueService.NonExistentQueue", awserr.ErrNotFound)
	ErrQueueAlreadyExists    = awserr.New("QueueAlreadyExists", awserr.ErrAlreadyExists)
	ErrInvalidAttribute      = errors.New("InvalidAttributeValue")
	ErrInvalidBatchEntry     = errors.New("AWS.SimpleQueueService.EmptyBatchRequest")
	ErrReceiptHandleInvalid  = errors.New("ReceiptHandleIsInvalid")
	ErrMessageNotInflight    = errors.New("MessageNotInflight")
	ErrTooManyEntriesInBatch = errors.New("AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
	ErrUnknownAction         = errors.New("InvalidAction")
)
