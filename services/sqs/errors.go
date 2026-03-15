package sqs

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors for SQS operations.
var (
	ErrQueueNotFound            = awserr.New("AWS.SimpleQueueService.NonExistentQueue", awserr.ErrNotFound)
	ErrQueueAlreadyExists       = awserr.New("QueueAlreadyExists", awserr.ErrAlreadyExists)
	ErrInvalidAttribute         = errors.New("InvalidAttributeValue")
	ErrInvalidBatchEntry        = errors.New("AWS.SimpleQueueService.EmptyBatchRequest")
	ErrReceiptHandleInvalid     = errors.New("ReceiptHandleIsInvalid")
	ErrMessageNotInflight       = errors.New("MessageNotInflight")
	ErrTooManyEntriesInBatch    = errors.New("AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
	ErrBatchEntryIDsNotDistinct = errors.New("AWS.SimpleQueueService.BatchEntryIdsNotDistinct")
	ErrUnknownAction            = errors.New("InvalidAction")
	ErrMessageTooLarge          = errors.New("MessageTooLarge")
	ErrInvalidWaitTime          = errors.New("InvalidParameterValue")
	ErrInvalidVisibilityTimeout = errors.New("InvalidParameterValue.VisibilityTimeout")
	ErrMissingMessageGroupID    = errors.New("InvalidParameterValue.MissingMessageGroupID")
	ErrMissingDeduplicationID   = errors.New("InvalidParameterValue.MissingDeduplicationID")
)
