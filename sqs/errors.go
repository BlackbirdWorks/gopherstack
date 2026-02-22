package sqs

import "errors"

// Sentinel errors for SQS operations.
var (
	ErrQueueNotFound         = errors.New("AWS.SimpleQueueService.NonExistentQueue")
	ErrQueueAlreadyExists    = errors.New("QueueAlreadyExists")
	ErrInvalidAttribute      = errors.New("InvalidAttributeValue")
	ErrInvalidBatchEntry     = errors.New("AWS.SimpleQueueService.EmptyBatchRequest")
	ErrReceiptHandleInvalid  = errors.New("ReceiptHandleIsInvalid")
	ErrTooManyEntriesInBatch = errors.New("AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
	ErrUnknownAction         = errors.New("InvalidAction")
)
