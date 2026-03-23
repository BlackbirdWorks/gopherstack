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
	ErrTaskHandleInvalid        = errors.New("InvalidParameterValue.TaskHandle")
	ErrInvalidPermissionLabel   = errors.New("InvalidParameterValue.PermissionLabel")
	ErrMoveTaskAlreadyRunning   = errors.New("ResourceInConflict.MoveTaskAlreadyRunning")
	// ErrMoveTaskNotRunning is returned by CancelMessageMoveTask when the referenced
	// task exists but is not in RUNNING or CANCELLING status.
	ErrMoveTaskNotRunning = errors.New("ResourceInConflict.MoveTaskNotRunning")
	// ErrInvalidPermissionActions is returned by AddPermission when Actions is empty.
	ErrInvalidPermissionActions = errors.New("InvalidParameterValue.PermissionActions")
	// ErrInvalidPermissionAccountIDs is returned by AddPermission when AWSAccountIDs is empty.
	ErrInvalidPermissionAccountIDs = errors.New("InvalidParameterValue.PermissionAccountIDs")
	// ErrInvalidSourceArn is returned by StartMessageMoveTask when SourceArn is empty or invalid.
	ErrInvalidSourceArn = errors.New("InvalidParameterValue.SourceArn")
	// ErrInvalidMaxMessagesPerSecond is returned by StartMessageMoveTask when
	// MaxNumberOfMessagesPerSecond is negative.
	ErrInvalidMaxMessagesPerSecond = errors.New("InvalidParameterValue.MaxNumberOfMessagesPerSecond")
)
