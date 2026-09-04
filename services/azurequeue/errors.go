package azurequeue

import "errors"

// Sentinel errors for Azure Queue Storage operations.
var (
	ErrQueueNotFound        = errors.New("azurequeue: queue not found")
	ErrQueueAlreadyExists   = errors.New("azurequeue: queue already exists with different metadata")
	ErrMessageNotFound      = errors.New("azurequeue: message not found")
	ErrPopReceiptMismatch   = errors.New("azurequeue: pop receipt mismatch")
	ErrInvalidQueryParam    = errors.New("azurequeue: invalid query parameter value")
	ErrOutOfRangeQueryParam = errors.New("azurequeue: query parameter value out of range")

	// ErrSnapshotQueueNull and ErrSnapshotMessageNull are returned by Restore
	// when a snapshot's "queues" map (or a queue's "Messages" slice) holds a
	// JSON null entry, which decodes to a nil pointer that would panic on
	// first dereference if stored as-is. See persistence.go.
	ErrSnapshotQueueNull   = errors.New("azurequeue: restore snapshot: queue is null")
	ErrSnapshotMessageNull = errors.New("azurequeue: restore snapshot: message is null")
)
