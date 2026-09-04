// Package azurequeue provides a local, in-memory emulation of Azure Queue
// Storage's REST+XML wire protocol (queue CRUD plus the full message
// lifecycle: put/get/peek/delete/update-visibility/clear), Azurite-compatible
// enough for unmodified azure-sdk-for-go clients to operate against. See
// AZURE.md and PARITY.md for scope and known gaps.
package azurequeue

import "time"

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend defines the interface for an Azure Queue Storage backend.
// Shaped after services/azureblob's StorageBackend: a narrow, testable seam
// between the wire handler and storage, so handler tests can substitute a
// fake.
type StorageBackend interface {
	CreateQueue(name string) (created bool, err error)
	DeleteQueue(name string) error
	ListQueues() []QueueInfo

	PutMessage(queue, text string, visibilityTimeout, ttl time.Duration) (MessageInfo, error)
	// GetMessages and PeekMessages return ErrOutOfRangeQueryParam if
	// numOfMessages is outside [MinNumOfMessages, MaxNumOfMessages] -- a
	// defense-in-depth check against direct callers that bypass the
	// handler's own range validation (see messages.go's parseNumOfMessages),
	// since an unvalidated negative value would otherwise panic on
	// allocation.
	GetMessages(queue string, numOfMessages int, visibilityTimeout time.Duration) ([]MessageInfo, error)
	PeekMessages(queue string, numOfMessages int) ([]MessageInfo, error)
	DeleteMessage(queue, messageID, popReceipt string) error
	UpdateMessage(
		queue, messageID, popReceipt string, visibilityTimeout time.Duration, text *string,
	) (MessageInfo, error)
	ClearMessages(queue string) error

	// Reset clears all in-memory state. Used by the
	// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
	Reset()
}
