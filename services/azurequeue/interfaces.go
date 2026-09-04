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
