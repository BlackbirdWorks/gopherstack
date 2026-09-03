package azureblob

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend defines the interface for an Azure Blob Storage backend.
// Shaped after services/sqs's StorageBackend: a narrow, testable seam between
// the wire handler and storage, so handler tests can substitute a fake.
type StorageBackend interface {
	CreateContainer(name string) error
	DeleteContainer(name string) error
	ListContainers() []ContainerInfo

	PutBlob(container, blob string, data []byte, contentType string) (BlobInfo, error)
	GetBlob(container, blob string) (BlobInfo, []byte, error)
	HeadBlob(container, blob string) (BlobInfo, error)
	DeleteBlob(container, blob string) error
	ListBlobs(container string) ([]BlobInfo, error)

	// Reset clears all in-memory state. Used by the
	// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
	Reset()
}
