package azureblob

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend defines the interface for an Azure Blob Storage backend.
// Shaped after services/sqs's StorageBackend: a narrow, testable seam between
// the wire handler and storage, so handler tests can substitute a fake.
type StorageBackend interface {
	CreateContainerWith(name string, opts ContainerOptions) (ContainerInfo, error)
	DeleteContainer(name string) error
	ListContainers() []ContainerInfo
	GetContainer(name string) (ContainerInfo, error)
	SetContainerMetadata(name string, md map[string]string) (ContainerInfo, error)
	SetContainerACL(name, publicAccess string, ids []SignedIdentifier) (ContainerInfo, error)

	PutBlobWith(container, blob string, data []byte, attrs BlobAttrs) (BlobInfo, error)
	GetBlobAt(container, blob, snapshot string) (BlobInfo, []byte, error)
	HeadBlobAt(container, blob, snapshot string) (BlobInfo, error)
	DeleteBlobAt(container, blob, snapshot, mode string) error
	ListBlobsPage(container string, opts ListBlobsOptions) (BlobPage, error)
	SetBlobProperties(container, blob string, attrs BlobAttrs) (BlobInfo, error)
	SetBlobMetadata(container, blob string, md map[string]string) (BlobInfo, error)
	SetBlobTier(container, blob, tier string) (string, BlobInfo, error)
	SnapshotBlob(container, blob string, md map[string]string) (BlobInfo, error)
	CopyBlob(srcContainer, srcBlob, srcSnapshot, dstContainer, dstBlob string, md map[string]string) (BlobInfo, error)

	// Reset clears all in-memory state. Used by the
	// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
	Reset()
}
