package azureblob

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend defines the interface for an Azure Blob Storage backend.
// Shaped after services/sqs's StorageBackend: a narrow, testable seam between
// the wire handler and storage, so handler tests can substitute a fake.
type StorageBackend interface {
	CreateContainerWith(name string, opts ContainerOptions) (ContainerInfo, error)
	DeleteContainer(name string, pre ...Precond) error
	ListContainers() []ContainerInfo
	ListContainersPage(opts ListContainersOptions) (ContainerPage, error)
	GetContainer(name string, pre ...Precond) (ContainerInfo, error)
	SetContainerMetadata(name string, md map[string]string, pre ...Precond) (ContainerInfo, error)
	SetContainerACL(name, publicAccess string, ids []SignedIdentifier, pre ...Precond) (ContainerInfo, error)
	LeaseContainer(container string, req LeaseRequest) (LeaseResult, error)

	PutBlobWith(container, blob string, data []byte, attrs BlobAttrs, pre ...Precond) (BlobInfo, error)
	GetBlobAt(container, blob, snapshot string, pre ...Precond) (BlobInfo, []byte, error)
	HeadBlobAt(container, blob, snapshot string, pre ...Precond) (BlobInfo, error)
	DeleteBlobAt(container, blob, snapshot, mode string, pre ...Precond) error
	ListBlobsPage(container string, opts ListBlobsOptions) (BlobPage, error)
	SetBlobProperties(container, blob string, attrs BlobAttrs, pre ...Precond) (BlobInfo, error)
	SetBlobMetadata(container, blob string, md map[string]string, pre ...Precond) (BlobInfo, error)
	SetBlobTier(container, blob, tier string, pre ...Precond) (string, BlobInfo, error)
	SnapshotBlob(container, blob string, md map[string]string, pre ...Precond) (BlobInfo, error)
	CopyBlob(
		srcContainer, srcBlob, srcSnapshot, dstContainer, dstBlob string, md map[string]string, pre ...Precond,
	) (BlobInfo, error)
	LeaseBlob(container, blob string, req LeaseRequest) (LeaseResult, error)

	PutBlock(container, blob, blockID string, data []byte, pre ...Precond) error
	CommitBlockList(container, blob string, refs []BlockRef, attrs BlobAttrs, pre ...Precond) (BlobInfo, error)
	GetBlockList(container, blob, snapshot, listType string, pre ...Precond) (BlockList, error)

	// Reset clears all in-memory state. Used by the
	// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
	Reset()
}
