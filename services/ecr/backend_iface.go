package ecr

// Backend defines the interface for ECR control-plane operations.
// InMemoryBackend implements this interface; alternative backends (e.g. one
// that delegates to a real Docker daemon registry, or a test double) can
// implement it too, keeping the Handler backend-agnostic.
type Backend interface {
	// CreateRepository creates a new ECR repository and returns its metadata.
	// Returns ErrRepositoryAlreadyExists if a repository with that name already
	// exists, or ErrInvalidRepositoryName when name is empty.
	CreateRepository(name string) (*Repository, error)

	// DescribeRepositories returns repository metadata, optionally filtered by
	// the provided names. Passing an empty slice returns all repositories.
	// Returns ErrRepositoryNotFound if any requested name does not exist.
	DescribeRepositories(names []string) ([]Repository, error)

	// DeleteRepository removes the named repository and returns its metadata.
	// Returns ErrRepositoryNotFound if the repository does not exist.
	DeleteRepository(name string) (*Repository, error)

	// ProxyEndpoint returns the registry endpoint embedded in repository URIs
	// and returned by GetAuthorizationToken.
	ProxyEndpoint() string

	// SetEndpoint updates the registry endpoint used in new repository URIs.
	// It should be called once the server's listening address is known.
	SetEndpoint(endpoint string)
}

// Snapshottable is an optional interface that a Backend may implement to
// support state serialisation and restoration (e.g. for --persist mode).
// Backends that do not implement it are silently skipped during snapshot/restore.
type Snapshottable interface {
	Snapshot() []byte
	Restore(data []byte) error
}
