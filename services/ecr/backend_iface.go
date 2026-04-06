package ecr

// Backend defines the interface for ECR control-plane operations.
// InMemoryBackend implements this interface; alternative backends (e.g. one
// that delegates to a real Docker daemon registry, or a test double) can
// implement it too, keeping the Handler backend-agnostic.
type Backend interface {
	// CreateRepository creates a new ECR repository and returns its metadata.
	// Returns ErrRepositoryAlreadyExists if a repository with that name already
	// exists, or ErrInvalidRepositoryName when name is empty.
	// imageTagMutability defaults to "MUTABLE" when empty.
	CreateRepository(name, imageTagMutability string) (*Repository, error)

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

	// BatchCheckLayerAvailability checks the availability of image layers.
	BatchCheckLayerAvailability(
		repositoryName string,
		layerDigests []string,
	) ([]LayerAvailability, []LayerFailure, error)

	// BatchDeleteImage deletes specified images from a repository.
	BatchDeleteImage(repositoryName string, imageIDs []ImageIdentifier) ([]ImageIdentifier, []ImageFailure, error)

	// BatchGetImage gets details for specified images.
	BatchGetImage(repositoryName string, imageIDs []ImageIdentifier) ([]Image, []ImageFailure, error)

	// BatchGetRepositoryScanningConfiguration gets scanning config for repositories.
	BatchGetRepositoryScanningConfiguration(
		repositoryNames []string,
	) ([]RepositoryScanningConfiguration, []RepositoryScanningConfigurationFailure, error)

	// CompleteLayerUpload completes the upload of an image layer.
	CompleteLayerUpload(repositoryName, uploadID string, layerDigests []string) (*CompleteLayerUploadResult, error)

	// CreatePullThroughCacheRule creates a pull-through cache rule.
	CreatePullThroughCacheRule(
		prefix, upstreamURL, credentialArn, upstreamRegistry string,
	) (*PullThroughCacheRule, error)

	// CreateRepositoryCreationTemplate creates a repository creation template.
	CreateRepositoryCreationTemplate(req *RepositoryCreationTemplate) (*RepositoryCreationTemplate, error)

	// DeleteLifecyclePolicy deletes the lifecycle policy for a repository.
	DeleteLifecyclePolicy(repositoryName string) (*LifecyclePolicyResult, error)

	// PutLifecyclePolicy creates or replaces the lifecycle policy for a repository.
	PutLifecyclePolicy(repositoryName, policyText string) (*LifecyclePolicyResult, error)

	// DeletePullThroughCacheRule deletes a pull-through cache rule by prefix.
	DeletePullThroughCacheRule(prefix string) (*PullThroughCacheRule, error)

	// DeleteRegistryPolicy deletes the registry-level policy.
	DeleteRegistryPolicy() (*RegistryPolicyResult, error)

	// PutRegistryPolicy creates or replaces the registry-level IAM policy.
	PutRegistryPolicy(policyText string) (*RegistryPolicyResult, error)

	// TagResource associates tags with a resource identified by ARN.
	TagResource(resourceArn string, tags map[string]string) error

	// UntagResource removes tags from a resource identified by ARN.
	UntagResource(resourceArn string, tagKeys []string) error

	// ListTagsForResource returns all tags for a resource identified by ARN.
	ListTagsForResource(resourceArn string) (map[string]string, error)

	// Reset clears all backend state.
	Reset()
}

// Snapshottable is an optional interface that a Backend may implement to
// support state serialisation and restoration (e.g. for --persist mode).
// Backends that do not implement it are silently skipped during snapshot/restore.
type Snapshottable interface {
	Snapshot() []byte
	Restore(data []byte) error
}
