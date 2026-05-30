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
	// encryptionType defaults to "AES256" when empty; kmsKey is only used with "KMS".
	CreateRepository(
		name, imageTagMutability string,
		scanOnPush bool,
		encryptionType, kmsKey string,
	) (*Repository, error)

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

	// DescribeImages returns image details for a repository.
	DescribeImages(repositoryName string, imageIDs []ImageIdentifier) ([]Image, error)

	// BatchGetRepositoryScanningConfiguration gets scanning config for repositories.
	BatchGetRepositoryScanningConfiguration(
		repositoryNames []string,
	) ([]RepositoryScanningConfiguration, []RepositoryScanningConfigurationFailure, error)

	// CompleteLayerUpload completes the upload of an image layer.
	CompleteLayerUpload(repositoryName, uploadID string, layerDigests []string) (*CompleteLayerUploadResult, error)

	// GetDownloadURLForLayer resolves a download URL for a repository layer.
	GetDownloadURLForLayer(repositoryName, layerDigest string) (string, error)

	// InitiateLayerUpload starts a layer upload session.
	InitiateLayerUpload(repositoryName string) (*LayerUploadInitiation, error)

	// UploadLayerPart records uploaded bytes for an existing session.
	UploadLayerPart(
		repositoryName, uploadID string,
		firstByte, lastByte int64,
		blob []byte,
	) (*LayerUploadPartResult, error)

	// CreatePullThroughCacheRule creates a pull-through cache rule.
	CreatePullThroughCacheRule(
		prefix, upstreamURL, credentialArn, upstreamRegistry, customRoleArn, upstreamRepositoryPrefix string,
	) (*PullThroughCacheRule, error)

	// DescribePullThroughCacheRules lists pull-through cache rules.
	DescribePullThroughCacheRules(prefixes []string) ([]PullThroughCacheRule, error)

	// CreateRepositoryCreationTemplate creates a repository creation template.
	CreateRepositoryCreationTemplate(req *RepositoryCreationTemplate) (*RepositoryCreationTemplate, error)

	// DeleteRepositoryCreationTemplate deletes a repository creation template.
	DeleteRepositoryCreationTemplate(prefix string) (*RepositoryCreationTemplate, error)

	// DescribeRepositoryCreationTemplates lists repository creation templates.
	DescribeRepositoryCreationTemplates(prefixes []string) ([]RepositoryCreationTemplate, error)

	// DeleteLifecyclePolicy deletes the lifecycle policy for a repository.
	DeleteLifecyclePolicy(repositoryName string) (*LifecyclePolicyResult, error)

	// GetLifecyclePolicy returns the lifecycle policy for a repository.
	GetLifecyclePolicy(repositoryName string) (*LifecyclePolicyResult, error)

	// GetLifecyclePolicyPreview returns the current lifecycle policy preview.
	GetLifecyclePolicyPreview(repositoryName string) (*LifecyclePolicyPreviewResult, error)

	// PutLifecyclePolicy creates or replaces the lifecycle policy for a repository.
	PutLifecyclePolicy(repositoryName, policyText string) (*LifecyclePolicyResult, error)

	// StartLifecyclePolicyPreview starts or refreshes a lifecycle policy preview.
	StartLifecyclePolicyPreview(repositoryName, policyText string) (*LifecyclePolicyPreviewResult, error)

	// DeletePullThroughCacheRule deletes a pull-through cache rule by prefix.
	DeletePullThroughCacheRule(prefix string) (*PullThroughCacheRule, error)

	// UpdatePullThroughCacheRule updates a pull-through cache rule by prefix.
	UpdatePullThroughCacheRule(prefix, credentialArn, customRoleArn string) (*PullThroughCacheRule, error)

	// ValidatePullThroughCacheRule validates a pull-through cache rule by prefix.
	ValidatePullThroughCacheRule(prefix string) (*ValidatePullThroughCacheRuleResult, error)

	// DeleteRegistryPolicy deletes the registry-level policy.
	DeleteRegistryPolicy() (*RegistryPolicyResult, error)

	// DescribeRegistry returns registry-wide metadata.
	DescribeRegistry() (*RegistryDescription, error)

	// GetRegistryPolicy returns the registry-level policy.
	GetRegistryPolicy() (*RegistryPolicyResult, error)

	// GetRegistryScanningConfiguration returns the registry scanning configuration.
	GetRegistryScanningConfiguration() (*RegistryScanningSettings, error)

	// PutRegistryPolicy creates or replaces the registry-level IAM policy.
	PutRegistryPolicy(policyText string) (*RegistryPolicyResult, error)

	// PutRegistryScanningConfiguration updates the registry scanning configuration.
	PutRegistryScanningConfiguration(*RegistryScanningSettings) (*RegistryScanningSettings, error)

	// PutReplicationConfiguration updates the registry replication configuration.
	PutReplicationConfiguration(*ReplicationConfig) (*ReplicationConfig, error)

	// GetRepositoryPolicy returns the repository-level policy.
	GetRepositoryPolicy(repositoryName string) (*RepositoryPolicyResult, error)

	// SetRepositoryPolicy creates or replaces the repository-level IAM policy.
	SetRepositoryPolicy(repositoryName, policyText string) (*RepositoryPolicyResult, error)

	// DeleteRepositoryPolicy deletes the repository-level policy.
	DeleteRepositoryPolicy(repositoryName string) (*RepositoryPolicyResult, error)

	// GetSigningConfiguration returns the current registry signing configuration.
	GetSigningConfiguration() (*SigningSettings, error)

	// PutSigningConfiguration updates the registry signing configuration.
	PutSigningConfiguration(*SigningSettings) (*SigningSettings, error)

	// DeleteSigningConfiguration removes the registry signing configuration.
	DeleteSigningConfiguration() (*SigningSettings, error)

	// DescribeImageSigningStatus returns signing status for an image.
	DescribeImageSigningStatus(repositoryName string, imageID ImageIdentifier) (*ImageSigningStatusResult, error)

	// DescribeImageScanFindings returns scan findings for an image.
	DescribeImageScanFindings(repositoryName string, imageID ImageIdentifier) (*ImageScanFindingsResult, error)

	// StartImageScan starts an image scan and returns the scan status.
	StartImageScan(repositoryName string, imageID ImageIdentifier) (*ImageScanStartResult, error)

	// ListImages lists image identifiers for a repository.
	// tagStatusFilter can be "TAGGED", "UNTAGGED", or "" / "ANY" for all images.
	ListImages(repositoryName, tagStatusFilter string) ([]ImageIdentifier, error)

	// ListImageReferrers lists image referrers for a subject image.
	ListImageReferrers(repositoryName string, subject ImageIdentifier) ([]ImageReferrer, error)

	// PutImage creates or replaces an image manifest.
	PutImage(repositoryName string, image Image) (*Image, error)

	// PutImageScanningConfiguration updates per-repository scan-on-push config.
	PutImageScanningConfiguration(repositoryName string, scanOnPush bool) (*RepositoryScanningConfiguration, error)

	// PutImageTagMutability updates per-repository tag mutability.
	PutImageTagMutability(
		repositoryName, imageTagMutability string,
		exclusionFilters []ImageTagMutabilityExclusionFilter,
	) (*Repository, error)

	// DescribeImageReplicationStatus returns the current replication status for an image.
	DescribeImageReplicationStatus(
		repositoryName string,
		imageID ImageIdentifier,
	) (*ImageReplicationStatusResult, error)

	// UpdateImageStorageClass updates the storage class for an image.
	UpdateImageStorageClass(
		repositoryName string,
		imageID ImageIdentifier,
		target string,
	) (*ImageStorageClassResult, error)

	// GetAccountSetting returns a registry account setting.
	GetAccountSetting(name string) (string, error)

	// PutAccountSetting updates a registry account setting.
	PutAccountSetting(name, value string) (string, error)

	// RegisterPullTimeUpdateExclusion creates a pull time update exclusion.
	RegisterPullTimeUpdateExclusion(principalArn string) (*PullTimeUpdateExclusion, error)

	// DeregisterPullTimeUpdateExclusion deletes a pull time update exclusion.
	DeregisterPullTimeUpdateExclusion(principalArn string) (*PullTimeUpdateExclusion, error)

	// ListPullTimeUpdateExclusions lists pull time update exclusions.
	ListPullTimeUpdateExclusions() ([]PullTimeUpdateExclusion, error)

	// UpdateRepositoryCreationTemplate updates a repository creation template.
	UpdateRepositoryCreationTemplate(req *RepositoryCreationTemplate) (*RepositoryCreationTemplate, error)

	// TagResource associates tags with a resource identified by ARN.
	TagResource(resourceArn string, tags map[string]string) error

	// UntagResource removes tags from a resource identified by ARN.
	UntagResource(resourceArn string, tagKeys []string) error

	// ListTagsForResource returns all tags for a resource identified by ARN.
	ListTagsForResource(resourceArn string) (map[string]string, error)

	// Reset clears all backend state.
	Reset()

	// AccountID returns the AWS account ID associated with this registry.
	AccountID() string
}

// Snapshottable is an optional interface that a Backend may implement to
// support state serialisation and restoration (e.g. for --persist mode).
// Backends that do not implement it are silently skipped during snapshot/restore.
type Snapshottable interface {
	Snapshot() []byte
	Restore(data []byte) error
}
