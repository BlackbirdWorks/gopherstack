package ecr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	layerUploadPartSize = 20 * 1024 * 1024
	// layerUploadTTL bounds how long an in-progress layer upload is retained
	// before it is treated as abandoned and pruned. AWS expires unfinished
	// uploads after a period of inactivity; this prevents the layerUploads map
	// from leaking entries for uploads that are initiated but never completed.
	layerUploadTTL      = 24 * time.Hour
	scanTypeBasic       = "BASIC"
	mutabilityMutable   = "MUTABLE"
	mutabilityImmutable = "IMMUTABLE"
	scanStatusComplete  = "COMPLETE"
	imageStatusActive   = "ACTIVE"
	msgNoScanFindings   = "The scan completed successfully with no findings."
)

var (
	// ErrRepositoryNotFound is returned when a repository does not exist.
	ErrRepositoryNotFound = awserr.New("RepositoryNotFoundException", awserr.ErrNotFound)
	// ErrRepositoryAlreadyExists is returned when a repository already exists.
	ErrRepositoryAlreadyExists = awserr.New("RepositoryAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrRepositoryNotEmpty is returned when deleting a non-empty repository without the force flag.
	ErrRepositoryNotEmpty = awserr.New("RepositoryNotEmptyException", awserr.ErrConflict)
	// ErrImageTagAlreadyExists is returned when re-tagging an image in an IMMUTABLE repository.
	ErrImageTagAlreadyExists = awserr.New("ImageTagAlreadyExistsException", awserr.ErrConflict)
	// ErrInvalidRepositoryName is returned when the repository name is invalid.
	ErrInvalidRepositoryName = errors.New("InvalidParameterException")
	// ErrPullThroughCacheRuleNotFound is returned when a pull-through cache rule does not exist.
	ErrPullThroughCacheRuleNotFound = awserr.New("PullThroughCacheRuleNotFoundException", awserr.ErrNotFound)
	// ErrPullThroughCacheRuleAlreadyExists is returned when a pull-through cache rule already exists.
	ErrPullThroughCacheRuleAlreadyExists = awserr.New(
		"PullThroughCacheRuleAlreadyExistsException",
		awserr.ErrAlreadyExists,
	)
	// ErrLifecyclePolicyNotFound is returned when a lifecycle policy does not exist.
	ErrLifecyclePolicyNotFound = awserr.New("LifecyclePolicyNotFoundException", awserr.ErrNotFound)
	// ErrRepositoryCreationTemplateNotFound is returned when a creation template does not exist.
	ErrRepositoryCreationTemplateNotFound = awserr.New("TemplateNotFoundException", awserr.ErrNotFound)
	// ErrRepositoryCreationTemplateAlreadyExists is returned when a creation template prefix already exists.
	ErrRepositoryCreationTemplateAlreadyExists = awserr.New("TemplateAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrRegistryPolicyNotFound is returned when the registry policy does not exist.
	ErrRegistryPolicyNotFound = awserr.New("RegistryPolicyNotFoundException", awserr.ErrNotFound)
	// ErrRepositoryPolicyNotFound is returned when a repository-level IAM policy does not exist.
	ErrRepositoryPolicyNotFound = awserr.New("RepositoryPolicyNotFoundException", awserr.ErrNotFound)
	// ErrImageNotFound is returned when a requested image does not exist in a repository.
	ErrImageNotFound = awserr.New("ImageNotFoundException", awserr.ErrNotFound)
)

// Repository represents an ECR repository.
type Repository struct {
	CreatedAt                          time.Time                           `json:"createdAt"`
	EncryptionType                     string                              `json:"encryptionType"`
	KMSKey                             string                              `json:"kmsKey,omitempty"`
	RegistryID                         string                              `json:"registryId"`
	RepositoryARN                      string                              `json:"repositoryArn"`
	RepositoryName                     string                              `json:"repositoryName"`
	RepositoryURI                      string                              `json:"repositoryUri"`
	ImageTagMutability                 string                              `json:"imageTagMutability"`
	ImageTagMutabilityExclusionFilters []ImageTagMutabilityExclusionFilter `json:"imageTagMutabilityExclusionFilters,omitempty"` //nolint:lll // AWS-compatible JSON field name is long.
	ScanOnPush                         bool                                `json:"scanOnPush"`
}

// ImageIdentifier identifies a specific image by digest or tag.
type ImageIdentifier struct {
	ImageDigest string `json:"imageDigest,omitempty"`
	ImageTag    string `json:"imageTag,omitempty"`
}

// Image represents a Docker image in ECR.
type Image struct {
	ImagePushedAt          time.Time       `json:"imagePushedAt"`
	ImageID                ImageIdentifier `json:"imageId"`
	ImageDigest            string          `json:"imageDigest"`
	ImageManifest          string          `json:"imageManifest,omitempty"`
	ImageManifestMediaType string          `json:"imageManifestMediaType,omitempty"`
	ImageStatus            string          `json:"imageStatus,omitempty"`
	RepositoryName         string          `json:"repositoryName"`
	RegistryID             string          `json:"registryId"`
	StorageClass           string          `json:"storageClass,omitempty"`
	Tags                   []string        `json:"-"`
	ImageSizeInBytes       int64           `json:"imageSizeInBytes,omitempty"`
}

// LayerAvailability represents the availability of an image layer.
type LayerAvailability struct {
	LayerDigest       string `json:"layerDigest"`
	LayerAvailability string `json:"layerAvailability"`
	MediaType         string `json:"mediaType,omitempty"`
	LayerSize         int64  `json:"layerSize,omitempty"`
}

// LayerFailure represents a layer that could not be checked.
type LayerFailure struct {
	LayerDigest   string `json:"layerDigest,omitempty"`
	FailureCode   string `json:"failureCode"`
	FailureReason string `json:"failureReason"`
}

// ImageFailure represents an image that could not be processed.
type ImageFailure struct {
	ImageID       ImageIdentifier `json:"imageId"`
	FailureCode   string          `json:"failureCode"`
	FailureReason string          `json:"failureReason"`
}

// RepositoryScanningConfiguration represents scanning configuration for a repository.
type RepositoryScanningConfiguration struct {
	RepositoryARN  string `json:"repositoryArn,omitempty"`
	RepositoryName string `json:"repositoryName"`
	ScanFrequency  string `json:"scanFrequency"`
	ScanOnPush     bool   `json:"scanOnPush"`
}

// RepositoryScanningConfigurationFailure represents a failure in getting scanning config.
type RepositoryScanningConfigurationFailure struct {
	RepositoryName string `json:"repositoryName"`
	FailureCode    string `json:"failureCode"`
	FailureReason  string `json:"failureReason"`
}

// CompleteLayerUploadResult is the result of a layer upload completion.
type CompleteLayerUploadResult struct {
	LayerDigest    string `json:"layerDigest"`
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId"`
	UploadID       string `json:"uploadId"`
}

// LayerUploadInitiation is returned when starting an ECR layer upload.
type LayerUploadInitiation struct {
	UploadID string `json:"uploadId"`
	PartSize int64  `json:"partSize"`
}

// LayerUploadPartResult records a received layer upload part.
type LayerUploadPartResult struct {
	RepositoryName   string `json:"repositoryName"`
	RegistryID       string `json:"registryId"`
	UploadID         string `json:"uploadId"`
	LastByteReceived int64  `json:"lastByteReceived"`
}

// PullThroughCacheRule represents a pull-through cache rule.
type PullThroughCacheRule struct {
	CreatedAt                time.Time `json:"createdAt"`
	UpdatedAt                time.Time `json:"updatedAt"`
	EcrRepositoryPrefix      string    `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL      string    `json:"upstreamRegistryUrl"`
	CredentialArn            string    `json:"credentialArn,omitempty"`
	CustomRoleArn            string    `json:"customRoleArn,omitempty"`
	UpstreamRegistry         string    `json:"upstreamRegistry,omitempty"`
	UpstreamRepositoryPrefix string    `json:"upstreamRepositoryPrefix,omitempty"`
	RegistryID               string    `json:"registryId"`
}

// RepositoryCreationTemplate represents a repository creation template.
type RepositoryCreationTemplate struct {
	CreatedAt                          time.Time                           `json:"createdAt"`
	UpdatedAt                          time.Time                           `json:"updatedAt"`
	ResourceTags                       map[string]string                   `json:"resourceTags,omitempty"`
	ImageTagMutability                 string                              `json:"imageTagMutability,omitempty"`
	EncryptionType                     string                              `json:"encryptionType,omitempty"`
	KMSKey                             string                              `json:"kmsKey,omitempty"`
	Description                        string                              `json:"description,omitempty"`
	RepositoryPolicy                   string                              `json:"repositoryPolicy,omitempty"`
	LifecyclePolicy                    string                              `json:"lifecyclePolicy,omitempty"`
	CustomRoleArn                      string                              `json:"customRoleArn,omitempty"`
	Prefix                             string                              `json:"prefix"`
	ImageTagMutabilityExclusionFilters []ImageTagMutabilityExclusionFilter `json:"imageTagMutabilityExclusionFilters,omitempty"` //nolint:lll // AWS-compatible JSON field name is long.
	AppliedFor                         []string                            `json:"appliedFor,omitempty"`
}

// LifecyclePolicyResult is the result of DeleteLifecyclePolicy.
type LifecyclePolicyResult struct {
	LifecyclePolicyText string    `json:"lifecyclePolicyText"`
	LastEvaluatedAt     time.Time `json:"lastEvaluatedAt"`
	RepositoryName      string    `json:"repositoryName"`
	RegistryID          string    `json:"registryId"`
}

// RegistryPolicyResult is the result of DeleteRegistryPolicy.
type RegistryPolicyResult struct {
	PolicyText string `json:"policyText"`
	RegistryID string `json:"registryId"`
	Status     string `json:"status"`
}

// LifecyclePolicyPreviewResult is an in-memory lifecycle preview snapshot.
type LifecyclePolicyPreviewResult struct {
	LifecyclePolicyText string            `json:"lifecyclePolicyText"`
	RepositoryName      string            `json:"repositoryName"`
	RegistryID          string            `json:"registryId"`
	Status              string            `json:"status"`
	PreviewResults      []ImageIdentifier `json:"previewResults"`
}

// RegistryDescription stores registry-wide ECR configuration.
type RegistryDescription struct {
	ReplicationConfiguration *ReplicationConfig `json:"replicationConfiguration,omitempty"`
	RegistryID               string             `json:"registryId"`
}

// RegistryScanningSettings stores registry-wide scan configuration.
type RegistryScanningSettings struct {
	ScanType string                 `json:"scanType,omitempty"`
	Rules    []RegistryScanningRule `json:"rules,omitempty"`
}

// RegistryScanningRule is a registry scan rule.
type RegistryScanningRule struct {
	ScanFrequency     string             `json:"scanFrequency,omitempty"`
	RepositoryFilters []RepositoryFilter `json:"repositoryFilters,omitempty"`
}

// ReplicationConfig stores ECR replication destinations.
type ReplicationConfig struct {
	Rules []ReplicationRule `json:"rules,omitempty"`
}

// ReplicationRule is an ECR replication rule.
type ReplicationRule struct {
	Destinations      []ReplicationDestination `json:"destinations,omitempty"`
	RepositoryFilters []RepositoryFilter       `json:"repositoryFilters,omitempty"`
}

// ReplicationDestination identifies a replication target.
type ReplicationDestination struct {
	Region     string `json:"region,omitempty"`
	RegistryID string `json:"registryId,omitempty"`
}

// RepositoryFilter is used by scanning, signing, and replication configs.
type RepositoryFilter struct {
	Filter     string `json:"filter,omitempty"`
	FilterType string `json:"filterType,omitempty"`
}

// RepositoryPolicyResult stores a repository policy.
type RepositoryPolicyResult struct {
	PolicyText     string `json:"policyText"`
	RegistryID     string `json:"registryId"`
	RepositoryName string `json:"repositoryName"`
}

// SigningSettings stores registry signing config.
type SigningSettings struct {
	Rules []SigningRule `json:"rules,omitempty"`
}

// SigningRule is a registry signing rule.
type SigningRule struct {
	SigningProfileArn string             `json:"signingProfileArn,omitempty"`
	RepositoryFilters []RepositoryFilter `json:"repositoryFilters,omitempty"`
}

// ImageSigningStatusRecord is a signing status entry.
type ImageSigningStatusRecord struct {
	FailureCode       string `json:"failureCode,omitempty"`
	FailureReason     string `json:"failureReason,omitempty"`
	SigningProfileArn string `json:"signingProfileArn,omitempty"`
	Status            string `json:"status"`
}

// ImageSigningStatusResult stores signing status for an image.
type ImageSigningStatusResult struct {
	ImageID         ImageIdentifier            `json:"imageId"`
	RegistryID      string                     `json:"registryId"`
	RepositoryName  string                     `json:"repositoryName"`
	SigningStatuses []ImageSigningStatusRecord `json:"signingStatuses"`
}

// ImageScanFinding is an image scan finding.
type ImageScanFinding struct {
	Attributes  map[string]string `json:"attributes,omitempty"`
	Description string            `json:"description,omitempty"`
	Name        string            `json:"name,omitempty"`
	Severity    string            `json:"severity,omitempty"`
	URI         string            `json:"uri,omitempty"`
}

// ImageScanFindingsResult stores scan findings for an image.
type ImageScanFindingsResult struct {
	CompletedAt           time.Time          `json:"completedAt"`
	FindingSeverityCounts map[string]int32   `json:"findingSeverityCounts,omitempty"`
	ImageID               ImageIdentifier    `json:"imageId"`
	RepositoryName        string             `json:"repositoryName"`
	RegistryID            string             `json:"registryId"`
	Status                string             `json:"status"`
	Description           string             `json:"description"`
	Findings              []ImageScanFinding `json:"findings,omitempty"`
}

// ImageScanStartResult is returned by StartImageScan.
type ImageScanStartResult struct {
	ImageID        ImageIdentifier `json:"imageId"`
	RepositoryName string          `json:"repositoryName"`
	RegistryID     string          `json:"registryId"`
	Status         string          `json:"status"`
	Description    string          `json:"description"`
}

// ImageReferrer is an OCI referrer summary.
type ImageReferrer struct {
	Annotations    map[string]string `json:"annotations,omitempty"`
	Digest         string            `json:"digest,omitempty"`
	MediaType      string            `json:"mediaType,omitempty"`
	ArtifactStatus string            `json:"artifactStatus,omitempty"`
	ArtifactType   string            `json:"artifactType,omitempty"`
	Size           int64             `json:"size,omitempty"`
}

// ImageReplicationStatusResult stores image replication status.
type ImageReplicationStatusResult struct {
	ImageID           ImageIdentifier `json:"imageId"`
	RepositoryName    string          `json:"repositoryName"`
	ReplicationStatus string          `json:"replicationStatus"`
}

// ImageStorageClassResult stores the image status after storage class updates.
type ImageStorageClassResult struct {
	ImageID        ImageIdentifier `json:"imageId"`
	ImageStatus    string          `json:"imageStatus"`
	RegistryID     string          `json:"registryId"`
	RepositoryName string          `json:"repositoryName"`
}

// PullTimeUpdateExclusion is an account-level pull time exclusion.
type PullTimeUpdateExclusion struct {
	CreatedAt    time.Time `json:"createdAt"`
	PrincipalArn string    `json:"principalArn"`
}

// ValidatePullThroughCacheRuleResult is returned by ValidatePullThroughCacheRule.
type ValidatePullThroughCacheRuleResult struct {
	CredentialArn            string `json:"credentialArn,omitempty"`
	CustomRoleArn            string `json:"customRoleArn,omitempty"`
	EcrRepositoryPrefix      string `json:"ecrRepositoryPrefix"`
	Failure                  string `json:"failure,omitempty"`
	RegistryID               string `json:"registryId"`
	UpstreamRegistryURL      string `json:"upstreamRegistryUrl,omitempty"`
	UpstreamRepositoryPrefix string `json:"upstreamRepositoryPrefix,omitempty"`
	IsValid                  bool   `json:"isValid"`
}

// ImageTagMutabilityExclusionFilter configures tag mutability exceptions.
type ImageTagMutabilityExclusionFilter struct {
	Filter     string `json:"filter,omitempty"`
	FilterType string `json:"filterType,omitempty"`
}

type layerUploadState struct {
	CreatedAt      time.Time
	RepositoryName string
	Data           []byte
	Size           int64
}

// compile-time assertion: InMemoryBackend must satisfy the Backend interface.
var _ Backend = (*InMemoryBackend)(nil)

// InMemoryBackend stores ECR repository state in memory.
type InMemoryBackend struct {
	repos                       map[string]*Repository
	images                      map[string]map[string]*Image // repoName → digest → image
	tagIndex                    map[string]map[string]string // repoName → tag → digest
	pullThroughCacheRules       map[string]*PullThroughCacheRule
	repositoryCreationTemplates map[string]*RepositoryCreationTemplate
	lifecyclePolicies           map[string]string
	lifecyclePolicyPreviews     map[string]*LifecyclePolicyPreviewResult
	uploadedLayers              map[string]map[string]int64
	layerUploads                map[string]*layerUploadState
	repoTags                    map[string]map[string]string
	repositoryPolicies          map[string]string
	imageScanFindings           map[string]map[string]*ImageScanFindingsResult
	accountSettings             map[string]string
	pullTimeUpdateExclusions    map[string]*PullTimeUpdateExclusion
	mu                          *lockmetrics.RWMutex
	registryScanningConfig      *RegistryScanningSettings
	replicationConfig           *ReplicationConfig
	registryPolicy              string
	signingConfig               *SigningSettings
	accountID                   string
	region                      string
	endpoint                    string
}

// NewInMemoryBackend creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackend(accountID, region, endpoint string) *InMemoryBackend {
	return &InMemoryBackend{
		repos:                       make(map[string]*Repository),
		images:                      make(map[string]map[string]*Image),
		tagIndex:                    make(map[string]map[string]string),
		pullThroughCacheRules:       make(map[string]*PullThroughCacheRule),
		repositoryCreationTemplates: make(map[string]*RepositoryCreationTemplate),
		lifecyclePolicies:           make(map[string]string),
		lifecyclePolicyPreviews:     make(map[string]*LifecyclePolicyPreviewResult),
		uploadedLayers:              make(map[string]map[string]int64),
		layerUploads:                make(map[string]*layerUploadState),
		repoTags:                    make(map[string]map[string]string),
		repositoryPolicies:          make(map[string]string),
		imageScanFindings:           make(map[string]map[string]*ImageScanFindingsResult),
		accountSettings:             make(map[string]string),
		pullTimeUpdateExclusions:    make(map[string]*PullTimeUpdateExclusion),
		registryScanningConfig:      &RegistryScanningSettings{ScanType: scanTypeBasic},
		replicationConfig:           &ReplicationConfig{},
		mu:                          lockmetrics.New("ecr"),
		accountID:                   accountID,
		region:                      region,
		endpoint:                    endpoint,
	}
}

// SetEndpoint updates the registry endpoint used in repository URIs.
func (b *InMemoryBackend) SetEndpoint(endpoint string) {
	b.mu.Lock("SetEndpoint")
	defer b.mu.Unlock()

	b.endpoint = endpoint
}

// ProxyEndpoint returns the registry endpoint used in repository URIs and
// authorization tokens. It satisfies the Backend interface.
func (b *InMemoryBackend) ProxyEndpoint() string {
	b.mu.RLock("ProxyEndpoint")
	defer b.mu.RUnlock()

	return b.endpoint
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string {
	b.mu.RLock("Region")
	defer b.mu.RUnlock()

	return b.region
}

// AccountID returns the AWS account ID associated with this registry.
func (b *InMemoryBackend) AccountID() string {
	b.mu.RLock("AccountID")
	defer b.mu.RUnlock()

	return b.accountID
}

// regionFor resolves the region to use for a request, preferring the per-request
// region carried on the context (via pkgs/awsmeta) and falling back to the
// backend's configured region when the context carries none.
func (b *InMemoryBackend) regionFor(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return b.region
}

// CreateRepository creates a new ECR repository.
func (b *InMemoryBackend) CreateRepository(
	ctx context.Context,
	name, imageTagMutability string,
	scanOnPush bool,
	encryptionType, kmsKey string,
) (*Repository, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", ErrInvalidRepositoryName)
	}

	if imageTagMutability == "" {
		imageTagMutability = mutabilityMutable
	}

	if encryptionType == "" {
		encryptionType = "AES256"
	}

	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if _, ok := b.repos[name]; ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryAlreadyExists, name)
	}

	region := b.regionFor(ctx)

	endpoint := b.endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", b.accountID, region)
	}

	repo := &Repository{
		CreatedAt:          time.Now(),
		EncryptionType:     encryptionType,
		KMSKey:             kmsKey,
		RegistryID:         b.accountID,
		RepositoryARN:      fmt.Sprintf("arn:aws:ecr:%s:%s:repository/%s", region, b.accountID, name),
		RepositoryName:     name,
		RepositoryURI:      fmt.Sprintf("%s/%s", endpoint, name),
		ImageTagMutability: imageTagMutability,
		ScanOnPush:         scanOnPush,
	}
	b.repos[name] = repo

	cp := *repo

	return &cp, nil
}

// DescribeRepositories returns all repositories, optionally filtered by name.
func (b *InMemoryBackend) DescribeRepositories(
	ctx context.Context, //nolint:revive // existing issue.
	names []string,
) ([]Repository, error) {
	b.mu.RLock("DescribeRepositories")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		out := make([]Repository, 0, len(b.repos))
		for _, r := range b.repos {
			out = append(out, *r)
		}

		sort.Slice(out, func(i, j int) bool {
			return out[i].RepositoryName < out[j].RepositoryName
		})

		return out, nil
	}

	out := make([]Repository, 0, len(names))

	for _, name := range names {
		r, ok := b.repos[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
		}

		out = append(out, *r)
	}

	return out, nil
}

// DeleteRepository removes a repository by name.
func (b *InMemoryBackend) DeleteRepository(
	ctx context.Context, //nolint:revive // existing issue.
	name string,
) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	r, ok := b.repos[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	delete(b.repos, name)
	delete(b.images, name)
	delete(b.tagIndex, name)
	delete(b.uploadedLayers, name)
	delete(b.lifecyclePolicies, name)
	delete(b.lifecyclePolicyPreviews, name)
	delete(b.repoTags, r.RepositoryARN)
	delete(b.repositoryPolicies, name)
	delete(b.imageScanFindings, name)

	// Clean up any in-progress layer uploads associated with this repository.
	for uploadID, upload := range b.layerUploads {
		if upload.RepositoryName == name {
			delete(b.layerUploads, uploadID)
		}
	}

	cp := *r

	return &cp, nil
}

// BatchCheckLayerAvailability checks the availability of image layers in a repository.
func (b *InMemoryBackend) BatchCheckLayerAvailability(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	layerDigests []string,
) ([]LayerAvailability, []LayerFailure, error) {
	b.mu.RLock("BatchCheckLayerAvailability")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	layers := make([]LayerAvailability, 0, len(layerDigests))
	failures := make([]LayerFailure, 0, len(layerDigests))

	repoLayers := b.uploadedLayers[repositoryName]
	for _, digest := range layerDigests {
		if size, ok := repoLayers[digest]; ok {
			layers = append(layers, LayerAvailability{
				LayerDigest:       digest,
				LayerAvailability: "AVAILABLE",
				LayerSize:         size,
			})
		} else {
			failures = append(failures, LayerFailure{
				LayerDigest:   digest,
				FailureCode:   "MissingLayerDigest",
				FailureReason: "the layer digest does not exist in the repository",
			})
		}
	}

	return layers, failures, nil
}

// deleteByDigestLocked removes an image by digest, deletes all tag bindings for
// that digest, and returns true if the image was found.
func deleteByDigestLocked(repoImages map[string]*Image, repoTags map[string]string, digest string) bool {
	if _, ok := repoImages[digest]; !ok {
		return false
	}

	// Remove all tag bindings for this digest.
	for tag, d := range repoTags {
		if d == digest {
			delete(repoTags, tag)
		}
	}

	delete(repoImages, digest)

	return true
}

// deleteByTagLocked removes a tag binding, clears the image's tag field if it
// matches, and falls back to a linear scan for legacy images. Returns true if found.
func deleteByTagLocked(repoImages map[string]*Image, repoTags map[string]string, tag string) bool {
	if digest, ok := repoTags[tag]; ok {
		// Remove tag binding only; keep image accessible by digest.
		delete(repoTags, tag)
		if img, exists := repoImages[digest]; exists {
			img.ImageID.ImageTag = ""
		}

		return true
	}

	// Fallback: linear scan for images stored with pre-tagIndex tag.
	for _, img := range repoImages {
		if img.ImageID.ImageTag == tag {
			img.ImageID.ImageTag = ""

			return true
		}
	}

	return false
}

// BatchDeleteImage deletes the specified images from a repository.
// When deleting by digest, all associated tags are removed and the image is deleted.
// When deleting by tag, only that tag binding is removed; the image remains accessible
// by digest (it becomes untagged if it had no other tags).
func (b *InMemoryBackend) BatchDeleteImage(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageIDs []ImageIdentifier,
) ([]ImageIdentifier, []ImageFailure, error) {
	b.mu.Lock("BatchDeleteImage")
	defer b.mu.Unlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	deleted := make([]ImageIdentifier, 0, len(imageIDs))
	failures := make([]ImageFailure, 0, len(imageIDs))

	repoImages := b.images[repositoryName]
	repoTags := b.tagIndex[repositoryName]

	for _, id := range imageIDs {
		var found bool

		if id.ImageDigest != "" {
			found = deleteByDigestLocked(repoImages, repoTags, id.ImageDigest)
		} else if id.ImageTag != "" {
			found = deleteByTagLocked(repoImages, repoTags, id.ImageTag)
		}

		if found {
			deleted = append(deleted, id)
		} else {
			failures = append(failures, ImageFailure{
				ImageID:       id,
				FailureCode:   "ImageNotFound",
				FailureReason: "requested image not found",
			})
		}
	}

	return deleted, failures, nil
}

// BatchGetImage retrieves details for the specified images.
func (b *InMemoryBackend) BatchGetImage(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageIDs []ImageIdentifier,
) ([]Image, []ImageFailure, error) {
	b.mu.RLock("BatchGetImage")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	imgs := make([]Image, 0, len(imageIDs))
	failures := make([]ImageFailure, 0, len(imageIDs))

	repoImages := b.images[repositoryName]
	repoTagIdx := b.tagIndex[repositoryName]

	for _, id := range imageIDs {
		img, ok := findImageLocked(repoImages, repoTagIdx, id)
		if ok {
			cp := *img
			// Preserve requested tag in imageId for the response.
			if id.ImageTag != "" {
				cp.ImageID.ImageTag = id.ImageTag
			}
			imgs = append(imgs, cp)
		} else {
			failures = append(failures, ImageFailure{
				ImageID:       id,
				FailureCode:   "ImageNotFound",
				FailureReason: "requested image not found",
			})
		}
	}

	return imgs, failures, nil
}

// buildDigestTagsLocked builds a reverse digest→[]tag map from a tag index.
// Ranging over a nil map is safe in Go, so no nil check is needed.
func buildDigestTagsLocked(repoTagIdx map[string]string) map[string][]string {
	digestTags := make(map[string][]string)
	for tag, digest := range repoTagIdx {
		digestTags[digest] = append(digestTags[digest], tag)
	}

	return digestTags
}

// DescribeImages returns image details for a repository.
func (b *InMemoryBackend) DescribeImages(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageIDs []ImageIdentifier,
) ([]Image, error) {
	b.mu.RLock("DescribeImages")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	repoImages := b.images[repositoryName]
	repoTagIdx := b.tagIndex[repositoryName]

	// Build digest → []tag reverse map for multi-tag annotation.
	digestTags := buildDigestTagsLocked(repoTagIdx)

	annotate := func(img Image) Image {
		tags := digestTags[img.ImageDigest]
		if len(tags) == 0 && img.ImageID.ImageTag != "" {
			tags = []string{img.ImageID.ImageTag}
		}
		// Sort for stable output.
		sort.Strings(tags)
		img.Tags = tags

		return img
	}

	out := make([]Image, 0, len(repoImages))
	if len(imageIDs) == 0 {
		for _, img := range repoImages {
			out = append(out, annotate(*img))
		}
	} else {
		for _, id := range imageIDs {
			img, ok := findImageLocked(repoImages, repoTagIdx, id)
			if !ok {
				return nil, fmt.Errorf("%w: image not found", ErrImageNotFound)
			}

			out = append(out, annotate(*img))
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ImageDigest < out[j].ImageDigest })

	return out, nil
}

// BatchGetRepositoryScanningConfiguration returns scanning configuration for repositories.
func (b *InMemoryBackend) BatchGetRepositoryScanningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryNames []string,
) ([]RepositoryScanningConfiguration, []RepositoryScanningConfigurationFailure, error) {
	b.mu.RLock("BatchGetRepositoryScanningConfiguration")
	defer b.mu.RUnlock()

	configs := make([]RepositoryScanningConfiguration, 0, len(repositoryNames))
	failures := make([]RepositoryScanningConfigurationFailure, 0, len(repositoryNames))

	for _, name := range repositoryNames {
		repo, ok := b.repos[name]
		if !ok {
			failures = append(failures, RepositoryScanningConfigurationFailure{
				RepositoryName: name,
				FailureCode:    "RepositoryNotFoundException",
				FailureReason:  fmt.Sprintf("repository %s not found", name),
			})

			continue
		}

		configs = append(configs, RepositoryScanningConfiguration{
			RepositoryARN:  repo.RepositoryARN,
			RepositoryName: name,
			ScanOnPush:     repo.ScanOnPush,
			ScanFrequency:  scanFrequency(repo.ScanOnPush),
		})
	}

	return configs, failures, nil
}

// ErrLayerDigestMismatch is returned when the provided digest does not match the uploaded bytes.
var ErrLayerDigestMismatch = awserr.New("InvalidLayerException", awserr.ErrInvalidParameter)

// CompleteLayerUpload finalises the upload of an image layer.
// If an upload session exists, it computes the SHA256 of accumulated bytes and verifies the digest.
// If no session exists (direct digest path), the provided digest is trusted as-is.
func (b *InMemoryBackend) CompleteLayerUpload(ctx context.Context, //nolint:revive // existing issue.
	repositoryName, uploadID string,
	layerDigests []string,
) (*CompleteLayerUploadResult, error) {
	b.mu.Lock("CompleteLayerUpload")
	defer b.mu.Unlock()

	var digest string
	var size int64

	upload, ok := b.layerUploads[uploadID]
	switch {
	case ok && upload.RepositoryName == repositoryName && len(upload.Data) > 0:
		computed := "sha256:" + hex.EncodeToString(sha256Sum(upload.Data))

		provided := ""
		if len(layerDigests) > 0 {
			provided = layerDigests[0]
		}

		if provided != "" {
			// Only enforce digest verification for full 64-char SHA256 digests.
			if isFullSHA256Digest(provided) && provided != computed {
				return nil, fmt.Errorf("%w: digest mismatch: got %s, want %s",
					ErrLayerDigestMismatch, provided, computed)
			}

			digest = provided
		} else {
			digest = computed
		}

		size = upload.Size
		delete(b.layerUploads, uploadID)

	case ok && upload.RepositoryName == repositoryName:
		if len(layerDigests) > 0 {
			digest = layerDigests[0]
		}

		size = upload.Size
		delete(b.layerUploads, uploadID)

	case len(layerDigests) > 0:
		// Direct digest path: no prior InitiateLayerUpload.
		digest = layerDigests[0]
	}

	if b.uploadedLayers[repositoryName] == nil {
		b.uploadedLayers[repositoryName] = make(map[string]int64)
	}

	b.uploadedLayers[repositoryName][digest] = size

	return &CompleteLayerUploadResult{
		LayerDigest:    digest,
		RepositoryName: repositoryName,
		RegistryID:     b.accountID,
		UploadID:       uploadID,
	}, nil
}

// sha256Sum returns the SHA256 hash of data.
func sha256Sum(data []byte) []byte {
	h := sha256.New()
	h.Write(data)

	return h.Sum(nil)
}

// isFullSHA256Digest returns true when s is a properly-formed "sha256:<64 hex>" digest.
func isFullSHA256Digest(s string) bool {
	const prefix = "sha256:"
	if len(s) != len(prefix)+64 {
		return false
	}

	if s[:len(prefix)] != prefix {
		return false
	}

	for _, c := range s[len(prefix):] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}

	return true
}

// GetDownloadURLForLayer resolves a local download URL for an uploaded layer.
func (b *InMemoryBackend) GetDownloadURLForLayer(
	ctx context.Context,
	repositoryName, layerDigest string,
) (string, error) {
	b.mu.RLock("GetDownloadURLForLayer")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return "", fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if _, ok := b.uploadedLayers[repositoryName][layerDigest]; !ok {
		return "", fmt.Errorf("%w: layer not found", ErrRepositoryNotFound)
	}

	endpoint := b.endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", b.accountID, b.regionFor(ctx))
	}

	return fmt.Sprintf("http://%s/v2/%s/blobs/%s", endpoint, repositoryName, layerDigest), nil
}

// InitiateLayerUpload starts a layer upload session.
func (b *InMemoryBackend) InitiateLayerUpload(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*LayerUploadInitiation, error) {
	b.mu.Lock("InitiateLayerUpload")
	defer b.mu.Unlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	now := time.Now()

	// Prune abandoned uploads (initiated but never completed) so the map cannot
	// grow without bound on a long-lived registry.
	for id, upload := range b.layerUploads {
		if now.Sub(upload.CreatedAt) > layerUploadTTL {
			delete(b.layerUploads, id)
		}
	}

	uploadID := fmt.Sprintf("upload-%d", now.UnixNano())
	b.layerUploads[uploadID] = &layerUploadState{RepositoryName: repositoryName, CreatedAt: now}

	return &LayerUploadInitiation{PartSize: layerUploadPartSize, UploadID: uploadID}, nil
}

// UploadLayerPart records uploaded bytes for an existing upload session.
func (b *InMemoryBackend) UploadLayerPart(ctx context.Context, //nolint:revive // existing issue.
	repositoryName, uploadID string,
	_, lastByte int64,
	blob []byte,
) (*LayerUploadPartResult, error) {
	b.mu.Lock("UploadLayerPart")
	defer b.mu.Unlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	upload, ok := b.layerUploads[uploadID]
	if !ok || upload.RepositoryName != repositoryName {
		return nil, fmt.Errorf("%w: upload not found", ErrRepositoryNotFound)
	}

	upload.Data = append(upload.Data, blob...)
	upload.Size = int64(len(upload.Data))
	// Refresh the activity timestamp so an in-progress multi-part upload is not
	// pruned as abandoned while parts are still arriving.
	upload.CreatedAt = time.Now()

	if lastByte < 0 && len(blob) > 0 {
		lastByte = upload.Size - 1
	}

	return &LayerUploadPartResult{
		LastByteReceived: lastByte,
		RepositoryName:   repositoryName,
		RegistryID:       b.accountID,
		UploadID:         uploadID,
	}, nil
}

// CreatePullThroughCacheRule creates a new pull-through cache rule.
func (b *InMemoryBackend) CreatePullThroughCacheRule(ctx context.Context, //nolint:revive // existing issue.
	prefix, upstreamURL, credentialArn, upstreamRegistry, customRoleArn, upstreamRepositoryPrefix string,
) (*PullThroughCacheRule, error) {
	if prefix == "" {
		return nil, fmt.Errorf("%w: ecrRepositoryPrefix is required", ErrInvalidRepositoryName)
	}

	b.mu.Lock("CreatePullThroughCacheRule")
	defer b.mu.Unlock()

	if _, ok := b.pullThroughCacheRules[prefix]; ok {
		return nil, fmt.Errorf("%w: %s", ErrPullThroughCacheRuleAlreadyExists, prefix)
	}

	now := time.Now()
	rule := &PullThroughCacheRule{
		EcrRepositoryPrefix:      prefix,
		UpstreamRegistryURL:      upstreamURL,
		CredentialArn:            credentialArn,
		CustomRoleArn:            customRoleArn,
		UpstreamRegistry:         upstreamRegistry,
		UpstreamRepositoryPrefix: upstreamRepositoryPrefix,
		RegistryID:               b.accountID,
		CreatedAt:                now,
		UpdatedAt:                now,
	}
	b.pullThroughCacheRules[prefix] = rule

	cp := *rule

	return &cp, nil
}

// DescribePullThroughCacheRules lists pull-through cache rules.
func (b *InMemoryBackend) DescribePullThroughCacheRules(
	ctx context.Context, //nolint:revive // existing issue.
	prefixes []string,
) ([]PullThroughCacheRule, error) {
	b.mu.RLock("DescribePullThroughCacheRules")
	defer b.mu.RUnlock()

	out := make([]PullThroughCacheRule, 0, len(b.pullThroughCacheRules))
	if len(prefixes) == 0 {
		for _, rule := range b.pullThroughCacheRules {
			out = append(out, *rule)
		}
	} else {
		for _, prefix := range prefixes {
			rule, ok := b.pullThroughCacheRules[prefix]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrPullThroughCacheRuleNotFound, prefix)
			}

			out = append(out, *rule)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].EcrRepositoryPrefix < out[j].EcrRepositoryPrefix })

	return out, nil
}

// CreateRepositoryCreationTemplate creates a new repository creation template.
func (b *InMemoryBackend) CreateRepositoryCreationTemplate(ctx context.Context, //nolint:revive // existing issue.
	req *RepositoryCreationTemplate,
) (*RepositoryCreationTemplate, error) {
	if req.Prefix == "" {
		return nil, fmt.Errorf("%w: prefix is required", ErrInvalidRepositoryName)
	}

	b.mu.Lock("CreateRepositoryCreationTemplate")
	defer b.mu.Unlock()

	if _, ok := b.repositoryCreationTemplates[req.Prefix]; ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryCreationTemplateAlreadyExists, req.Prefix)
	}

	now := time.Now()
	tmpl := &RepositoryCreationTemplate{
		Prefix:                             req.Prefix,
		Description:                        req.Description,
		EncryptionType:                     req.EncryptionType,
		KMSKey:                             req.KMSKey,
		ImageTagMutability:                 req.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: req.ImageTagMutabilityExclusionFilters,
		RepositoryPolicy:                   req.RepositoryPolicy,
		LifecyclePolicy:                    req.LifecyclePolicy,
		AppliedFor:                         req.AppliedFor,
		CustomRoleArn:                      req.CustomRoleArn,
		ResourceTags:                       copyStringMap(req.ResourceTags),
		CreatedAt:                          now,
		UpdatedAt:                          now,
	}
	b.repositoryCreationTemplates[req.Prefix] = tmpl

	cp := *tmpl

	return &cp, nil
}

// DeleteRepositoryCreationTemplate deletes a repository creation template.
func (b *InMemoryBackend) DeleteRepositoryCreationTemplate(
	ctx context.Context, //nolint:revive // existing issue.
	prefix string,
) (*RepositoryCreationTemplate, error) {
	b.mu.Lock("DeleteRepositoryCreationTemplate")
	defer b.mu.Unlock()

	tmpl, ok := b.repositoryCreationTemplates[prefix]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryCreationTemplateNotFound, prefix)
	}

	delete(b.repositoryCreationTemplates, prefix)
	cp := copyRepositoryCreationTemplate(tmpl)

	return &cp, nil
}

// DescribeRepositoryCreationTemplates lists repository creation templates.
func (b *InMemoryBackend) DescribeRepositoryCreationTemplates(
	ctx context.Context, //nolint:revive // existing issue.
	prefixes []string,
) ([]RepositoryCreationTemplate, error) {
	b.mu.RLock("DescribeRepositoryCreationTemplates")
	defer b.mu.RUnlock()

	out := make([]RepositoryCreationTemplate, 0, len(b.repositoryCreationTemplates))
	if len(prefixes) == 0 {
		for _, tmpl := range b.repositoryCreationTemplates {
			out = append(out, copyRepositoryCreationTemplate(tmpl))
		}
	} else {
		for _, prefix := range prefixes {
			tmpl, ok := b.repositoryCreationTemplates[prefix]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrRepositoryCreationTemplateNotFound, prefix)
			}

			out = append(out, copyRepositoryCreationTemplate(tmpl))
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Prefix < out[j].Prefix })

	return out, nil
}

// DeleteLifecyclePolicy deletes the lifecycle policy for a repository.
func (b *InMemoryBackend) DeleteLifecyclePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*LifecyclePolicyResult, error) {
	b.mu.Lock("DeleteLifecyclePolicy")
	defer b.mu.Unlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if _, ok := b.lifecyclePolicies[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrLifecyclePolicyNotFound, repositoryName)
	}

	policyText := b.lifecyclePolicies[repositoryName]
	delete(b.lifecyclePolicies, repositoryName)

	return &LifecyclePolicyResult{
		LifecyclePolicyText: policyText,
		LastEvaluatedAt:     time.Now(),
		RepositoryName:      repositoryName,
		RegistryID:          b.accountID,
	}, nil
}

// GetLifecyclePolicy returns the lifecycle policy for a repository.
func (b *InMemoryBackend) GetLifecyclePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*LifecyclePolicyResult, error) {
	b.mu.RLock("GetLifecyclePolicy")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	policyText, ok := b.lifecyclePolicies[repositoryName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLifecyclePolicyNotFound, repositoryName)
	}

	return &LifecyclePolicyResult{
		LifecyclePolicyText: policyText,
		LastEvaluatedAt:     time.Now(),
		RepositoryName:      repositoryName,
		RegistryID:          b.accountID,
	}, nil
}

// GetLifecyclePolicyPreview returns the current lifecycle policy preview.
func (b *InMemoryBackend) GetLifecyclePolicyPreview(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*LifecyclePolicyPreviewResult, error) {
	b.mu.RLock("GetLifecyclePolicyPreview")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	preview, ok := b.lifecyclePolicyPreviews[repositoryName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLifecyclePolicyNotFound, repositoryName)
	}

	cp := *preview
	cp.PreviewResults = append([]ImageIdentifier(nil), preview.PreviewResults...)

	return &cp, nil
}

// PutLifecyclePolicy creates or replaces the lifecycle policy for a repository.
func (b *InMemoryBackend) PutLifecyclePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName, policyText string,
) (*LifecyclePolicyResult, error) {
	b.mu.Lock("PutLifecyclePolicy")
	defer b.mu.Unlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	b.lifecyclePolicies[repositoryName] = policyText

	return &LifecyclePolicyResult{
		LifecyclePolicyText: policyText,
		LastEvaluatedAt:     time.Now(),
		RepositoryName:      repositoryName,
		RegistryID:          b.accountID,
	}, nil
}

// StartLifecyclePolicyPreview starts or refreshes a lifecycle policy preview.
func (b *InMemoryBackend) StartLifecyclePolicyPreview(ctx context.Context, //nolint:revive // existing issue.
	repositoryName, policyText string,
) (*LifecyclePolicyPreviewResult, error) {
	b.mu.Lock("StartLifecyclePolicyPreview")
	defer b.mu.Unlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if policyText == "" {
		policyText = b.lifecyclePolicies[repositoryName]
	}

	expired := evaluateLifecyclePolicy(policyText, b.images[repositoryName])

	preview := &LifecyclePolicyPreviewResult{
		LifecyclePolicyText: policyText,
		PreviewResults:      expired,
		RepositoryName:      repositoryName,
		RegistryID:          b.accountID,
		Status:              scanStatusComplete,
	}
	b.lifecyclePolicyPreviews[repositoryName] = preview

	cp := *preview
	cp.PreviewResults = append([]ImageIdentifier(nil), preview.PreviewResults...)

	return &cp, nil
}

// DeletePullThroughCacheRule deletes a pull-through cache rule by prefix.
func (b *InMemoryBackend) DeletePullThroughCacheRule(
	ctx context.Context, //nolint:revive // existing issue.
	prefix string,
) (*PullThroughCacheRule, error) {
	b.mu.Lock("DeletePullThroughCacheRule")
	defer b.mu.Unlock()

	rule, ok := b.pullThroughCacheRules[prefix]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPullThroughCacheRuleNotFound, prefix)
	}

	delete(b.pullThroughCacheRules, prefix)

	cp := *rule

	return &cp, nil
}

// UpdatePullThroughCacheRule updates a pull-through cache rule by prefix.
func (b *InMemoryBackend) UpdatePullThroughCacheRule(ctx context.Context, //nolint:revive // existing issue.
	prefix, credentialArn, customRoleArn string,
) (*PullThroughCacheRule, error) {
	b.mu.Lock("UpdatePullThroughCacheRule")
	defer b.mu.Unlock()

	rule, ok := b.pullThroughCacheRules[prefix]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPullThroughCacheRuleNotFound, prefix)
	}

	if credentialArn != "" {
		rule.CredentialArn = credentialArn
	}

	if customRoleArn != "" {
		rule.CustomRoleArn = customRoleArn
	}

	rule.UpdatedAt = time.Now()
	cp := *rule

	return &cp, nil
}

// ValidatePullThroughCacheRule validates a pull-through cache rule by prefix.
func (b *InMemoryBackend) ValidatePullThroughCacheRule(
	ctx context.Context, //nolint:revive // existing issue.
	prefix string,
) (*ValidatePullThroughCacheRuleResult, error) {
	b.mu.RLock("ValidatePullThroughCacheRule")
	defer b.mu.RUnlock()

	rule, ok := b.pullThroughCacheRules[prefix]
	if !ok {
		return &ValidatePullThroughCacheRuleResult{
			EcrRepositoryPrefix: prefix,
			Failure:             "Pull through cache rule not found",
			IsValid:             false,
			RegistryID:          b.accountID,
		}, nil
	}

	return &ValidatePullThroughCacheRuleResult{
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		IsValid:                  true,
		RegistryID:               rule.RegistryID,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
	}, nil
}

// AddImageInternal seeds an image directly into the backend for testing.
// repositoryName is the repository to add the image to; img is the image to add.
func (b *InMemoryBackend) AddImageInternal(repositoryName string, img Image) {
	b.mu.Lock("AddImageInternal")
	defer b.mu.Unlock()

	if b.images[repositoryName] == nil {
		b.images[repositoryName] = make(map[string]*Image)
	}

	cp := img
	b.images[repositoryName][img.ImageDigest] = &cp
}

// SetRegistryPolicyInternal sets the registry policy directly for testing.
func (b *InMemoryBackend) SetRegistryPolicyInternal(policy string) {
	b.mu.Lock("SetRegistryPolicyInternal")
	defer b.mu.Unlock()

	b.registryPolicy = policy
}

// DeleteRegistryPolicy deletes the registry-level IAM policy.
func (b *InMemoryBackend) DeleteRegistryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
) (*RegistryPolicyResult, error) {
	b.mu.Lock("DeleteRegistryPolicy")
	defer b.mu.Unlock()

	if b.registryPolicy == "" {
		return nil, fmt.Errorf("%w: no registry policy found", ErrRegistryPolicyNotFound)
	}

	policy := b.registryPolicy
	b.registryPolicy = ""

	return &RegistryPolicyResult{
		PolicyText: policy,
		RegistryID: b.accountID,
		Status:     "DELETED",
	}, nil
}

// DescribeRegistry returns registry-wide metadata.
func (b *InMemoryBackend) DescribeRegistry(
	ctx context.Context, //nolint:revive // existing issue.
) (*RegistryDescription, error) {
	b.mu.RLock("DescribeRegistry")
	defer b.mu.RUnlock()

	return &RegistryDescription{
		RegistryID:               b.accountID,
		ReplicationConfiguration: copyReplicationConfig(b.replicationConfig),
	}, nil
}

// GetRegistryPolicy returns the registry-level IAM policy.
func (b *InMemoryBackend) GetRegistryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
) (*RegistryPolicyResult, error) {
	b.mu.RLock("GetRegistryPolicy")
	defer b.mu.RUnlock()

	if b.registryPolicy == "" {
		return nil, fmt.Errorf("%w: no registry policy found", ErrRegistryPolicyNotFound)
	}

	return &RegistryPolicyResult{
		PolicyText: b.registryPolicy,
		RegistryID: b.accountID,
		Status:     imageStatusActive,
	}, nil
}

// GetRegistryScanningConfiguration returns the registry scanning configuration.
func (b *InMemoryBackend) GetRegistryScanningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
) (*RegistryScanningSettings, error) {
	b.mu.RLock("GetRegistryScanningConfiguration")
	defer b.mu.RUnlock()

	return copyRegistryScanningSettings(b.registryScanningConfig), nil
}

// PutRegistryPolicy creates or replaces the registry-level IAM policy.
func (b *InMemoryBackend) PutRegistryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	policyText string,
) (*RegistryPolicyResult, error) {
	b.mu.Lock("PutRegistryPolicy")
	defer b.mu.Unlock()

	b.registryPolicy = policyText

	return &RegistryPolicyResult{
		PolicyText: policyText,
		RegistryID: b.accountID,
		Status:     "SetComplete",
	}, nil
}

// PutRegistryScanningConfiguration updates the registry scanning configuration.
func (b *InMemoryBackend) PutRegistryScanningConfiguration(ctx context.Context, //nolint:revive // existing issue.
	settings *RegistryScanningSettings,
) (*RegistryScanningSettings, error) {
	b.mu.Lock("PutRegistryScanningConfiguration")
	defer b.mu.Unlock()

	if settings == nil {
		settings = &RegistryScanningSettings{ScanType: scanTypeBasic}
	}

	if settings.ScanType == "" {
		settings.ScanType = scanTypeBasic
	}

	b.registryScanningConfig = copyRegistryScanningSettings(settings)

	return copyRegistryScanningSettings(b.registryScanningConfig), nil
}

// PutReplicationConfiguration updates the registry replication configuration.
func (b *InMemoryBackend) PutReplicationConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	cfg *ReplicationConfig,
) (*ReplicationConfig, error) {
	b.mu.Lock("PutReplicationConfiguration")
	defer b.mu.Unlock()

	if cfg == nil {
		cfg = &ReplicationConfig{}
	}

	b.replicationConfig = copyReplicationConfig(cfg)

	return copyReplicationConfig(b.replicationConfig), nil
}

// GetRepositoryPolicy returns the repository-level policy.
func (b *InMemoryBackend) GetRepositoryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*RepositoryPolicyResult, error) {
	b.mu.RLock("GetRepositoryPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	policyText, ok := b.repositoryPolicies[repositoryName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryPolicyNotFound, repositoryName)
	}

	return &RepositoryPolicyResult{
		PolicyText:     policyText,
		RegistryID:     b.accountID,
		RepositoryName: repositoryName,
	}, nil
}

// SetRepositoryPolicy creates or replaces the repository-level IAM policy.
func (b *InMemoryBackend) SetRepositoryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName, policyText string,
) (*RepositoryPolicyResult, error) {
	b.mu.Lock("SetRepositoryPolicy")
	defer b.mu.Unlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	b.repositoryPolicies[repositoryName] = policyText

	return &RepositoryPolicyResult{
		PolicyText:     policyText,
		RegistryID:     b.accountID,
		RepositoryName: repositoryName,
	}, nil
}

// DeleteRepositoryPolicy deletes the repository-level policy.
func (b *InMemoryBackend) DeleteRepositoryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*RepositoryPolicyResult, error) {
	b.mu.Lock("DeleteRepositoryPolicy")
	defer b.mu.Unlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	policyText, ok := b.repositoryPolicies[repositoryName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryPolicyNotFound, repositoryName)
	}

	delete(b.repositoryPolicies, repositoryName)

	return &RepositoryPolicyResult{
		PolicyText:     policyText,
		RegistryID:     b.accountID,
		RepositoryName: repositoryName,
	}, nil
}

// GetSigningConfiguration returns the current registry signing configuration.
func (b *InMemoryBackend) GetSigningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
) (*SigningSettings, error) {
	b.mu.RLock("GetSigningConfiguration")
	defer b.mu.RUnlock()

	return copySigningSettings(b.signingConfig), nil
}

// PutSigningConfiguration updates the registry signing configuration.
func (b *InMemoryBackend) PutSigningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	settings *SigningSettings,
) (*SigningSettings, error) {
	b.mu.Lock("PutSigningConfiguration")
	defer b.mu.Unlock()

	b.signingConfig = copySigningSettings(settings)

	return copySigningSettings(b.signingConfig), nil
}

// DeleteSigningConfiguration removes the registry signing configuration.
func (b *InMemoryBackend) DeleteSigningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
) (*SigningSettings, error) {
	b.mu.Lock("DeleteSigningConfiguration")
	defer b.mu.Unlock()

	settings := copySigningSettings(b.signingConfig)
	b.signingConfig = nil

	return settings, nil
}

// DescribeImageSigningStatus returns signing status for an image.
func (b *InMemoryBackend) DescribeImageSigningStatus(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
) (*ImageSigningStatusResult, error) {
	b.mu.RLock("DescribeImageSigningStatus")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if _, ok := findImageLocked(b.images[repositoryName], b.tagIndex[repositoryName], imageID); !ok {
		return nil, fmt.Errorf("%w: image not found", ErrRepositoryNotFound)
	}

	out := []ImageSigningStatusRecord{{Status: scanStatusComplete}}
	if b.signingConfig != nil && len(b.signingConfig.Rules) > 0 {
		out[0].SigningProfileArn = b.signingConfig.Rules[0].SigningProfileArn
	}

	return &ImageSigningStatusResult{
		ImageID:         imageID,
		RegistryID:      b.accountID,
		RepositoryName:  repositoryName,
		SigningStatuses: out,
	}, nil
}

// DescribeImageScanFindings returns scan findings for an image.
func (b *InMemoryBackend) DescribeImageScanFindings(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
) (*ImageScanFindingsResult, error) {
	b.mu.RLock("DescribeImageScanFindings")
	defer b.mu.RUnlock()

	img, ok := findImageLocked(b.images[repositoryName], b.tagIndex[repositoryName], imageID)
	if !ok {
		return nil, fmt.Errorf("%w: image not found", ErrRepositoryNotFound)
	}

	findings := b.imageScanFindings[repositoryName][img.ImageDigest]
	if findings == nil {
		findings = &ImageScanFindingsResult{
			FindingSeverityCounts: map[string]int32{},
			ImageID:               img.ImageID,
			RepositoryName:        repositoryName,
			RegistryID:            b.accountID,
			Status:                scanStatusComplete,
			Description:           msgNoScanFindings,
			CompletedAt:           time.Now(),
		}
	}

	cp := copyImageScanFindingsResult(findings)

	return &cp, nil
}

// StartImageScan starts an image scan and returns the scan status.
func (b *InMemoryBackend) StartImageScan(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
) (*ImageScanStartResult, error) {
	b.mu.Lock("StartImageScan")
	defer b.mu.Unlock()

	img, ok := findImageLocked(b.images[repositoryName], b.tagIndex[repositoryName], imageID)
	if !ok {
		return nil, fmt.Errorf("%w: image not found", ErrRepositoryNotFound)
	}

	if b.imageScanFindings[repositoryName] == nil {
		b.imageScanFindings[repositoryName] = make(map[string]*ImageScanFindingsResult)
	}

	result := generateMockScanFindings(img.ImageDigest, repositoryName, b.accountID, img.ImageID)
	b.imageScanFindings[repositoryName][img.ImageDigest] = result

	return &ImageScanStartResult{
		ImageID:        img.ImageID,
		RepositoryName: repositoryName,
		RegistryID:     b.accountID,
		Status:         result.Status,
		Description:    result.Description,
	}, nil
}

// imageTagsLocked returns the tags for an image, falling back to the legacy
// per-image tag field if the digest is not in the tag index.
func imageTagsLocked(img *Image, digestTags map[string][]string) []string {
	tags := digestTags[img.ImageDigest]
	if len(tags) == 0 && img.ImageID.ImageTag != "" {
		// Legacy: tag stored on image directly (before tagIndex).
		return []string{img.ImageID.ImageTag}
	}

	return tags
}

// passesTagFilter reports whether an image with the given tagged state should be
// included given tagStatusFilter ("TAGGED", "UNTAGGED", or anything else for all).
func passesTagFilter(isTagged bool, tagStatusFilter string) bool {
	switch tagStatusFilter {
	case "TAGGED":
		return isTagged
	case "UNTAGGED":
		return !isTagged
	default:
		return true
	}
}

// ListImages lists image identifiers for a repository.
// tagStatusFilter controls which images to return: "TAGGED", "UNTAGGED", or "ANY" (default).
func (b *InMemoryBackend) ListImages(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName, tagStatusFilter string,
) ([]ImageIdentifier, error) {
	b.mu.RLock("ListImages")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	repoTagIdx := b.tagIndex[repositoryName]

	// Build a reverse map: digest → []tag from tagIndex.
	digestTags := buildDigestTagsLocked(repoTagIdx)

	out := make([]ImageIdentifier, 0, len(b.images[repositoryName]))
	for _, img := range b.images[repositoryName] {
		tags := imageTagsLocked(img, digestTags)
		isTagged := len(tags) > 0

		if !passesTagFilter(isTagged, tagStatusFilter) {
			continue
		}

		if isTagged {
			// Emit one identifier per tag (matching AWS ListImages behaviour).
			for _, tag := range tags {
				out = append(out, ImageIdentifier{ImageDigest: img.ImageDigest, ImageTag: tag})
			}
		} else {
			out = append(out, ImageIdentifier{ImageDigest: img.ImageDigest})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ImageDigest == out[j].ImageDigest {
			return out[i].ImageTag < out[j].ImageTag
		}

		return out[i].ImageDigest < out[j].ImageDigest
	})

	return out, nil
}

// ListImageReferrers lists image referrers for a subject image.
func (b *InMemoryBackend) ListImageReferrers(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	subject ImageIdentifier,
) ([]ImageReferrer, error) {
	b.mu.RLock("ListImageReferrers")
	defer b.mu.RUnlock()

	if _, ok := b.repos[repositoryName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if _, ok := findImageLocked(b.images[repositoryName], b.tagIndex[repositoryName], subject); !ok &&
		subject.ImageDigest != "" {
		return nil, fmt.Errorf("%w: image not found", ErrImageNotFound)
	}

	return []ImageReferrer{}, nil
}

// retagImageLocked moves a tag to a new digest: if the tag already maps to a
// different digest, it clears the old image's ImageTag field so it becomes untagged.
func retagImageLocked(repoImages map[string]*Image, repoTags map[string]string, tag, newDigest string) {
	oldDigest, has := repoTags[tag]
	if !has || oldDigest == newDigest {
		return
	}

	if oldImg, exists := repoImages[oldDigest]; exists {
		if oldImg.ImageID.ImageTag == tag {
			oldImg.ImageID.ImageTag = ""
		}
	}
}

// PutImage creates or replaces an image manifest.
// Digest is computed from the manifest content only (not the tag), matching AWS behaviour.
// When the repository imageTagMutability is IMMUTABLE, attempts to retag an
// existing image (same tag, different digest) are rejected with ErrImageTagAlreadyExists.
// When pushing to a MUTABLE repo with a tag that already exists on a different digest,
// the tag is moved to the new digest (the old image becomes untagged).
// normalizeImageFields fills in any zero-valued metadata fields on an image
// using the repository name and account ID.
func normalizeImageFields(image *Image, repositoryName, accountID string) {
	if image.ImageID.ImageDigest == "" {
		image.ImageID.ImageDigest = image.ImageDigest
	}

	if image.RepositoryName == "" {
		image.RepositoryName = repositoryName
	}

	if image.RegistryID == "" {
		image.RegistryID = accountID
	}

	if image.ImagePushedAt.IsZero() {
		image.ImagePushedAt = time.Now()
	}

	if image.ImageStatus == "" {
		image.ImageStatus = imageStatusActive
	}
}

func (b *InMemoryBackend) PutImage(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	image Image,
) (*Image, error) {
	b.mu.Lock("PutImage")
	defer b.mu.Unlock()

	repo, ok := b.repos[repositoryName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	// Compute digest from manifest only (not tag), matching AWS ECR behaviour.
	if image.ImageDigest == "" {
		sum := sha256.Sum256([]byte(image.ImageManifest))
		image.ImageDigest = "sha256:" + hex.EncodeToString(sum[:])
	}

	tag := image.ImageID.ImageTag

	// Ensure per-repo tagIndex exists.
	if b.tagIndex[repositoryName] == nil {
		b.tagIndex[repositoryName] = make(map[string]string)
	}
	repoTags := b.tagIndex[repositoryName]

	// IMMUTABLE enforcement: reject retagging to a different digest.
	if repo.ImageTagMutability == mutabilityImmutable && tag != "" {
		if existingDigest, has := repoTags[tag]; has && existingDigest != image.ImageDigest {
			return nil, fmt.Errorf("%w: tag %s already exists in immutable repository %s",
				ErrImageTagAlreadyExists, tag, repositoryName)
		}
	}

	// If tag already points to a different digest, untag the old image.
	if tag != "" {
		retagImageLocked(b.images[repositoryName], repoTags, tag, image.ImageDigest)
	}

	normalizeImageFields(&image, repositoryName, b.accountID)

	if b.images[repositoryName] == nil {
		b.images[repositoryName] = make(map[string]*Image)
	}

	stored := image
	b.images[repositoryName][image.ImageDigest] = &stored

	// Update tag index.
	if tag != "" {
		repoTags[tag] = image.ImageDigest
	}

	ret := stored

	return &ret, nil
}

// PutImageScanningConfiguration updates per-repository scan-on-push config.
func (b *InMemoryBackend) PutImageScanningConfiguration(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	scanOnPush bool,
) (*RepositoryScanningConfiguration, error) {
	b.mu.Lock("PutImageScanningConfiguration")
	defer b.mu.Unlock()

	repo, ok := b.repos[repositoryName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	repo.ScanOnPush = scanOnPush

	return &RepositoryScanningConfiguration{
		RepositoryARN:  repo.RepositoryARN,
		RepositoryName: repositoryName,
		ScanFrequency:  scanFrequency(scanOnPush),
		ScanOnPush:     scanOnPush,
	}, nil
}

// PutImageTagMutability updates per-repository tag mutability.
func (b *InMemoryBackend) PutImageTagMutability(ctx context.Context, //nolint:revive // existing issue.
	repositoryName, imageTagMutability string,
	exclusionFilters []ImageTagMutabilityExclusionFilter,
) (*Repository, error) {
	b.mu.Lock("PutImageTagMutability")
	defer b.mu.Unlock()

	repo, ok := b.repos[repositoryName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if imageTagMutability == "" {
		imageTagMutability = mutabilityMutable
	}

	repo.ImageTagMutability = imageTagMutability
	repo.ImageTagMutabilityExclusionFilters = append([]ImageTagMutabilityExclusionFilter(nil), exclusionFilters...)
	cp := *repo

	return &cp, nil
}

// DescribeImageReplicationStatus returns the current replication status for an image.
func (b *InMemoryBackend) DescribeImageReplicationStatus(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
) (*ImageReplicationStatusResult, error) {
	b.mu.RLock("DescribeImageReplicationStatus")
	defer b.mu.RUnlock()

	img, ok := findImageLocked(b.images[repositoryName], b.tagIndex[repositoryName], imageID)
	if !ok {
		return nil, fmt.Errorf("%w: image not found", ErrRepositoryNotFound)
	}

	return &ImageReplicationStatusResult{
		ImageID:           img.ImageID,
		RepositoryName:    repositoryName,
		ReplicationStatus: scanStatusComplete,
	}, nil
}

// UpdateImageStorageClass updates the storage class for an image.
func (b *InMemoryBackend) UpdateImageStorageClass(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
	target string,
) (*ImageStorageClassResult, error) {
	b.mu.Lock("UpdateImageStorageClass")
	defer b.mu.Unlock()

	img, ok := findImageLocked(b.images[repositoryName], b.tagIndex[repositoryName], imageID)
	if !ok {
		return nil, fmt.Errorf("%w: image not found", ErrRepositoryNotFound)
	}

	if target == "ARCHIVE" {
		img.StorageClass = target
		img.ImageStatus = "ARCHIVED"
	} else {
		img.StorageClass = "STANDARD"
		img.ImageStatus = "ACTIVE"
	}

	return &ImageStorageClassResult{
		ImageID:        img.ImageID,
		ImageStatus:    img.ImageStatus,
		RegistryID:     b.accountID,
		RepositoryName: repositoryName,
	}, nil
}

// GetAccountSetting returns a registry account setting.
func (b *InMemoryBackend) GetAccountSetting(
	ctx context.Context, //nolint:revive // existing issue.
	name string,
) (string, error) {
	b.mu.RLock("GetAccountSetting")
	defer b.mu.RUnlock()

	return b.accountSettings[name], nil
}

// PutAccountSetting updates a registry account setting.
func (b *InMemoryBackend) PutAccountSetting(
	ctx context.Context, //nolint:revive // existing issue.
	name, value string,
) (string, error) {
	b.mu.Lock("PutAccountSetting")
	defer b.mu.Unlock()

	b.accountSettings[name] = value

	return value, nil
}

// RegisterPullTimeUpdateExclusion creates a pull time update exclusion.
func (b *InMemoryBackend) RegisterPullTimeUpdateExclusion(
	ctx context.Context, //nolint:revive // existing issue.
	principalArn string,
) (*PullTimeUpdateExclusion, error) {
	b.mu.Lock("RegisterPullTimeUpdateExclusion")
	defer b.mu.Unlock()

	exclusion := &PullTimeUpdateExclusion{CreatedAt: time.Now(), PrincipalArn: principalArn}
	b.pullTimeUpdateExclusions[principalArn] = exclusion
	cp := *exclusion

	return &cp, nil
}

// DeregisterPullTimeUpdateExclusion deletes a pull time update exclusion.
func (b *InMemoryBackend) DeregisterPullTimeUpdateExclusion(
	ctx context.Context, //nolint:revive // existing issue.
	principalArn string,
) (*PullTimeUpdateExclusion, error) {
	b.mu.Lock("DeregisterPullTimeUpdateExclusion")
	defer b.mu.Unlock()

	exclusion, ok := b.pullTimeUpdateExclusions[principalArn]
	if !ok {
		return &PullTimeUpdateExclusion{PrincipalArn: principalArn}, nil
	}

	delete(b.pullTimeUpdateExclusions, principalArn)
	cp := *exclusion

	return &cp, nil
}

// ListPullTimeUpdateExclusions lists pull time update exclusions.
func (b *InMemoryBackend) ListPullTimeUpdateExclusions(
	ctx context.Context, //nolint:revive // existing issue.
) ([]PullTimeUpdateExclusion, error) {
	b.mu.RLock("ListPullTimeUpdateExclusions")
	defer b.mu.RUnlock()

	out := make([]PullTimeUpdateExclusion, 0, len(b.pullTimeUpdateExclusions))
	for _, exclusion := range b.pullTimeUpdateExclusions {
		out = append(out, *exclusion)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PrincipalArn < out[j].PrincipalArn })

	return out, nil
}

// UpdateRepositoryCreationTemplate updates a repository creation template.
func (b *InMemoryBackend) UpdateRepositoryCreationTemplate(ctx context.Context, //nolint:revive // existing issue.
	req *RepositoryCreationTemplate,
) (*RepositoryCreationTemplate, error) {
	b.mu.Lock("UpdateRepositoryCreationTemplate")
	defer b.mu.Unlock()

	tmpl, ok := b.repositoryCreationTemplates[req.Prefix]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryCreationTemplateNotFound, req.Prefix)
	}

	tmpl.Description = req.Description
	tmpl.EncryptionType = req.EncryptionType
	tmpl.KMSKey = req.KMSKey
	tmpl.ImageTagMutability = req.ImageTagMutability
	tmpl.ImageTagMutabilityExclusionFilters = append(
		[]ImageTagMutabilityExclusionFilter(nil),
		req.ImageTagMutabilityExclusionFilters...,
	)
	tmpl.RepositoryPolicy = req.RepositoryPolicy
	tmpl.LifecyclePolicy = req.LifecyclePolicy
	tmpl.CustomRoleArn = req.CustomRoleArn
	tmpl.AppliedFor = append([]string(nil), req.AppliedFor...)
	tmpl.ResourceTags = copyStringMap(req.ResourceTags)
	tmpl.UpdatedAt = time.Now()
	cp := copyRepositoryCreationTemplate(tmpl)

	return &cp, nil
}

// TagResource associates tags with an ECR resource identified by its ARN.
func (b *InMemoryBackend) TagResource(
	ctx context.Context, //nolint:revive // existing issue.
	resourceArn string,
	tags map[string]string,
) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.findResourceTagsLocked(resourceArn)
	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from an ECR resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context, //nolint:revive // existing issue.
	resourceArn string,
	tagKeys []string,
) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.findResourceTagsLocked(resourceArn)
	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for an ECR resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context, //nolint:revive // existing issue.
	resourceArn string,
) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.repoTags[resourceArn]
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out, nil
}

// findResourceTagsLocked returns (creating if absent) the tag map for the given ARN.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) findResourceTagsLocked(resourceArn string) map[string]string {
	if _, ok := b.repoTags[resourceArn]; !ok {
		b.repoTags[resourceArn] = make(map[string]string)
	}

	return b.repoTags[resourceArn]
}

// sortedTagKeys returns the keys of the given map sorted alphabetically.
func sortedTagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// findImageLocked looks up an image by digest or tag.
// tagIdx is the per-repository tag→digest index; it may be nil for older callers.
func findImageLocked(images map[string]*Image, tagIdx map[string]string, id ImageIdentifier) (*Image, bool) {
	if id.ImageDigest != "" {
		img, ok := images[id.ImageDigest]

		return img, ok
	}

	if id.ImageTag != "" {
		// Fast path via tag index.
		if tagIdx != nil {
			if digest, ok := tagIdx[id.ImageTag]; ok {
				img, exists := images[digest]

				return img, exists
			}
		}
		// Fallback: linear scan for images without tag index entry.
		for _, img := range images {
			if img.ImageID.ImageTag == id.ImageTag {
				return img, true
			}
		}
	}

	return nil, false
}

func scanFrequency(scanOnPush bool) string {
	if scanOnPush {
		return "SCAN_ON_PUSH"
	}

	return "MANUAL"
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}

	out := make(map[string]string, len(in))
	maps.Copy(out, in)

	return out
}

func copyRepositoryCreationTemplate(in *RepositoryCreationTemplate) RepositoryCreationTemplate {
	cp := *in
	cp.AppliedFor = append([]string(nil), in.AppliedFor...)
	cp.ImageTagMutabilityExclusionFilters = append(
		[]ImageTagMutabilityExclusionFilter(nil),
		in.ImageTagMutabilityExclusionFilters...,
	)
	cp.ResourceTags = copyStringMap(in.ResourceTags)

	return cp
}

func copyRegistryScanningSettings(in *RegistryScanningSettings) *RegistryScanningSettings {
	if in == nil {
		return &RegistryScanningSettings{ScanType: scanTypeBasic}
	}

	out := &RegistryScanningSettings{
		Rules:    make([]RegistryScanningRule, len(in.Rules)),
		ScanType: in.ScanType,
	}
	for i, rule := range in.Rules {
		out.Rules[i] = RegistryScanningRule{
			RepositoryFilters: append([]RepositoryFilter(nil), rule.RepositoryFilters...),
			ScanFrequency:     rule.ScanFrequency,
		}
	}

	return out
}

func copyReplicationConfig(in *ReplicationConfig) *ReplicationConfig {
	if in == nil {
		return &ReplicationConfig{}
	}

	out := &ReplicationConfig{Rules: make([]ReplicationRule, len(in.Rules))}
	for i, rule := range in.Rules {
		out.Rules[i] = ReplicationRule{
			Destinations:      append([]ReplicationDestination(nil), rule.Destinations...),
			RepositoryFilters: append([]RepositoryFilter(nil), rule.RepositoryFilters...),
		}
	}

	return out
}

func copySigningSettings(in *SigningSettings) *SigningSettings {
	if in == nil {
		return &SigningSettings{}
	}

	out := &SigningSettings{Rules: make([]SigningRule, len(in.Rules))}
	for i, rule := range in.Rules {
		out.Rules[i] = SigningRule{
			SigningProfileArn: rule.SigningProfileArn,
			RepositoryFilters: append([]RepositoryFilter(nil), rule.RepositoryFilters...),
		}
	}

	return out
}

func copyImageScanFindingsResult(in *ImageScanFindingsResult) ImageScanFindingsResult {
	cp := *in
	cp.FindingSeverityCounts = make(map[string]int32, len(in.FindingSeverityCounts))
	maps.Copy(cp.FindingSeverityCounts, in.FindingSeverityCounts)
	cp.Findings = make([]ImageScanFinding, len(in.Findings))
	for i, finding := range in.Findings {
		cp.Findings[i] = finding
		cp.Findings[i].Attributes = copyStringMap(finding.Attributes)
	}

	return cp
}

// Reset clears all state in the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.repos = make(map[string]*Repository)
	b.images = make(map[string]map[string]*Image)
	b.tagIndex = make(map[string]map[string]string)
	b.pullThroughCacheRules = make(map[string]*PullThroughCacheRule)
	b.repositoryCreationTemplates = make(map[string]*RepositoryCreationTemplate)
	b.lifecyclePolicies = make(map[string]string)
	b.lifecyclePolicyPreviews = make(map[string]*LifecyclePolicyPreviewResult)
	b.uploadedLayers = make(map[string]map[string]int64)
	b.layerUploads = make(map[string]*layerUploadState)
	b.repoTags = make(map[string]map[string]string)
	b.repositoryPolicies = make(map[string]string)
	b.imageScanFindings = make(map[string]map[string]*ImageScanFindingsResult)
	b.accountSettings = make(map[string]string)
	b.pullTimeUpdateExclusions = make(map[string]*PullTimeUpdateExclusion)
	b.registryPolicy = ""
	b.registryScanningConfig = &RegistryScanningSettings{ScanType: scanTypeBasic}
	b.replicationConfig = &ReplicationConfig{}
	b.signingConfig = nil
}

// AddRepositoryInternal seeds a repository directly into the backend for testing.
func (b *InMemoryBackend) AddRepositoryInternal(repo Repository) {
	b.mu.Lock("AddRepositoryInternal")
	defer b.mu.Unlock()

	cp := repo
	b.repos[repo.RepositoryName] = &cp
}

// AddLifecyclePolicyInternal seeds a lifecycle policy directly into the backend for testing.
func (b *InMemoryBackend) AddLifecyclePolicyInternal(repositoryName, policy string) {
	b.mu.Lock("AddLifecyclePolicyInternal")
	defer b.mu.Unlock()

	b.lifecyclePolicies[repositoryName] = policy
}
