package ecr

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrRepositoryNotFound is returned when a repository does not exist.
	ErrRepositoryNotFound = awserr.New("RepositoryNotFoundException", awserr.ErrNotFound)
	// ErrRepositoryAlreadyExists is returned when a repository already exists.
	ErrRepositoryAlreadyExists = awserr.New("RepositoryAlreadyExistsException", awserr.ErrAlreadyExists)
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
)

// Repository represents an ECR repository.
type Repository struct {
	CreatedAt          time.Time `json:"createdAt"`
	RegistryID         string    `json:"registryId"`
	RepositoryARN      string    `json:"repositoryArn"`
	RepositoryName     string    `json:"repositoryName"`
	RepositoryURI      string    `json:"repositoryUri"`
	ImageTagMutability string    `json:"imageTagMutability"`
}

// ImageIdentifier identifies a specific image by digest or tag.
type ImageIdentifier struct {
	ImageDigest string `json:"imageDigest,omitempty"`
	ImageTag    string `json:"imageTag,omitempty"`
}

// Image represents a Docker image in ECR.
type Image struct {
	ImageDigest    string          `json:"imageDigest"`
	ImageManifest  string          `json:"imageManifest,omitempty"`
	ImageID        ImageIdentifier `json:"imageId"`
	RepositoryName string          `json:"repositoryName"`
	RegistryID     string          `json:"registryId"`
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

// PullThroughCacheRule represents a pull-through cache rule.
type PullThroughCacheRule struct {
	CreatedAt           time.Time `json:"createdAt"`
	UpdatedAt           time.Time `json:"updatedAt"`
	EcrRepositoryPrefix string    `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL string    `json:"upstreamRegistryUrl"`
	CredentialArn       string    `json:"credentialArn,omitempty"`
	UpstreamRegistry    string    `json:"upstreamRegistry,omitempty"`
	RegistryID          string    `json:"registryId"`
}

// RepositoryCreationTemplate represents a repository creation template.
type RepositoryCreationTemplate struct {
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
	Prefix             string    `json:"prefix"`
	Description        string    `json:"description,omitempty"`
	ImageTagMutability string    `json:"imageTagMutability,omitempty"`
	RepositoryPolicy   string    `json:"repositoryPolicy,omitempty"`
	LifecyclePolicy    string    `json:"lifecyclePolicy,omitempty"`
	CustomRoleArn      string    `json:"customRoleArn,omitempty"`
	AppliedFor         []string  `json:"appliedFor,omitempty"`
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

// compile-time assertion: InMemoryBackend must satisfy the Backend interface.
var _ Backend = (*InMemoryBackend)(nil)

// InMemoryBackend stores ECR repository state in memory.
type InMemoryBackend struct {
	repos                       map[string]*Repository
	images                      map[string]map[string]*Image
	pullThroughCacheRules       map[string]*PullThroughCacheRule
	repositoryCreationTemplates map[string]*RepositoryCreationTemplate
	lifecyclePolicies           map[string]string
	uploadedLayers              map[string]map[string]int64
	repoTags                    map[string]map[string]string
	mu                          *lockmetrics.RWMutex
	registryPolicy              string
	accountID                   string
	region                      string
	endpoint                    string
}

// NewInMemoryBackend creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackend(accountID, region, endpoint string) *InMemoryBackend {
	return &InMemoryBackend{
		repos:                       make(map[string]*Repository),
		images:                      make(map[string]map[string]*Image),
		pullThroughCacheRules:       make(map[string]*PullThroughCacheRule),
		repositoryCreationTemplates: make(map[string]*RepositoryCreationTemplate),
		lifecyclePolicies:           make(map[string]string),
		uploadedLayers:              make(map[string]map[string]int64),
		repoTags:                    make(map[string]map[string]string),
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

// CreateRepository creates a new ECR repository.
func (b *InMemoryBackend) CreateRepository(name, imageTagMutability string) (*Repository, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", ErrInvalidRepositoryName)
	}

	if imageTagMutability == "" {
		imageTagMutability = "MUTABLE"
	}

	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if _, ok := b.repos[name]; ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryAlreadyExists, name)
	}

	endpoint := b.endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", b.accountID, b.region)
	}

	repo := &Repository{
		CreatedAt:          time.Now(),
		RegistryID:         b.accountID,
		RepositoryARN:      fmt.Sprintf("arn:aws:ecr:%s:%s:repository/%s", b.region, b.accountID, name),
		RepositoryName:     name,
		RepositoryURI:      fmt.Sprintf("%s/%s", endpoint, name),
		ImageTagMutability: imageTagMutability,
	}
	b.repos[name] = repo

	cp := *repo

	return &cp, nil
}

// DescribeRepositories returns all repositories, optionally filtered by name.
func (b *InMemoryBackend) DescribeRepositories(names []string) ([]Repository, error) {
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
func (b *InMemoryBackend) DeleteRepository(name string) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	r, ok := b.repos[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	delete(b.repos, name)
	delete(b.images, name)
	delete(b.uploadedLayers, name)
	delete(b.lifecyclePolicies, name)
	delete(b.repoTags, r.RepositoryARN)

	cp := *r

	return &cp, nil
}

// BatchCheckLayerAvailability checks the availability of image layers in a repository.
func (b *InMemoryBackend) BatchCheckLayerAvailability(
	repositoryName string,
	layerDigests []string,
) ([]LayerAvailability, []LayerFailure, error) {
	b.mu.RLock("BatchCheckLayerAvailability")
	defer b.mu.RUnlock()

	layers := make([]LayerAvailability, 0, len(layerDigests))
	failures := make([]LayerFailure, 0)

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

// BatchDeleteImage deletes the specified images from a repository.
func (b *InMemoryBackend) BatchDeleteImage(
	repositoryName string,
	imageIDs []ImageIdentifier,
) ([]ImageIdentifier, []ImageFailure, error) {
	b.mu.Lock("BatchDeleteImage")
	defer b.mu.Unlock()

	deleted := make([]ImageIdentifier, 0, len(imageIDs))
	failures := make([]ImageFailure, 0)

	repoImages := b.images[repositoryName]
	for _, id := range imageIDs {
		found := false

		if id.ImageDigest != "" {
			if _, ok := repoImages[id.ImageDigest]; ok {
				delete(repoImages, id.ImageDigest)
				deleted = append(deleted, id)
				found = true
			}
		} else if id.ImageTag != "" {
			for digest, img := range repoImages {
				if img.ImageID.ImageTag == id.ImageTag {
					delete(repoImages, digest)
					deleted = append(deleted, id)
					found = true

					break
				}
			}
		}

		if !found {
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
func (b *InMemoryBackend) BatchGetImage(
	repositoryName string,
	imageIDs []ImageIdentifier,
) ([]Image, []ImageFailure, error) {
	b.mu.RLock("BatchGetImage")
	defer b.mu.RUnlock()

	imgs := make([]Image, 0, len(imageIDs))
	failures := make([]ImageFailure, 0)

	repoImages := b.images[repositoryName]
	for _, id := range imageIDs {
		found := false

		if id.ImageDigest != "" {
			if img, ok := repoImages[id.ImageDigest]; ok {
				imgs = append(imgs, *img)
				found = true
			}
		} else if id.ImageTag != "" {
			for _, img := range repoImages {
				if img.ImageID.ImageTag == id.ImageTag {
					imgs = append(imgs, *img)
					found = true

					break
				}
			}
		}

		if !found {
			failures = append(failures, ImageFailure{
				ImageID:       id,
				FailureCode:   "ImageNotFound",
				FailureReason: "requested image not found",
			})
		}
	}

	return imgs, failures, nil
}

// BatchGetRepositoryScanningConfiguration returns scanning configuration for repositories.
func (b *InMemoryBackend) BatchGetRepositoryScanningConfiguration(
	repositoryNames []string,
) ([]RepositoryScanningConfiguration, []RepositoryScanningConfigurationFailure, error) {
	b.mu.RLock("BatchGetRepositoryScanningConfiguration")
	defer b.mu.RUnlock()

	configs := make([]RepositoryScanningConfiguration, 0, len(repositoryNames))
	failures := make([]RepositoryScanningConfigurationFailure, 0)

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
			ScanOnPush:     false,
			ScanFrequency:  "MANUAL",
		})
	}

	return configs, failures, nil
}

// CompleteLayerUpload finalises the upload of an image layer.
func (b *InMemoryBackend) CompleteLayerUpload(
	repositoryName, uploadID string,
	layerDigests []string,
) (*CompleteLayerUploadResult, error) {
	b.mu.Lock("CompleteLayerUpload")
	defer b.mu.Unlock()

	digest := ""
	if len(layerDigests) > 0 {
		digest = layerDigests[0]
	}

	if b.uploadedLayers[repositoryName] == nil {
		b.uploadedLayers[repositoryName] = make(map[string]int64)
	}

	if digest != "" {
		b.uploadedLayers[repositoryName][digest] = 1234
	}

	return &CompleteLayerUploadResult{
		LayerDigest:    digest,
		RepositoryName: repositoryName,
		RegistryID:     b.accountID,
		UploadID:       uploadID,
	}, nil
}

// CreatePullThroughCacheRule creates a new pull-through cache rule.
func (b *InMemoryBackend) CreatePullThroughCacheRule(
	prefix, upstreamURL, credentialArn, upstreamRegistry string,
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
		EcrRepositoryPrefix: prefix,
		UpstreamRegistryURL: upstreamURL,
		CredentialArn:       credentialArn,
		UpstreamRegistry:    upstreamRegistry,
		RegistryID:          b.accountID,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	b.pullThroughCacheRules[prefix] = rule

	cp := *rule

	return &cp, nil
}

// CreateRepositoryCreationTemplate creates a new repository creation template.
func (b *InMemoryBackend) CreateRepositoryCreationTemplate(
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
		Prefix:             req.Prefix,
		Description:        req.Description,
		ImageTagMutability: req.ImageTagMutability,
		RepositoryPolicy:   req.RepositoryPolicy,
		LifecyclePolicy:    req.LifecyclePolicy,
		AppliedFor:         req.AppliedFor,
		CustomRoleArn:      req.CustomRoleArn,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	b.repositoryCreationTemplates[req.Prefix] = tmpl

	cp := *tmpl

	return &cp, nil
}

// DeleteLifecyclePolicy deletes the lifecycle policy for a repository.
func (b *InMemoryBackend) DeleteLifecyclePolicy(repositoryName string) (*LifecyclePolicyResult, error) {
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

// PutLifecyclePolicy creates or replaces the lifecycle policy for a repository.
func (b *InMemoryBackend) PutLifecyclePolicy(repositoryName, policyText string) (*LifecyclePolicyResult, error) {
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

// DeletePullThroughCacheRule deletes a pull-through cache rule by prefix.
func (b *InMemoryBackend) DeletePullThroughCacheRule(prefix string) (*PullThroughCacheRule, error) {
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
func (b *InMemoryBackend) DeleteRegistryPolicy() (*RegistryPolicyResult, error) {
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

// PutRegistryPolicy creates or replaces the registry-level IAM policy.
func (b *InMemoryBackend) PutRegistryPolicy(policyText string) (*RegistryPolicyResult, error) {
	b.mu.Lock("PutRegistryPolicy")
	defer b.mu.Unlock()

	b.registryPolicy = policyText

	return &RegistryPolicyResult{
		PolicyText: policyText,
		RegistryID: b.accountID,
		Status:     "SetComplete",
	}, nil
}

// TagResource associates tags with an ECR resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.findResourceTagsLocked(resourceArn)
	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from an ECR resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.findResourceTagsLocked(resourceArn)
	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for an ECR resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
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

// Reset clears all state in the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.repos = make(map[string]*Repository)
	b.images = make(map[string]map[string]*Image)
	b.pullThroughCacheRules = make(map[string]*PullThroughCacheRule)
	b.repositoryCreationTemplates = make(map[string]*RepositoryCreationTemplate)
	b.lifecyclePolicies = make(map[string]string)
	b.uploadedLayers = make(map[string]map[string]int64)
	b.repoTags = make(map[string]map[string]string)
	b.registryPolicy = ""
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
