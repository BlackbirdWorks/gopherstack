package ecr

import "time"

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

	scanTypeEnhanced            = "ENHANCED"
	replicationStatusComplete   = "COMPLETE"
	replicationStatusInProgress = "IN_PROGRESS"
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

// LifecyclePolicyResult is the result of DeleteLifecyclePolicy, GetLifecyclePolicy,
// and PutLifecyclePolicy. This is gopherstack's internal domain type (retains
// time.Time); the JSON wire shape is built separately by
// toLifecyclePolicyResultView so that LastEvaluatedAt serializes as an
// epoch-seconds number, matching AWS.
type LifecyclePolicyResult struct {
	LifecyclePolicyText string
	LastEvaluatedAt     time.Time
	RepositoryName      string
	RegistryID          string
}

// RegistryPolicyResult is the result of DeleteRegistryPolicy, GetRegistryPolicy,
// and PutRegistryPolicy. It intentionally has no "status" field: the real AWS
// DeleteRegistryPolicyOutput/GetRegistryPolicyOutput/PutRegistryPolicyOutput
// shapes carry only policyText and registryId — gopherstack previously
// fabricated a status string ("DELETED"/"ACTIVE"/"SetComplete") that does not
// exist in the real API and has been removed.
type RegistryPolicyResult struct {
	PolicyText string `json:"policyText"`
	RegistryID string `json:"registryId"`
}

// LifecyclePolicyPreviewResult is an in-memory lifecycle preview snapshot.
// This is gopherstack's internal domain type (retains time.Time for internal
// use); the JSON wire shape is built separately by toLifecyclePolicyPreviewView
// so that ImagePushedAt serializes as an epoch-seconds number, matching AWS.
type LifecyclePolicyPreviewResult struct {
	LifecyclePolicyText string
	RepositoryName      string
	RegistryID          string
	Status              string
	PreviewResults      []LifecyclePolicyPreviewEntry
}

// LifecyclePolicyPreviewEntry is a single per-image entry in a lifecycle
// policy preview (real AWS wire name: LifecyclePolicyPreviewResult; renamed
// here to avoid colliding with gopherstack's top-level preview-request type
// above).
type LifecyclePolicyPreviewEntry struct {
	ImagePushedAt       time.Time
	ImageDigest         string
	StorageClass        string
	ActionType          string
	ImageTags           []string
	AppliedRulePriority int
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
//
// ImageScanCompletedAt and VulnerabilitySourceUpdatedAt are epoch-seconds
// numbers (float64), matching the real ECR wire shape: the SDK deserializer
// (awsAwsjson11_deserializeDocumentImageScanFindings) parses both as
// smithytime.ParseEpochSeconds(json.Number), and the real field name is
// "imageScanCompletedAt" — not "completedAt".
type ImageScanFindingsResult struct {
	FindingSeverityCounts map[string]int32   `json:"findingSeverityCounts,omitempty"`
	ImageID               ImageIdentifier    `json:"imageId"`
	RepositoryName        string             `json:"repositoryName"`
	RegistryID            string             `json:"registryId"`
	Status                string             `json:"status"`
	Description           string             `json:"description"`
	Findings              []ImageScanFinding `json:"findings,omitempty"`
	// EnhancedFindings carries Inspector-style, package-level findings produced by
	// ENHANCED registry scanning. It is empty for BASIC scans, which populate
	// Findings instead — so the two scan types return genuinely different shapes.
	EnhancedFindings             []EnhancedImageScanFinding `json:"enhancedFindings,omitempty"`
	ImageScanCompletedAt         float64                    `json:"imageScanCompletedAt"`
	VulnerabilitySourceUpdatedAt float64                    `json:"vulnerabilitySourceUpdatedAt,omitempty"`
}

// EnhancedImageScanFinding is an Inspector-style enhanced scan finding, returned
// under enhancedFindings when the registry uses ENHANCED scanning. It carries
// package-level vulnerability detail that the BASIC finding shape lacks.
type EnhancedImageScanFinding struct {
	PackageVulnerabilityDetails *PackageVulnerabilityDetails `json:"packageVulnerabilityDetails,omitempty"`
	Remediation                 *Remediation                 `json:"remediation,omitempty"`
	Description                 string                       `json:"description,omitempty"`
	AwsAccountID                string                       `json:"awsAccountId,omitempty"`
	FindingArn                  string                       `json:"findingArn,omitempty"`
	Severity                    string                       `json:"severity,omitempty"`
	Status                      string                       `json:"status,omitempty"`
	Title                       string                       `json:"title,omitempty"`
	Type                        string                       `json:"type,omitempty"`
	FixAvailable                string                       `json:"fixAvailable,omitempty"`
	Resources                   []EnhancedFindingResource    `json:"resources,omitempty"`
	UpdatedAt                   float64                      `json:"updatedAt"`
	LastObservedAt              float64                      `json:"lastObservedAt"`
	FirstObservedAt             float64                      `json:"firstObservedAt"`
	Score                       float64                      `json:"score,omitempty"`
}

// PackageVulnerabilityDetails describes the vulnerability behind an enhanced
// finding, including CVSS scoring and the affected packages.
type PackageVulnerabilityDetails struct {
	VulnerabilityID        string              `json:"vulnerabilityId,omitempty"`
	Source                 string              `json:"source,omitempty"`
	SourceURL              string              `json:"sourceUrl,omitempty"`
	VendorSeverity         string              `json:"vendorSeverity,omitempty"`
	Cvss                   []CVSSScore         `json:"cvss,omitempty"`
	ReferenceUrls          []string            `json:"referenceUrls,omitempty"`
	RelatedVulnerabilities []string            `json:"relatedVulnerabilities,omitempty"`
	VulnerablePackages     []VulnerablePackage `json:"vulnerablePackages,omitempty"`
	VendorCreatedAt        float64             `json:"vendorCreatedAt"`
}

// CVSSScore is a single CVSS scoring entry for an enhanced finding.
type CVSSScore struct {
	ScoringVector string  `json:"scoringVector,omitempty"`
	Source        string  `json:"source,omitempty"`
	Version       string  `json:"version,omitempty"`
	BaseScore     float64 `json:"baseScore,omitempty"`
}

// VulnerablePackage describes an individual affected package in an enhanced finding.
type VulnerablePackage struct {
	Arch            string `json:"arch,omitempty"`
	FilePath        string `json:"filePath,omitempty"`
	Name            string `json:"name,omitempty"`
	PackageManager  string `json:"packageManager,omitempty"`
	Release         string `json:"release,omitempty"`
	SourceLayerHash string `json:"sourceLayerHash,omitempty"`
	Version         string `json:"version,omitempty"`
	FixedInVersion  string `json:"fixedInVersion,omitempty"`
	Remediation     string `json:"remediation,omitempty"`
	Epoch           int    `json:"epoch,omitempty"`
}

// Remediation carries the recommended fix for an enhanced finding.
type Remediation struct {
	Recommendation RemediationRecommendation `json:"recommendation"`
}

// RemediationRecommendation is the human-readable remediation guidance.
type RemediationRecommendation struct {
	Text string `json:"text,omitempty"`
	URL  string `json:"url,omitempty"`
}

// EnhancedFindingResource identifies the resource an enhanced finding applies to.
type EnhancedFindingResource struct {
	ID      string                         `json:"id,omitempty"`
	Type    string                         `json:"type,omitempty"`
	Details EnhancedFindingResourceDetails `json:"details"`
}

// EnhancedFindingResourceDetails wraps the resource-type-specific detail.
type EnhancedFindingResourceDetails struct {
	AwsEcrContainerImage AwsEcrContainerImageDetails `json:"awsEcrContainerImage"`
}

// AwsEcrContainerImageDetails describes the ECR image an enhanced finding covers.
type AwsEcrContainerImageDetails struct {
	Architecture   string   `json:"architecture,omitempty"`
	ImageHash      string   `json:"imageHash,omitempty"`
	Platform       string   `json:"platform,omitempty"`
	RegistryID     string   `json:"registryId,omitempty"`
	RepositoryName string   `json:"repositoryName,omitempty"`
	ImageTags      []string `json:"imageTags,omitempty"`
	PushedAt       float64  `json:"pushedAt"`
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
	ImageID             ImageIdentifier               `json:"imageId"`
	RepositoryName      string                        `json:"repositoryName"`
	ReplicationStatuses []ImageReplicationStatusEntry `json:"replicationStatuses"`
}

// ImageReplicationStatusEntry is the replication status for a single destination.
type ImageReplicationStatusEntry struct {
	Region        string `json:"region,omitempty"`
	RegistryID    string `json:"registryId,omitempty"`
	Status        string `json:"status"`
	FailureCode   string `json:"failureCode,omitempty"`
	FailureReason string `json:"failureReason,omitempty"`
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

// layerUploadQueueEntry is a FIFO entry for O(k) TTL pruning in InitiateLayerUpload.
type layerUploadQueueEntry struct {
	id string
}

type layerUploadState struct {
	CreatedAt      time.Time
	RepositoryName string
	Data           []byte
	Size           int64
}
