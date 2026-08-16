package s3

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// NullVersion is the version ID used when versioning is not enabled.
const NullVersion = "null"

// StoredBucket represents an S3 bucket in memory.
//
// Buckets are keyed by Name (globally unique -- CreateBucket enforces this across
// all regions, mirroring real S3's global bucket-namespace). Region never changes
// after creation (S3 has no "move bucket to another region" operation).
type StoredBucket struct {
	CreationDate                  time.Time                `json:"creationDate"`
	Objects                       map[string]*StoredObject `json:"objects,omitempty"`
	mu                            *lockmetrics.RWMutex
	Region                        string                       `json:"region,omitempty"`
	WebsiteConfig                 string                       `json:"websiteConfig,omitempty"`
	PublicAccessBlockConfig       string                       `json:"publicAccessBlockConfig,omitempty"`
	LifecycleConfig               string                       `json:"lifecycleConfig,omitempty"`
	NotificationConfig            string                       `json:"notificationConfig,omitempty"`
	ObjectLockConfig              string                       `json:"objectLockConfig,omitempty"`
	Policy                        string                       `json:"policy,omitempty"`
	EncryptionConfig              string                       `json:"encryptionConfig,omitempty"`
	CORSConfig                    string                       `json:"corsConfig,omitempty"`
	OwnershipControlsConfig       string                       `json:"ownershipControlsConfig,omitempty"`
	LoggingConfig                 string                       `json:"loggingConfig,omitempty"`
	ReplicationConfig             string                       `json:"replicationConfig,omitempty"`
	AnalyticsConfigs              map[string]string            `json:"analyticsConfigs,omitempty"`
	IntelligentTieringConfigs     map[string]string            `json:"intelligentTieringConfigs,omitempty"`
	InventoryConfigs              map[string]string            `json:"inventoryConfigs,omitempty"`
	MetadataConfig                string                       `json:"metadataConfig,omitempty"`
	MetadataTableConfig           string                       `json:"metadataTableConfig,omitempty"`
	AbacConfig                    string                       `json:"abacConfig,omitempty"`
	MetadataInventoryTableConfig  string                       `json:"metadataInventoryTableConfig,omitempty"`
	MetadataJournalTableConfig    string                       `json:"metadataJournalTableConfig,omitempty"`
	MetadataAnnotationTableConfig string                       `json:"metadataAnnotationTableConfig,omitempty"`
	MetricsConfigs                map[string]string            `json:"metricsConfigs,omitempty"`
	Versioning                    types.BucketVersioningStatus `json:"versioning,omitempty"`
	// MFADelete is stored as a plain string, not a typed SDK enum, because the
	// real request and response shapes use two DIFFERENT Go types for the same
	// concept (VersioningConfiguration.MFADelete is types.MFADelete;
	// GetBucketVersioningOutput.MFADelete is types.MFADeleteStatus,
	// s3@v1.106.5 api_op_GetBucketVersioning.go/types/enums.go) -- both hold
	// the same "Enabled"/"Disabled" strings on the wire.
	MFADelete           string      `json:"mfaDelete,omitempty"`
	Name                string      `json:"name"`
	ACL                 string      `json:"acl,omitempty"`
	AccelerateStatus    string      `json:"accelerateStatus,omitempty"`
	RequestPaymentPayer string      `json:"requestPaymentPayer,omitempty"`
	Tags                []types.Tag `json:"tags,omitempty"`
	DeletePending       bool        `json:"deletePending,omitempty"`
	IsDirectoryBucket   bool        `json:"isDirectoryBucket,omitempty"`
	// ObjectLockEnabled records whether CreateBucket was called with
	// x-amz-bucket-object-lock-enabled: true. Real S3 requires this at bucket
	// creation before PutObjectLockConfiguration will accept a configuration
	// (gopherstack-pzth) -- it cannot be turned on for an existing bucket.
	ObjectLockEnabled bool `json:"objectLockEnabled,omitempty"`
}

// StoredObject represents an S3 object with its version history.
type StoredObject struct {
	Versions        map[string]*StoredObjectVersion `json:"versions,omitempty"`
	mu              *lockmetrics.RWMutex
	Key             string `json:"key"`
	LatestVersionID string `json:"latestVersionID"`
}

// StoredObjectVersion represents a specific version of an S3 object.
type StoredObjectVersion struct {
	RetainUntil             time.Time                    `json:"retainUntil"`
	RestoreExpiry           time.Time                    `json:"restoreExpiry,omitzero"`
	LastModified            time.Time                    `json:"lastModified"`
	ChecksumSHA1            *string                      `json:"checksumSHA1,omitempty"`
	Metadata                map[string]string            `json:"metadata,omitempty"`
	Annotations             map[string]*StoredAnnotation `json:"annotations,omitempty"`
	ChecksumCRC64NVME       *string                      `json:"checksumCRC64NVME,omitempty"`
	ChecksumCRC32C          *string                      `json:"checksumCRC32C,omitempty"`
	ChecksumCRC32           *string                      `json:"checksumCRC32,omitempty"`
	ChecksumSHA256          *string                      `json:"checksumSHA256,omitempty"`
	SSEAlgorithm            string                       `json:"sseAlgorithm,omitempty"`
	VersionID               string                       `json:"versionID"`
	ChecksumAlgorithm       types.ChecksumAlgorithm      `json:"checksumAlgorithm,omitempty"`
	SSECKeyMD5              string                       `json:"sseCKeyMD5,omitempty"`
	SSECAlgorithm           string                       `json:"sseCAlgorithm,omitempty"`
	Key                     string                       `json:"key"`
	ETag                    string                       `json:"etag"`
	ContentType             string                       `json:"contentType"`
	ContentEncoding         string                       `json:"contentEncoding,omitempty"`
	ContentDisposition      string                       `json:"contentDisposition,omitempty"`
	RetentionMode           string                       `json:"retentionMode,omitempty"`
	StorageClass            string                       `json:"storageClass,omitempty"`
	ACL                     string                       `json:"acl,omitempty"`
	SSEKMSKeyID             string                       `json:"sseKMSKeyID,omitempty"`
	Parts                   []StoredObjectPart           `json:"parts,omitempty"`
	EncryptionNonce         []byte                       `json:"encryptionNonce,omitempty"`
	Data                    []byte                       `json:"data,omitempty"`
	StorageClassTransitions []StorageClassTransition     `json:"storageClassTransitions,omitempty"`
	EncryptionDEK           []byte                       `json:"encryptionDEK,omitempty"`
	Size                    int64                        `json:"size"`
	IsCompressed            bool                         `json:"isCompressed,omitempty"`
	IsLatest                bool                         `json:"isLatest"`
	Deleted                 bool                         `json:"deleted,omitempty"`
	LegalHold               bool                         `json:"legalHold,omitempty"`
	OngoingRestore          bool                         `json:"ongoingRestore,omitempty"`
}

// StoredObjectPart represents metadata for an individual completed multipart upload part.
type StoredObjectPart struct {
	ChecksumCRC32     *string `json:"checksumCRC32,omitempty"`
	ChecksumCRC32C    *string `json:"checksumCRC32C,omitempty"`
	ChecksumCRC64NVME *string `json:"checksumCRC64NVME,omitempty"`
	ChecksumSHA1      *string `json:"checksumSHA1,omitempty"`
	ChecksumSHA256    *string `json:"checksumSHA256,omitempty"`
	PartNumber        int32   `json:"partNumber"`
	Size              int64   `json:"size"`
}

// StoredAnnotation represents a single named annotation attached to an
// object version (PutObjectAnnotation / GetObjectAnnotation / ListObjectAnnotations).
type StoredAnnotation struct {
	LastModified      time.Time               `json:"lastModified"`
	Name              string                  `json:"name"`
	ETag              string                  `json:"etag"`
	ChecksumAlgorithm types.ChecksumAlgorithm `json:"checksumAlgorithm,omitempty"`
	ChecksumCRC32     *string                 `json:"checksumCRC32,omitempty"`
	ChecksumCRC32C    *string                 `json:"checksumCRC32C,omitempty"`
	ChecksumSHA1      *string                 `json:"checksumSHA1,omitempty"`
	ChecksumSHA256    *string                 `json:"checksumSHA256,omitempty"`
	ChecksumCRC64NVME *string                 `json:"checksumCRC64NVME,omitempty"`
	Payload           []byte                  `json:"payload"`
}

// StorageClassTransition records a single storage class change applied by a lifecycle rule.
type StorageClassTransition struct {
	TransitionedAt time.Time `json:"transitionedAt"`
	FromClass      string    `json:"fromClass"`
	ToClass        string    `json:"toClass"`
	RuleID         string    `json:"ruleID,omitempty"`
}

// StoredMultipartUpload represents an ongoing multipart upload session.
type StoredMultipartUpload struct {
	Initiated time.Time             `json:"initiated"`
	Parts     map[int32]*StoredPart `json:"parts,omitempty"`
	mu        *lockmetrics.RWMutex  `json:"-"`
	UploadID  string                `json:"uploadID"`
	Bucket    string                `json:"bucket"`
	Key       string                `json:"key"`
	// Tagging holds the URL-encoded tag string from the X-Amz-Tagging header
	// supplied at CreateMultipartUpload time. It is applied to the resulting
	// object version when CompleteMultipartUpload succeeds.
	Tagging string `json:"tagging,omitempty"`
	// SSE captures the encryption headers from CreateMultipartUpload so the
	// completed object's assembled body can be sealed with the same envelope
	// (matching real S3 — SSE is fixed at session-init). Persisted so that an
	// in-flight upload that survives a snapshot/restore still completes with
	// the caller's chosen encryption rather than silently landing unencrypted.
	// (The SSE-C customer key inside sseInfo stays request-scoped — see
	// sseInfo.SSECKeyB64 — so SSE-C uploads still require the key on Complete.)
	SSE sseInfo `json:"sse"`
	// StorageClass is the x-amz-storage-class header from CreateMultipartUpload
	// (real S3 fixes storage class at session-init, same as SSE above). Applied
	// to the resulting object version on CompleteMultipartUpload and reported
	// back verbatim by ListMultipartUploads.
	StorageClass string `json:"storageClass,omitempty"`
	// closed is set to true by AbortMultipartUpload or CompleteMultipartUpload
	// before the upload is removed from the index, so that concurrent UploadPart
	// calls that already hold a pointer to this struct can detect the invalidation.
	closed bool `json:"-"`
}

// StoredPart represents a single part of a multipart upload.
type StoredPart struct {
	ETag              string  `json:"etag"`
	ChecksumCRC32     *string `json:"checksumCRC32,omitempty"`
	ChecksumCRC32C    *string `json:"checksumCRC32C,omitempty"`
	ChecksumCRC64NVME *string `json:"checksumCRC64NVME,omitempty"`
	ChecksumSHA1      *string `json:"checksumSHA1,omitempty"`
	ChecksumSHA256    *string `json:"checksumSHA256,omitempty"`
	Data              []byte  `json:"data,omitempty"`
	PartNumber        int32   `json:"partNumber"`
	Size              int64   `json:"size"`
}

// ObjectMetadata holds internal metadata for storage operations.
// (Keeping this compatibility type if needed, though mostly replaced by SDK types usage).
type ObjectMetadata struct {
	Tags              *tags.Tags
	UserMetadata      map[string]string
	ContentType       string
	ChecksumAlgorithm string
	ChecksumValue     string
}
