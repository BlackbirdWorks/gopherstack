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
type StoredBucket struct {
	CreationDate              time.Time                `json:"creationDate"`
	Objects                   map[string]*StoredObject `json:"objects,omitempty"`
	mu                        *lockmetrics.RWMutex
	WebsiteConfig             string                       `json:"websiteConfig,omitempty"`
	PublicAccessBlockConfig   string                       `json:"publicAccessBlockConfig,omitempty"`
	LifecycleConfig           string                       `json:"lifecycleConfig,omitempty"`
	NotificationConfig        string                       `json:"notificationConfig,omitempty"`
	ObjectLockConfig          string                       `json:"objectLockConfig,omitempty"`
	Policy                    string                       `json:"policy,omitempty"`
	EncryptionConfig          string                       `json:"encryptionConfig,omitempty"`
	CORSConfig                string                       `json:"corsConfig,omitempty"`
	OwnershipControlsConfig   string                       `json:"ownershipControlsConfig,omitempty"`
	LoggingConfig             string                       `json:"loggingConfig,omitempty"`
	ReplicationConfig         string                       `json:"replicationConfig,omitempty"`
	AnalyticsConfigs          map[string]string            `json:"analyticsConfigs,omitempty"`
	IntelligentTieringConfigs map[string]string            `json:"intelligentTieringConfigs,omitempty"`
	InventoryConfigs          map[string]string            `json:"inventoryConfigs,omitempty"`
	MetadataConfig            string                       `json:"metadataConfig,omitempty"`
	MetadataTableConfig       string                       `json:"metadataTableConfig,omitempty"`
	MetricsConfigs            map[string]string            `json:"metricsConfigs,omitempty"`
	Versioning                types.BucketVersioningStatus `json:"versioning,omitempty"`
	Name                      string                       `json:"name"`
	ACL                       string                       `json:"acl,omitempty"`
	AccelerateStatus          string                       `json:"accelerateStatus,omitempty"`
	RequestPaymentPayer       string                       `json:"requestPaymentPayer,omitempty"`
	Tags                      []types.Tag                  `json:"tags,omitempty"`
	DeletePending             bool                         `json:"deletePending,omitempty"`
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
	LastModified      time.Time         `json:"lastModified"`
	RetainUntil       time.Time         `json:"retainUntil"`
	RestoreExpiry     time.Time         `json:"restoreExpiry,omitzero"`
	ChecksumSHA1      *string           `json:"checksumSHA1,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	ChecksumSHA256    *string           `json:"checksumSHA256,omitempty"`
	ChecksumCRC32     *string           `json:"checksumCRC32,omitempty"`
	ChecksumCRC32C    *string           `json:"checksumCRC32C,omitempty"`
	ChecksumCRC64NVME *string           `json:"checksumCRC64NVME,omitempty"`
	SSEAlgorithm      string            `json:"sseAlgorithm,omitempty"`
	SSEKMSKeyID       string            `json:"sseKMSKeyID,omitempty"`
	SSECAlgorithm     string            `json:"sseCAlgorithm,omitempty"`
	SSECKeyMD5        string            `json:"sseCKeyMD5,omitempty"`
	// EncryptionDEK is the AES-256 data encryption key randomly generated on
	// PUT for SSE-S3/SSE-KMS objects. Real S3 wraps this under a KMS CMK and
	// stores only the wrapped form; for an in-memory mock the storage is
	// the same address space so we keep the raw key. SSE-C objects don't
	// store the key — the customer re-supplies it on GET.
	EncryptionDEK []byte `json:"-"`
	// EncryptionNonce is the GCM nonce/IV used for this object's ciphertext.
	// Stored alongside the ciphertext (in StoredObjectVersion.Data) so GET
	// can decrypt without re-deriving anything.
	EncryptionNonce    []byte                  `json:"-"`
	Key                string                  `json:"key"`
	ETag               string                  `json:"etag"`
	ContentType        string                  `json:"contentType"`
	ContentEncoding    string                  `json:"contentEncoding,omitempty"`
	ContentDisposition string                  `json:"contentDisposition,omitempty"`
	RetentionMode      string                  `json:"retentionMode,omitempty"`
	StorageClass       string                  `json:"storageClass,omitempty"`
	ACL                string                  `json:"acl,omitempty"`
	ChecksumAlgorithm  types.ChecksumAlgorithm `json:"checksumAlgorithm,omitempty"`
	VersionID          string                  `json:"versionID"`
	Data               []byte                  `json:"data,omitempty"`
	Size               int64                   `json:"size"`
	IsCompressed       bool                    `json:"isCompressed,omitempty"`
	IsLatest           bool                    `json:"isLatest"`
	Deleted            bool                    `json:"deleted,omitempty"`
	LegalHold              bool                    `json:"legalHold,omitempty"`
	OngoingRestore         bool                    `json:"ongoingRestore,omitempty"`
	StorageClassTransitions []StorageClassTransition `json:"storageClassTransitions,omitempty"`
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
	// (matching real S3 — SSE is fixed at session-init).
	SSE sseInfo `json:"-"`
	// closed is set to true by AbortMultipartUpload or CompleteMultipartUpload
	// before the upload is removed from the index, so that concurrent UploadPart
	// calls that already hold a pointer to this struct can detect the invalidation.
	closed bool `json:"-"`
}

// StoredPart represents a single part of a multipart upload.
type StoredPart struct {
	ETag       string `json:"etag"`
	Data       []byte `json:"data,omitempty"`
	PartNumber int32  `json:"partNumber"`
	Size       int64  `json:"size"`
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
