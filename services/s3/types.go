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
	Objects                 map[string]*StoredObject `json:"objects,omitempty"`
	mu                      *lockmetrics.RWMutex
	Name                    string    `json:"name"`
	Policy                  string    `json:"policy,omitempty"`
	CORSConfig              string    `json:"corsConfig,omitempty"`
	LifecycleConfig         string    `json:"lifecycleConfig,omitempty"`
	NotificationConfig      string    `json:"notificationConfig,omitempty"`
	ObjectLockConfig        string    `json:"objectLockConfig,omitempty"`
	WebsiteConfig           string    `json:"websiteConfig,omitempty"`
	EncryptionConfig        string    `json:"encryptionConfig,omitempty"`
	PublicAccessBlockConfig string    `json:"publicAccessBlockConfig,omitempty"`
	OwnershipControlsConfig string    `json:"ownershipControlsConfig,omitempty"`
	LoggingConfig           string    `json:"loggingConfig,omitempty"`
	ReplicationConfig       string    `json:"replicationConfig,omitempty"`
	CreationDate            time.Time `json:"creationDate"`
	ACL                     string    `json:"acl,omitempty"`
	// Versioning and DeletePending are placed last so the non-pointer bool field
	// sits at the end of the struct with no trailing padding words needing GC scan.
	Versioning types.BucketVersioningStatus `json:"versioning,omitempty"`
	// DeletePending is true when the bucket is queued for async deletion by the Janitor.
	// Operations on a DeletePending bucket behave as if the bucket does not exist.
	DeletePending bool `json:"deletePending,omitempty"`
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
	LastModified       time.Time               `json:"lastModified"`
	RetainUntil        time.Time               `json:"retainUntil"`
	ChecksumSHA1       *string                 `json:"checksumSHA1,omitempty"`
	Metadata           map[string]string       `json:"metadata,omitempty"`
	ChecksumSHA256     *string                 `json:"checksumSHA256,omitempty"`
	ChecksumCRC32      *string                 `json:"checksumCRC32,omitempty"`
	ChecksumCRC32C     *string                 `json:"checksumCRC32C,omitempty"`
	Key                string                  `json:"key"`
	ETag               string                  `json:"etag"`
	ContentType        string                  `json:"contentType"`
	ContentEncoding    string                  `json:"contentEncoding,omitempty"`
	ContentDisposition string                  `json:"contentDisposition,omitempty"`
	RetentionMode      string                  `json:"retentionMode,omitempty"`
	ChecksumAlgorithm  types.ChecksumAlgorithm `json:"checksumAlgorithm,omitempty"`
	VersionID          string                  `json:"versionID"`
	Data               []byte                  `json:"data,omitempty"`
	Size               int64                   `json:"size"`
	IsCompressed       bool                    `json:"isCompressed,omitempty"`
	IsLatest           bool                    `json:"isLatest"`
	Deleted            bool                    `json:"deleted,omitempty"`
	LegalHold          bool                    `json:"legalHold,omitempty"`
}

// StoredMultipartUpload represents an ongoing multipart upload session.
type StoredMultipartUpload struct {
	Initiated time.Time             `json:"initiated"`
	Parts     map[int32]*StoredPart `json:"parts,omitempty"`
	UploadID  string                `json:"uploadID"`
	Bucket    string                `json:"bucket"`
	Key       string                `json:"key"`
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
