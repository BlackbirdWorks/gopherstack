package s3

import (
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// NullVersion is the version ID used when versioning is not enabled.
const NullVersion = "null"

// StoredBucket represents an S3 bucket in memory.
type StoredBucket struct {
	Objects      map[string]*StoredObject
	mu           *lockmetrics.RWMutex
	Name         string
	Policy       string // JSON policy document
	CORSConfig   string // XML CORS configuration
	CreationDate time.Time
	ACL          string
	// Versioning must precede non-pointer fields so its trailing len word falls
	// outside the GC scan range, reducing pointer bytes to 64.
	Versioning types.BucketVersioningStatus
	// DeletePending is true when the bucket is queued for async deletion by the Janitor.
	// Operations on a DeletePending bucket behave as if the bucket does not exist.
	DeletePending bool
}

// StoredObject represents an S3 object with its version history.
type StoredObject struct {
	Versions        map[string]*StoredObjectVersion
	Key             string
	LatestVersionID string // Cache of the latest version ID to avoid scanning all versions
	mu              sync.RWMutex
}

// StoredObjectVersion represents a specific version of an S3 object.
type StoredObjectVersion struct {
	LastModified       time.Time
	ChecksumSHA1       *string
	Metadata           map[string]string
	ChecksumSHA256     *string
	ChecksumCRC32      *string
	ChecksumCRC32C     *string
	Key                string
	ETag               string
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	ChecksumAlgorithm  types.ChecksumAlgorithm
	VersionID          string
	Data               []byte
	Size               int64
	IsCompressed       bool
	IsLatest           bool
	Deleted            bool
}

// StoredMultipartUpload represents an ongoing multipart upload session.
type StoredMultipartUpload struct {
	Initiated time.Time
	Parts     map[int32]*StoredPart
	UploadID  string
	Bucket    string
	Key       string
}

// StoredPart represents a single part of a multipart upload.
type StoredPart struct {
	ETag       string
	Data       []byte
	PartNumber int32
	Size       int64
}

// ObjectMetadata holds internal metadata for storage operations.
// (Keeping this compatibility type if needed, though mostly replaced by SDK types usage).
type ObjectMetadata struct {
	Tags              map[string]string
	UserMetadata      map[string]string
	ContentType       string
	ChecksumAlgorithm string
	ChecksumValue     string
}
