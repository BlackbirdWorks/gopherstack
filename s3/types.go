package s3

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// NullVersion is the version ID used when versioning is not enabled.
const NullVersion = "null"

// StoredBucket represents an S3 bucket in memory.
type StoredBucket struct {
	CreationDate time.Time
	Objects      map[string]*StoredObject
	Name         string
	Versioning   types.BucketVersioningStatus
}

// StoredObject represents an S3 object with its version history.
type StoredObject struct {
	Versions map[string]*StoredObjectVersion
	Key      string
}

// StoredObjectVersion represents a specific version of an S3 object.
type StoredObjectVersion struct {
	LastModified      time.Time
	ChecksumSHA1      *string
	Metadata          map[string]string
	ChecksumSHA256    *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	Key               string
	ETag              string
	ContentType       string
	ChecksumAlgorithm types.ChecksumAlgorithm
	VersionID         string
	Data              []byte
	Size              int64
	IsCompressed      bool
	IsLatest          bool
	Deleted           bool
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
