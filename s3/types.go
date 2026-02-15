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
	Key      string
	Versions map[string]*StoredObjectVersion
}

// StoredObjectVersion represents a specific version of an S3 object.
type StoredObjectVersion struct {
	VersionID    string
	Key          string
	Data         []byte
	IsCompressed bool
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
	Metadata     map[string]string

	// Checksums
	ChecksumAlgorithm types.ChecksumAlgorithm // The algorithm used for calculation (if known/stored)
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string

	IsLatest bool
	Deleted  bool
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
// (Keeping this compatibility type if needed, though mostly replaced by SDK types usage)
type ObjectMetadata struct {
	Tags              map[string]string
	UserMetadata      map[string]string
	ContentType       string
	ChecksumAlgorithm string
	ChecksumValue     string
}
