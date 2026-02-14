package s3

import (
	"time"
)

// NullVersion is the version ID used when versioning is not enabled.
const NullVersion = "null"

// VersioningStatus represents the versioning state of a bucket.
type VersioningStatus string

const (
	// VersioningEnabled means versioning is enabled on the bucket.
	VersioningEnabled VersioningStatus = "Enabled"
	// VersioningSuspended means versioning is suspended on the bucket.
	VersioningSuspended VersioningStatus = "Suspended"
)

// Bucket represents an S3 bucket.
type Bucket struct {
	Name         string
	CreationDate time.Time
	Versioning   VersioningStatus
}

// Object represents an S3 object with its version history.
type Object struct {
	LastModified time.Time
	Tags         map[string]string
	Key          string
	ContentType  string
	Versions     []ObjectVersion
	Size         int64
}

// ObjectVersion represents a specific version of an S3 object.
type ObjectVersion struct {
	LastModified      time.Time
	VersionID         string
	ETag              string
	ChecksumAlgorithm string
	ChecksumValue     string
	Data              []byte
	Size              int64
	IsLatest          bool
	Deleted           bool
}

// ObjectMetadata holds metadata provided with PutObject calls.
type ObjectMetadata struct {
	Tags              map[string]string
	ContentType       string
	ChecksumAlgorithm string
	ChecksumValue     string
}
