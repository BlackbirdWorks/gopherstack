package s3

import (
	"context"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

type StorageBackend interface {
	CreateBucket(ctx context.Context, name string) error
	DeleteBucket(ctx context.Context, name string) error
	HeadBucket(ctx context.Context, name string) (*Bucket, error)
	ListBuckets(ctx context.Context) ([]*Bucket, error)

	PutObject(ctx context.Context, bucket string, key string, data []byte, meta ObjectMetadata) (*ObjectVersion, error)
	GetObject(ctx context.Context, bucket, key, versionID string) (*ObjectVersion, error)
	HeadObject(ctx context.Context, bucket, key, versionID string) (*ObjectVersion, error)
	DeleteObject(ctx context.Context, bucket, key, versionID string) (deleteMarkerVersionID string, err error)
	ListObjects(ctx context.Context, bucket, prefix string) ([]*Object, error)
	ListObjectVersions(ctx context.Context, bucket, prefix string) ([]ObjectVersion, error)

	// Versioning
	PutBucketVersioning(ctx context.Context, bucket string, status VersioningStatus) error
	GetBucketVersioning(ctx context.Context, bucket string) (VersioningConfiguration, error)

	// Tagging
	PutObjectTagging(ctx context.Context, bucket, key, versionID string, tags map[string]string) error
	GetObjectTagging(ctx context.Context, bucket, key, versionID string) (map[string]string, error)
	DeleteObjectTagging(ctx context.Context, bucket, key, versionID string) error

	// Multipart
	InitiateMultipartUpload(ctx context.Context, bucket, key string) (string, error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, data []byte) (string, error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []CompletedPartXML) (*ObjectVersion, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
}
