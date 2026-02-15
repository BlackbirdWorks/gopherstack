package s3

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

type StorageBackend interface {
	CreateBucket(name string) error
	DeleteBucket(name string) error
	HeadBucket(name string) (*Bucket, error)
	ListBuckets() ([]*Bucket, error)

	PutObject(bucket string, key string, data []byte, meta ObjectMetadata) (*ObjectVersion, error)
	GetObject(bucket, key, versionID string) (*ObjectVersion, error)
	HeadObject(bucket, key, versionID string) (*ObjectVersion, error)
	DeleteObject(bucket, key, versionID string) (deleteMarkerVersionID string, err error)
	ListObjects(bucket, prefix string) ([]*Object, error)
	ListObjectVersions(bucket, prefix string) ([]ObjectVersion, error)

	// Versioning
	PutBucketVersioning(bucket string, status VersioningStatus) error

	// Tagging
	PutObjectTagging(bucket, key, versionID string, tags map[string]string) error
	GetObjectTagging(bucket, key, versionID string) (map[string]string, error)
	DeleteObjectTagging(bucket, key, versionID string) error
}
