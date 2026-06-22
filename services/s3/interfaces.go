package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

type StorageBackend interface {
	CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error)
	DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error)
	HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error)
	ListBuckets(ctx context.Context, input *s3.ListBucketsInput) (*s3.ListBucketsOutput, error)

	PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	DeleteObjects(
		ctx context.Context,
		input *s3.DeleteObjectsInput,
	) (*s3.DeleteObjectsOutput, error)
	ListObjects(ctx context.Context, input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error)
	ListObjectsV2(
		ctx context.Context,
		input *s3.ListObjectsV2Input,
	) (*s3.ListObjectsV2Output, error)
	ListObjectVersions(
		ctx context.Context,
		input *s3.ListObjectVersionsInput,
	) (*s3.ListObjectVersionsOutput, error)

	// Versioning
	PutBucketVersioning(
		ctx context.Context,
		input *s3.PutBucketVersioningInput,
	) (*s3.PutBucketVersioningOutput, error)
	GetBucketVersioning(
		ctx context.Context,
		input *s3.GetBucketVersioningInput,
	) (*s3.GetBucketVersioningOutput, error)

	// Tagging
	PutObjectTagging(
		ctx context.Context,
		input *s3.PutObjectTaggingInput,
	) (*s3.PutObjectTaggingOutput, error)
	GetObjectTagging(
		ctx context.Context,
		input *s3.GetObjectTaggingInput,
	) (*s3.GetObjectTaggingOutput, error)
	DeleteObjectTagging(
		ctx context.Context,
		input *s3.DeleteObjectTaggingInput,
	) (*s3.DeleteObjectTaggingOutput, error)

	// Bucket Tagging
	PutBucketTagging(ctx context.Context, bucket string, tags []types.Tag) error
	GetBucketTagging(ctx context.Context, bucket string) ([]types.Tag, error)
	DeleteBucketTagging(ctx context.Context, bucket string) error

	// ACL
	PutBucketACL(ctx context.Context, bucket, acl string) error
	GetBucketACL(ctx context.Context, bucket string) (string, error)

	// Policy
	PutBucketPolicy(ctx context.Context, bucket, policy string) error
	GetBucketPolicy(ctx context.Context, bucket string) (string, error)
	DeleteBucketPolicy(ctx context.Context, bucket string) error

	// CORS
	PutBucketCORS(ctx context.Context, bucket, corsXML string) error
	GetBucketCORS(ctx context.Context, bucket string) (string, error)
	DeleteBucketCORS(ctx context.Context, bucket string) error

	// Lifecycle
	PutBucketLifecycleConfiguration(ctx context.Context, bucket, lifecycleXML string) error
	GetBucketLifecycleConfiguration(ctx context.Context, bucket string) (string, error)
	DeleteBucketLifecycleConfiguration(ctx context.Context, bucket string) error

	// Website
	PutBucketWebsite(ctx context.Context, bucket, websiteXML string) error
	GetBucketWebsite(ctx context.Context, bucket string) (string, error)
	DeleteBucketWebsite(ctx context.Context, bucket string) error

	// Encryption
	PutBucketEncryption(ctx context.Context, bucket, encryptionXML string) error
	GetBucketEncryption(ctx context.Context, bucket string) (string, error)
	DeleteBucketEncryption(ctx context.Context, bucket string) error

	// Public Access Block
	PutPublicAccessBlock(ctx context.Context, bucket, configXML string) error
	GetPublicAccessBlock(ctx context.Context, bucket string) (string, error)
	DeletePublicAccessBlock(ctx context.Context, bucket string) error

	// Ownership Controls
	PutBucketOwnershipControls(ctx context.Context, bucket, configXML string) error
	GetBucketOwnershipControls(ctx context.Context, bucket string) (string, error)
	DeleteBucketOwnershipControls(ctx context.Context, bucket string) error

	// Logging
	PutBucketLogging(ctx context.Context, bucket, loggingXML string) error
	GetBucketLogging(ctx context.Context, bucket string) (string, error)

	// Replication
	PutBucketReplication(ctx context.Context, bucket, replicationXML string) error
	GetBucketReplication(ctx context.Context, bucket string) (string, error)
	DeleteBucketReplication(ctx context.Context, bucket string) error

	// Notifications
	PutBucketNotificationConfiguration(ctx context.Context, bucket, notifXML string) error
	GetBucketNotificationConfiguration(ctx context.Context, bucket string) (string, error)

	// Object Lock
	PutObjectLockConfiguration(ctx context.Context, bucket, configXML string) error
	GetObjectLockConfiguration(ctx context.Context, bucket string) (string, error)
	PutObjectRetention(
		ctx context.Context,
		bucket, key string,
		versionID *string,
		mode string,
		retainUntil time.Time,
	) error
	GetObjectRetention(
		ctx context.Context,
		bucket, key string,
		versionID *string,
	) (mode string, retainUntil time.Time, err error)
	PutObjectLegalHold(
		ctx context.Context,
		bucket, key string,
		versionID *string,
		status string,
	) error
	GetObjectLegalHold(
		ctx context.Context,
		bucket, key string,
		versionID *string,
	) (status string, err error)

	// Multipart
	CreateMultipartUpload(
		ctx context.Context,
		input *s3.CreateMultipartUploadInput,
	) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(
		ctx context.Context,
		input *s3.CompleteMultipartUploadInput,
	) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(
		ctx context.Context,
		input *s3.AbortMultipartUploadInput,
	) (*s3.AbortMultipartUploadOutput, error)
	ListMultipartUploads(
		ctx context.Context,
		input *s3.ListMultipartUploadsInput,
	) (*s3.ListMultipartUploadsOutput, error)
	ListParts(
		ctx context.Context,
		input *s3.ListPartsInput,
	) (*s3.ListPartsOutput, error)

	// Metadata helpers
	GetBucketMetadata(
		ctx context.Context,
		bucketName string,
	) (region string, lifecycleXML string, tags []types.Tag, err error)

	// Analytics (supports multiple configs per bucket via id)
	PutBucketAnalyticsConfiguration(ctx context.Context, bucket, id, configXML string) error
	GetBucketAnalyticsConfiguration(ctx context.Context, bucket, id string) (string, error)
	DeleteBucketAnalyticsConfiguration(ctx context.Context, bucket, id string) error
	ListBucketAnalyticsConfigurations(ctx context.Context, bucket string) ([]string, error)

	// Intelligent Tiering (supports multiple configs per bucket via id)
	PutBucketIntelligentTieringConfiguration(
		ctx context.Context,
		bucket, id, configXML string,
	) error
	GetBucketIntelligentTieringConfiguration(ctx context.Context, bucket, id string) (string, error)
	DeleteBucketIntelligentTieringConfiguration(ctx context.Context, bucket, id string) error
	ListBucketIntelligentTieringConfigurations(ctx context.Context, bucket string) ([]string, error)

	// Inventory (supports multiple configs per bucket via id)
	PutBucketInventoryConfiguration(ctx context.Context, bucket, id, configXML string) error
	GetBucketInventoryConfiguration(ctx context.Context, bucket, id string) (string, error)
	DeleteBucketInventoryConfiguration(ctx context.Context, bucket, id string) error
	ListBucketInventoryConfigurations(ctx context.Context, bucket string) ([]string, error)

	// Lifecycle (legacy alias)
	DeleteBucketLifecycle(ctx context.Context, bucket string) error

	// Metadata Configuration
	CreateBucketMetadataConfiguration(ctx context.Context, bucket, configXML string) error
	GetBucketMetadataConfiguration(ctx context.Context, bucket string) (string, error)
	DeleteBucketMetadataConfiguration(ctx context.Context, bucket string) error

	// Metadata Table Configuration
	CreateBucketMetadataTableConfiguration(ctx context.Context, bucket, configXML string) error
	GetBucketMetadataTableConfiguration(ctx context.Context, bucket string) (string, error)
	DeleteBucketMetadataTableConfiguration(ctx context.Context, bucket string) error

	// Metrics (supports multiple configs per bucket via id)
	PutBucketMetricsConfiguration(ctx context.Context, bucket, id, configXML string) error
	GetBucketMetricsConfiguration(ctx context.Context, bucket, id string) (string, error)
	DeleteBucketMetricsConfiguration(ctx context.Context, bucket, id string) error
	ListBucketMetricsConfigurations(ctx context.Context, bucket string) ([]string, error)

	// Session
	CreateSession(ctx context.Context, bucket string) (string, error)

	// Accelerate / RequestPayment configurations
	PutBucketAccelerateConfiguration(ctx context.Context, bucket, status string) error
	GetBucketAccelerateConfiguration(ctx context.Context, bucket string) (string, error)
	PutBucketRequestPayment(ctx context.Context, bucket, payer string) error
	GetBucketRequestPayment(ctx context.Context, bucket string) (string, error)

	// ABAC Configuration (S3 Tables / Express)
	PutBucketAbac(ctx context.Context, bucket, configXML string) error
	GetBucketAbac(ctx context.Context, bucket string) (string, error)

	// S3 Express directory buckets
	ListDirectoryBuckets(ctx context.Context) ([]types.Bucket, error)

	// Metadata Inventory / Journal Table Configurations (S3 Tables)
	UpdateBucketMetadataInventoryTableConfig(ctx context.Context, bucket, configXML string) error
	UpdateBucketMetadataJournalTableConfig(ctx context.Context, bucket, configXML string) error

	// GetObjectAttributes / RestoreObject / RenameObject
	GetObjectAttributes(
		ctx context.Context,
		bucket, key, versionID string,
	) (*ObjectAttributes, error)
	RestoreObject(ctx context.Context, bucket, key string, days int) error
	RenameObject(ctx context.Context, bucket, sourceKey, targetKey string) error

	// Object ACLs
	PutObjectACL(ctx context.Context, bucket, key, versionID, acl string) error
	GetObjectACL(ctx context.Context, bucket, key, versionID string) (string, error)

	// Per-object SSE updates
	UpdateObjectEncryption(ctx context.Context, bucket, key, algorithm, kmsKeyID string) error
}
