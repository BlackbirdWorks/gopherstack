package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	PutObjectLegalHold(ctx context.Context, bucket, key string, versionID *string, status string) error
	GetObjectLegalHold(ctx context.Context, bucket, key string, versionID *string) (status string, err error)

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
}
