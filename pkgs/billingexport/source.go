// Package billingexport holds the cost-line source, S3 sink and file encoders shared by the
// Cost and Usage Reports and Billing and Cost Management Data Exports emulators.
package billingexport

import (
	"context"
	"time"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// CostLine is one day of cost for a service/region/usage-type/account combination.
type CostLine struct {
	Day       time.Time
	Service   string
	Region    string
	UsageType string
	Account   string
	Blended   float64
	Unblended float64
	Usage     float64
}

// CostSource yields the daily cost lines with start <= Day < end.
type CostSource interface {
	CostLines(start, end time.Time) []CostLine
}

// ObjectStore is the S3 surface report delivery writes to.
type ObjectStore interface {
	PutObject(ctx context.Context, in *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error)
	HeadBucket(ctx context.Context, in *sdk_s3.HeadBucketInput) (*sdk_s3.HeadBucketOutput, error)
}

// Put writes body to bucket/key.
func Put(ctx context.Context, store ObjectStore, bucket, key string, body []byte, contentType string) error {
	in := &sdk_s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytesReader(body)}
	if contentType != "" {
		in.ContentType = &contentType
	}

	_, err := store.PutObject(ctx, in)

	return err
}

// BucketExists reports whether bucket is present in store.
func BucketExists(ctx context.Context, store ObjectStore, bucket string) bool {
	_, err := store.HeadBucket(ctx, &sdk_s3.HeadBucketInput{Bucket: &bucket})

	return err == nil
}
