// Package billingexport holds the cost-line source, S3 sink and file encoders shared by the
// Cost and Usage Reports and Billing and Cost Management Data Exports emulators.
package billingexport

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// CostLine is one day of cost for a service/region/usage-type/account combination.
type CostLine struct {
	Day              time.Time
	Tags             map[string]string
	Service          string
	Region           string
	UsageType        string
	Account          string
	AvailabilityZone string
	InstanceType     string
	Operation        string
	ChargeType       string
	PurchaseType     string
	ProductFamily    string
	UsageTypeGroup   string
	ResourceID       string
	Blended          float64
	Unblended        float64
	Usage            float64
}

const (
	keyFields   = 12
	chargeUsage = "Usage"
)

// Key identifies the dimensions a monthly roll-up merges lines on.
func (l CostLine) Key() string {
	parts := make([]string, 0, keyFields+len(l.Tags))
	parts = append(parts,
		l.Service, l.Region, l.UsageType, l.Account, l.AvailabilityZone, l.InstanceType, l.Operation,
		l.ChargeType, l.PurchaseType, l.ProductFamily, l.UsageTypeGroup, l.ResourceID,
	)

	for _, k := range slices.Sorted(maps.Keys(l.Tags)) {
		parts = append(parts, k+"="+l.Tags[k])
	}

	return strings.Join(parts, "\x00")
}

// LineItemType maps the ledger charge type onto a CUR lineItem/LineItemType value.
func (l CostLine) LineItemType() string {
	switch l.ChargeType {
	case "", chargeUsage:
		return chargeUsage
	case "Support", "Subscription", "Upfront":
		return "Fee"
	case "Recurring":
		return "RIFee"
	default:
		return l.ChargeType
	}
}

// ChargeCategory maps the ledger charge type onto a FOCUS ChargeCategory value.
func (l CostLine) ChargeCategory() string {
	switch l.ChargeType {
	case "", chargeUsage:
		return chargeUsage
	case "Tax":
		return "Tax"
	case "Credit":
		return "Credit"
	case "Refund", "Discount":
		return "Adjustment"
	default:
		return "Purchase"
	}
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
