package elasticbeanstalk

import "context"

// CreateStorageLocation returns the S3 bucket used for storing Elastic Beanstalk data.
// The bucket name is fixed per region and account, and creation is idempotent.
func (b *InMemoryBackend) CreateStorageLocation(ctx context.Context) string {
	region := getRegion(ctx, b.region)

	return "elasticbeanstalk-" + region + "-" + b.accountID
}
