// Package arn provides utilities for building AWS ARNs.
package arn

import "fmt"

// Build constructs an AWS ARN from the given components.
// For IAM (service == "iam"), the region is omitted as IAM is a global service.
// Format: arn:aws:{service}:{region}:{accountID}:{resource}.
func Build(service, region, accountID, resource string) string {
	if service == "iam" {
		return fmt.Sprintf("arn:aws:iam::%s:%s", accountID, resource)
	}

	return fmt.Sprintf("arn:aws:%s:%s:%s:%s", service, region, accountID, resource)
}

// BuildS3 constructs an ARN for an S3 resource.
// S3 ARNs have no region or account ID component.
// Format: arn:aws:s3:::{resource}.
func BuildS3(resource string) string {
	return "arn:aws:s3:::" + resource
}
