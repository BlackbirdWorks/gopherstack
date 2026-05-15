// Package arn provides utilities for building AWS ARNs.
package arn

import (
	"fmt"
	"strings"
)

// PartitionForRegion returns the AWS partition (e.g. "aws", "aws-us-gov",
// "aws-cn", "aws-iso", "aws-iso-b") for the given region. Empty or unknown
// regions resolve to the standard "aws" partition.
//
// Real AWS resources living in GovCloud or China use distinct ARN partitions
// and clients reject cross-partition ARNs. Use this helper when constructing
// ARNs from a region whose partition is not known a priori.
func PartitionForRegion(region string) string {
	switch {
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-iso-"):
		return "aws-iso"
	case strings.HasPrefix(region, "us-isob-"):
		return "aws-iso-b"
	default:
		return "aws"
	}
}

// Build constructs an AWS ARN from the given components.
// For IAM (service == "iam"), the region is omitted as IAM is a global service.
// The partition is derived from the region so GovCloud, China, and ISO
// regions produce wire-correct ARNs.
// Format: arn:{partition}:{service}:{region}:{accountID}:{resource}.
func Build(service, region, accountID, resource string) string {
	partition := PartitionForRegion(region)
	if service == "iam" {
		return fmt.Sprintf("arn:%s:iam::%s:%s", partition, accountID, resource)
	}

	return fmt.Sprintf("arn:%s:%s:%s:%s:%s", partition, service, region, accountID, resource)
}

// BuildS3 constructs an ARN for an S3 resource.
// S3 ARNs have no region or account ID component.
// Format: arn:aws:s3:::{resource}.
func BuildS3(resource string) string {
	return "arn:aws:s3:::" + resource
}
