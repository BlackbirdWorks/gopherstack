package backup

import "strings"

const arnResourceParts = 6

// resourceTypeFromARN maps a protected-resource ARN to AWS Backup's ResourceType label; "" when unclassified.
func resourceTypeFromARN(resourceArn string) string {
	parts := strings.SplitN(resourceArn, ":", arnResourceParts)
	if len(parts) < arnResourceParts {
		return ""
	}

	switch parts[2] {
	case "ec2":
		return prefixedResourceType(parts[5], "volume/", "EBS", "instance/", "EC2")
	case "rds":
		return prefixedResourceType(parts[5], "db:", "RDS", "cluster:", "Aurora")
	}

	return map[string]string{
		"dynamodb":          "DynamoDB",
		"s3":                "S3",
		"elasticfilesystem": "EFS",
		"fsx":               "FSx",
		"storagegateway":    "Storage Gateway",
		"redshift":          "Redshift",
		"timestream":        "Timestream",
		"cloudformation":    "CloudFormation",
	}[parts[2]]
}

func prefixedResourceType(resource, prefixA, typeA, prefixB, typeB string) string {
	switch {
	case strings.HasPrefix(resource, prefixA):
		return typeA
	case strings.HasPrefix(resource, prefixB):
		return typeB
	}

	return ""
}

// resourceNameFromARN returns the trailing resource identifier of an ARN (table name, bucket, volume id).
func resourceNameFromARN(resourceArn string) string {
	parts := strings.SplitN(resourceArn, ":", arnResourceParts)
	if len(parts) < arnResourceParts {
		return ""
	}

	resource := parts[5]
	if i := strings.LastIndexAny(resource, "/:"); i >= 0 {
		return resource[i+1:]
	}

	return resource
}
