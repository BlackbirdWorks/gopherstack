package iam

import (
	"context"
	"net/http"
	"strings"
)

// ResourcePolicyProvider is implemented by service backends that support
// resource-based policies (e.g. S3 bucket policies, SQS queue policies).
// GetResourcePolicy returns the JSON policy document for the given resource ARN,
// or ("", nil) when the resource has no policy.
type ResourcePolicyProvider interface {
	GetResourcePolicy(ctx context.Context, resourceARN string) (string, error)
}

// ActionExtractor is an optional interface that service handlers can implement
// to provide IAM action extraction for their specific request patterns.
// It is used by the enforcement middleware as a fallback when the global action
// mapper cannot determine the IAM action (e.g. for REST-based services like
// Lambda and Route53 that do not use X-Amz-Target or form-encoded bodies).
//
// Each extractor must first check whether the request belongs to its service
// (e.g. by path prefix) and return "" when the request is not its own.
type ActionExtractor interface {
	IAMAction(r *http.Request) string
}

// extractResourceARN attempts to derive the AWS ARN of the resource being
// accessed from the HTTP request path and context.
//
// Rules:
//   - S3 path /bucket  → arn:aws:s3:::bucket
//   - S3 path /bucket/key → arn:aws:s3:::bucket/key
//   - SQS paths  /{accountID}/{queue} → arn:aws:sqs:{region}:{accountID}:{queue}
//
// Returns "" when the resource ARN cannot be determined.
func extractResourceARN(r *http.Request, accountID, region string) string {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		return ""
	}

	// SQS URL format: /{accountID}/{queueName}
	if looksLikeSQSPath(path, accountID) {
		return extractSQSResourceARN(path, region, accountID)
	}

	// S3 path-style: /bucket or /bucket/key
	return extractS3ResourceARN(path)
}

// looksLikeSQSPath returns true when the path starts with the mock account ID,
// which is the pattern used for SQS queue URLs.
func looksLikeSQSPath(path, accountID string) bool {
	return strings.HasPrefix(path, accountID+"/")
}

// extractSQSResourceARN extracts the SQS ARN from a path like {accountID}/{queueName}.
func extractSQSResourceARN(path, region, accountID string) string {
	// Strip the leading accountID segment.
	queueName := strings.TrimPrefix(path, accountID+"/")
	if queueName == "" || strings.Contains(queueName, "/") {
		return ""
	}

	return "arn:aws:sqs:" + region + ":" + accountID + ":" + queueName
}

// extractS3ResourceARN builds an S3 ARN from a path-style S3 URL path.
func extractS3ResourceARN(path string) string {
	// S3 ARNs have no region or account component.
	return "arn:aws:s3:::" + path
}
