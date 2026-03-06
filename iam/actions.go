package iam

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// targetServiceMap maps X-Amz-Target prefixes to IAM service prefixes.
// The key is the target service portion (before the dot), the value is the IAM action prefix.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var targetServiceMap = map[string]string{
	"DynamoDB_20120810":        "dynamodb",
	"DynamoDB_20111205":        "dynamodb",
	"TrentService":             "kms",
	"secretsmanager":           "secretsmanager",
	"Logs_20140328":            "logs",
	"AmazonEventBridge":        "events",
	"AWSEvents":                "events",
	"AmazonStates":             "states",
	"AWSStepFunctions":         "states",
	"Kinesis_20130901":         "kinesis",
	"AmazonSSM":                "ssm",
	"AmazonSQS":                "sqs",
	"Firehose_20150804":        "firehose",
	"AmazonSWF_20120125":       "swf",
	"ACM":                      "acm",
	"Route53Resolver_20180901": "route53resolver",
	"CodeBuild_20161006":       "codebuild",
	"TranscribeService":        "transcribe",
	"ElasticLoadBalancingV2":   "elasticloadbalancing",
	"ECS":                      "ecs",
	"ECR":                      "ecr",
	"AmazonScheduler":          "scheduler",
	"APIGateway":               "apigateway",
	"AWSLambda":                "lambda",
}

// formVersionServiceMap maps the Version field in form-encoded requests to IAM prefixes.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var formVersionServiceMap = map[string]string{
	"2010-05-08": "iam",
	"2011-06-15": "sts",
	"2010-03-31": "sns",
	"2016-11-15": "ec2",
	"2014-10-31": "rds",
	"2015-02-02": "elasticache",
	"2012-12-01": "redshift",
	"2010-05-15": "cloudformation",
}

// nonS3RESTPathPrefixes contains path prefixes that belong to other REST-based
// services and must not be misidentified as S3 paths.
// These services provide IAM action extraction via ActionExtractor or
// will be handled by dedicated path-specific extractors added to the list.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var nonS3RESTPathPrefixes = []string{
	"/2015-03-31/", // Lambda v1
	"/2020-06-30/", // Lambda v2
	"/2018-10-31/", // Lambda layers
	"/2013-04-01/", // Route53
	"/2021-01-01/", // OpenSearch management
	"/restapis/",   // API Gateway data-plane
}

// s3MethodActionMap maps HTTP method to S3 IAM action for object-level requests.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var s3MethodActionMap = map[string]string{
	http.MethodGet:    "s3:GetObject",
	http.MethodHead:   "s3:GetObject",
	http.MethodPut:    "s3:PutObject",
	http.MethodDelete: "s3:DeleteObject",
	http.MethodPost:   "s3:PutObject",
}

// s3BucketMethodActionMap maps HTTP method to S3 IAM action for bucket-level requests.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var s3BucketMethodActionMap = map[string]string{
	http.MethodGet:    "s3:ListBucket",
	http.MethodHead:   "s3:ListBucket",
	http.MethodPut:    "s3:CreateBucket",
	http.MethodDelete: "s3:DeleteBucket",
}

// cloudwatchActions is the set of supported CloudWatch form-encoded actions.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var cloudwatchActions = map[string]bool{
	"PutMetricData":           true,
	"GetMetricData":           true,
	"GetMetricStatistics":     true,
	"ListMetrics":             true,
	"DescribeAlarms":          true,
	"PutMetricAlarm":          true,
	"DeleteAlarms":            true,
	"EnableAlarmActions":      true,
	"DisableAlarmActions":     true,
	"SetAlarmState":           true,
	"DescribeAlarmsForMetric": true,
	"ListTagsForResource":     true,
	"TagResource":             true,
	"UntagResource":           true,
}

// ExtractIAMAction determines the IAM action string for an HTTP request.
// Returns the action in "service:Operation" format (e.g., "s3:PutObject", "dynamodb:GetItem").
// Returns an empty string if the action cannot be determined.
func ExtractIAMAction(r *http.Request) string {
	// 1. X-Amz-Target based services (DynamoDB, KMS, CloudWatch Logs, EventBridge, etc.)
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		return targetToIAMAction(target)
	}

	// 2. Form-encoded services: parse body or query string for Action + Version
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/x-www-form-urlencoded") {
		return formActionToIAMAction(r)
	}

	// 3. S3 path-based routing (catch-all — must come last)
	return extractS3IAMAction(r)
}

// targetToIAMAction converts an X-Amz-Target header value to an IAM action string.
// e.g., "DynamoDB_20120810.GetItem" → "dynamodb:GetItem".
func targetToIAMAction(target string) string {
	before, operation, found := strings.Cut(target, ".")
	if !found || operation == "" {
		return ""
	}

	if iamPrefix, ok := targetServiceMap[before]; ok {
		return iamPrefix + ":" + operation
	}

	return ""
}

// formActionToIAMAction parses a form-encoded request body to extract
// the Action and Version fields, then maps them to an IAM action string.
func formActionToIAMAction(r *http.Request) string {
	body, err := readBodyPreserving(r)
	if err != nil {
		return ""
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	action := vals.Get("Action")
	if action == "" {
		return ""
	}

	version := vals.Get("Version")
	if svcPrefix, ok := formVersionServiceMap[version]; ok {
		return svcPrefix + ":" + action
	}

	// CloudWatch uses form-encoding but its versions may vary; identify by known actions.
	if cloudwatchActions[action] {
		return "cloudwatch:" + action
	}

	return ""
}

// extractS3IAMAction determines the S3 IAM action from the HTTP method and URL path.
// It returns "" for paths that belong to other REST-based services.
func extractS3IAMAction(r *http.Request) string {
	path := r.URL.Path

	// Guard: skip known non-S3 REST API paths so they are not misidentified as
	// S3 bucket/object requests. These services are either handled by the target
	// map, the form-version map, or an ActionExtractor registered per-service.
	for _, prefix := range nonS3RESTPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return ""
		}
	}

	path = strings.TrimPrefix(path, "/")
	if path == "" {
		// ListBuckets
		if r.Method == http.MethodGet {
			return "s3:ListAllMyBuckets"
		}

		return ""
	}

	// Determine if path refers to a bucket (no slash after bucket name) or an object.
	slashIdx := strings.Index(path, "/")
	isObject := slashIdx >= 0 && slashIdx < len(path)-1

	if isObject {
		if action, ok := s3MethodActionMap[r.Method]; ok {
			return action
		}
	} else {
		if action, ok := s3BucketMethodActionMap[r.Method]; ok {
			return action
		}
	}

	return ""
}

// readBodyPreserving reads the request body and restores it so downstream handlers can re-read it.
func readBodyPreserving(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	r.Body = io.NopCloser(bytes.NewReader(body))

	return body, nil
}
