package iam

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
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

// RegisterableActionExtractor wraps any service.Registerable as an ActionExtractor.
type RegisterableActionExtractor struct {
	Service service.Registerable
}

// NewRegisterableActionExtractor creates an ActionExtractor from a service.Registerable.
func NewRegisterableActionExtractor(svc service.Registerable) *RegisterableActionExtractor {
	return &RegisterableActionExtractor{Service: svc}
}

// IAMAction extracts the IAM action string for a request using the service's RouteMatcher and ExtractOperation.
func (a *RegisterableActionExtractor) IAMAction(r *http.Request) string {
	if a.Service == nil {
		return ""
	}

	c := echo.New().NewContext(r, nil)
	matcher := a.Service.RouteMatcher()
	if matcher != nil && !matcher(c) {
		return ""
	}

	op := a.Service.ExtractOperation(c)
	if op == "" || op == "Unknown" {
		return ""
	}

	prefix := ""
	if cn, ok := a.Service.(interface{ ChaosServiceName() string }); ok {
		prefix = cn.ChaosServiceName()
	}
	if prefix == "" {
		prefix = strings.ToLower(a.Service.Name())
	}

	return prefix + ":" + op
}

type jsonTargetResourcePayload struct {
	TableName       string `json:"TableName"`
	KeyID           string `json:"KeyId"`
	SecretID        string `json:"SecretId"`
	QueueURL        string `json:"QueueUrl"`
	QueueName       string `json:"QueueName"`
	EventBusName    string `json:"EventBusName"`
	Rule            string `json:"Rule"`
	StateMachineArn string `json:"stateMachineArn"`
	ExecutionArn    string `json:"executionArn"`
	StreamName      string `json:"StreamName"`
	StreamARN       string `json:"StreamARN"`
	RepositoryName  string `json:"repositoryName"`
	TargetARN       string `json:"TargetArn"`
	ResourceARN     string `json:"ResourceArn"`
	Name            string `json:"Name"`
}

func extractDynamoDBResourceARN(tableName, region, accountID string) string {
	if tableName == "" {
		return ""
	}

	return "arn:aws:dynamodb:" + region + ":" + accountID + ":table/" + tableName
}

func extractKMSResourceARN(keyID, region, accountID string) string {
	if keyID == "" {
		return ""
	}
	if strings.HasPrefix(keyID, "arn:") {
		return keyID
	}

	return "arn:aws:kms:" + region + ":" + accountID + ":key/" + keyID
}

func extractSecretsManagerResourceARN(secretID, region, accountID string) string {
	if secretID == "" {
		return ""
	}
	if strings.HasPrefix(secretID, "arn:") {
		return secretID
	}

	return "arn:aws:secretsmanager:" + region + ":" + accountID + ":secret:" + secretID
}

func extractSSMResourceARN(name, region, accountID string) string {
	if name == "" {
		return ""
	}
	paramName := strings.TrimPrefix(name, "/")

	return "arn:aws:ssm:" + region + ":" + accountID + ":parameter/" + paramName
}

func extractEventBridgeResourceARN(payload jsonTargetResourcePayload, region, accountID string) string {
	if payload.EventBusName != "" {
		if strings.HasPrefix(payload.EventBusName, "arn:") {
			return payload.EventBusName
		}

		return "arn:aws:events:" + region + ":" + accountID + ":event-bus/" + payload.EventBusName
	}
	if payload.Rule != "" {
		if strings.HasPrefix(payload.Rule, "arn:") {
			return payload.Rule
		}

		return "arn:aws:events:" + region + ":" + accountID + ":rule/" + payload.Rule
	}

	return ""
}

func extractStepFunctionsResourceARN(payload jsonTargetResourcePayload) string {
	if payload.StateMachineArn != "" {
		return payload.StateMachineArn
	}

	return payload.ExecutionArn
}

func extractKinesisResourceARN(payload jsonTargetResourcePayload, region, accountID string) string {
	if payload.StreamARN != "" {
		return payload.StreamARN
	}
	if payload.StreamName != "" {
		return "arn:aws:kinesis:" + region + ":" + accountID + ":stream/" + payload.StreamName
	}

	return ""
}

func extractECRResourceARN(repoName, region, accountID string) string {
	if repoName == "" {
		return ""
	}

	return "arn:aws:ecr:" + region + ":" + accountID + ":repository/" + repoName
}

func extractSQSTargetResourceARN(payload jsonTargetResourcePayload, region, accountID string) string {
	if payload.QueueURL != "" {
		path := strings.TrimPrefix(payload.QueueURL, "http://")
		path = strings.TrimPrefix(path, "https://")
		if _, rest, found := strings.Cut(path, "/"); found {
			return extractSQSResourceARN(rest, region, accountID)
		}
	}
	if payload.QueueName != "" {
		return "arn:aws:sqs:" + region + ":" + accountID + ":" + payload.QueueName
	}

	return ""
}

func dispatchTargetResourceARN(service, region, accountID string, payload jsonTargetResourcePayload) string {
	switch service {
	case "DynamoDB_20120810", "DynamoDB_20111205":
		return extractDynamoDBResourceARN(payload.TableName, region, accountID)
	case "TrentService":
		return extractKMSResourceARN(payload.KeyID, region, accountID)
	case serviceSecretsManager:
		return extractSecretsManagerResourceARN(payload.SecretID, region, accountID)
	case "AmazonSSM":
		return extractSSMResourceARN(payload.Name, region, accountID)
	case "AmazonEventBridge", "AWSEvents":
		return extractEventBridgeResourceARN(payload, region, accountID)
	case "AmazonStates", "AWSStepFunctions":
		return extractStepFunctionsResourceARN(payload)
	case "Kinesis_20130901", "Kinesis_20131202":
		return extractKinesisResourceARN(payload, region, accountID)
	case "ECR", "AmazonEC2ContainerRegistry_V20150921":
		return extractECRResourceARN(payload.RepositoryName, region, accountID)
	case "AmazonSQS":
		return extractSQSTargetResourceARN(payload, region, accountID)
	default:
		return ""
	}
}

func extractJSONTargetResourceARN(r *http.Request, target, accountID, region string) string {
	body, err := readBodyPreserving(r)
	if err != nil || len(body) == 0 {
		return ""
	}

	var payload jsonTargetResourcePayload
	if jsonErr := json.Unmarshal(body, &payload); jsonErr != nil {
		return ""
	}

	if payload.ResourceARN != "" {
		return payload.ResourceARN
	}
	if payload.TargetARN != "" {
		return payload.TargetARN
	}

	if region == "" {
		region = "us-east-1"
	}
	if accountID == "" {
		accountID = "000000000000"
	}

	service, _, _ := strings.Cut(target, ".")

	return dispatchTargetResourceARN(service, region, accountID, payload)
}

func extractVirtualHostedBucket(host string) string {
	if host == "" {
		return ""
	}

	if stripped, _, err := net.SplitHostPort(host); err == nil {
		host = stripped
	}

	if bucket, ok := strings.CutSuffix(host, ".localhost"); ok {
		if !strings.Contains(bucket, ".") && bucket != "" {
			return bucket
		}
	}

	if trimmed, ok := strings.CutSuffix(host, ".amazonaws.com"); ok {
		parts := strings.Split(trimmed, ".")
		if len(parts) >= 2 && (parts[1] == "s3" || parts[len(parts)-1] == "s3") {
			return parts[0]
		}
	}

	return ""
}

func extractFormResourceARN(r *http.Request) string {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		return ""
	}

	body, err := readBodyPreserving(r)
	if err != nil || len(body) == 0 {
		return ""
	}

	values, parseErr := url.ParseQuery(string(body))
	if parseErr != nil {
		return ""
	}

	if tArn := values.Get("TopicArn"); tArn != "" {
		return tArn
	}

	return values.Get("TargetArn")
}

func extractURLQueryResourceARN(r *http.Request) string {
	if r.URL != nil {
		if tArn := r.URL.Query().Get("TopicArn"); tArn != "" {
			return tArn
		}
		if tArn := r.URL.Query().Get("TargetArn"); tArn != "" {
			return tArn
		}
	}

	return extractFormResourceARN(r)
}

func extractVirtualHostedResourceARN(r *http.Request, path string) string {
	if r.Host == "" {
		return ""
	}

	vhBucket := extractVirtualHostedBucket(r.Host)
	if vhBucket == "" {
		return ""
	}

	if path == "" {
		return "arn:aws:s3:::" + vhBucket
	}

	return "arn:aws:s3:::" + vhBucket + "/" + path
}

func extractPathBasedResourceARN(path, region, accountID string) string {
	if path == "" {
		return ""
	}

	if looksLikeSQSPath(path, accountID) {
		return extractSQSResourceARN(path, region, accountID)
	}

	if fnName, ok := extractLambdaFunctionName(path); ok && fnName != "" {
		if region != "" && accountID != "" {
			return "arn:aws:lambda:" + region + ":" + accountID + ":function:" + fnName
		}
	}

	return extractS3ResourceARN(path)
}

// extractResourceARN attempts to derive the AWS ARN of the resource being
// accessed from the HTTP request path and context.
func extractResourceARN(r *http.Request, accountID, region string) string {
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		if arn := extractJSONTargetResourceARN(r, target, accountID, region); arn != "" {
			return arn
		}
	}

	if arn := extractURLQueryResourceARN(r); arn != "" {
		return arn
	}

	path := strings.TrimPrefix(r.URL.Path, "/")

	if arn := extractVirtualHostedResourceARN(r, path); arn != "" {
		return arn
	}

	return extractPathBasedResourceARN(path, region, accountID)
}

// extractLambdaFunctionName parses a function name from a Lambda REST URI path.
func extractLambdaFunctionName(path string) (string, bool) {
	const prefix = "2015-03-31/functions/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}

	rest := strings.TrimPrefix(path, prefix)
	fn, _, _ := strings.Cut(rest, "/")

	return fn, true
}

// looksLikeSQSPath returns true when the path starts with the mock account ID,
// which is the pattern used for SQS queue URLs.
func looksLikeSQSPath(path, accountID string) bool {
	return strings.HasPrefix(path, accountID+"/")
}

// extractSQSResourceARN extracts the SQS ARN from a path like {accountID}/{queueName}.
func extractSQSResourceARN(path, region, accountID string) string {
	queueName := strings.TrimPrefix(path, accountID+"/")
	if queueName == "" || strings.Contains(queueName, "/") {
		return ""
	}

	return "arn:aws:sqs:" + region + ":" + accountID + ":" + queueName
}

// extractS3ResourceARN builds an S3 ARN from a path-style S3 URL path.
func extractS3ResourceARN(path string) string {
	return "arn:aws:s3:::" + path
}
