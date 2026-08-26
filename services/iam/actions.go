package iam

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	serviceBedrock              = "bedrock"
	serviceSecretsManager       = "secretsmanager"
	serviceElasticLoadBalancing = "elasticloadbalancing"
	serviceDynamoDB             = "dynamodb"
	serviceCloudControl         = "cloudcontrol"
	actionS3ListBucket          = "s3:ListBucket"
)

// targetServiceMap maps X-Amz-Target prefixes to IAM service prefixes.
// The key is the target service portion (before the dot), the value is the IAM action prefix.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var targetServiceMap = map[string]string{
	"DynamoDB_20120810":                    serviceDynamoDB,
	"DynamoDB_20111205":                    serviceDynamoDB,
	"TrentService":                         "kms",
	serviceSecretsManager:                  serviceSecretsManager,
	"Logs_20140328":                        "logs",
	"AmazonEventBridge":                    "events",
	"AWSEvents":                            "events",
	"AmazonStates":                         "states",
	"AWSStepFunctions":                     "states",
	"Kinesis_20131202":                     "kinesis",
	"Kinesis_20130901":                     "kinesis",
	"AmazonSSM":                            "ssm",
	"AmazonSQS":                            "sqs",
	"Firehose_20150804":                    "firehose",
	"AmazonSWF_20120125":                   "swf",
	"ACM":                                  "acm",
	"Route53Resolver":                      "route53resolver",
	"Route53Resolver_20180901":             "route53resolver",
	"CodeBuild_20161006":                   "codebuild",
	"TranscribeService":                    "transcribe",
	"Textract":                             "textract",
	"ElasticLoadBalancingV2":               serviceElasticLoadBalancing,
	"ECS":                                  "ecs",
	"AmazonEC2ContainerServiceV20141113":   "ecs",
	"ECR":                                  "ecr",
	"AmazonEC2ContainerRegistry_V20150921": "ecr",
	"AmazonScheduler":                      "scheduler",
	"APIGateway":                           "apigateway",
	"AWSLambda":                            "lambda",
	"AWSCognitoIdentityProviderService":    "cognito-idp",
	"AWSCognitoIdentityService":            "cognito-identity",
	"RedshiftServerless":                   "redshift-serverless",
	"OpenSearchServerless":                 "aoss",
	"AmazonAthena":                         "athena",
	"CloudTrail_20131101":                  "cloudtrail",
	"AmazonBedrock":                        serviceBedrock,
	"BedrockAgent":                         serviceBedrock,
	"BedrockAgentRuntime":                  serviceBedrock,
	"Comprehend_20171127":                  "comprehend",
	"Glue":                                 "glue",
	"Kafka":                                "kafka",
	"MemoryDB":                             "memorydb",
	"SageMaker":                            "sagemaker",
	"SecurityHub":                          "securityhub",
	"AWSInsightsIndexService":              "ce",
	"CertificateManager":                   "acm",
	"ACMPCA":                               "acm-pca",
	"ACMPrivateCA":                         "acm-pca",
	"AnyScaleFrontendService":              "application-autoscaling",
	"AppRunner":                            "apprunner",
	"StarlingDoveService":                  "config",
	"AWSSchemas":                           "schemas",
	"AWSSupport_20130415":                  "support",
	"AWSGlue":                              "glue",
	"CodeCommit_20150413":                  "codecommit",
	"CodeDeploy_20141006":                  "codedeploy",
	"AmazonForecast":                       "forecast",
	"Lightsail_20161128":                   "lightsail",
	"ResourceGroups":                       "resource-groups",
	"SimpleWorkflowService":                "swf",
	"TransferService":                      "transfer",
	"Transfer":                             "transfer",
	"DirectoryService_20150416":            "ds",
	"AppStream_20161201":                   "appstream",
	"PhotonAdminProxyService":              "appstream",
	"CloudControl":                         serviceCloudControl,
	"CloudControl_20210930":                serviceCloudControl,
	"CloudApiService":                      serviceCloudControl,
	"CodeConnections_20231201":             "codeconnections",
	"CodePipeline_20150709":                "codepipeline",
	"CodeStar_connections_20191201":        "codestar-connections",
	"FmrsService":                          "datasync",
	"AmazonDAXV3":                          "dax",
	"AmazonDMSv20160101":                   "dms",
	"ElasticMapReduce":                     "elasticmapreduce",
	"AWSSimbaAPIService_v20180301":         "fsx",
	"KinesisAnalytics_20150814":            "kinesisanalytics",
	"MediaStore_20170901":                  "mediastore",
	"AmazonMemoryDB":                       "memorydb",
	"OpsWorks_20130218":                    "opsworks",
	"AWSOrganizationsV20161128":            "organizations",
	"AmazonPersonalize":                    "personalize",
	"AmazonPersonalizeRuntime":             "personalize",
	"RedshiftData":                         "redshift-data",
	"RekognitionService":                   "rekognition",
	"ResourceGroupsTaggingAPI_20170126":    "tagging",
	"AWSScheduler":                         "scheduler",
	"Route53AutoNaming_v20170314":          "servicediscovery",
	"AWSShield_20160616":                   "shield",
	"AWSShineFrontendService_20170701":     "translate",
	"AWSWAF_20150824":                      "waf",
	"AWSWAF_20190729":                      "wafv2",
	"WorkMailService":                      "workmail",
	"OvertureService":                      "directconnect",
	"DynamoDBStreams_20120810":             serviceDynamoDB,
	"AWSIdentityStore":                     "identitystore",
	"KinesisAnalytics_20180523":            "kinesisanalytics",
	"SWBExternalService":                   "sso",
	"Timestream_20181101":                  "timestream",
	"Transcribe":                           "transcribe",
	"VerifiedPermissions":                  "verifiedpermissions",
	"WorkspacesService":                    "workspaces",
	"SynthesizeSpeech":                     "polly",
	"Translate":                            "translate",
	"WAF_Regional_20161128":                "waf-regional",
	"WAF_20150824":                         "waf",
	"WAF_v2":                               "wafv2",
	"XRay":                                 "xray",
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
	"2012-08-10": "sqs",
	"2010-08-01": "cloudwatch",
	"2011-01-01": "ses",
	"2014-05-30": "autoscaling",
	"2012-06-01": serviceElasticLoadBalancing,
	"2015-12-01": serviceElasticLoadBalancing,
	"2010-12-01": "ses",
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
	"/v20180820/",  // S3 Control
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
	http.MethodGet:    actionS3ListBucket,
	http.MethodHead:   actionS3ListBucket,
	http.MethodPut:    "s3:CreateBucket",
	http.MethodDelete: "s3:DeleteBucket",
	http.MethodPost:   actionS3ListBucket,
}

// cloudwatchActions contains known Action names for CloudWatch form requests.
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
	if action := ExtractTargetOrFormIAMAction(r); action != "" {
		return action
	}

	// S3 path-based routing (catch-all — must come last)
	return extractS3IAMAction(r)
}

// ExtractTargetOrFormIAMAction extracts the IAM action from X-Amz-Target or form-encoded requests.
func ExtractTargetOrFormIAMAction(r *http.Request) string {
	// 1. X-Amz-Target based services (DynamoDB, KMS, CloudWatch Logs, EventBridge, etc.)
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		return targetToIAMAction(target)
	}

	// 2. Form-encoded services: parse body or query string for Action + Version
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/x-www-form-urlencoded") {
		return formActionToIAMAction(r)
	}

	return ""
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

// parseFormBodyAction reads a form-encoded request body to extract Action and Version.
func parseFormBodyAction(r *http.Request) (string, string) {
	body, err := readBodyPreserving(r)
	if err != nil {
		return "", ""
	}

	vals, parseErr := url.ParseQuery(string(body))
	if parseErr != nil {
		return "", ""
	}

	return vals.Get("Action"), vals.Get("Version")
}

func isAutoScalingAction(action string) bool {
	return strings.Contains(action, "AutoScaling") ||
		strings.Contains(action, "Scaling") ||
		strings.Contains(action, "Traffic") ||
		strings.Contains(action, "LifecycleHook")
}

func isSESAction(action string) bool {
	return strings.Contains(action, "Email") ||
		strings.Contains(action, "Identit") ||
		strings.Contains(action, "Template") ||
		strings.Contains(action, "Send") ||
		strings.Contains(action, "DKIM") ||
		strings.Contains(action, "Receipt")
}

// formActionToIAMAction parses a form-encoded request body or URL query to extract
// the Action and Version fields, then maps them to an IAM action string.
func formActionToIAMAction(r *http.Request) string {
	action := ""
	version := ""

	if r.URL != nil {
		action = r.URL.Query().Get("Action")
		version = r.URL.Query().Get("Version")
	}

	if action == "" {
		action, version = parseFormBodyAction(r)
	}

	if action == "" {
		return ""
	}

	if svcPrefix, ok := formVersionServiceMap[version]; ok {
		if version == "2011-01-01" && isAutoScalingAction(action) {
			return "autoscaling:" + action
		}
		if version == "2010-12-01" && !isSESAction(action) {
			return "elasticbeanstalk:" + action
		}

		return svcPrefix + ":" + action
	}

	// CloudWatch uses form-encoding but its versions may vary; identify by known actions.
	if cloudwatchActions[action] {
		return "cloudwatch:" + action
	}

	return ""
}

func extractVirtualHostedS3Action(r *http.Request, path string) (string, bool) {
	if r.Host == "" {
		return "", false
	}

	vhBucket := extractVirtualHostedBucket(r.Host)
	if vhBucket == "" {
		return "", false
	}

	if path != "" {
		if action, ok := s3MethodActionMap[r.Method]; ok {
			return action, true
		}
	} else if action, ok := s3BucketMethodActionMap[r.Method]; ok {
		return action, true
	}

	return "", true
}

func extractPathStyleS3Action(r *http.Request, path string) string {
	if path == "" {
		if r.Method == http.MethodGet {
			return "s3:ListAllMyBuckets"
		}

		return ""
	}

	slashIdx := strings.Index(path, "/")
	isObject := slashIdx >= 0 && slashIdx < len(path)-1

	if isObject {
		if action, ok := s3MethodActionMap[r.Method]; ok {
			return action
		}
	} else if action, ok := s3BucketMethodActionMap[r.Method]; ok {
		return action
	}

	return ""
}

// extractS3IAMAction determines the S3 IAM action from the HTTP method and URL path.
// It returns "" for paths that belong to other REST-based services.
func extractS3IAMAction(r *http.Request) string {
	path := r.URL.Path

	for _, prefix := range nonS3RESTPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return ""
		}
	}

	path = strings.TrimPrefix(path, "/")

	if action, ok := extractVirtualHostedS3Action(r, path); ok {
		return action
	}

	return extractPathStyleS3Action(r, path)
}

// readBodyPreserving reads the request body and restores it so downstream handlers can re-read it.
// The body is capped at maxIAMActionBodyBytes to prevent unbounded memory usage on
// attacker-supplied form bodies.
func readBodyPreserving(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}

	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxIAMActionBodyBytes))
	if err != nil {
		return nil, err
	}

	r.Body = io.NopCloser(bytes.NewReader(body))

	return body, nil
}

// maxIAMActionBodyBytes caps the IAM Action lookup body. AWS SigV4 form-encoded
// IAM bodies are well under a megabyte; cap conservatively to 1 MiB.
const maxIAMActionBodyBytes = 1 << 20
