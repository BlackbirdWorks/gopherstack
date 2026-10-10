package main

import (
	_ "embed" // routePathGatesData
	"strings"
)

// namespaced returns prefix plus the form botocore clients send, which prepends "com.amazonaws.<ns>".
func namespaced(ns, prefix string) []string {
	return []string{prefix, "com.amazonaws." + ns + prefix}
}

// routeTargetGates maps each service to the X-Amz-Target prefixes its RouteMatcher requires;
// TestRouteTargetGatesHold proves the matchers return false outside them.
func routeTargetGates() map[string][]string {
	return map[string][]string{
		"ACM":                      {"CertificateManager."},
		"ACMPCA":                   {"ACMPrivateCA."},
		"AWSConfig":                {"StarlingDoveService."},
		"AppRunner":                {"AppRunner."},
		"ApplicationAutoscaling":   {"AnyScaleFrontendService."},
		"Athena":                   {"AmazonAthena"},
		"Ce":                       {"AWSInsightsIndexService."},
		"CloudControl":             {"CloudApiService."},
		"CloudTrail":               namespaced("cloudtrail.v20131101.", "CloudTrail_20131101."),
		"CloudWatchLogs":           {"Logs_20140328."},
		"CodeBuild":                {"CodeBuild_20161006."},
		"CodeCommit":               {"CodeCommit_20150413."},
		"CodeConnections":          namespaced("codeconnections.", "CodeConnections_20231201."),
		"CodeDeploy":               {"CodeDeploy_20141006."},
		"CodePipeline":             {"CodePipeline_20150709."},
		"CodeStarConnections":      namespaced("codestar.connections.", "CodeStar_connections_20191201."),
		"CognitoIdentity":          {"AWSCognitoIdentityService."},
		"Comprehend":               {"Comprehend_20171127."},
		"DAX":                      {"AmazonDAXV3."},
		"DMS":                      {"AmazonDMSv20160101."},
		"DataSync":                 {"FmrsService."},
		"DirectConnect":            {"OvertureService."},
		"DirectoryService":         {"DirectoryService_20150416."},
		"DynamoDB":                 {"DynamoDB_"},
		"DynamoDBStreams":          {"DynamoDBStreams_20120810."},
		"ECRPublic":                {"SpencerFrontendService."},
		"ECS":                      {"AmazonEC2ContainerServiceV20141113."},
		"EMR":                      {"ElasticMapReduce."},
		"FSx":                      {"AWSSimbaAPIService_v20180301."},
		"Firehose":                 {"Firehose_20150804."},
		"Forecast":                 {"AmazonForecast."},
		"Glue":                     {"AWSGlue."},
		"IdentityStore":            {"AWSIdentityStore."},
		"KMS":                      {"TrentService"},
		"Kinesis":                  {"Kinesis_20131202."},
		"KinesisAnalytics":         {"KinesisAnalytics_20150814."},
		"KinesisAnalyticsV2":       {"KinesisAnalytics_20180523."},
		"Lightsail":                {"Lightsail_20161128."},
		"MediaStore":               {"MediaStore_20170901."},
		"OpsWorks":                 {"OpsWorks_20130218."},
		"Organizations":            {"AWSOrganizationsV20161128."},
		"RedshiftData":             {"RedshiftData."},
		"RedshiftServerless":       {"RedshiftServerless."},
		"Rekognition":              {"RekognitionService."},
		"ResourceGroupsTaggingAPI": {"ResourceGroupsTaggingAPI_20170126."},
		"Route53Resolver":          {"Route53Resolver."},
		"SSM":                      {"AmazonSSM"},
		"SWF":                      {"SimpleWorkflowService."},
		"SageMaker":                {"SageMaker."},
		"SecretsManager":           {"secretsmanager"},
		"ServiceDiscovery":         {"Route53AutoNaming_v20170314."},
		"Shield":                   {"AWSShield_20160616."},
		"SsoAdmin":                 {"SWBExternalService."},
		"StepFunctions":            {"AmazonStates.", "AWSStepFunctions."},
		"Support":                  {"AWSSupport_20130415."},
		"Textract":                 {"Textract."},
		"TimestreamWrite":          {"Timestream_20181101."},
		"Transcribe":               {"Transcribe."},
		"Transfer":                 {"TransferService."},
		"Translate":                {"AWSShineFrontendService_20170701."},
		"VerifiedPermissions":      {"VerifiedPermissions."},
		"WAF":                      {"AWSWAF_20150824."},
		"Wafv2":                    {"AWSWAF_20190729."},
		"WorkMail":                 {"WorkMailService."},
		"WorkSpaces":               {"WorkspacesService."},
	}
}

//go:embed cli_route_path_gates.txt
var routePathGatesData string

// routePathGates maps each service to the path prefixes its matcher requires ("Name<TAB>prefixes");
// TestRoutePathGatesHold proves the matchers return false outside them.
func routePathGates() map[string][]string {
	gates := map[string][]string{}

	for line := range strings.SplitSeq(routePathGatesData, "\n") {
		if name, prefixes, ok := strings.Cut(line, "\t"); ok {
			gates[name] = strings.Fields(prefixes)
		}
	}

	return gates
}
