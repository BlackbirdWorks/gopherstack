package routegates

import (
	_ "embed" // pathGatesData
	"strings"
)

// namespaced returns prefix plus the form botocore clients send, which prepends "com.amazonaws.<ns>".
func namespaced(ns, prefix string) []string {
	return []string{prefix, "com.amazonaws." + ns + prefix}
}

// TargetGates maps each service to the X-Amz-Target prefixes its RouteMatcher requires;
// TestRouteTargetGatesHold proves the matchers return false outside them.
func TargetGates() map[string][]string {
	return map[string][]string{
		"ACM":                      {"CertificateManager."},
		"ACMPCA":                   {"ACMPrivateCA."},
		"AWSConfig":                {"StarlingDoveService."},
		"BCMDataExports":           {"AWSBillingAndCostManagementDataExports."},
		"CUR":                      {"AWSOrigamiServiceGatewayService."},
		"AppRunner":                {"AppRunner."},
		"ApplicationAutoscaling":   {"AnyScaleFrontendService."},
		"Athena":                   {"AmazonAthena"},
		"Budgets":                  {"AWSBudgetServiceGateway."},
		"Ce":                       {"AWSInsightsIndexService."},
		"CloudControl":             {"CloudApiService."},
		"CloudHSMV2":               {"BaldrApiService."},
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
		"GlobalAccelerator":        {"GlobalAccelerator_V20180706."},
		"Glue":                     {"AWSGlue."},
		"IdentityStore":            {"AWSIdentityStore."},
		"KMS":                      {"TrentService"},
		"Kinesis":                  {"Kinesis_20131202."},
		"KinesisAnalytics":         {"KinesisAnalytics_20150814."},
		"KinesisAnalyticsV2":       {"KinesisAnalytics_20180523."},
		"Lightsail":                {"Lightsail_20161128."},
		"MarketplaceMetering":      {"AWSMPMeteringService."},
		"MediaStore":               {"MediaStore_20170901."},
		"NetworkFirewall":          {"NetworkFirewall_20201112."},
		"OpsWorks":                 {"OpsWorks_20130218."},
		"Organizations":            {"AWSOrganizationsV20161128."},
		"Pricing":                  {"AWSPriceListService."},
		"RedshiftData":             {"RedshiftData."},
		"RedshiftServerless":       {"RedshiftServerless."},
		"Rekognition":              {"RekognitionService."},
		"ResourceGroupsTaggingAPI": {"ResourceGroupsTaggingAPI_20170126."},
		"Route53Resolver":          {"Route53Resolver."},
		"SWF":                      {"SimpleWorkflowService."},
		"TimestreamInfluxDB":       {"AmazonTimestreamInfluxDB."},
		"SageMaker":                {"SageMaker."},
		"SecretsManager":           {"secretsmanager"},
		"ServiceCatalog":           {"AWS242ServiceCatalogService."},
		"ServiceDiscovery":         {"Route53AutoNaming_v20170314."},
		"ServiceQuotas":            {"ServiceQuotasV20190624."},
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

//go:embed path_gates.txt
var pathGatesData string

// PathGates maps each service to the path prefixes its matcher requires ("Name<TAB>prefixes");
// TestRoutePathGatesHold proves the matchers return false outside them.
func PathGates() map[string][]string {
	gates := map[string][]string{}

	for line := range strings.SplitSeq(pathGatesData, "\n") {
		if name, prefixes, ok := strings.Cut(line, "\t"); ok {
			gates[name] = strings.Fields(prefixes)
		}
	}

	return gates
}
