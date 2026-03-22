package cloudwatchlogs_test

import (
	"testing"

	cloudwatchlogssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudwatchlogs client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cloudwatchlogssdk.Client{}, h.GetSupportedOperations(), []string{
		"AssociateKmsKey",
		"AssociateSourceToS3TableIntegration",
		"CancelExportTask",
		"CancelImportTask",
		"CreateDelivery",
		"CreateExportTask",
		"CreateImportTask",
		"CreateLogAnomalyDetector",
		"CreateScheduledQuery",
		"DeleteAccountPolicy",
		"DeleteDataProtectionPolicy",
		"DeleteDelivery",
		"DeleteDeliveryDestination",
		"DeleteDeliveryDestinationPolicy",
		"DeleteDeliverySource",
		"DeleteDestination",
		"DeleteIndexPolicy",
		"DeleteIntegration",
		"DeleteLogAnomalyDetector",
		"DeleteMetricFilter",
		"DeleteQueryDefinition",
		"DeleteResourcePolicy",
		"DeleteScheduledQuery",
		"DeleteTransformer",
		"DescribeAccountPolicies",
		"DescribeConfigurationTemplates",
		"DescribeDeliveries",
		"DescribeDeliveryDestinations",
		"DescribeDeliverySources",
		"DescribeDestinations",
		"DescribeExportTasks",
		"DescribeFieldIndexes",
		"DescribeImportTaskBatches",
		"DescribeImportTasks",
		"DescribeIndexPolicies",
		"DescribeMetricFilters",
		"DescribeQueryDefinitions",
		"DescribeResourcePolicies",
		"DisassociateKmsKey",
		"DisassociateSourceFromS3TableIntegration",
		"GetDataProtectionPolicy",
		"GetDelivery",
		"GetDeliveryDestination",
		"GetDeliveryDestinationPolicy",
		"GetDeliverySource",
		"GetIntegration",
		"GetLogAnomalyDetector",
		"GetLogFields",
		"GetLogGroupFields",
		"GetLogObject",
		"GetLogRecord",
		"GetScheduledQuery",
		"GetScheduledQueryHistory",
		"GetTransformer",
		"ListAggregateLogGroupSummaries",
		"ListAnomalies",
		"ListIntegrations",
		"ListLogAnomalyDetectors",
		"ListLogGroups",
		"ListLogGroupsForQuery",
		"ListScheduledQueries",
		"ListSourcesForS3TableIntegration",
		"PutAccountPolicy",
		"PutBearerTokenAuthentication",
		"PutDataProtectionPolicy",
		"PutDeliveryDestination",
		"PutDeliveryDestinationPolicy",
		"PutDeliverySource",
		"PutDestination",
		"PutDestinationPolicy",
		"PutIndexPolicy",
		"PutIntegration",
		"PutLogGroupDeletionProtection",
		"PutMetricFilter",
		"PutQueryDefinition",
		"PutResourcePolicy",
		"PutTransformer",
		"StartLiveTail",
		"TestMetricFilter",
		"TestTransformer",
		"UpdateAnomaly",
		"UpdateDeliveryConfiguration",
		"UpdateLogAnomalyDetector",
		"UpdateScheduledQuery",
	})
}
