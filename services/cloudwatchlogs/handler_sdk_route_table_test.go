package cloudwatchlogs_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real CloudWatch
// Logs operation, extracted from cloudwatchlogs@v1.81.1 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("Logs_20140328.<Op>")
// and always request.Request.Method = "POST" against path "/" --
// CloudWatch Logs is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a
// REST-family service there is no path template to get wrong: dispatch is
// entirely by this one header. ExtractOperation and Handler() both derive
// the action the same way (TrimPrefix on "Logs_20140328."), so the class
// of bug this table can catch is a dispatch-table key that doesn't exactly
// match the real op name (typo, wrong case -- CloudWatch Logs is
// case-sensitive JSON-RPC), not a route-template mismatch. Note
// "Logs_20140328" (not e.g. "CloudWatchLogs") is the real, historical
// target prefix -- confirmed directly in the pinned serializer, not
// guessed.
//
// This table covers all 118 real CloudWatch Logs ops, which is also
// gopherstack's full implemented set (h.GetSupportedOperations(), 118/118)
// as of cloudwatchlogs@v1.81.1 -- confirmed by diffing both
// GetSupportedOperations() and the actual h.ops dispatch map against this
// exact list, zero mismatches either direction in both comparisons.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Logs_20140328.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AssociateKmsKey", "Logs_20140328.AssociateKmsKey"},
		{"AssociateSourceToS3TableIntegration", "Logs_20140328.AssociateSourceToS3TableIntegration"},
		{"CancelExportTask", "Logs_20140328.CancelExportTask"},
		{"CancelImportTask", "Logs_20140328.CancelImportTask"},
		{"CreateDelivery", "Logs_20140328.CreateDelivery"},
		{"CreateExportTask", "Logs_20140328.CreateExportTask"},
		{"CreateImportTask", "Logs_20140328.CreateImportTask"},
		{"CreateLogAnomalyDetector", "Logs_20140328.CreateLogAnomalyDetector"},
		{"CreateLogGroup", "Logs_20140328.CreateLogGroup"},
		{"CreateLogStream", "Logs_20140328.CreateLogStream"},
		{"CreateLookupTable", "Logs_20140328.CreateLookupTable"},
		{"CreateScheduledQuery", "Logs_20140328.CreateScheduledQuery"},
		{"DeleteAccountPolicy", "Logs_20140328.DeleteAccountPolicy"},
		{"DeleteDataProtectionPolicy", "Logs_20140328.DeleteDataProtectionPolicy"},
		{"DeleteDelivery", "Logs_20140328.DeleteDelivery"},
		{"DeleteDeliveryDestination", "Logs_20140328.DeleteDeliveryDestination"},
		{"DeleteDeliveryDestinationPolicy", "Logs_20140328.DeleteDeliveryDestinationPolicy"},
		{"DeleteDeliverySource", "Logs_20140328.DeleteDeliverySource"},
		{"DeleteDestination", "Logs_20140328.DeleteDestination"},
		{"DeleteIndexPolicy", "Logs_20140328.DeleteIndexPolicy"},
		{"DeleteIntegration", "Logs_20140328.DeleteIntegration"},
		{"DeleteLogAnomalyDetector", "Logs_20140328.DeleteLogAnomalyDetector"},
		{"DeleteLogGroup", "Logs_20140328.DeleteLogGroup"},
		{"DeleteLogStream", "Logs_20140328.DeleteLogStream"},
		{"DeleteLookupTable", "Logs_20140328.DeleteLookupTable"},
		{"DeleteMetricFilter", "Logs_20140328.DeleteMetricFilter"},
		{"DeleteQueryDefinition", "Logs_20140328.DeleteQueryDefinition"},
		{"DeleteResourcePolicy", "Logs_20140328.DeleteResourcePolicy"},
		{"DeleteRetentionPolicy", "Logs_20140328.DeleteRetentionPolicy"},
		{"DeleteScheduledQuery", "Logs_20140328.DeleteScheduledQuery"},
		{"DeleteSubscriptionFilter", "Logs_20140328.DeleteSubscriptionFilter"},
		{"DeleteSyslogConfiguration", "Logs_20140328.DeleteSyslogConfiguration"},
		{"DeleteTransformer", "Logs_20140328.DeleteTransformer"},
		{"DescribeAccountPolicies", "Logs_20140328.DescribeAccountPolicies"},
		{"DescribeConfigurationTemplates", "Logs_20140328.DescribeConfigurationTemplates"},
		{"DescribeDeliveries", "Logs_20140328.DescribeDeliveries"},
		{"DescribeDeliveryDestinations", "Logs_20140328.DescribeDeliveryDestinations"},
		{"DescribeDeliverySources", "Logs_20140328.DescribeDeliverySources"},
		{"DescribeDestinations", "Logs_20140328.DescribeDestinations"},
		{"DescribeExportTasks", "Logs_20140328.DescribeExportTasks"},
		{"DescribeFieldIndexes", "Logs_20140328.DescribeFieldIndexes"},
		{"DescribeImportTaskBatches", "Logs_20140328.DescribeImportTaskBatches"},
		{"DescribeImportTasks", "Logs_20140328.DescribeImportTasks"},
		{"DescribeIndexPolicies", "Logs_20140328.DescribeIndexPolicies"},
		{"DescribeLogGroups", "Logs_20140328.DescribeLogGroups"},
		{"DescribeLogStreams", "Logs_20140328.DescribeLogStreams"},
		{"DescribeLookupTables", "Logs_20140328.DescribeLookupTables"},
		{"DescribeMetricFilters", "Logs_20140328.DescribeMetricFilters"},
		{"DescribeQueries", "Logs_20140328.DescribeQueries"},
		{"DescribeQueryDefinitions", "Logs_20140328.DescribeQueryDefinitions"},
		{"DescribeResourcePolicies", "Logs_20140328.DescribeResourcePolicies"},
		{"DescribeSubscriptionFilters", "Logs_20140328.DescribeSubscriptionFilters"},
		{"DisassociateKmsKey", "Logs_20140328.DisassociateKmsKey"},
		{"DisassociateSourceFromS3TableIntegration", "Logs_20140328.DisassociateSourceFromS3TableIntegration"},
		{"FilterLogEvents", "Logs_20140328.FilterLogEvents"},
		{"GetDataProtectionPolicy", "Logs_20140328.GetDataProtectionPolicy"},
		{"GetDelivery", "Logs_20140328.GetDelivery"},
		{"GetDeliveryDestination", "Logs_20140328.GetDeliveryDestination"},
		{"GetDeliveryDestinationPolicy", "Logs_20140328.GetDeliveryDestinationPolicy"},
		{"GetDeliverySource", "Logs_20140328.GetDeliverySource"},
		{"GetIntegration", "Logs_20140328.GetIntegration"},
		{"GetLogAnomalyDetector", "Logs_20140328.GetLogAnomalyDetector"},
		{"GetLogEvents", "Logs_20140328.GetLogEvents"},
		{"GetLogFields", "Logs_20140328.GetLogFields"},
		{"GetLogGroupFields", "Logs_20140328.GetLogGroupFields"},
		{"GetLogObject", "Logs_20140328.GetLogObject"},
		{"GetLogRecord", "Logs_20140328.GetLogRecord"},
		{"GetLookupTable", "Logs_20140328.GetLookupTable"},
		{"GetQueryResults", "Logs_20140328.GetQueryResults"},
		{"GetScheduledQuery", "Logs_20140328.GetScheduledQuery"},
		{"GetScheduledQueryHistory", "Logs_20140328.GetScheduledQueryHistory"},
		{"GetStorageTierPolicy", "Logs_20140328.GetStorageTierPolicy"},
		{"GetTransformer", "Logs_20140328.GetTransformer"},
		{"ListAggregateLogGroupSummaries", "Logs_20140328.ListAggregateLogGroupSummaries"},
		{"ListAnomalies", "Logs_20140328.ListAnomalies"},
		{"ListIntegrations", "Logs_20140328.ListIntegrations"},
		{"ListLogAnomalyDetectors", "Logs_20140328.ListLogAnomalyDetectors"},
		{"ListLogGroups", "Logs_20140328.ListLogGroups"},
		{"ListLogGroupsForQuery", "Logs_20140328.ListLogGroupsForQuery"},
		{"ListScheduledQueries", "Logs_20140328.ListScheduledQueries"},
		{"ListSourcesForS3TableIntegration", "Logs_20140328.ListSourcesForS3TableIntegration"},
		{"ListSyslogConfigurations", "Logs_20140328.ListSyslogConfigurations"},
		{"ListTagsForResource", "Logs_20140328.ListTagsForResource"},
		{"ListTagsLogGroup", "Logs_20140328.ListTagsLogGroup"},
		{"PutAccountPolicy", "Logs_20140328.PutAccountPolicy"},
		{"PutBearerTokenAuthentication", "Logs_20140328.PutBearerTokenAuthentication"},
		{"PutDataProtectionPolicy", "Logs_20140328.PutDataProtectionPolicy"},
		{"PutDeliveryDestination", "Logs_20140328.PutDeliveryDestination"},
		{"PutDeliveryDestinationPolicy", "Logs_20140328.PutDeliveryDestinationPolicy"},
		{"PutDeliverySource", "Logs_20140328.PutDeliverySource"},
		{"PutDestination", "Logs_20140328.PutDestination"},
		{"PutDestinationPolicy", "Logs_20140328.PutDestinationPolicy"},
		{"PutIndexPolicy", "Logs_20140328.PutIndexPolicy"},
		{"PutIntegration", "Logs_20140328.PutIntegration"},
		{"PutLogEvents", "Logs_20140328.PutLogEvents"},
		{"PutLogGroupDeletionProtection", "Logs_20140328.PutLogGroupDeletionProtection"},
		{"PutMetricFilter", "Logs_20140328.PutMetricFilter"},
		{"PutQueryDefinition", "Logs_20140328.PutQueryDefinition"},
		{"PutResourcePolicy", "Logs_20140328.PutResourcePolicy"},
		{"PutRetentionPolicy", "Logs_20140328.PutRetentionPolicy"},
		{"PutStorageTierPolicy", "Logs_20140328.PutStorageTierPolicy"},
		{"PutSubscriptionFilter", "Logs_20140328.PutSubscriptionFilter"},
		{"PutSyslogConfiguration", "Logs_20140328.PutSyslogConfiguration"},
		{"PutTransformer", "Logs_20140328.PutTransformer"},
		{"StartLiveTail", "Logs_20140328.StartLiveTail"},
		{"StartQuery", "Logs_20140328.StartQuery"},
		{"StopQuery", "Logs_20140328.StopQuery"},
		{"TagLogGroup", "Logs_20140328.TagLogGroup"},
		{"TagResource", "Logs_20140328.TagResource"},
		{"TestMetricFilter", "Logs_20140328.TestMetricFilter"},
		{"TestTransformer", "Logs_20140328.TestTransformer"},
		{"UntagLogGroup", "Logs_20140328.UntagLogGroup"},
		{"UntagResource", "Logs_20140328.UntagResource"},
		{"UpdateAnomaly", "Logs_20140328.UpdateAnomaly"},
		{"UpdateDeliveryConfiguration", "Logs_20140328.UpdateDeliveryConfiguration"},
		{"UpdateLogAnomalyDetector", "Logs_20140328.UpdateLogAnomalyDetector"},
		{"UpdateLookupTable", "Logs_20140328.UpdateLookupTable"},
		{"UpdateScheduledQuery", "Logs_20140328.UpdateScheduledQuery"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real CloudWatch Logs
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the "UnknownOperationException"
// sentinel that a dispatch-table key mismatch would produce. That sentinel
// (errUnknownOperation in handler.go) has exactly one production call
// site -- the dispatch() miss in the h.ops map lookup -- so it cannot
// collide with a legitimate error on this all-empty-body table.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
