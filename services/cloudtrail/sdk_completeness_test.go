package cloudtrail_test

import (
	"testing"

	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudtrail client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	h := cloudtrail.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cloudtrailsdk.Client{}, h.GetSupportedOperations(), []string{
		"CancelQuery",
		"CreateChannel",
		"CreateDashboard",
		"CreateEventDataStore",
		"DeleteChannel",
		"DeleteDashboard",
		"DeleteEventDataStore",
		"DeleteResourcePolicy",
		"DeregisterOrganizationDelegatedAdmin",
		"DescribeQuery",
		"DisableFederation",
		"EnableFederation",
		"GenerateQuery",
		"GetChannel",
		"GetDashboard",
		"GetEventConfiguration",
		"GetEventDataStore",
		"GetImport",
		"GetInsightSelectors",
		"GetQueryResults",
		"GetResourcePolicy",
		"ListChannels",
		"ListDashboards",
		"ListEventDataStores",
		"ListImportFailures",
		"ListImports",
		"ListInsightsData",
		"ListInsightsMetricData",
		"ListPublicKeys",
		"ListQueries",
		"PutEventConfiguration",
		"PutInsightSelectors",
		"PutResourcePolicy",
		"RegisterOrganizationDelegatedAdmin",
		"RestoreEventDataStore",
		"SearchSampleQueries",
		"StartDashboardRefresh",
		"StartEventDataStoreIngestion",
		"StartImport",
		"StartQuery",
		"StopEventDataStoreIngestion",
		"StopImport",
		"UpdateChannel",
		"UpdateDashboard",
		"UpdateEventDataStore",
	})
}
