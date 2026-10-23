package bedrockagentcore_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := bedrockagentcore.NewHandler(bedrockagentcore.NewInMemoryBackend(testAccount, testRegion))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), notImplemented())
}

func notImplemented() []string {
	return []string{
		"CompleteResourceTokenAuth", "CreateABTest", "CreatePaymentInstrument", "CreatePaymentSession",
		"DeleteABTest", "DeleteBatchEvaluation", "DeleteCapacityProviderSession", "DeletePaymentInstrument",
		"DeletePaymentSession", "DeleteRecommendation", "Evaluate", "GetABTest", "GetBatchEvaluation",
		"GetPaymentInstrument", "GetPaymentInstrumentBalance", "GetPaymentSession", "GetRecommendation",
		"GetResourcePaymentToken", "IngestData", "InvokeAgentRuntimeCommand", "InvokeBrowser", "InvokeHarness",
		"ListABTests", "ListBatchEvaluations", "ListMemoryExtractionJobs", "ListPaymentInstruments",
		"ListPaymentSessions", "ListRecommendations", "ProcessPayment", "SaveBrowserSessionProfile",
		"SearchRegistryRecords", "StartBatchEvaluation", "StartMemoryExtractionJob", "StartRecommendation",
		"StopBatchEvaluation", "UpdateABTest", "UpdateBrowserStream",
	}
}
