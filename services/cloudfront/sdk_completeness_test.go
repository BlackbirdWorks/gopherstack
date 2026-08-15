package cloudfront_test

import (
	"testing"

	cloudfrontsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudfront client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
//
// This only checks the main cloudfront.Client surface. The five CloudFront
// KeyValueStore *data-plane* ops (GetKey/PutKey/DeleteKey/ListKeys/UpdateKeys)
// plus DescribeKeyValueStore's data-plane variant belong to the separate
// cloudfrontkeyvaluestore SDK client/protocol and are served by
// services/cloudfrontkeyvaluestore, which has its own sdk_completeness_test.go.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := cloudfront.NewHandler(backend)

	sdkcheck.CheckCompleteness(t, &cloudfrontsdk.Client{}, h.GetSupportedOperations(), []string{})
}
