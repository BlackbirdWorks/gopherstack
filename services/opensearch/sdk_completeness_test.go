package opensearch_test

import (
	"testing"

	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// opensearch client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := opensearch.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &opensearchsdk.Client{}, h.GetSupportedOperations(), []string{})
}
