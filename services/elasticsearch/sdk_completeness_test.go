package elasticsearch_test

import (
	"testing"

	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// elasticsearchservice client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := elasticsearch.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &elasticsearchsdk.Client{}, h.GetSupportedOperations(), []string{})
}
