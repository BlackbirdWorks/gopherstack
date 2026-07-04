package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// elasticache client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	h := elasticache.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &elasticachesdk.Client{}, h.GetSupportedOperations(), []string{})
}
