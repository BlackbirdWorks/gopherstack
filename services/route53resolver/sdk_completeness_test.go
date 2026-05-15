package route53resolver_test

import (
	"testing"

	route53resolversdk "github.com/aws/aws-sdk-go-v2/service/route53resolver"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// route53resolver client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	h := route53resolver.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &route53resolversdk.Client{}, h.GetSupportedOperations(), []string{})
}
