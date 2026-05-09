package route53_test

import (
	"testing"

	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// route53 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := route53.NewInMemoryBackend()
	h := route53.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &route53sdk.Client{}, h.GetSupportedOperations(), []string{})
}
