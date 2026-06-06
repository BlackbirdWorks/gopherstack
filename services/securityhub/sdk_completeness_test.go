package securityhub_test

import (
	"testing"

	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// securityhub client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(backend)

	// Operations not yet implemented — organization/member management,
	// finding aggregators, configuration policies, V2 endpoints,
	// invitation management, and advanced hub management.
	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &securityhubsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
