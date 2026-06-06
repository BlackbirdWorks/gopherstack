package inspector2_test

import (
	"testing"

	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// inspector2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend("000000000000", "us-east-1")
	h := inspector2.NewHandler(backend)

	// Operations not yet implemented — member management, org configuration,
	// CIS scanning, code security, SBOM export, findings reports, coverage,
	// deep inspection, encryption keys, and usage/search operations.
	notImplemented := []string{}

	sdkcheck.CheckCompleteness(t, &inspector2sdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
