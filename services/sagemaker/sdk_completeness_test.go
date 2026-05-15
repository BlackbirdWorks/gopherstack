package sagemaker_test

import (
	"testing"

	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// sagemaker client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	h := sagemaker.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &sagemakersdk.Client{}, h.GetSupportedOperations(), []string{})
}
