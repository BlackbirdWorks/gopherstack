package kinesisanalyticsv2_test

import (
	"testing"

	kinesisanalyticsv2sdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// kinesisanalyticsv2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
	h := kinesisanalyticsv2.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &kinesisanalyticsv2sdk.Client{}, h.GetSupportedOperations(), []string{})
}
