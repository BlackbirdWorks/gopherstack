package iotwireless_test

import (
	"testing"

	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// iotwireless client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := iotwireless.NewInMemoryBackend()
	h := iotwireless.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &iotwirelesssdk.Client{}, h.GetSupportedOperations(), []string{})
}
