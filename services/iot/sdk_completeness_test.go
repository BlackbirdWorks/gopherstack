package iot_test

import (
	"testing"

	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// iot client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
//
// Device Shadow operations (GetThingShadow/UpdateThingShadow/DeleteThingShadow/
// ListNamedShadowsForThing) are not part of the iot control-plane SDK at all --
// they belong exclusively to the separate iotdataplane client and are covered
// by services/iotdataplane (gopherstack-1252), so this handler claims none of
// that surface.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)

	// All 152 previously-missing control-plane ops are now covered by stub
	// handlers registered in GetSupportedOperations(). The notImplemented
	// list is empty.
	sdkcheck.CheckCompleteness(t, &iotsdk.Client{}, h.GetSupportedOperations(), []string{})
}
