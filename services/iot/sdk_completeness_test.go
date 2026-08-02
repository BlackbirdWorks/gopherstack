package iot_test

import (
	"testing"

	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// iot client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)

	// deviceShadowOps are the IoT device-shadow operations. AWS models these
	// on the separate IoT Data Plane SDK client (iotdataplane.Client), not
	// the IoT control-plane client (iotsdk.Client) checked below.
	// gopherstack's Handler implements the shadow REST routes directly
	// (alongside the separate services/iotdataplane package, which covers
	// the rest of that client's surface: Publish, connections, retained
	// messages) and reports them from the same GetSupportedOperations() as
	// the control-plane ops, so this test splits them before checking each
	// half against the SDK client that actually owns it.
	deviceShadowOps := map[string]bool{
		"DeleteThingShadow":        true,
		"GetThingShadow":           true,
		"ListNamedShadowsForThing": true,
		"UpdateThingShadow":        true,
	}

	var controlPlaneOps, shadowOps []string
	for _, op := range h.GetSupportedOperations() {
		if deviceShadowOps[op] {
			shadowOps = append(shadowOps, op)
		} else {
			controlPlaneOps = append(controlPlaneOps, op)
		}
	}

	// All 152 previously-missing control-plane ops are now covered by stub
	// handlers registered in GetSupportedOperations(). The notImplemented
	// list is empty.
	sdkcheck.CheckCompleteness(t, &iotsdk.Client{}, controlPlaneOps, []string{})
	// This Handler only implements the device-shadow slice of the IoT Data
	// Plane surface; the rest (Publish, connections, retained messages,
	// direct messaging) is covered by the separate services/iotdataplane
	// package and is not part of this handler's job, so it's listed as
	// notImplemented here rather than in shadowOps.
	sdkcheck.CheckCompleteness(t, &iotdataplanesdk.Client{}, shadowOps, []string{
		"DeleteConnection",
		"GetConnection",
		"GetRetainedMessage",
		"ListRetainedMessages",
		"ListSubscriptions",
		"Publish",
		"SendDirectMessage",
	})
}
