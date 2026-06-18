package ecs_test

import (
	"testing"

	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// ecs client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())
	h := ecs.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &ecssdk.Client{}, h.GetSupportedOperations(), []string{
		"ContinueServiceDeployment",
	})
}
