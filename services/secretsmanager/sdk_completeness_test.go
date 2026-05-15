package secretsmanager_test

import (
	"testing"

	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// secretsmanager client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &secretsmanagersdk.Client{}, h.GetSupportedOperations(), []string{})
}
