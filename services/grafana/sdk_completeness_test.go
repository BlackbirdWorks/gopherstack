package grafana_test

import (
	"testing"

	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/grafana"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// grafana client is routed by GetSupportedOperations(). The test fails when
// the upstream SDK adds a new operation gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := grafana.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := grafana.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &grafanasdk.Client{}, h.GetSupportedOperations(), []string{})
}
