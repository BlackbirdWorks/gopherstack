package qldbsession_test

import (
	"testing"

	//nolint:staticcheck // AWS deprecated the upstream SDK, but this service remains intentionally supported here.
	qldbsessionsdk "github.com/aws/aws-sdk-go-v2/service/qldbsession"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/qldbsession"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// qldbsession client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := qldbsession.NewInMemoryBackend("000000000000", "us-east-1")
	h := qldbsession.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &qldbsessionsdk.Client{}, h.GetSupportedOperations(), nil)
}
