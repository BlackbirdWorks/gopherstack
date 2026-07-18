package sqs_test

import (
	"testing"

	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// sqs client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := sqs.NewInMemoryBackend()
	t.Cleanup(backend.Close)
	h := sqs.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &sqssdk.Client{}, h.GetSupportedOperations(), []string{})
}

// TestSDKOpsSorted verifies GetSupportedOperations is sorted.
func TestSDKOpsSorted(t *testing.T) {
	t.Parallel()

	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}
