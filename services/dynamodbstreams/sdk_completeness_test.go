package dynamodbstreams_test

import (
	"testing"

	dynamodbstreamssdk "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// dynamodbstreams client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	h := dynamodbstreams.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &dynamodbstreamssdk.Client{}, h.GetSupportedOperations(), nil)
}
