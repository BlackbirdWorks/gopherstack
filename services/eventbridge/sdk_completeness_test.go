package eventbridge_test

import (
	"testing"

	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	schemassdk "github.com/aws/aws-sdk-go-v2/service/schemas"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// eventbridge client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
//
// GetSupportedOperations() reports the union of two distinct AWS API
// surfaces gopherstack's single Handler implements together (EventBridge
// proper and the Schema Registry), each with its own SDK client, so this
// test splits the list and checks each half against the client that
// actually owns it. A third surface, Pipes, used to be hosted here too
// (CreatePipe/DeletePipe/DescribePipe/ListPipes/UpdatePipe) but was removed:
// it duplicated the correctly-routed services/pipes directory and was
// unreachable by any real client anyway (see gopherstack-92ft) -- Pipes
// coverage is exercised by services/pipes's own TestSDKCompleteness now.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	h := eventbridge.NewHandler(backend)

	// schemaOps are the EventBridge Schema Registry operations. AWS models
	// these on a separate SDK client, schemas.Client, distinct from the main
	// eventbridge.Client checked below.
	schemaOps := map[string]bool{
		"CreateRegistry":       true,
		"CreateSchema":         true,
		"DeleteRegistry":       true,
		"DeleteSchema":         true,
		"DeleteSchemaVersion":  true,
		"DescribeCodeBinding":  true,
		"DescribeRegistry":     true,
		"DescribeSchema":       true,
		"GetCodeBindingSource": true,
		"GetDiscoveredSchema":  true,
		"ListRegistries":       true,
		"ListSchemaVersions":   true,
		"ListSchemas":          true,
		"PutCodeBinding":       true,
		"SearchSchemas":        true,
		"UpdateRegistry":       true,
		"UpdateSchema":         true,
	}

	var mainOps, sOps []string
	for _, op := range h.GetSupportedOperations() {
		if schemaOps[op] {
			sOps = append(sOps, op)
		} else {
			mainOps = append(mainOps, op)
		}
	}

	sdkcheck.CheckCompleteness(t, &eventbridgesdk.Client{}, mainOps, []string{})
	// This Handler only implements schemas.Client's registry/schema/
	// code-binding surface; schema discoverers and resource policies are not
	// implemented.
	sdkcheck.CheckCompleteness(t, &schemassdk.Client{}, sOps, []string{
		"CreateDiscoverer",
		"DeleteDiscoverer",
		"DeleteResourcePolicy",
		"DescribeDiscoverer",
		"ExportSchema",
		"GetResourcePolicy",
		"ListDiscoverers",
		"ListTagsForResource",
		"PutResourcePolicy",
		"StartDiscoverer",
		"StopDiscoverer",
		"TagResource",
		"UntagResource",
		"UpdateDiscoverer",
	})
}
