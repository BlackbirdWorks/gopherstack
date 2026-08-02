package eventbridge_test

import (
	"testing"

	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	schemassdk "github.com/aws/aws-sdk-go-v2/service/schemas"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// eventbridge client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
//
// GetSupportedOperations() reports the union of three distinct AWS API
// surfaces gopherstack's single Handler implements together (EventBridge
// proper, Pipes, and the Schema Registry), each with its own SDK client, so
// this test splits the list and checks each third against the client that
// actually owns it.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	h := eventbridge.NewHandler(backend)

	// pipeOps are the EventBridge Pipes operations. AWS models these on a
	// separate SDK client, pipes.Client, distinct from the main
	// eventbridge.Client checked below.
	pipeOps := map[string]bool{
		"CreatePipe":   true,
		"DeletePipe":   true,
		"DescribePipe": true,
		"ListPipes":    true,
		"UpdatePipe":   true,
	}

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

	var mainOps, pOps, sOps []string
	for _, op := range h.GetSupportedOperations() {
		switch {
		case pipeOps[op]:
			pOps = append(pOps, op)
		case schemaOps[op]:
			sOps = append(sOps, op)
		default:
			mainOps = append(mainOps, op)
		}
	}

	sdkcheck.CheckCompleteness(t, &eventbridgesdk.Client{}, mainOps, []string{})
	// This Handler only implements pipes.Client's core CRUD ops; start/stop
	// and tagging are not implemented.
	sdkcheck.CheckCompleteness(t, &pipessdk.Client{}, pOps, []string{
		"ListTagsForResource",
		"StartPipe",
		"StopPipe",
		"TagResource",
		"UntagResource",
	})
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
