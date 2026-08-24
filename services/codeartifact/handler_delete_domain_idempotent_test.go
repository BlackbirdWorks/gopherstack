package codeartifact_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

// TestHandler_DeleteDomain_NonexistentIsIdempotent drives a real codeartifact
// client's DeleteDomain against a domain that was never created. Before this
// fix, the backend returned ErrNotFound (ResourceNotFoundException) here, but
// codeartifact@v1.41.4's awsRestjson1_deserializeOpErrorDeleteDomain switch
// does not type ResourceNotFoundException at all (unlike every sibling Delete
// op), so a real client saw an untyped smithy.GenericAPIError instead of any
// modeled exception. DeleteDomain is now idempotent for a missing domain,
// which real AWS's own wire shape supports: DeleteDomainOutput.Domain is a
// nilable pointer, so a no-op success omits it rather than fabricating data.
func TestHandler_DeleteDomain_NonexistentIsIdempotent(t *testing.T) {
	t.Parallel()

	h := codeartifact.NewHandler(codeartifact.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCodeArtifactClient(t, h)

	out, err := client.DeleteDomain(t.Context(), &casdk.DeleteDomainInput{
		Domain: aws.String("never-created"),
	})
	require.NoError(t, err)
	require.Nil(t, out.Domain)
}
