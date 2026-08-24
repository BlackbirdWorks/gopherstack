package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	crsdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

// TestHandler_DeleteCollaboration_NonexistentIsIdempotent drives a real
// cleanrooms client's DeleteCollaboration against a collaboration ID that
// was never created. Before this fix, the backend returned ErrNotFound
// (ResourceNotFoundException) here, but cleanrooms@v1.49.4's
// awsRestjson1_deserializeOpErrorDeleteCollaboration switch does not type
// ResourceNotFoundException at all (only AccessDeniedException,
// InternalServerException, ThrottlingException, ValidationException), so a
// real client saw an untyped smithy.GenericAPIError instead of any modeled
// exception. DeleteCollaboration is now idempotent for a missing
// collaboration; DeleteCollaborationOutput carries no fields at all, so an
// empty success fabricates nothing.
func TestHandler_DeleteCollaboration_NonexistentIsIdempotent(t *testing.T) {
	t.Parallel()

	h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCleanRoomsClient(t, h)

	_, err := client.DeleteCollaboration(t.Context(), &crsdk.DeleteCollaborationInput{
		CollaborationIdentifier: aws.String("00000000-0000-0000-0000-000000000000"),
	})
	require.NoError(t, err)
}
