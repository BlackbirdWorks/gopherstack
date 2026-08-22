package cleanrooms_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	"github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

// TestHandler_OversizedBodySurfacesInternalServerException drives a real
// Clean Rooms client's CreateCollaboration with a Description large enough to
// push the request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the restJson1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo, cleanrooms@v1.49.4
// deserializers.go reads the same __type/message shape) cannot parse plain
// text, so the client saw smithy.GenericAPIError{Code:"UnknownError"} instead
// of the typed InternalServerException handleError already produces for
// every genuine backend error (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalServerException(t *testing.T) {
	t.Parallel()

	backend := cleanrooms.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCleanRoomsClient(t, cleanrooms.NewHandler(backend))

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.CreateCollaboration(t.Context(), &cleanroomssdk.CreateCollaborationInput{
		CreatorDisplayName:     aws.String("creator"),
		CreatorMemberAbilities: []types.MemberAbility{types.MemberAbilityCanQuery},
		Members:                []types.MemberSpecification{},
		Name:                   aws.String("oversized-body-collab"),
		QueryLogStatus:         types.CollaborationQueryLogStatusDisabled,
		Description:            huge,
	}, func(o *cleanroomssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
