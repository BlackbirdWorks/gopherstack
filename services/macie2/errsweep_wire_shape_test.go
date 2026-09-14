package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestSDK_GetCustomDataIdentifier_NotFound_ErrorCodeSurvives pins
// handleError's error shape: __type must carry the AWS exception name,
// separately from message. handleError used to call errBody(msg, msg) --
// the error's message text in both fields -- which happened to still
// produce the right __type today only because every macie2 sentinel in
// errors.go is constructed with its message set to the bare exception
// name, by repo convention, not because the code was correct. See
// guardduty's a14236a6a, which fixed the identical shape.
func TestSDK_GetCustomDataIdentifier_NotFound_ErrorCodeSurvives(t *testing.T) {
	t.Parallel()

	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	h := macie2.NewHandler(backend)
	client := newTestMacie2SDKClient(t, h)

	_, err := client.GetCustomDataIdentifier(t.Context(), &macie2sdk.GetCustomDataIdentifierInput{
		Id: aws.String("no-such-identifier"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
	assert.NotContains(t, apiErr.ErrorCode(), "no-such-identifier",
		"code must not be the raw error message text")

	var target *types.ResourceNotFoundException

	require.ErrorAs(t, err, &target)
}

// TestSDK_GetSensitiveDataOccurrences_WrongCategory_ErrorCodeSurvives covers
// the one call site (reveal_configuration.go) whose exception name never
// matched its awserr.ErrInvalidParameter sentinel category 1:1 --
// UnprocessableEntityException and AccessDeniedException both wrap
// ErrInvalidParameter, alongside ValidationException. A handleError fixed
// the way guardduty's was -- one hardcoded __type per sentinel category --
// would have collapsed both into ValidationException. handleError instead
// classifies ErrRevealNotClassification/ErrRevealNotEnabled ahead of the
// coarse ErrInvalidParameter case.
func TestSDK_GetSensitiveDataOccurrences_WrongCategory_ErrorCodeSurvives(t *testing.T) {
	t.Parallel()

	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.CreateSampleFindings([]string{"Policy:IAMUser/S3BucketPublic"}))

	findingIDs, _, err := backend.ListFindings(nil, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, findingIDs, 1)

	h := macie2.NewHandler(backend)
	client := newTestMacie2SDKClient(t, h)

	_, err = client.GetSensitiveDataOccurrences(t.Context(), &macie2sdk.GetSensitiveDataOccurrencesInput{
		FindingId: aws.String(findingIDs[0]),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "UnprocessableEntityException", apiErr.ErrorCode())

	var target *types.UnprocessableEntityException

	require.ErrorAs(t, err, &target)
}

// TestSDK_GetSensitiveDataOccurrences_RevealNotEnabled_ErrorCodeSurvives
// covers the other exception name sharing awserr.ErrInvalidParameter's
// sentinel category -- see the wrong-category test above.
func TestSDK_GetSensitiveDataOccurrences_RevealNotEnabled_ErrorCodeSurvives(t *testing.T) {
	t.Parallel()

	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.CreateSampleFindings(nil))

	findingIDs, _, err := backend.ListFindings(nil, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, findingIDs, 1)

	h := macie2.NewHandler(backend)
	client := newTestMacie2SDKClient(t, h)

	_, err = client.GetSensitiveDataOccurrences(t.Context(), &macie2sdk.GetSensitiveDataOccurrencesInput{
		FindingId: aws.String(findingIDs[0]),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "AccessDeniedException", apiErr.ErrorCode())

	var target *types.AccessDeniedException

	require.ErrorAs(t, err, &target)
}
