package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestSDK_GetDetector_NotFound_ErrorCodeSurvives pins handleError's error
// shape: __type must carry the AWS exception name, separately from message.
// handleError used to call errBody(msg, msg) -- the error's message text in
// both fields -- which happened to still produce the right __type today
// only because every guardduty sentinel in errors.go was constructed via
// awserr.New(<bare exception name>, sentinel), so msg IS the exception name
// by repo convention, not because the code was correct. Any error whose
// message ever describes the failure instead of repeating the exception
// name (awserr.Newf, or a wrapped fmt.Errorf) would have sent that message
// text through restjson.GetErrorInfo's __type field (aws-sdk-go-v2
// aws/protocol/restjson/decoder_util.go:15) as an unmatched, untyped code.
// The fix passes the classified AWS exception name explicitly instead of
// reusing the error's message.
func TestSDK_GetDetector_NotFound_ErrorCodeSurvives(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("000000000000", "us-east-1")
	h := guardduty.NewHandler(backend)
	client := newTestGuardDutyClient(t, h)

	_, err := client.GetDetector(t.Context(), &guarddutysdk.GetDetectorInput{
		DetectorId: aws.String("no-such-detector"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
	assert.NotContains(t, apiErr.ErrorCode(), "no-such-detector",
		"code must not be the raw error message text")
}
