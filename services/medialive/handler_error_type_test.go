package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestDescribeChannel_UnknownChannelSurfacesNotFoundException drives
// DescribeChannel for a channel that doesn't exist through a real SDK
// client. Before this fix, respondErr never set X-Amzn-Errortype nor a body
// code/__type field -- the exact bug fixed for the sibling mediatailor
// service in f41d5b42f -- so restjson.GetErrorInfo had nothing to read and
// every error, including this one, deserialized client-side as a generic
// UnknownError instead of the modeled NotFoundException (gopherstack-ifni).
func TestDescribeChannel_UnknownChannelSurfacesNotFoundException(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))

	_, err := client.DescribeChannel(t.Context(), &medialivesdk.DescribeChannelInput{
		ChannelId: aws.String("no-such-channel"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}
