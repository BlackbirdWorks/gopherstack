package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestSDKRoundTrip_DescribeThumbnails_ThumbnailDetailsWireKey proves, through
// the real aws-sdk-go-v2 medialive client (medialive@v1.101.4), that
// DescribeThumbnails' response decodes under the real wire key. Before the
// fix gopherstack emitted "ThumbnailDetails" (PascalCase); the real
// deserializer switches on lowerCamel "thumbnailDetails" (confirmed against
// awsRestjson1_deserializeOpDocumentDescribeThumbnailsOutput in
// deserializers.go), so a real client's ThumbnailDetails field stayed nil
// (its Go zero value, never decoded) rather than an initialized empty slice
// decoded from "[]". gopherstack's backend never synthesizes real thumbnail
// data (that requires an actual video pipeline), so the list is empty either
// way -- the signal this test proves is nil-because-never-decoded versus
// non-nil-because-decoded-under-the-right-key, not element count.
func TestSDKRoundTrip_DescribeThumbnails_ThumbnailDetailsWireKey(t *testing.T) {
	t.Parallel()

	h := medialive.NewHandler(medialive.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestMediaLiveClient(t, h)

	created, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
		Name: aws.String("thumb-channel"),
	})
	require.NoError(t, err)

	out, err := client.DescribeThumbnails(t.Context(), &medialivesdk.DescribeThumbnailsInput{
		ChannelId:     created.Channel.Id,
		PipelineId:    aws.String("0"),
		ThumbnailType: aws.String("CURRENT_ACTIVE"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ThumbnailDetails,
		"DescribeThumbnailsOutput.ThumbnailDetails must decode (non-nil) under the real wire key")
}
