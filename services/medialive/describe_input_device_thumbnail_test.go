package medialive_test

import (
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestDescribeInputDeviceThumbnail_HeadersNotBody_RealClient covers
// gopherstack-tp8x: DescribeInputDeviceThumbnailOutput's ContentType/
// ContentLength/ETag/LastModified are all HTTP response headers (bound via
// awsRestjson1_deserializeOpHttpBindingsDescribeInputDeviceThumbnailOutput,
// medialive@v1.101.4 deserializers.go) and Body is the raw payload
// (awsRestjson1_deserializeOpDocumentDescribeInputDeviceThumbnailOutput sets
// it directly from response.Body, no JSON unwrapping at all) -- not JSON
// response fields. The old handler wrote a JSON object
// {"ContentType":"image/jpeg","ContentLength":0} with no such headers set, so
// a real client's ContentType field decoded as the zero value (empty
// AcceptHeader/ContentType string) regardless of what was sent. Driven
// through the real SDK client since this is a header-vs-body confusion no
// field-level diff of a JSON response can detect.
func TestDescribeInputDeviceThumbnail_HeadersNotBody_RealClient(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := backend.ClaimDevice("hd-tp8x-thumbnail")
	require.NoError(t, err)

	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))

	out, err := client.DescribeInputDeviceThumbnail(t.Context(), &medialivesdk.DescribeInputDeviceThumbnailInput{
		InputDeviceId: aws.String("hd-tp8x-thumbnail"),
		Accept:        types.AcceptHeaderImageJpeg,
	})
	require.NoError(t, err)
	require.NotNil(t, out)

	assert.Equal(t, types.ContentTypeImageJpeg, out.ContentType,
		"ContentType must round-trip via the real Content-Type response header")

	require.NotNil(t, out.Body)

	b, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.NoError(t, out.Body.Close())
	assert.Empty(t, b,
		"no real thumbnail image is captured by this backend; the body must still be raw bytes, not a JSON envelope")
}

// TestDescribeInputDeviceThumbnail_NotFound_RealClient guards the error path
// still works once the handler stopped writing a JSON body unconditionally.
func TestDescribeInputDeviceThumbnail_NotFound_RealClient(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))

	_, err := client.DescribeInputDeviceThumbnail(t.Context(), &medialivesdk.DescribeInputDeviceThumbnailInput{
		InputDeviceId: aws.String("hd-does-not-exist"),
		Accept:        types.AcceptHeaderImageJpeg,
	})
	require.Error(t, err)
}
