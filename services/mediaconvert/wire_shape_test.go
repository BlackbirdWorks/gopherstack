package mediaconvert_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	mediaconverttypes "github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// newSDKTestClient stands up the real aws-sdk-go-v2 MediaConvert client
// against an httptest server running this package's Handler through the
// same pkgs/service registry/router used in production. Round-tripping
// through the genuine SDK serializer/deserializer -- rather than decoding
// the raw JSON body with ad-hoc structs -- is what actually proves a
// response is wire-compatible (matches services/acm's wire_field_additions_test.go
// pattern).
func newSDKTestClient(t *testing.T, h *mediaconvert.Handler) *mediaconvertsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return mediaconvertsdk.NewFromConfig(cfg, func(o *mediaconvertsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_LastShareDetails proves Job.LastShareDetails decodes
// through the real SDK client. On the real wire it is a *string
// (aws-sdk-go-v2/service/mediaconvert@v1.97.1 types/types.go:6202,
// deserializers.go:19625 expects value.(string)), not a nested object. A
// structured object under the "lastShareDetails" key fails the ENTIRE
// GetJob deserialization for a real client -- see the hand-revert proof in
// this bd session, which reproduced exactly the DeserializationError
// asserted against below by reverting Job.LastShareDetails to the old
// *ShareDetails{ShareToken,SharedAt} shape.
func Test_SDKRoundTrip_LastShareDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newSDKTestClient(t, h)
	ctx := t.Context()

	jobOut, err := client.CreateJob(ctx, &mediaconvertsdk.CreateJobInput{
		Role:     aws.String("arn:aws:iam::123456789012:role/MediaConvert_Default_Role"),
		Settings: &mediaconverttypes.JobSettings{},
	})
	require.NoError(t, err)
	require.NotNil(t, jobOut.Job)

	jobID := aws.ToString(jobOut.Job.Id)

	_, err = client.CreateResourceShare(ctx, &mediaconvertsdk.CreateResourceShareInput{
		JobId:         aws.String(jobID),
		SupportCaseId: aws.String("case-1234"),
	})
	require.NoError(t, err)

	getOut, err := client.GetJob(ctx, &mediaconvertsdk.GetJobInput{Id: aws.String(jobID)})
	require.NoError(t, err, "GetJob after CreateResourceShare must decode cleanly through the real SDK client")
	require.NotNil(t, getOut.Job)
	assert.Equal(t, mediaconverttypes.ShareStatusShared, getOut.Job.ShareStatus)
	require.NotNil(t, getOut.Job.LastShareDetails)
	assert.NotEmpty(t, aws.ToString(getOut.Job.LastShareDetails))
}
