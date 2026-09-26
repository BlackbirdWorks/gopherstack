package pinpoint_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// TestDeleteUserEndpoints_EndpointsResponse_RealClient covers gopherstack-r80d
// (required-output-member sweep, fifth batch). DeleteUserEndpointsOutput
// requires EndpointsResponse (pinpoint@v1.42.4 api_op_DeleteUserEndpoints.go:44-51),
// and the real deserializer feeds the entire HTTP body directly into that field
// (deserializers.go:5482, awsRestjson1_deserializeDocumentEndpointsResponse) --
// there is no wrapper key, the response body IS the EndpointsResponse. The
// handler wrote a bare 204 No Content. The real client's JSON decoder treats an
// empty body as io.EOF, which the generated deserializer explicitly tolerates
// (deserializers.go:5472, "err != io.EOF"), so the call still "succeeds" but
// EndpointsResponse.Item (and the pointer struct itself) never gets set --
// exactly the lambda DeleteCapacityProvider empty-body class from batch one.
// Driven through the real SDK client since a hand-built response fixture
// would not surface a structurally-empty body the way the SDK's own
// EOF-tolerant decoder does.
func TestDeleteUserEndpoints_EndpointsResponse_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{
			Name: aws.String("r80d-batch5-app"),
		},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	const distinguishingAddress = "r80d-batch5-distinguishing@example.com"

	_, err = client.UpdateEndpoint(t.Context(), &pinpointsdk.UpdateEndpointInput{
		ApplicationId: aws.String(appID),
		EndpointId:    aws.String("ep-r80d-batch5"),
		EndpointRequest: &types.EndpointRequest{
			ChannelType: types.ChannelTypeEmail,
			Address:     aws.String(distinguishingAddress),
			User:        &types.EndpointUser{UserId: aws.String("user-r80d-batch5")},
		},
	})
	require.NoError(t, err)

	out, err := client.DeleteUserEndpoints(t.Context(), &pinpointsdk.DeleteUserEndpointsInput{
		ApplicationId: aws.String(appID),
		UserId:        aws.String("user-r80d-batch5"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.EndpointsResponse)
	require.Len(t, out.EndpointsResponse.Item, 1)
	assert.Equal(t, distinguishingAddress, aws.ToString(out.EndpointsResponse.Item[0].Address))

	// The endpoint must actually be gone afterward.
	afterOut, err := client.GetUserEndpoints(t.Context(), &pinpointsdk.GetUserEndpointsInput{
		ApplicationId: aws.String(appID),
		UserId:        aws.String("user-r80d-batch5"),
	})
	require.NoError(t, err)
	assert.Empty(t, afterOut.EndpointsResponse.Item)
}

// TestCreateCampaign_MissingSegmentID_RealClient covers the 2026-09-19
// required-output-member re-sweep. CampaignResponse.SegmentId and
// .SegmentVersion are both required (pinpoint@v1.42.4 types/types.go:1547-1555),
// but WriteCampaignRequest.SegmentId is optional on the request side (*string,
// no "This member is required" trait) and gopherstack copied it straight
// through with no validation. Both response fields are JSON-tagged
// `omitempty`, so a campaign created without SegmentId would decode with
// both fields silently omitted from the wire instead of present-but-empty --
// a real client reading CampaignResponse.SegmentId always expects a non-nil
// pointer. There is no sensible default segment to fabricate, so the fix
// rejects the request the same way AWS does: a campaign always targets a
// segment. Locked here (missing case) and by every other CreateCampaign
// test in this package continuing to pass with an explicit SegmentId
// (present case).
func TestCreateCampaign_MissingSegmentID_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{
			Name: aws.String("r80d-resweep-app"),
		},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.CreateCampaign(t.Context(), &pinpointsdk.CreateCampaignInput{
		ApplicationId: aws.String(appID),
		WriteCampaignRequest: &types.WriteCampaignRequest{
			Name: aws.String("no-segment-campaign"),
		},
	})
	require.Error(t, err)

	var badReq *types.BadRequestException
	require.ErrorAs(t, err, &badReq)
}
