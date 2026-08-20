package cloudcontrol_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cloudcontrolsdk "github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	cctypes "github.com/aws/aws-sdk-go-v2/service/cloudcontrol/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

// newTestCloudControlSDKClient stands up the real aws-sdk-go-v2 cloudcontrol
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production -- so a
// shape is verified by the real client's own generated deserializer
// (awsAwsjson10_deserializeOpDocument<Op>Output), not by gopherstack's own
// JSON tags matching themselves.
func newTestCloudControlSDKClient(t *testing.T, h *cloudcontrol.Handler) *cloudcontrolsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return cloudcontrolsdk.NewFromConfig(cfg, func(o *cloudcontrolsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCloudControl_SDKRoundTrip drives every one of the 8 real Cloud Control
// operations through the real SDK client end to end, proving each op's
// wrapper key, nesting, and per-member types against the live
// awsAwsjson10_deserializeOpDocument<Op>Output generated code -- not against
// gopherstack's own wire.go tags, which could agree with each other while
// both being wrong.
//
// In particular this proves, via the real client's typed fields (which
// would fail to populate, or the call would error, on any shape mismatch):
//   - ResourceDescription.Properties and ProgressEvent.ResourceModel are
//     JSON STRINGS on the wire (types.ResourceDescription.Properties /
//     types.ProgressEvent.ResourceModel are both *string in the pinned SDK).
//   - GetResource's wrapper is {ResourceDescription, TypeName}; ListResources'
//     is {ResourceDescriptions, TypeName, NextToken}; ListResourceRequests'
//     is {ResourceRequestStatusSummaries, NextToken} -- three distinct
//     wrapper shapes for three different list/get ops.
//   - CreateResource/UpdateResource/DeleteResource/CancelResourceRequest all
//     wrap a single ProgressEvent field; GetResourceRequestStatus wraps both
//     ProgressEvent and HooksProgressEvent.
//   - EventTime deserializes to a real time.Time (proving the wire form is
//     an epoch-seconds JSON number, since the SDK's ProgressEvent deserializer
//     rejects anything else -- see awsAwsjson10_deserializeDocumentProgressEvent).
func TestCloudControl_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudcontrol.NewInMemoryBackend("123456789012", "us-east-1")
	h := cloudcontrol.NewHandler(backend)
	client := newTestCloudControlSDKClient(t, h)

	const typeName = "AWS::S3::Bucket"

	created, err := client.CreateResource(t.Context(), &cloudcontrolsdk.CreateResourceInput{
		TypeName:     aws.String(typeName),
		DesiredState: aws.String(`{"BucketName":"roundtrip-bucket"}`),
	})
	require.NoError(t, err)
	require.NotNil(t, created.ProgressEvent)
	assert.Equal(t, cctypes.OperationCreate, created.ProgressEvent.Operation)
	assert.Equal(t, cctypes.OperationStatusSuccess, created.ProgressEvent.OperationStatus)
	assert.False(t, created.ProgressEvent.EventTime.IsZero())
	require.NotNil(t, created.ProgressEvent.ResourceModel)
	assert.JSONEq(t, `{"BucketName":"roundtrip-bucket"}`, aws.ToString(created.ProgressEvent.ResourceModel))
	identifier := aws.ToString(created.ProgressEvent.Identifier)
	require.NotEmpty(t, identifier)

	gotResource, err := client.GetResource(t.Context(), &cloudcontrolsdk.GetResourceInput{
		TypeName:   aws.String(typeName),
		Identifier: aws.String(identifier),
	})
	require.NoError(t, err)
	require.NotNil(t, gotResource.ResourceDescription)
	assert.Equal(t, typeName, aws.ToString(gotResource.TypeName))
	assert.Equal(t, identifier, aws.ToString(gotResource.ResourceDescription.Identifier))
	assert.JSONEq(t, `{"BucketName":"roundtrip-bucket"}`, aws.ToString(gotResource.ResourceDescription.Properties))

	listed, err := client.ListResources(t.Context(), &cloudcontrolsdk.ListResourcesInput{
		TypeName: aws.String(typeName),
	})
	require.NoError(t, err)
	assert.Equal(t, typeName, aws.ToString(listed.TypeName))
	require.Len(t, listed.ResourceDescriptions, 1)
	assert.Equal(t, identifier, aws.ToString(listed.ResourceDescriptions[0].Identifier))

	updated, err := client.UpdateResource(t.Context(), &cloudcontrolsdk.UpdateResourceInput{
		TypeName:      aws.String(typeName),
		Identifier:    aws.String(identifier),
		PatchDocument: aws.String(`[{"op":"replace","path":"/BucketName","value":"renamed-bucket"}]`),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.ProgressEvent)
	assert.Equal(t, cctypes.OperationUpdate, updated.ProgressEvent.Operation)
	assert.JSONEq(t, `{"BucketName":"renamed-bucket"}`, aws.ToString(updated.ProgressEvent.ResourceModel))

	status, err := client.GetResourceRequestStatus(t.Context(), &cloudcontrolsdk.GetResourceRequestStatusInput{
		RequestToken: updated.ProgressEvent.RequestToken,
	})
	require.NoError(t, err)
	require.NotNil(t, status.ProgressEvent)
	assert.Equal(t, aws.ToString(updated.ProgressEvent.RequestToken), aws.ToString(status.ProgressEvent.RequestToken))
	assert.Empty(t, status.HooksProgressEvent)

	reqList, err := client.ListResourceRequests(t.Context(), &cloudcontrolsdk.ListResourceRequestsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, reqList.ResourceRequestStatusSummaries)

	deleted, err := client.DeleteResource(t.Context(), &cloudcontrolsdk.DeleteResourceInput{
		TypeName:   aws.String(typeName),
		Identifier: aws.String(identifier),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.ProgressEvent)
	assert.Equal(t, cctypes.OperationDelete, deleted.ProgressEvent.Operation)
	assert.Equal(t, cctypes.OperationStatusSuccess, deleted.ProgressEvent.OperationStatus)

	_, err = client.GetResource(t.Context(), &cloudcontrolsdk.GetResourceInput{
		TypeName:   aws.String(typeName),
		Identifier: aws.String(identifier),
	})
	require.Error(t, err)
	var notFound *cctypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestCloudControl_CancelResourceRequest_SDKRoundTrip proves
// CancelResourceRequestOutput's single ProgressEvent wrapper key and that a
// cancelled request reports OperationStatus CANCEL_COMPLETE, against the
// real SDK's own enum type (not a bare gopherstack string constant).
func TestCloudControl_CancelResourceRequest_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudcontrol.NewInMemoryBackend("123456789012", "us-east-1")
	h := cloudcontrol.NewHandler(backend)
	client := newTestCloudControlSDKClient(t, h)

	backend.AddProgressEvent(&cloudcontrol.ProgressEvent{
		TypeName:        "AWS::S3::Bucket",
		Identifier:      "cancel-me",
		RequestToken:    "cancel-token",
		Operation:       "CREATE",
		OperationStatus: "IN_PROGRESS",
	})

	cancelled, err := client.CancelResourceRequest(t.Context(), &cloudcontrolsdk.CancelResourceRequestInput{
		RequestToken: aws.String("cancel-token"),
	})
	require.NoError(t, err)
	require.NotNil(t, cancelled.ProgressEvent)
	assert.Equal(t, cctypes.OperationStatusCancelComplete, cancelled.ProgressEvent.OperationStatus)
}
