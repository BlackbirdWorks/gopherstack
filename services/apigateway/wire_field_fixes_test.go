package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestCreateStage_AccessLogAndMethodSettings_ViaUpdateStageRealClient covers
// gopherstack-wksweep-apigw-1: real CreateStageInput (apigateway@v1.42.4
// api_op_CreateStage.go) has no AccessLogSettings, MethodSettings, or
// ClientCertificateId members at all -- the Go SDK struct structurally
// cannot carry them at creation time. They're only settable afterward via
// UpdateStage's PATCH operations. This proves the real two-step workflow: a
// freshly created stage has none of these set, and UpdateStage's
// PatchOperations round-trip them.
func TestCreateStage_AccessLogAndMethodSettings_ViaUpdateStageRealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))
	ctx := t.Context()

	api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("wire-fix-stage-api")})
	require.NoError(t, err)

	depl, err := client.CreateDeployment(ctx, &apigwsdk.CreateDeploymentInput{RestApiId: api.Id})
	require.NoError(t, err)

	cert, err := client.GenerateClientCertificate(ctx, &apigwsdk.GenerateClientCertificateInput{})
	require.NoError(t, err)

	created, err := client.CreateStage(ctx, &apigwsdk.CreateStageInput{
		RestApiId:    api.Id,
		DeploymentId: depl.Id,
		StageName:    aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Nil(t, created.AccessLogSettings, "CreateStageInput has no AccessLogSettings member; must not be set")
	assert.Empty(t, created.MethodSettings, "CreateStageInput has no MethodSettings member; must not be set")
	assert.Empty(t, aws.ToString(created.ClientCertificateId),
		"CreateStageInput has no ClientCertificateId member; must not be set")

	updated, err := client.UpdateStage(ctx, &apigwsdk.UpdateStageInput{
		RestApiId: api.Id,
		StageName: aws.String("prod"),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/accessLogSettings/destinationArn"),
				Value: aws.String("arn:aws:logs:us-east-1:123456789012:log-group:my-api")},
			{Op: apigwtypes.OpReplace, Path: aws.String("/accessLogSettings/format"),
				Value: aws.String("$context.requestId")},
			{Op: apigwtypes.OpReplace, Path: aws.String("/clientCertificateId"),
				Value: cert.ClientCertificateId},
			{Op: apigwtypes.OpReplace, Path: aws.String("/*/*/logging/loglevel"), Value: aws.String("INFO")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.AccessLogSettings)
	assert.Equal(t, "arn:aws:logs:us-east-1:123456789012:log-group:my-api",
		aws.ToString(updated.AccessLogSettings.DestinationArn))
	assert.Equal(t, "$context.requestId", aws.ToString(updated.AccessLogSettings.Format))
	assert.Equal(t, aws.ToString(cert.ClientCertificateId), aws.ToString(updated.ClientCertificateId))
	require.Contains(t, updated.MethodSettings, "*/*")
	assert.Equal(t, "INFO", aws.ToString(updated.MethodSettings["*/*"].LoggingLevel))

	got, err := client.GetStage(ctx, &apigwsdk.GetStageInput{RestApiId: api.Id, StageName: aws.String("prod")})
	require.NoError(t, err)
	require.NotNil(t, got.AccessLogSettings)
	assert.Equal(t, "arn:aws:logs:us-east-1:123456789012:log-group:my-api",
		aws.ToString(got.AccessLogSettings.DestinationArn))
	assert.Equal(t, aws.ToString(cert.ClientCertificateId), aws.ToString(got.ClientCertificateId))
}

// TestGetAndDeleteDocumentationPart_RealSDKPathParamRoundTrip documents the
// gopherstack-wksweep-apigw-2 investigation result: acceptguard flagged
// getDocumentationPartInput.DocPartID/deleteDocumentationPartInput.DocPartID
// (handler_documentation.go:8,18) as matching no real Input member. That's a
// false positive for this pair -- DocumentationPartId/RestApiId are
// httpLabel-bound (apigateway@v1.42.4 serializers.go:4815-4831,
// encoder.SetURI, not a JSON body field at all), so no real client ever
// sends a member named "documentationPartId" on the wire; the value is a
// positional URL segment. The router (handler_router.go:70) already parses
// that segment positionally off the real URL and threads it through under
// gopherstack's own internal key name -- "DocPartID" is plumbing between the
// router and the action handler, not a wire member. This proves a real typed
// client's GetDocumentationPart/DeleteDocumentationPart still work.
func TestGetAndDeleteDocumentationPart_RealSDKPathParamRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))
	ctx := t.Context()

	api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("docpart-wire-fix-api")})
	require.NoError(t, err)

	part, err := client.CreateDocumentationPart(ctx, &apigwsdk.CreateDocumentationPartInput{
		RestApiId:  api.Id,
		Location:   &apigwtypes.DocumentationPartLocation{Type: apigwtypes.DocumentationPartTypeApi},
		Properties: aws.String(`{"description":"wire fix test"}`),
	})
	require.NoError(t, err)
	require.NotNil(t, part.Id)

	got, err := client.GetDocumentationPart(ctx, &apigwsdk.GetDocumentationPartInput{
		RestApiId:           api.Id,
		DocumentationPartId: part.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(part.Id), aws.ToString(got.Id))

	_, err = client.DeleteDocumentationPart(ctx, &apigwsdk.DeleteDocumentationPartInput{
		RestApiId:           api.Id,
		DocumentationPartId: part.Id,
	})
	require.NoError(t, err)

	_, err = client.GetDocumentationPart(ctx, &apigwsdk.GetDocumentationPartInput{
		RestApiId:           api.Id,
		DocumentationPartId: part.Id,
	})
	require.Error(t, err, "deleted documentation part must not still be retrievable")
}
