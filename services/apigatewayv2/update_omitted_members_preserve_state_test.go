package apigatewayv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// These tests prove the zeroguard fix (cmd/zeroguard): Update*Input fields
// that the real SDK models as pointers must be decoded/applied as pointers,
// so an omitted PATCH member preserves the stored value and an explicit
// empty string clears it. Before the fix every field below was plain string
// guarded by `!= ""`, so omitted and explicit-empty were indistinguishable
// and a client could never clear the field.

func TestUpdateModel_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name: aws.String("model-api"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	created, err := client.CreateModel(t.Context(), &apigatewayv2sdk.CreateModelInput{
		ApiId:  api.ApiId,
		Name:   aws.String("M1"),
		Schema: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)

	updated, err := client.UpdateModel(t.Context(), &apigatewayv2sdk.UpdateModelInput{
		ApiId:       api.ApiId,
		ModelId:     created.ModelId,
		Name:        aws.String("M2"),
		Schema:      aws.String(`{"type":"string"}`),
		ContentType: aws.String("application/json"),
		Description: aws.String("first desc"),
	})
	require.NoError(t, err)
	assert.Equal(t, "M2", aws.ToString(updated.Name))
	assert.JSONEq(t, `{"type":"string"}`, aws.ToString(updated.Schema))
	assert.Equal(t, "application/json", aws.ToString(updated.ContentType))
	assert.Equal(t, "first desc", aws.ToString(updated.Description))

	// Omitted update must preserve every prior value.
	preserved, err := client.UpdateModel(t.Context(), &apigatewayv2sdk.UpdateModelInput{
		ApiId: api.ApiId, ModelId: created.ModelId,
	})
	require.NoError(t, err)
	assert.Equal(t, "M2", aws.ToString(preserved.Name))
	assert.JSONEq(t, `{"type":"string"}`, aws.ToString(preserved.Schema))
	assert.Equal(t, "application/json", aws.ToString(preserved.ContentType))
	assert.Equal(t, "first desc", aws.ToString(preserved.Description))

	// Explicit empty string clears description; unrelated fields untouched.
	cleared, err := client.UpdateModel(t.Context(), &apigatewayv2sdk.UpdateModelInput{
		ApiId: api.ApiId, ModelId: created.ModelId,
		Description: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.Equal(t, "M2", aws.ToString(cleared.Name), "unrelated field untouched")
}

func TestUpdateDeployment_PreservesOmittedDescription(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name: aws.String("dep-api"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	dep, err := client.CreateDeployment(t.Context(), &apigatewayv2sdk.CreateDeploymentInput{ApiId: api.ApiId})
	require.NoError(t, err)

	updated, err := client.UpdateDeployment(t.Context(), &apigatewayv2sdk.UpdateDeploymentInput{
		ApiId: api.ApiId, DeploymentId: dep.DeploymentId, Description: aws.String("v1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", aws.ToString(updated.Description))

	out, err := client.UpdateDeployment(t.Context(), &apigatewayv2sdk.UpdateDeploymentInput{
		ApiId: api.ApiId, DeploymentId: dep.DeploymentId,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", aws.ToString(out.Description), "omitted description must survive")

	cleared, err := client.UpdateDeployment(t.Context(), &apigatewayv2sdk.UpdateDeploymentInput{
		ApiId: api.ApiId, DeploymentId: dep.DeploymentId, Description: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
}

func TestUpdateIntegrationResponse_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name: aws.String("ir-api"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	integ, err := client.CreateIntegration(t.Context(), &apigatewayv2sdk.CreateIntegrationInput{
		ApiId: api.ApiId, IntegrationType: apigatewayv2types.IntegrationTypeHttpProxy,
		IntegrationUri: aws.String("https://example.com"),
	})
	require.NoError(t, err)

	ir, err := client.CreateIntegrationResponse(t.Context(), &apigatewayv2sdk.CreateIntegrationResponseInput{
		ApiId: api.ApiId, IntegrationId: integ.IntegrationId, IntegrationResponseKey: aws.String("$default"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateIntegrationResponse(t.Context(), &apigatewayv2sdk.UpdateIntegrationResponseInput{
		ApiId: api.ApiId, IntegrationId: integ.IntegrationId, IntegrationResponseId: ir.IntegrationResponseId,
		IntegrationResponseKey:      aws.String("/2\\d{2}/"),
		TemplateSelectionExpression: aws.String("$default"),
	})
	require.NoError(t, err)
	assert.Equal(t, "/2\\d{2}/", aws.ToString(updated.IntegrationResponseKey))
	assert.Equal(t, "$default", aws.ToString(updated.TemplateSelectionExpression))

	out, err := client.UpdateIntegrationResponse(t.Context(), &apigatewayv2sdk.UpdateIntegrationResponseInput{
		ApiId: api.ApiId, IntegrationId: integ.IntegrationId, IntegrationResponseId: ir.IntegrationResponseId,
	})
	require.NoError(t, err)
	assert.Equal(t, "/2\\d{2}/", aws.ToString(out.IntegrationResponseKey), "omitted key must survive")
	assert.Equal(t, "$default", aws.ToString(out.TemplateSelectionExpression), "omitted expression must survive")

	cleared, err := client.UpdateIntegrationResponse(t.Context(), &apigatewayv2sdk.UpdateIntegrationResponseInput{
		ApiId: api.ApiId, IntegrationId: integ.IntegrationId, IntegrationResponseId: ir.IntegrationResponseId,
		TemplateSelectionExpression: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.TemplateSelectionExpression))
	assert.Equal(t, "/2\\d{2}/", aws.ToString(cleared.IntegrationResponseKey), "unrelated field untouched")
}

func TestUpdateRouteResponse_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name: aws.String("rr-api"), ProtocolType: apigatewayv2types.ProtocolTypeWebsocket,
		RouteSelectionExpression: aws.String("$request.body.action"),
	})
	require.NoError(t, err)

	route, err := client.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
		ApiId: api.ApiId, RouteKey: aws.String("sendmessage"),
	})
	require.NoError(t, err)

	rr, err := client.CreateRouteResponse(t.Context(), &apigatewayv2sdk.CreateRouteResponseInput{
		ApiId: api.ApiId, RouteId: route.RouteId, RouteResponseKey: aws.String("$default"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateRouteResponse(t.Context(), &apigatewayv2sdk.UpdateRouteResponseInput{
		ApiId: api.ApiId, RouteId: route.RouteId, RouteResponseId: rr.RouteResponseId,
		ModelSelectionExpression: aws.String("$default"),
	})
	require.NoError(t, err)
	assert.Equal(t, "$default", aws.ToString(updated.ModelSelectionExpression))

	out, err := client.UpdateRouteResponse(t.Context(), &apigatewayv2sdk.UpdateRouteResponseInput{
		ApiId: api.ApiId, RouteId: route.RouteId, RouteResponseId: rr.RouteResponseId,
	})
	require.NoError(t, err)
	assert.Equal(t, "$default", aws.ToString(out.ModelSelectionExpression), "omitted expression must survive")

	cleared, err := client.UpdateRouteResponse(t.Context(), &apigatewayv2sdk.UpdateRouteResponseInput{
		ApiId: api.ApiId, RouteId: route.RouteId, RouteResponseId: rr.RouteResponseId,
		ModelSelectionExpression: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.ModelSelectionExpression))
}

func TestUpdateVpcLink_PreservesOmittedName(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	vl, err := client.CreateVpcLink(t.Context(), &apigatewayv2sdk.CreateVpcLinkInput{
		Name: aws.String("vl1"), SubnetIds: []string{"subnet-1"}, SecurityGroupIds: []string{"sg-1"},
	})
	require.NoError(t, err)

	updated, err := client.UpdateVpcLink(t.Context(), &apigatewayv2sdk.UpdateVpcLinkInput{
		VpcLinkId: vl.VpcLinkId, Name: aws.String("vl2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "vl2", aws.ToString(updated.Name))

	out, err := client.UpdateVpcLink(t.Context(), &apigatewayv2sdk.UpdateVpcLinkInput{VpcLinkId: vl.VpcLinkId})
	require.NoError(t, err)
	assert.Equal(t, "vl2", aws.ToString(out.Name), "omitted name must survive")
}

func TestUpdateStage_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name: aws.String("stage-api"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	_, err = client.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
		ApiId: api.ApiId, StageName: aws.String("dev"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateStage(t.Context(), &apigatewayv2sdk.UpdateStageInput{
		ApiId: api.ApiId, StageName: aws.String("dev"),
		DeploymentId: aws.String("dep-1"), Description: aws.String("d1"),
		ClientCertificateId: aws.String("cert-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "dep-1", aws.ToString(updated.DeploymentId))
	assert.Equal(t, "d1", aws.ToString(updated.Description))
	assert.Equal(t, "cert-1", aws.ToString(updated.ClientCertificateId))

	out, err := client.UpdateStage(t.Context(), &apigatewayv2sdk.UpdateStageInput{
		ApiId: api.ApiId, StageName: aws.String("dev"),
	})
	require.NoError(t, err)
	assert.Equal(t, "dep-1", aws.ToString(out.DeploymentId), "omitted deploymentId must survive")
	assert.Equal(t, "d1", aws.ToString(out.Description), "omitted description must survive")
	assert.Equal(t, "cert-1", aws.ToString(out.ClientCertificateId), "omitted clientCertificateId must survive")

	cleared, err := client.UpdateStage(t.Context(), &apigatewayv2sdk.UpdateStageInput{
		ApiId: api.ApiId, StageName: aws.String("dev"),
		DeploymentId: aws.String(""), Description: aws.String(""), ClientCertificateId: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.DeploymentId))
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.Empty(t, aws.ToString(cleared.ClientCertificateId))
}

func TestUpdateRoute_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name: aws.String("route-api"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	route, err := client.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
		ApiId: api.ApiId, RouteKey: aws.String("GET /a"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateRoute(t.Context(), &apigatewayv2sdk.UpdateRouteInput{
		ApiId: api.ApiId, RouteId: route.RouteId,
		RouteKey: aws.String("GET /b"), Target: aws.String("integrations/i-1"),
		AuthorizerId: aws.String("auth-1"), OperationName: aws.String("op1"),
		ModelSelectionExpression: aws.String("$default"),
	})
	require.NoError(t, err)
	assert.Equal(t, "GET /b", aws.ToString(updated.RouteKey))
	assert.Equal(t, "integrations/i-1", aws.ToString(updated.Target))
	assert.Equal(t, "auth-1", aws.ToString(updated.AuthorizerId))
	assert.Equal(t, "op1", aws.ToString(updated.OperationName))
	assert.Equal(t, "$default", aws.ToString(updated.ModelSelectionExpression))

	// Omitted update must preserve every prior value.
	preserved, err := client.UpdateRoute(t.Context(), &apigatewayv2sdk.UpdateRouteInput{
		ApiId: api.ApiId, RouteId: route.RouteId,
	})
	require.NoError(t, err)
	assert.Equal(t, "GET /b", aws.ToString(preserved.RouteKey))
	assert.Equal(t, "integrations/i-1", aws.ToString(preserved.Target))
	assert.Equal(t, "auth-1", aws.ToString(preserved.AuthorizerId))
	assert.Equal(t, "op1", aws.ToString(preserved.OperationName))
	assert.Equal(t, "$default", aws.ToString(preserved.ModelSelectionExpression))

	// Explicit empty string clears target/authorizer/operationName/
	// modelSelectionExpression; RouteKey has no legitimate empty-string form
	// (AWS requires "$default" or "METHOD /path"), so it has no explicit-zero
	// case and is asserted untouched here instead.
	cleared, err := client.UpdateRoute(t.Context(), &apigatewayv2sdk.UpdateRouteInput{
		ApiId: api.ApiId, RouteId: route.RouteId,
		Target: aws.String(""), AuthorizerId: aws.String(""),
		OperationName: aws.String(""), ModelSelectionExpression: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Target))
	assert.Empty(t, aws.ToString(cleared.AuthorizerId))
	assert.Empty(t, aws.ToString(cleared.OperationName))
	assert.Empty(t, aws.ToString(cleared.ModelSelectionExpression))
	assert.Equal(t, "GET /b", aws.ToString(cleared.RouteKey), "unrelated field untouched")
}

func TestUpdateIntegration_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name: aws.String("integ-api"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	integ, err := client.CreateIntegration(t.Context(), &apigatewayv2sdk.CreateIntegrationInput{
		ApiId: api.ApiId, IntegrationType: apigatewayv2types.IntegrationTypeHttpProxy,
		IntegrationUri: aws.String("https://a.example.com"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateIntegration(t.Context(), &apigatewayv2sdk.UpdateIntegrationInput{
		ApiId: api.ApiId, IntegrationId: integ.IntegrationId,
		IntegrationSubtype:          aws.String("StepFunctions-StartExecution"),
		IntegrationMethod:           aws.String("POST"),
		IntegrationUri:              aws.String("https://b.example.com"),
		Description:                 aws.String("d1"),
		PayloadFormatVersion:        aws.String("1.0"),
		TemplateSelectionExpression: aws.String("$default"),
		CredentialsArn:              aws.String("arn:aws:iam::123456789012:role/r1"),
		ConnectionType:              apigatewayv2types.ConnectionTypeVpcLink,
		ConnectionId:                aws.String("vpclink-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "StepFunctions-StartExecution", aws.ToString(updated.IntegrationSubtype))
	assert.Equal(t, "POST", aws.ToString(updated.IntegrationMethod))
	assert.Equal(t, "https://b.example.com", aws.ToString(updated.IntegrationUri))
	assert.Equal(t, "d1", aws.ToString(updated.Description))
	assert.Equal(t, "1.0", aws.ToString(updated.PayloadFormatVersion))
	assert.Equal(t, "$default", aws.ToString(updated.TemplateSelectionExpression))
	assert.Equal(t, "arn:aws:iam::123456789012:role/r1", aws.ToString(updated.CredentialsArn))
	assert.Equal(t, "vpclink-1", aws.ToString(updated.ConnectionId))

	// Omitted update must preserve every prior value.
	preserved, err := client.UpdateIntegration(t.Context(), &apigatewayv2sdk.UpdateIntegrationInput{
		ApiId: api.ApiId, IntegrationId: integ.IntegrationId,
	})
	require.NoError(t, err)
	assert.Equal(t, "StepFunctions-StartExecution", aws.ToString(preserved.IntegrationSubtype))
	assert.Equal(t, "POST", aws.ToString(preserved.IntegrationMethod))
	assert.Equal(t, "https://b.example.com", aws.ToString(preserved.IntegrationUri))
	assert.Equal(t, "d1", aws.ToString(preserved.Description))
	assert.Equal(t, "1.0", aws.ToString(preserved.PayloadFormatVersion))
	assert.Equal(t, "$default", aws.ToString(preserved.TemplateSelectionExpression))
	assert.Equal(t, "arn:aws:iam::123456789012:role/r1", aws.ToString(preserved.CredentialsArn))
	assert.Equal(t, "vpclink-1", aws.ToString(preserved.ConnectionId))

	// Explicit empty string clears method/uri/description/payloadFormatVersion/
	// subtype/credentials/templateExpr; unrelated connectionId untouched.
	cleared, err := client.UpdateIntegration(t.Context(), &apigatewayv2sdk.UpdateIntegrationInput{
		ApiId: api.ApiId, IntegrationId: integ.IntegrationId,
		IntegrationSubtype:          aws.String(""),
		IntegrationMethod:           aws.String(""),
		IntegrationUri:              aws.String(""),
		Description:                 aws.String(""),
		PayloadFormatVersion:        aws.String(""),
		TemplateSelectionExpression: aws.String(""),
		CredentialsArn:              aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.IntegrationSubtype))
	assert.Empty(t, aws.ToString(cleared.IntegrationMethod))
	assert.Empty(t, aws.ToString(cleared.IntegrationUri))
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.Empty(t, aws.ToString(cleared.PayloadFormatVersion))
	assert.Empty(t, aws.ToString(cleared.TemplateSelectionExpression))
	assert.Empty(t, aws.ToString(cleared.CredentialsArn))
	assert.Equal(t, "vpclink-1", aws.ToString(cleared.ConnectionId), "unrelated field untouched")

	// connectionId can only be cleared together with switching connectionType
	// away from VPC_LINK: VPC_LINK always requires a non-empty connectionId.
	clearedConn, err := client.UpdateIntegration(t.Context(), &apigatewayv2sdk.UpdateIntegrationInput{
		ApiId: api.ApiId, IntegrationId: integ.IntegrationId,
		ConnectionType: apigatewayv2types.ConnectionTypeInternet,
		ConnectionId:   aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(clearedConn.ConnectionId))
	assert.Equal(t, apigatewayv2types.ConnectionTypeInternet, clearedConn.ConnectionType)
}

func TestUpdatePortalProduct_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	created, err := client.CreatePortalProduct(t.Context(), &apigatewayv2sdk.CreatePortalProductInput{
		DisplayName: aws.String("p1"),
	})
	require.NoError(t, err)

	updated, err := client.UpdatePortalProduct(t.Context(), &apigatewayv2sdk.UpdatePortalProductInput{
		PortalProductId: created.PortalProductId,
		DisplayName:     aws.String("p2"),
		Description:     aws.String("d1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "p2", aws.ToString(updated.DisplayName))
	assert.Equal(t, "d1", aws.ToString(updated.Description))

	out, err := client.UpdatePortalProduct(t.Context(), &apigatewayv2sdk.UpdatePortalProductInput{
		PortalProductId: created.PortalProductId,
	})
	require.NoError(t, err)
	assert.Equal(t, "p2", aws.ToString(out.DisplayName), "omitted displayName must survive")
	assert.Equal(t, "d1", aws.ToString(out.Description), "omitted description must survive")

	cleared, err := client.UpdatePortalProduct(t.Context(), &apigatewayv2sdk.UpdatePortalProductInput{
		PortalProductId: created.PortalProductId,
		Description:     aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.Equal(t, "p2", aws.ToString(cleared.DisplayName), "unrelated field untouched")
}

func TestUpdatePortal_PreservesOmittedRumAppMonitorName(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	created, err := client.CreatePortal(t.Context(), &apigatewayv2sdk.CreatePortalInput{
		Authorization:         &apigatewayv2types.Authorization{None: &apigatewayv2types.None{}},
		EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{None: &apigatewayv2types.None{}},
		PortalContent:         testPortalContent(),
	})
	require.NoError(t, err)

	updated, err := client.UpdatePortal(t.Context(), &apigatewayv2sdk.UpdatePortalInput{
		PortalId: created.PortalId, RumAppMonitorName: aws.String("rum-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "rum-1", aws.ToString(updated.RumAppMonitorName))

	out, err := client.UpdatePortal(t.Context(), &apigatewayv2sdk.UpdatePortalInput{PortalId: created.PortalId})
	require.NoError(t, err)
	assert.Equal(t, "rum-1", aws.ToString(out.RumAppMonitorName), "omitted rumAppMonitorName must survive")

	cleared, err := client.UpdatePortal(t.Context(), &apigatewayv2sdk.UpdatePortalInput{
		PortalId: created.PortalId, RumAppMonitorName: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.RumAppMonitorName))
}

// TestUpdatePortal_PreservesOmittedLogoURI exercises the backend directly:
// LogoUri is write-only on the real wire (present on
// Create/UpdatePortalInput, absent from every *Output -- confirmed against
// aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_GetPortal.go and
// api_op_UpdatePortal.go), so a typed-client round trip cannot observe it.
func TestUpdatePortal_PreservesOmittedLogoURI(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	p, err := b.CreatePortal(apigatewayv2.CreatePortalInput{
		Authorization:         &apigatewayv2.Authorization{None: &apigatewayv2.None{}},
		EndpointConfiguration: &apigatewayv2.EndpointConfigurationRequest{None: &apigatewayv2.None{}},
		PortalContent: &apigatewayv2.PortalContent{
			DisplayName: "p1",
			Theme: &apigatewayv2.PortalTheme{
				CustomColors: &apigatewayv2.CustomColors{
					AccentColor: "#000000", BackgroundColor: "#000000", ErrorValidationColor: "#000000",
					HeaderColor: "#000000", NavigationColor: "#000000", TextColor: "#000000",
				},
			},
		},
	})
	require.NoError(t, err)

	updated, err := b.UpdatePortal(
		p.PortalID,
		apigatewayv2.UpdatePortalInput{LogoURI: aws.String("https://logo/1.png")},
	)
	require.NoError(t, err)
	assert.Equal(t, "https://logo/1.png", updated.LogoURI)

	out, err := b.UpdatePortal(p.PortalID, apigatewayv2.UpdatePortalInput{})
	require.NoError(t, err)
	assert.Equal(t, "https://logo/1.png", out.LogoURI, "omitted logoUri must survive")

	cleared, err := b.UpdatePortal(p.PortalID, apigatewayv2.UpdatePortalInput{LogoURI: aws.String("")})
	require.NoError(t, err)
	assert.Empty(t, cleared.LogoURI)
}
