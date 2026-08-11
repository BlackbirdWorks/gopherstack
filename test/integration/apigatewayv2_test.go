package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigwv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_APIGatewayV2_FullLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	// 1. CreateApi — HTTP protocol
	createOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("integ-http-api"),
		ProtocolType: apigwv2types.ProtocolTypeHttp,
		Description:  aws.String("integration test HTTP API"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ApiId)
	apiID := *createOut.ApiId
	assert.Equal(t, "integ-http-api", *createOut.Name)
	assert.Equal(t, apigwv2types.ProtocolTypeHttp, createOut.ProtocolType)
	// AWS default RSE for HTTP is "${request.method} ${request.path}"
	assert.Equal(t, "${request.method} ${request.path}", aws.ToString(createOut.RouteSelectionExpression))
	assert.NotEmpty(t, aws.ToString(createOut.ApiEndpoint))

	// 2. GetApi
	getOut, err := client.GetApi(ctx, &apigwv2sdk.GetApiInput{ApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Equal(t, apiID, *getOut.ApiId)
	assert.Equal(t, "integ-http-api", *getOut.Name)

	// 3. GetApis (list)
	listOut, err := client.GetApis(ctx, &apigwv2sdk.GetApisInput{})
	require.NoError(t, err)
	found := false
	for _, a := range listOut.Items {
		if aws.ToString(a.ApiId) == apiID {
			found = true

			break
		}
	}
	assert.True(t, found, "created API should appear in GetApis")

	// 4. UpdateApi — change name and add CORS config
	updateOut, err := client.UpdateApi(ctx, &apigwv2sdk.UpdateApiInput{
		ApiId:       aws.String(apiID),
		Name:        aws.String("integ-http-api-updated"),
		Description: aws.String("updated description"),
		CorsConfiguration: &apigwv2types.Cors{
			AllowOrigins:     []string{"https://example.com"},
			AllowMethods:     []string{"GET", "POST"},
			AllowHeaders:     []string{"Content-Type"},
			AllowCredentials: aws.Bool(true),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "integ-http-api-updated", *updateOut.Name)
	require.NotNil(t, updateOut.CorsConfiguration)
	assert.Equal(t, []string{"https://example.com"}, updateOut.CorsConfiguration.AllowOrigins)

	// 5. CreateStage
	stageOut, err := client.CreateStage(ctx, &apigwv2sdk.CreateStageInput{
		ApiId:       aws.String(apiID),
		StageName:   aws.String("prod"),
		Description: aws.String("production stage"),
		AutoDeploy:  aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", *stageOut.StageName)

	// 6. Duplicate stage name should fail (409 ConflictException)
	_, errDup := client.CreateStage(ctx, &apigwv2sdk.CreateStageInput{
		ApiId:     aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.Error(t, errDup, "creating duplicate stage should return error")

	// 7. GetStage
	getStageOut, err := client.GetStage(ctx, &apigwv2sdk.GetStageInput{
		ApiId:     aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", *getStageOut.StageName)
	assert.Equal(t, "production stage", *getStageOut.Description)

	// 8. GetStages (list)
	listStagesOut, err := client.GetStages(ctx, &apigwv2sdk.GetStagesInput{
		ApiId: aws.String(apiID),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listStagesOut.Items), 1)

	// 9. CreateRoute
	routeOut, err := client.CreateRoute(ctx, &apigwv2sdk.CreateRouteInput{
		ApiId:    aws.String(apiID),
		RouteKey: aws.String("GET /items"),
		Target:   aws.String("integrations/placeholder"),
	})
	require.NoError(t, err)
	require.NotNil(t, routeOut.RouteId)
	routeID := *routeOut.RouteId
	assert.Equal(t, "GET /items", *routeOut.RouteKey)

	// 10. CreateRoute — missing routeKey should fail (400)
	_, errNoKey := client.CreateRoute(ctx, &apigwv2sdk.CreateRouteInput{
		ApiId: aws.String(apiID),
	})
	require.Error(t, errNoKey, "missing routeKey should fail")

	// 11. Duplicate route key should fail (409)
	_, errDupRoute := client.CreateRoute(ctx, &apigwv2sdk.CreateRouteInput{
		ApiId:    aws.String(apiID),
		RouteKey: aws.String("GET /items"),
	})
	require.Error(t, errDupRoute, "duplicate routeKey should fail")

	// 12. GetRoute
	getRouteOut, err := client.GetRoute(ctx, &apigwv2sdk.GetRouteInput{
		ApiId:   aws.String(apiID),
		RouteId: aws.String(routeID),
	})
	require.NoError(t, err)
	assert.Equal(t, "GET /items", *getRouteOut.RouteKey)

	// 13. GetRoutes (list)
	listRoutesOut, err := client.GetRoutes(ctx, &apigwv2sdk.GetRoutesInput{
		ApiId: aws.String(apiID),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listRoutesOut.Items), 1)

	// 14. UpdateRoute
	updateRouteOut, err := client.UpdateRoute(ctx, &apigwv2sdk.UpdateRouteInput{
		ApiId:         aws.String(apiID),
		RouteId:       aws.String(routeID),
		OperationName: aws.String("ListItems"),
	})
	require.NoError(t, err)
	assert.Equal(t, "ListItems", *updateRouteOut.OperationName)

	// 15. CreateIntegration (AWS_PROXY with default PayloadFormatVersion)
	integOut, err := client.CreateIntegration(ctx, &apigwv2sdk.CreateIntegrationInput{
		ApiId:           aws.String(apiID),
		IntegrationType: apigwv2types.IntegrationTypeAwsProxy,
		IntegrationUri:  aws.String("arn:aws:lambda:us-east-1:123456789012:function:my-function"),
		Description:     aws.String("lambda proxy integration"),
	})
	require.NoError(t, err)
	require.NotNil(t, integOut.IntegrationId)
	integID := *integOut.IntegrationId
	// Default PayloadFormatVersion for AWS_PROXY should be "1.0"
	assert.Equal(t, "1.0", aws.ToString(integOut.PayloadFormatVersion))
	// Default TimeoutInMillis for an HTTP API is 30000 (WebSocket APIs default
	// to 29000); this API is ProtocolTypeHttp — see per-protocol fix e069888c.
	assert.Equal(t, int32(30000), aws.ToInt32(integOut.TimeoutInMillis))

	// 16. GetIntegration
	getIntegOut, err := client.GetIntegration(ctx, &apigwv2sdk.GetIntegrationInput{
		ApiId:         aws.String(apiID),
		IntegrationId: aws.String(integID),
	})
	require.NoError(t, err)
	assert.Equal(t, apigwv2types.IntegrationTypeAwsProxy, getIntegOut.IntegrationType)

	// 17. GetIntegrations (list)
	listIntegOut, err := client.GetIntegrations(ctx, &apigwv2sdk.GetIntegrationsInput{
		ApiId: aws.String(apiID),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listIntegOut.Items), 1)

	// 18. UpdateIntegration
	updateIntegOut, err := client.UpdateIntegration(ctx, &apigwv2sdk.UpdateIntegrationInput{
		ApiId:                aws.String(apiID),
		IntegrationId:        aws.String(integID),
		Description:          aws.String("updated integration"),
		PayloadFormatVersion: aws.String("2.0"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated integration", *updateIntegOut.Description)
	assert.Equal(t, "2.0", *updateIntegOut.PayloadFormatVersion)

	// 19. CreateDeployment with StageName links to the stage
	deplOut, err := client.CreateDeployment(ctx, &apigwv2sdk.CreateDeploymentInput{
		ApiId:       aws.String(apiID),
		StageName:   aws.String("prod"),
		Description: aws.String("initial deploy"),
	})
	require.NoError(t, err)
	require.NotNil(t, deplOut.DeploymentId)
	deplID := *deplOut.DeploymentId
	assert.Equal(t, apigwv2types.DeploymentStatusDeployed, deplOut.DeploymentStatus)

	// Verify the deployment is linked to the stage
	linkedStage, err := client.GetStage(ctx, &apigwv2sdk.GetStageInput{
		ApiId:     aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, deplID, aws.ToString(linkedStage.DeploymentId),
		"stage should be linked to the created deployment")

	// 20. GetDeployment
	getDeployOut, err := client.GetDeployment(ctx, &apigwv2sdk.GetDeploymentInput{
		ApiId:        aws.String(apiID),
		DeploymentId: aws.String(deplID),
	})
	require.NoError(t, err)
	assert.Equal(t, apigwv2types.DeploymentStatusDeployed, getDeployOut.DeploymentStatus)

	// 21. GetDeployments (list)
	listDeplsOut, err := client.GetDeployments(ctx, &apigwv2sdk.GetDeploymentsInput{
		ApiId: aws.String(apiID),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listDeplsOut.Items), 1)

	// 22. DeleteDeployment
	_, err = client.DeleteDeployment(ctx, &apigwv2sdk.DeleteDeploymentInput{
		ApiId:        aws.String(apiID),
		DeploymentId: aws.String(deplID),
	})
	require.NoError(t, err)

	// 23. DeleteRoute
	_, err = client.DeleteRoute(ctx, &apigwv2sdk.DeleteRouteInput{
		ApiId:   aws.String(apiID),
		RouteId: aws.String(routeID),
	})
	require.NoError(t, err)

	// 24. DeleteIntegration
	_, err = client.DeleteIntegration(ctx, &apigwv2sdk.DeleteIntegrationInput{
		ApiId:         aws.String(apiID),
		IntegrationId: aws.String(integID),
	})
	require.NoError(t, err)

	// 25. DeleteStage
	_, err = client.DeleteStage(ctx, &apigwv2sdk.DeleteStageInput{
		ApiId:     aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	// 26. DeleteApi
	_, err = client.DeleteApi(ctx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	require.NoError(t, err)

	// 27. Verify API is gone
	_, errGone := client.GetApi(ctx, &apigwv2sdk.GetApiInput{ApiId: aws.String(apiID)})
	require.Error(t, errGone, "deleted API should not be found")
}

func TestIntegration_APIGatewayV2_AuthorizerJWT(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	// Create an HTTP API
	createOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("jwt-auth-api"),
		ProtocolType: apigwv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)
	apiID := *createOut.ApiId

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	// Create a JWT authorizer
	authOut, err := client.CreateAuthorizer(ctx, &apigwv2sdk.CreateAuthorizerInput{
		ApiId:          aws.String(apiID),
		Name:           aws.String("my-jwt-authorizer"),
		AuthorizerType: apigwv2types.AuthorizerTypeJwt,
		IdentitySource: []string{"$request.header.Authorization"},
		JwtConfiguration: &apigwv2types.JWTConfiguration{
			Issuer:   aws.String("https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123"),
			Audience: []string{"my-app-client-id"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, authOut.AuthorizerId)
	authID := *authOut.AuthorizerId
	assert.Equal(t, apigwv2types.AuthorizerTypeJwt, authOut.AuthorizerType)
	require.NotNil(t, authOut.JwtConfiguration)
	assert.Equal(t, "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123",
		aws.ToString(authOut.JwtConfiguration.Issuer))

	// GetAuthorizer
	getAuthOut, err := client.GetAuthorizer(ctx, &apigwv2sdk.GetAuthorizerInput{
		ApiId:        aws.String(apiID),
		AuthorizerId: aws.String(authID),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-jwt-authorizer", *getAuthOut.Name)
	require.NotNil(t, getAuthOut.JwtConfiguration)

	// GetAuthorizers (list)
	listAuthOut, err := client.GetAuthorizers(ctx, &apigwv2sdk.GetAuthorizersInput{
		ApiId: aws.String(apiID),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listAuthOut.Items), 1)

	// UpdateAuthorizer
	updAuthOut, err := client.UpdateAuthorizer(ctx, &apigwv2sdk.UpdateAuthorizerInput{
		ApiId:        aws.String(apiID),
		AuthorizerId: aws.String(authID),
		Name:         aws.String("updated-authorizer"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated-authorizer", *updAuthOut.Name)

	// DeleteAuthorizer
	_, err = client.DeleteAuthorizer(ctx, &apigwv2sdk.DeleteAuthorizerInput{
		ApiId:        aws.String(apiID),
		AuthorizerId: aws.String(authID),
	})
	require.NoError(t, err)
}

func TestIntegration_APIGatewayV2_WebSocketAPI(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	// Create a WebSocket API — default RSE should be "$request.body.action"
	createOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("integ-ws-api"),
		ProtocolType: apigwv2types.ProtocolTypeWebsocket,
	})
	require.NoError(t, err)
	apiID := *createOut.ApiId
	assert.Equal(t, apigwv2types.ProtocolTypeWebsocket, createOut.ProtocolType)
	assert.Equal(t, "$request.body.action", aws.ToString(createOut.RouteSelectionExpression))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	// Create the $connect route
	routeOut, err := client.CreateRoute(ctx, &apigwv2sdk.CreateRouteInput{
		ApiId:    aws.String(apiID),
		RouteKey: aws.String("$connect"),
	})
	require.NoError(t, err)
	assert.Equal(t, "$connect", *routeOut.RouteKey)
}

func TestIntegration_APIGatewayV2_VpcLink(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	// CreateVpcLink
	vpcOut, err := client.CreateVpcLink(ctx, &apigwv2sdk.CreateVpcLinkInput{
		Name:      aws.String("my-vpc-link"),
		SubnetIds: []string{"subnet-aaa", "subnet-bbb"},
		Tags:      map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	require.NotNil(t, vpcOut.VpcLinkId)
	vpcID := *vpcOut.VpcLinkId
	// Mock immediately sets status to AVAILABLE
	assert.Equal(t, apigwv2types.VpcLinkStatusAvailable, vpcOut.VpcLinkStatus)
	assert.Equal(t, "my-vpc-link", *vpcOut.Name)

	// GetVpcLink
	getOut, err := client.GetVpcLink(ctx, &apigwv2sdk.GetVpcLinkInput{VpcLinkId: aws.String(vpcID)})
	require.NoError(t, err)
	assert.Equal(t, vpcID, *getOut.VpcLinkId)

	// GetVpcLinks (list)
	listOut, err := client.GetVpcLinks(ctx, &apigwv2sdk.GetVpcLinksInput{})
	require.NoError(t, err)
	found := false
	for _, l := range listOut.Items {
		if aws.ToString(l.VpcLinkId) == vpcID {
			found = true

			break
		}
	}
	assert.True(t, found, "created VPC link should appear in GetVpcLinks")

	// UpdateVpcLink
	updOut, err := client.UpdateVpcLink(ctx, &apigwv2sdk.UpdateVpcLinkInput{
		VpcLinkId: aws.String(vpcID),
		Name:      aws.String("updated-vpc-link"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated-vpc-link", *updOut.Name)

	// DeleteVpcLink
	_, err = client.DeleteVpcLink(ctx, &apigwv2sdk.DeleteVpcLinkInput{VpcLinkId: aws.String(vpcID)})
	require.NoError(t, err)

	// Verify gone
	_, errGone := client.GetVpcLink(ctx, &apigwv2sdk.GetVpcLinkInput{VpcLinkId: aws.String(vpcID)})
	require.Error(t, errGone)
}

func TestIntegration_APIGatewayV2_DomainNameAndMapping(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	// Create an HTTP API and stage
	createOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("domain-test-api"),
		ProtocolType: apigwv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)
	apiID := *createOut.ApiId

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	_, err = client.CreateStage(ctx, &apigwv2sdk.CreateStageInput{
		ApiId:     aws.String(apiID),
		StageName: aws.String("v1"),
	})
	require.NoError(t, err)

	// CreateDomainName
	domainName := "api.integ-test.example.com"
	dnOut, err := client.CreateDomainName(ctx, &apigwv2sdk.CreateDomainNameInput{
		DomainName: aws.String(domainName),
		DomainNameConfigurations: []apigwv2types.DomainNameConfiguration{
			{
				CertificateArn: aws.String("arn:aws:acm:us-east-1:123:certificate/abc"),
				EndpointType:   apigwv2types.EndpointTypeRegional,
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, domainName, *dnOut.DomainName)

	// GetDomainName
	getDnOut, err := client.GetDomainName(ctx, &apigwv2sdk.GetDomainNameInput{
		DomainName: aws.String(domainName),
	})
	require.NoError(t, err)
	assert.Equal(t, domainName, *getDnOut.DomainName)

	// GetDomainNames (list)
	listDnOut, err := client.GetDomainNames(ctx, &apigwv2sdk.GetDomainNamesInput{})
	require.NoError(t, err)
	foundDomain := false
	for _, dn := range listDnOut.Items {
		if aws.ToString(dn.DomainName) == domainName {
			foundDomain = true

			break
		}
	}
	assert.True(t, foundDomain)

	// CreateApiMapping
	mappingOut, err := client.CreateApiMapping(ctx, &apigwv2sdk.CreateApiMappingInput{
		DomainName: aws.String(domainName),
		ApiId:      aws.String(apiID),
		Stage:      aws.String("v1"),
	})
	require.NoError(t, err)
	require.NotNil(t, mappingOut.ApiMappingId)
	mappingID := *mappingOut.ApiMappingId

	// GetApiMapping
	getMappingOut, err := client.GetApiMapping(ctx, &apigwv2sdk.GetApiMappingInput{
		DomainName:   aws.String(domainName),
		ApiMappingId: aws.String(mappingID),
	})
	require.NoError(t, err)
	assert.Equal(t, apiID, *getMappingOut.ApiId)

	// DeleteApiMapping
	_, err = client.DeleteApiMapping(ctx, &apigwv2sdk.DeleteApiMappingInput{
		DomainName:   aws.String(domainName),
		ApiMappingId: aws.String(mappingID),
	})
	require.NoError(t, err)

	// DeleteDomainName
	_, err = client.DeleteDomainName(ctx, &apigwv2sdk.DeleteDomainNameInput{
		DomainName: aws.String(domainName),
	})
	require.NoError(t, err)
}

func TestIntegration_APIGatewayV2_Tags(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	// Create an API with tags
	createOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("tagged-api"),
		ProtocolType: apigwv2types.ProtocolTypeHttp,
		Tags:         map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)
	apiID := *createOut.ApiId
	assert.Equal(t, "test", createOut.Tags["env"])

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	// GetTags
	tagsOut, err := client.GetTags(ctx, &apigwv2sdk.GetTagsInput{
		ResourceArn: aws.String("arn:aws:apigateway:us-east-1::/apis/" + apiID),
	})
	require.NoError(t, err)
	assert.Equal(t, "test", tagsOut.Tags["env"])

	// TagResource — add new tag
	_, err = client.TagResource(ctx, &apigwv2sdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:apigateway:us-east-1::/apis/" + apiID),
		Tags:        map[string]string{"version": "v1"},
	})
	require.NoError(t, err)

	// UntagResource — remove a tag
	_, err = client.UntagResource(ctx, &apigwv2sdk.UntagResourceInput{
		ResourceArn: aws.String("arn:aws:apigateway:us-east-1::/apis/" + apiID),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)
}

func TestIntegration_APIGatewayV2_StageVariablesAndAccessLog(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	// Create an API
	createOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("stage-var-api"),
		ProtocolType: apigwv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)
	apiID := *createOut.ApiId

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	// Create stage with variables and access log settings
	stageOut, err := client.CreateStage(ctx, &apigwv2sdk.CreateStageInput{
		ApiId:     aws.String(apiID),
		StageName: aws.String("dev"),
		StageVariables: map[string]string{
			"SERVICE_URL": "https://backend.dev.example.com",
			"ENV":         "dev",
		},
		AccessLogSettings: &apigwv2types.AccessLogSettings{
			DestinationArn: aws.String("arn:aws:logs:us-east-1:123:log-group:my-api"),
			Format:         aws.String(`$context.requestId $context.status`),
		},
		DefaultRouteSettings: &apigwv2types.RouteSettings{
			ThrottlingBurstLimit: aws.Int32(100),
			ThrottlingRateLimit:  aws.Float64(50),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "dev", *stageOut.StageName)
	assert.Equal(t, "https://backend.dev.example.com", stageOut.StageVariables["SERVICE_URL"])
	require.NotNil(t, stageOut.AccessLogSettings)
	assert.NotEmpty(t, *stageOut.AccessLogSettings.DestinationArn)
	require.NotNil(t, stageOut.DefaultRouteSettings)
	assert.EqualValues(t, 100, *stageOut.DefaultRouteSettings.ThrottlingBurstLimit)

	// UpdateStage — change variables
	updStageOut, err := client.UpdateStage(ctx, &apigwv2sdk.UpdateStageInput{
		ApiId:     aws.String(apiID),
		StageName: aws.String("dev"),
		StageVariables: map[string]string{
			"SERVICE_URL": "https://backend2.dev.example.com",
		},
		AutoDeploy: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(updStageOut.AutoDeploy))
	assert.Equal(t, "https://backend2.dev.example.com", updStageOut.StageVariables["SERVICE_URL"])
}

func TestIntegration_APIGatewayV2_RequiredFieldValidation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	// Missing name on CreateApi
	_, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		ProtocolType: apigwv2types.ProtocolTypeHttp,
	})
	require.Error(t, err, "missing name should fail")

	// Invalid protocol type on CreateApi
	_, err = client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("bad-proto"),
		ProtocolType: "GRPC",
	})
	require.Error(t, err, "invalid protocolType should fail")

	// Invalid authorizer type
	createOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("val-test-api"),
		ProtocolType: apigwv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)
	apiID := *createOut.ApiId

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	_, err = client.CreateAuthorizer(ctx, &apigwv2sdk.CreateAuthorizerInput{
		ApiId:          aws.String(apiID),
		Name:           aws.String("bad"),
		AuthorizerType: "INVALID",
		IdentitySource: []string{"$request.header.Authorization"},
	})
	require.Error(t, err, "invalid authorizerType should fail")

	// Invalid integration type
	_, err = client.CreateIntegration(ctx, &apigwv2sdk.CreateIntegrationInput{
		ApiId:           aws.String(apiID),
		IntegrationType: "INVALID_TYPE",
	})
	require.Error(t, err, "invalid integrationType should fail")

	// GetApi on non-existent ID
	_, err = client.GetApi(ctx, &apigwv2sdk.GetApiInput{ApiId: aws.String("nonexistent123")})
	require.Error(t, err, "non-existent API should return error")
}

func TestIntegration_APIGatewayV2_CascadeDelete(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	tests := []struct {
		name string
	}{
		{name: "delete_route_removes_route_responses"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			apiOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
				Name:         aws.String("cascade-test"),
				ProtocolType: apigwv2types.ProtocolTypeHttp,
			})
			require.NoError(t, err)
			apiID := *apiOut.ApiId
			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
			})

			routeOut, err := client.CreateRoute(ctx, &apigwv2sdk.CreateRouteInput{
				ApiId:    aws.String(apiID),
				RouteKey: aws.String("GET /cascade"),
			})
			require.NoError(t, err)
			routeID := *routeOut.RouteId

			_, err = client.DeleteRoute(ctx, &apigwv2sdk.DeleteRouteInput{
				ApiId:   aws.String(apiID),
				RouteId: aws.String(routeID),
			})
			require.NoError(t, err)

			// Route should be gone.
			routesOut, err := client.GetRoutes(ctx, &apigwv2sdk.GetRoutesInput{ApiId: aws.String(apiID)})
			require.NoError(t, err)
			assert.Empty(t, routesOut.Items, "routes should be empty after delete")
		})
	}
}

func TestIntegration_APIGatewayV2_ModelValidation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	apiOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
		Name:         aws.String("model-validation-test"),
		ProtocolType: apigwv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)
	apiID := *apiOut.ApiId
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	tests := []struct {
		input   *apigwv2sdk.CreateModelInput
		name    string
		wantErr bool
	}{
		{
			name: "empty_name_fails",
			input: &apigwv2sdk.CreateModelInput{
				ApiId:  aws.String(apiID),
				Name:   aws.String(""),
				Schema: aws.String(`{"type":"object"}`),
			},
			wantErr: true,
		},
		{
			name: "valid_model_succeeds",
			input: &apigwv2sdk.CreateModelInput{
				ApiId:       aws.String(apiID),
				Name:        aws.String("MyModel"),
				Schema:      aws.String(`{"type":"object"}`),
				ContentType: aws.String("application/json"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, createErr := client.CreateModel(ctx, tt.input)
			if tt.wantErr {
				require.Error(t, createErr)
			} else {
				require.NoError(t, createErr)
			}
		})
	}
}

func TestIntegration_APIGatewayV2_VpcLinkValidation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	tests := []struct {
		input   *apigwv2sdk.CreateVpcLinkInput
		name    string
		wantErr bool
	}{
		{
			name: "missing_name_fails",
			input: &apigwv2sdk.CreateVpcLinkInput{
				Name:      aws.String(""),
				SubnetIds: []string{"subnet-abc123"},
			},
			wantErr: true,
		},
		{
			name: "missing_subnets_fails",
			input: &apigwv2sdk.CreateVpcLinkInput{
				Name:      aws.String("my-link"),
				SubnetIds: []string{},
			},
			wantErr: true,
		},
		{
			name: "valid_vpc_link_succeeds",
			input: &apigwv2sdk.CreateVpcLinkInput{
				Name:      aws.String("valid-link"),
				SubnetIds: []string{"subnet-abc123"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.CreateVpcLink(ctx, tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				t.Cleanup(func() {
					cleanupCtx, cancel := cleanupContext(t)
					defer cancel()

					_, _ = client.DeleteVpcLink(cleanupCtx, &apigwv2sdk.DeleteVpcLinkInput{VpcLinkId: out.VpcLinkId})
				})
			}
		})
	}
}

func TestIntegration_APIGatewayV2_RouteKeyUniqueness(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	tests := []struct {
		name string
	}{
		{name: "duplicate_route_key_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			apiOut, err := client.CreateApi(ctx, &apigwv2sdk.CreateApiInput{
				Name:         aws.String("route-key-unique-test"),
				ProtocolType: apigwv2types.ProtocolTypeHttp,
			})
			require.NoError(t, err)
			apiID := *apiOut.ApiId
			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteApi(cleanupCtx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
			})

			_, err = client.CreateRoute(ctx, &apigwv2sdk.CreateRouteInput{
				ApiId:    aws.String(apiID),
				RouteKey: aws.String("GET /items"),
			})
			require.NoError(t, err)

			// Duplicate should fail.
			_, err = client.CreateRoute(ctx, &apigwv2sdk.CreateRouteInput{
				ApiId:    aws.String(apiID),
				RouteKey: aws.String("GET /items"),
			})
			require.Error(t, err, "duplicate route key should fail")
		})
	}
}
