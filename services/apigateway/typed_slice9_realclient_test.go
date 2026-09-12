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

// newSlice9APIGatewayClient is a top-level (not closure-local) client
// constructor: cmd/clientcoverage's census only traces a var's SDK-module
// binding through a named top-level function with a declared *pkg.Client
// return type, not a local func-literal variable, so keeping this as a
// top-level func is load-bearing for accurate typed-coverage measurement.
func newSlice9APIGatewayClient(t *testing.T) *apigwsdk.Client {
	t.Helper()

	return newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))
}

// TestTypedSlice9RealClient drives apigateway's typed-coverage-blind ops
// (gopherstack-n3zi slice 9) through the real aws-sdk-go-v2 apigateway
// client, one subtest per named priority family, asserting decoded values.
func TestTypedSlice9RealClient(t *testing.T) {
	t.Parallel()

	t.Run("rest api lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-api")},
		)
		require.NoError(t, err)

		openAPIBody := []byte(`{
			"swagger": "2.0",
			"info": {"title": "s9-imported", "version": "1.0"},
			"paths": {}
		}`)

		putOut, err := client.PutRestApi(t.Context(), &apigwsdk.PutRestApiInput{
			RestApiId: api.Id,
			Mode:      apigwtypes.PutModeOverwrite,
			Body:      openAPIBody,
		})
		require.NoError(t, err)
		assert.Equal(t, "s9-imported", aws.ToString(putOut.Name))

		_, err = client.DeleteRestApi(t.Context(), &apigwsdk.DeleteRestApiInput{RestApiId: api.Id})
		require.NoError(t, err)

		_, err = client.GetRestApi(t.Context(), &apigwsdk.GetRestApiInput{RestApiId: api.Id})
		require.Error(t, err)
	})

	t.Run("resources and methods", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-res-api")},
		)
		require.NoError(t, err)

		resources, err := client.GetResources(
			t.Context(),
			&apigwsdk.GetResourcesInput{RestApiId: api.Id},
		)
		require.NoError(t, err)
		rootID := resources.Items[0].Id

		resource, err := client.CreateResource(t.Context(), &apigwsdk.CreateResourceInput{
			RestApiId: api.Id,
			ParentId:  rootID,
			PathPart:  aws.String("widgets"),
		})
		require.NoError(t, err)

		_, err = client.PutMethod(t.Context(), &apigwsdk.PutMethodInput{
			RestApiId:         api.Id,
			ResourceId:        resource.Id,
			HttpMethod:        aws.String("GET"),
			AuthorizationType: aws.String("NONE"),
		})
		require.NoError(t, err)

		getMethodOut, err := client.GetMethod(t.Context(), &apigwsdk.GetMethodInput{
			RestApiId:  api.Id,
			ResourceId: resource.Id,
			HttpMethod: aws.String("GET"),
		})
		require.NoError(t, err)
		assert.Equal(t, "NONE", aws.ToString(getMethodOut.AuthorizationType))

		_, err = client.PutMethodResponse(t.Context(), &apigwsdk.PutMethodResponseInput{
			RestApiId:  api.Id,
			ResourceId: resource.Id,
			HttpMethod: aws.String("GET"),
			StatusCode: aws.String("200"),
		})
		require.NoError(t, err)

		getMethodRespOut, err := client.GetMethodResponse(
			t.Context(),
			&apigwsdk.GetMethodResponseInput{
				RestApiId:  api.Id,
				ResourceId: resource.Id,
				HttpMethod: aws.String("GET"),
				StatusCode: aws.String("200"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "200", aws.ToString(getMethodRespOut.StatusCode))

		_, err = client.DeleteMethodResponse(t.Context(), &apigwsdk.DeleteMethodResponseInput{
			RestApiId:  api.Id,
			ResourceId: resource.Id,
			HttpMethod: aws.String("GET"),
			StatusCode: aws.String("200"),
		})
		require.NoError(t, err)

		_, err = client.DeleteMethod(t.Context(), &apigwsdk.DeleteMethodInput{
			RestApiId:  api.Id,
			ResourceId: resource.Id,
			HttpMethod: aws.String("GET"),
		})
		require.NoError(t, err)

		_, err = client.DeleteResource(t.Context(), &apigwsdk.DeleteResourceInput{
			RestApiId:  api.Id,
			ResourceId: resource.Id,
		})
		require.NoError(t, err)

		resourcesAfter, err := client.GetResources(
			t.Context(),
			&apigwsdk.GetResourcesInput{RestApiId: api.Id},
		)
		require.NoError(t, err)
		assert.Len(t, resourcesAfter.Items, 1)
	})

	t.Run("integrations", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-int-api")},
		)
		require.NoError(t, err)

		resources, err := client.GetResources(
			t.Context(),
			&apigwsdk.GetResourcesInput{RestApiId: api.Id},
		)
		require.NoError(t, err)
		rootID := resources.Items[0].Id

		_, err = client.PutMethod(t.Context(), &apigwsdk.PutMethodInput{
			RestApiId:         api.Id,
			ResourceId:        rootID,
			HttpMethod:        aws.String("GET"),
			AuthorizationType: aws.String("NONE"),
		})
		require.NoError(t, err)

		_, err = client.PutIntegration(t.Context(), &apigwsdk.PutIntegrationInput{
			RestApiId:  api.Id,
			ResourceId: rootID,
			HttpMethod: aws.String("GET"),
			Type:       apigwtypes.IntegrationTypeMock,
		})
		require.NoError(t, err)

		getIntOut, err := client.GetIntegration(t.Context(), &apigwsdk.GetIntegrationInput{
			RestApiId:  api.Id,
			ResourceId: rootID,
			HttpMethod: aws.String("GET"),
		})
		require.NoError(t, err)
		assert.Equal(t, apigwtypes.IntegrationTypeMock, getIntOut.Type)

		_, err = client.PutMethodResponse(t.Context(), &apigwsdk.PutMethodResponseInput{
			RestApiId:  api.Id,
			ResourceId: rootID,
			HttpMethod: aws.String("GET"),
			StatusCode: aws.String("200"),
		})
		require.NoError(t, err)

		_, err = client.PutIntegrationResponse(t.Context(), &apigwsdk.PutIntegrationResponseInput{
			RestApiId:  api.Id,
			ResourceId: rootID,
			HttpMethod: aws.String("GET"),
			StatusCode: aws.String("200"),
		})
		require.NoError(t, err)

		getIntRespOut, err := client.GetIntegrationResponse(
			t.Context(),
			&apigwsdk.GetIntegrationResponseInput{
				RestApiId:  api.Id,
				ResourceId: rootID,
				HttpMethod: aws.String("GET"),
				StatusCode: aws.String("200"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "200", aws.ToString(getIntRespOut.StatusCode))

		_, err = client.DeleteIntegrationResponse(
			t.Context(),
			&apigwsdk.DeleteIntegrationResponseInput{
				RestApiId:  api.Id,
				ResourceId: rootID,
				HttpMethod: aws.String("GET"),
				StatusCode: aws.String("200"),
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteIntegration(t.Context(), &apigwsdk.DeleteIntegrationInput{
			RestApiId:  api.Id,
			ResourceId: rootID,
			HttpMethod: aws.String("GET"),
		})
		require.NoError(t, err)

		_, err = client.GetIntegration(t.Context(), &apigwsdk.GetIntegrationInput{
			RestApiId:  api.Id,
			ResourceId: rootID,
			HttpMethod: aws.String("GET"),
		})
		require.Error(t, err)
	})

	t.Run("authorizers", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-auth-api")},
		)
		require.NoError(t, err)

		createOut, err := client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
			RestApiId:      api.Id,
			Name:           aws.String("s9-authorizer"),
			Type:           apigwtypes.AuthorizerTypeToken,
			IdentitySource: aws.String("method.request.header.Auth"),
			AuthorizerUri: aws.String(
				"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" +
					"arn:aws:lambda:us-east-1:000000000000:function:s9-authfn/invocations",
			),
		})
		require.NoError(t, err)
		authorizerID := createOut.Id

		getOut, err := client.GetAuthorizer(t.Context(), &apigwsdk.GetAuthorizerInput{
			RestApiId:    api.Id,
			AuthorizerId: authorizerID,
		})
		require.NoError(t, err)
		assert.Equal(t, "s9-authorizer", aws.ToString(getOut.Name))

		_, err = client.DeleteAuthorizer(t.Context(), &apigwsdk.DeleteAuthorizerInput{
			RestApiId:    api.Id,
			AuthorizerId: authorizerID,
		})
		require.NoError(t, err)

		_, err = client.GetAuthorizer(t.Context(), &apigwsdk.GetAuthorizerInput{
			RestApiId:    api.Id,
			AuthorizerId: authorizerID,
		})
		require.Error(t, err)
	})

	t.Run("deployments and stages", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-dep-api")},
		)
		require.NoError(t, err)

		deployOut, err := client.CreateDeployment(t.Context(), &apigwsdk.CreateDeploymentInput{
			RestApiId:        api.Id,
			StageName:        aws.String("s9-stage"),
			StageDescription: aws.String("s9 stage"),
		})
		require.NoError(t, err)
		deploymentID := deployOut.Id

		getDeployOut, err := client.GetDeployment(t.Context(), &apigwsdk.GetDeploymentInput{
			RestApiId:    api.Id,
			DeploymentId: deploymentID,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(deploymentID), aws.ToString(getDeployOut.Id))

		listDeployOut, err := client.GetDeployments(
			t.Context(),
			&apigwsdk.GetDeploymentsInput{RestApiId: api.Id},
		)
		require.NoError(t, err)
		require.Len(t, listDeployOut.Items, 1)

		updDeployOut, err := client.UpdateDeployment(t.Context(), &apigwsdk.UpdateDeploymentInput{
			RestApiId:    api.Id,
			DeploymentId: deploymentID,
			PatchOperations: []apigwtypes.PatchOperation{
				{
					Op:    apigwtypes.OpReplace,
					Path:  aws.String("/description"),
					Value: aws.String("s9 updated"),
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "s9 updated", aws.ToString(updDeployOut.Description))

		_, err = client.FlushStageAuthorizersCache(
			t.Context(),
			&apigwsdk.FlushStageAuthorizersCacheInput{
				RestApiId: api.Id,
				StageName: aws.String("s9-stage"),
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteStage(t.Context(), &apigwsdk.DeleteStageInput{
			RestApiId: api.Id,
			StageName: aws.String("s9-stage"),
		})
		require.NoError(t, err)

		_, err = client.DeleteDeployment(t.Context(), &apigwsdk.DeleteDeploymentInput{
			RestApiId:    api.Id,
			DeploymentId: deploymentID,
		})
		require.NoError(t, err)

		listAfter, err := client.GetDeployments(
			t.Context(),
			&apigwsdk.GetDeploymentsInput{RestApiId: api.Id},
		)
		require.NoError(t, err)
		assert.Empty(t, listAfter.Items)
	})

	t.Run("models", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-model-api")},
		)
		require.NoError(t, err)

		createOut, err := client.CreateModel(t.Context(), &apigwsdk.CreateModelInput{
			RestApiId:   api.Id,
			Name:        aws.String("S9Model"),
			ContentType: aws.String("application/json"),
			Schema: aws.String(
				`{"$schema":"http://json-schema.org/draft-04/schema#","title":"S9Model","type":"object"}`,
			),
		})
		require.NoError(t, err)

		getOut, err := client.GetModel(t.Context(), &apigwsdk.GetModelInput{
			RestApiId: api.Id,
			ModelName: aws.String("S9Model"),
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.Id), aws.ToString(getOut.Id))

		listOut, err := client.GetModels(t.Context(), &apigwsdk.GetModelsInput{RestApiId: api.Id})
		require.NoError(t, err)
		require.Len(t, listOut.Items, 1)

		templateOut, err := client.GetModelTemplate(t.Context(), &apigwsdk.GetModelTemplateInput{
			RestApiId: api.Id,
			ModelName: aws.String("S9Model"),
		})
		require.NoError(t, err)
		assert.NotNil(t, templateOut.Value)

		updOut, err := client.UpdateModel(t.Context(), &apigwsdk.UpdateModelInput{
			RestApiId: api.Id,
			ModelName: aws.String("S9Model"),
			PatchOperations: []apigwtypes.PatchOperation{
				{
					Op:    apigwtypes.OpReplace,
					Path:  aws.String("/description"),
					Value: aws.String("s9 model desc"),
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "s9 model desc", aws.ToString(updOut.Description))

		_, err = client.DeleteModel(t.Context(), &apigwsdk.DeleteModelInput{
			RestApiId: api.Id,
			ModelName: aws.String("S9Model"),
		})
		require.NoError(t, err)
	})

	t.Run("request validators", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-rv-api")},
		)
		require.NoError(t, err)

		createOut, err := client.CreateRequestValidator(
			t.Context(),
			&apigwsdk.CreateRequestValidatorInput{
				RestApiId:           api.Id,
				Name:                aws.String("s9-validator"),
				ValidateRequestBody: true,
			},
		)
		require.NoError(t, err)
		validatorID := createOut.Id

		getOut, err := client.GetRequestValidator(t.Context(), &apigwsdk.GetRequestValidatorInput{
			RestApiId:          api.Id,
			RequestValidatorId: validatorID,
		})
		require.NoError(t, err)
		assert.True(t, getOut.ValidateRequestBody)

		listOut, err := client.GetRequestValidators(
			t.Context(),
			&apigwsdk.GetRequestValidatorsInput{RestApiId: api.Id},
		)
		require.NoError(t, err)
		require.Len(t, listOut.Items, 1)

		updOut, err := client.UpdateRequestValidator(
			t.Context(),
			&apigwsdk.UpdateRequestValidatorInput{
				RestApiId:          api.Id,
				RequestValidatorId: validatorID,
				PatchOperations: []apigwtypes.PatchOperation{
					{
						Op:    apigwtypes.OpReplace,
						Path:  aws.String("/validateRequestParameters"),
						Value: aws.String("true"),
					},
				},
			},
		)
		require.NoError(t, err)
		assert.True(t, updOut.ValidateRequestParameters)

		_, err = client.DeleteRequestValidator(t.Context(), &apigwsdk.DeleteRequestValidatorInput{
			RestApiId:          api.Id,
			RequestValidatorId: validatorID,
		})
		require.NoError(t, err)
	})

	t.Run("gateway responses", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-gwr-api")},
		)
		require.NoError(t, err)

		putOut, err := client.PutGatewayResponse(t.Context(), &apigwsdk.PutGatewayResponseInput{
			RestApiId:    api.Id,
			ResponseType: apigwtypes.GatewayResponseTypeDefault4xx,
			StatusCode:   aws.String("404"),
			ResponseTemplates: map[string]string{
				"application/json": `{"message":"s9 not found"}`,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "404", aws.ToString(putOut.StatusCode))

		getOut, err := client.GetGatewayResponse(t.Context(), &apigwsdk.GetGatewayResponseInput{
			RestApiId:    api.Id,
			ResponseType: apigwtypes.GatewayResponseTypeDefault4xx,
		})
		require.NoError(t, err)
		assert.Equal(t, "404", aws.ToString(getOut.StatusCode))

		listOut, err := client.GetGatewayResponses(
			t.Context(),
			&apigwsdk.GetGatewayResponsesInput{RestApiId: api.Id},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, listOut.Items)

		updOut, err := client.UpdateGatewayResponse(
			t.Context(),
			&apigwsdk.UpdateGatewayResponseInput{
				RestApiId:    api.Id,
				ResponseType: apigwtypes.GatewayResponseTypeDefault4xx,
				PatchOperations: []apigwtypes.PatchOperation{
					{
						Op:    apigwtypes.OpReplace,
						Path:  aws.String("/statusCode"),
						Value: aws.String("410"),
					},
				},
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "410", aws.ToString(updOut.StatusCode))

		_, err = client.DeleteGatewayResponse(t.Context(), &apigwsdk.DeleteGatewayResponseInput{
			RestApiId:    api.Id,
			ResponseType: apigwtypes.GatewayResponseTypeDefault4xx,
		})
		require.NoError(t, err)
	})

	t.Run("usage plan keys", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		planOut, err := client.CreateUsagePlan(t.Context(), &apigwsdk.CreateUsagePlanInput{
			Name: aws.String("s9-usage-plan"),
		})
		require.NoError(t, err)

		keyOut, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{
			Name:    aws.String("s9-api-key"),
			Enabled: true,
		})
		require.NoError(t, err)

		_, err = client.CreateUsagePlanKey(t.Context(), &apigwsdk.CreateUsagePlanKeyInput{
			UsagePlanId: planOut.Id,
			KeyId:       keyOut.Id,
			KeyType:     aws.String("API_KEY"),
		})
		require.NoError(t, err)

		getOut, err := client.GetUsagePlanKey(t.Context(), &apigwsdk.GetUsagePlanKeyInput{
			UsagePlanId: planOut.Id,
			KeyId:       keyOut.Id,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(keyOut.Id), aws.ToString(getOut.Id))

		_, err = client.DeleteUsagePlanKey(t.Context(), &apigwsdk.DeleteUsagePlanKeyInput{
			UsagePlanId: planOut.Id,
			KeyId:       keyOut.Id,
		})
		require.NoError(t, err)
	})

	t.Run("api keys", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		createOut, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{
			Name:    aws.String("s9-getapikey"),
			Enabled: true,
		})
		require.NoError(t, err)

		getOut, err := client.GetApiKey(t.Context(), &apigwsdk.GetApiKeyInput{ApiKey: createOut.Id})
		require.NoError(t, err)
		assert.Equal(t, "s9-getapikey", aws.ToString(getOut.Name))

		updOut, err := client.UpdateApiKey(t.Context(), &apigwsdk.UpdateApiKeyInput{
			ApiKey: createOut.Id,
			PatchOperations: []apigwtypes.PatchOperation{
				{
					Op:    apigwtypes.OpReplace,
					Path:  aws.String("/enabled"),
					Value: aws.String("false"),
				},
			},
		})
		require.NoError(t, err)
		assert.False(t, updOut.Enabled)
	})

	t.Run("domain names and base path mappings", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-bpm-api")},
		)
		require.NoError(t, err)

		_, err = client.CreateDomainName(t.Context(), &apigwsdk.CreateDomainNameInput{
			DomainName: aws.String("s9.example.com"),
		})
		require.NoError(t, err)

		getDomainOut, err := client.GetDomainName(t.Context(), &apigwsdk.GetDomainNameInput{
			DomainName: aws.String("s9.example.com"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s9.example.com", aws.ToString(getDomainOut.DomainName))

		_, err = client.CreateBasePathMapping(t.Context(), &apigwsdk.CreateBasePathMappingInput{
			DomainName: aws.String("s9.example.com"),
			RestApiId:  api.Id,
			BasePath:   aws.String("v1"),
		})
		require.NoError(t, err)

		getBPMOut, err := client.GetBasePathMapping(t.Context(), &apigwsdk.GetBasePathMappingInput{
			DomainName: aws.String("s9.example.com"),
			BasePath:   aws.String("v1"),
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(api.Id), aws.ToString(getBPMOut.RestApiId))

		listBPMOut, err := client.GetBasePathMappings(
			t.Context(),
			&apigwsdk.GetBasePathMappingsInput{
				DomainName: aws.String("s9.example.com"),
			},
		)
		require.NoError(t, err)
		require.Len(t, listBPMOut.Items, 1)

		_, err = client.DeleteBasePathMapping(t.Context(), &apigwsdk.DeleteBasePathMappingInput{
			DomainName: aws.String("s9.example.com"),
			BasePath:   aws.String("v1"),
		})
		require.NoError(t, err)

		listAfter, err := client.GetBasePathMappings(
			t.Context(),
			&apigwsdk.GetBasePathMappingsInput{
				DomainName: aws.String("s9.example.com"),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, listAfter.Items)
	})

	t.Run("domain name access associations", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		createOut, err := client.CreateDomainNameAccessAssociation(
			t.Context(),
			&apigwsdk.CreateDomainNameAccessAssociationInput{
				DomainNameArn: aws.String(
					"arn:aws:apigateway:us-east-1::/domainnames/s9.example.com",
				),
				AccessAssociationSource:     aws.String("vpce-s9"),
				AccessAssociationSourceType: apigwtypes.AccessAssociationSourceTypeVpce,
			},
		)
		require.NoError(t, err)
		assocARN := createOut.DomainNameAccessAssociationArn

		listOut, err := client.GetDomainNameAccessAssociations(
			t.Context(),
			&apigwsdk.GetDomainNameAccessAssociationsInput{
				ResourceOwner: apigwtypes.ResourceOwnerSelf,
			},
		)
		require.NoError(t, err)
		require.Len(t, listOut.Items, 1)

		_, err = client.RejectDomainNameAccessAssociation(
			t.Context(),
			&apigwsdk.RejectDomainNameAccessAssociationInput{
				DomainNameAccessAssociationArn: assocARN,
				DomainNameArn: aws.String(
					"arn:aws:apigateway:us-east-1::/domainnames/s9.example.com",
				),
			},
		)
		require.NoError(t, err)

		createOut2, err := client.CreateDomainNameAccessAssociation(
			t.Context(),
			&apigwsdk.CreateDomainNameAccessAssociationInput{
				DomainNameArn: aws.String(
					"arn:aws:apigateway:us-east-1::/domainnames/s9-2.example.com",
				),
				AccessAssociationSource:     aws.String("vpce-s9-2"),
				AccessAssociationSourceType: apigwtypes.AccessAssociationSourceTypeVpce,
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteDomainNameAccessAssociation(
			t.Context(),
			&apigwsdk.DeleteDomainNameAccessAssociationInput{
				DomainNameAccessAssociationArn: createOut2.DomainNameAccessAssociationArn,
			},
		)
		require.NoError(t, err)
	})

	t.Run("vpc links", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		createOut, err := client.CreateVpcLink(t.Context(), &apigwsdk.CreateVpcLinkInput{
			Name: aws.String("s9-vpc-link"),
			TargetArns: []string{
				"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/s9/abc",
			},
		})
		require.NoError(t, err)
		vpcLinkID := createOut.Id

		getOut, err := client.GetVpcLink(
			t.Context(),
			&apigwsdk.GetVpcLinkInput{VpcLinkId: vpcLinkID},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9-vpc-link", aws.ToString(getOut.Name))

		listOut, err := client.GetVpcLinks(t.Context(), &apigwsdk.GetVpcLinksInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, listOut.Items)

		updOut, err := client.UpdateVpcLink(t.Context(), &apigwsdk.UpdateVpcLinkInput{
			VpcLinkId: vpcLinkID,
			PatchOperations: []apigwtypes.PatchOperation{
				{
					Op:    apigwtypes.OpReplace,
					Path:  aws.String("/name"),
					Value: aws.String("s9-vpc-link-renamed"),
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "s9-vpc-link-renamed", aws.ToString(updOut.Name))

		_, err = client.DeleteVpcLink(
			t.Context(),
			&apigwsdk.DeleteVpcLinkInput{VpcLinkId: vpcLinkID},
		)
		require.NoError(t, err)
	})

	t.Run("documentation", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-doc-api")},
		)
		require.NoError(t, err)

		importOut, err := client.ImportDocumentationParts(
			t.Context(),
			&apigwsdk.ImportDocumentationPartsInput{
				RestApiId: api.Id,
				Mode:      apigwtypes.PutModeMerge,
				Body: []byte(`{
				"documentationParts": [
					{"location": {"type": "API"}, "properties": {"description": "s9 api doc"}}
				]
			}`),
			},
		)
		require.NoError(t, err)
		require.Len(t, importOut.Ids, 1)
		docPartID := aws.String(importOut.Ids[0])

		updPartOut, err := client.UpdateDocumentationPart(
			t.Context(),
			&apigwsdk.UpdateDocumentationPartInput{
				RestApiId:           api.Id,
				DocumentationPartId: docPartID,
				PatchOperations: []apigwtypes.PatchOperation{
					{
						Op:    apigwtypes.OpReplace,
						Path:  aws.String("/properties"),
						Value: aws.String(`{"description":"s9 updated"}`),
					},
				},
			},
		)
		require.NoError(t, err)
		assert.Contains(t, aws.ToString(updPartOut.Properties), "s9 updated")

		createVerOut, err := client.CreateDocumentationVersion(
			t.Context(),
			&apigwsdk.CreateDocumentationVersionInput{
				RestApiId:            api.Id,
				DocumentationVersion: aws.String("s9-v1"),
				Description:          aws.String("s9 doc version"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9-v1", aws.ToString(createVerOut.Version))

		getVerOut, err := client.GetDocumentationVersion(
			t.Context(),
			&apigwsdk.GetDocumentationVersionInput{
				RestApiId:            api.Id,
				DocumentationVersion: aws.String("s9-v1"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9 doc version", aws.ToString(getVerOut.Description))

		listVerOut, err := client.GetDocumentationVersions(
			t.Context(),
			&apigwsdk.GetDocumentationVersionsInput{
				RestApiId: api.Id,
			},
		)
		require.NoError(t, err)
		require.Len(t, listVerOut.Items, 1)

		updVerOut, err := client.UpdateDocumentationVersion(
			t.Context(),
			&apigwsdk.UpdateDocumentationVersionInput{
				RestApiId:            api.Id,
				DocumentationVersion: aws.String("s9-v1"),
				PatchOperations: []apigwtypes.PatchOperation{
					{
						Op:    apigwtypes.OpReplace,
						Path:  aws.String("/description"),
						Value: aws.String("s9 doc version updated"),
					},
				},
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9 doc version updated", aws.ToString(updVerOut.Description))

		_, err = client.DeleteDocumentationVersion(
			t.Context(),
			&apigwsdk.DeleteDocumentationVersionInput{
				RestApiId:            api.Id,
				DocumentationVersion: aws.String("s9-v1"),
			},
		)
		require.NoError(t, err)
	})

	t.Run("client certificates", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		createOut, err := client.GenerateClientCertificate(
			t.Context(),
			&apigwsdk.GenerateClientCertificateInput{
				Description: aws.String("s9 cert"),
			},
		)
		require.NoError(t, err)
		certID := createOut.ClientCertificateId

		getOut, err := client.GetClientCertificate(t.Context(), &apigwsdk.GetClientCertificateInput{
			ClientCertificateId: certID,
		})
		require.NoError(t, err)
		assert.Equal(t, "s9 cert", aws.ToString(getOut.Description))

		updOut, err := client.UpdateClientCertificate(
			t.Context(),
			&apigwsdk.UpdateClientCertificateInput{
				ClientCertificateId: certID,
				PatchOperations: []apigwtypes.PatchOperation{
					{
						Op:    apigwtypes.OpReplace,
						Path:  aws.String("/description"),
						Value: aws.String("s9 cert updated"),
					},
				},
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9 cert updated", aws.ToString(updOut.Description))

		_, err = client.DeleteClientCertificate(t.Context(), &apigwsdk.DeleteClientCertificateInput{
			ClientCertificateId: certID,
		})
		require.NoError(t, err)
	})

	t.Run("sdk types", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		listOut, err := client.GetSdkTypes(t.Context(), &apigwsdk.GetSdkTypesInput{})
		require.NoError(t, err)
		require.NotEmpty(t, listOut.Items)

		getOut, err := client.GetSdkType(
			t.Context(),
			&apigwsdk.GetSdkTypeInput{Id: aws.String("java")},
		)
		require.NoError(t, err)
		assert.Equal(t, "java", aws.ToString(getOut.Id))
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayClient(t)

		api, err := client.CreateRestApi(
			t.Context(),
			&apigwsdk.CreateRestApiInput{Name: aws.String("s9-tag-api")},
		)
		require.NoError(t, err)

		resourceARN := "arn:aws:apigateway:us-east-1::/restapis/" + aws.ToString(api.Id)

		_, err = client.TagResource(t.Context(), &apigwsdk.TagResourceInput{
			ResourceArn: aws.String(resourceARN),
			Tags:        map[string]string{"team": "s9"},
		})
		require.NoError(t, err)

		getOut, err := client.GetTags(
			t.Context(),
			&apigwsdk.GetTagsInput{ResourceArn: aws.String(resourceARN)},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9", getOut.Tags["team"])

		_, err = client.UntagResource(t.Context(), &apigwsdk.UntagResourceInput{
			ResourceArn: aws.String(resourceARN),
			TagKeys:     []string{"team"},
		})
		require.NoError(t, err)

		getAfter, err := client.GetTags(
			t.Context(),
			&apigwsdk.GetTagsInput{ResourceArn: aws.String(resourceARN)},
		)
		require.NoError(t, err)
		assert.Empty(t, getAfter.Tags)
	})
}
