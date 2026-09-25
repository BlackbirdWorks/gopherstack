package terraform_test

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsvc "github.com/aws/aws-sdk-go-v2/service/apigateway"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeZipFixture writes a single-entry zip file at path containing a file
// named entryName with the given content.
func writeZipFixture(t *testing.T, path, entryName, content string) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err, "creating zip file %s", path)
	defer f.Close()

	zw := zip.NewWriter(f)
	entry, err := zw.Create(entryName)
	require.NoError(t, err, "creating zip entry %s", entryName)
	_, err = entry.Write([]byte(content))
	require.NoError(t, err, "writing zip entry %s", entryName)
	require.NoError(t, zw.Close(), "closing zip writer for %s", path)
}

// TestTerraform_LambdaAndApigateway provisions Lambda (alias, code signing config,
// function event invoke config, function recursion config, function URL,
// layer version + permission, runtime management config) and API Gateway
// (account, api key, authorizer, base path mapping, client certificate,
// documentation part/version, domain name, gateway response, integration
// response, method response, method settings, model, rest api policy,
// stage, usage plan + key, VPC link) resources via Terraform and verifies
// each through its own SDK client's Get/List path.
func TestTerraform_LambdaAndApigateway(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "lambda-and-apigateway",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()

				functionZip := filepath.Join(dir, "lagw-function.zip")
				writeZipFixture(t, functionZip, "index.py",
					"def handler(event, context):\n    return {'statusCode': 200}\n")

				layerZip := filepath.Join(dir, "lagw-layer.zip")
				writeZipFixture(t, layerZip, "python/lib/python3.12/site-packages/lagw.py",
					"# lagw layer module\n")

				return map[string]any{
					"FunctionZip": functionZip,
					"LayerZip":    layerZip,
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()

				lambdaClient := createLambdaClient(t)
				verifyLambdaAndApigatewayLambda(ctx, t, lambdaClient)

				apiClient := createAPIGatewayClient(t)
				verifyLambdaAndApigatewayAPIGateway(ctx, t, apiClient)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyLambdaAndApigatewayLambda(ctx context.Context, t *testing.T, lambdaClient *lambdasvc.Client) {
	t.Helper()

	const functionName = "lagw-function"

	fnOut, err := lambdaClient.GetFunction(ctx, &lambdasvc.GetFunctionInput{
		FunctionName: aws.String(functionName),
	})
	require.NoError(t, err, "GetFunction should succeed")
	require.NotNil(t, fnOut.Configuration)
	assert.Equal(t, "python3.12", string(fnOut.Configuration.Runtime))
	assert.NotEmpty(t, aws.ToString(fnOut.Configuration.Version))

	aliasOut, err := lambdaClient.GetAlias(ctx, &lambdasvc.GetAliasInput{
		FunctionName: aws.String(functionName),
		Name:         aws.String("live"),
	})
	require.NoError(t, err, "GetAlias should succeed")
	// Unqualified GetFunction always reports Version "$LATEST" (real AWS
	// behavior); the alias points at the actual published version instead.
	assert.Equal(t, "1", aws.ToString(aliasOut.FunctionVersion))

	cscOut, err := lambdaClient.ListCodeSigningConfigs(ctx, &lambdasvc.ListCodeSigningConfigsInput{})
	require.NoError(t, err, "ListCodeSigningConfigs should succeed")
	foundCSC := false
	for _, csc := range cscOut.CodeSigningConfigs {
		if aws.ToString(csc.Description) == "lagw code signing config" {
			foundCSC = true
			require.NotNil(t, csc.AllowedPublishers)
			assert.Len(t, csc.AllowedPublishers.SigningProfileVersionArns, 1)
		}
	}
	assert.True(t, foundCSC, "code signing config should be listed")

	invokeOut, err := lambdaClient.GetFunctionEventInvokeConfig(ctx, &lambdasvc.GetFunctionEventInvokeConfigInput{
		FunctionName: aws.String(functionName),
	})
	require.NoError(t, err, "GetFunctionEventInvokeConfig should succeed")
	assert.Equal(t, int32(1), aws.ToInt32(invokeOut.MaximumRetryAttempts))

	recursionOut, err := lambdaClient.GetFunctionRecursionConfig(ctx, &lambdasvc.GetFunctionRecursionConfigInput{
		FunctionName: aws.String(functionName),
	})
	require.NoError(t, err, "GetFunctionRecursionConfig should succeed")
	assert.Equal(t, "Allow", string(recursionOut.RecursiveLoop))

	urlOut, err := lambdaClient.GetFunctionUrlConfig(ctx, &lambdasvc.GetFunctionUrlConfigInput{
		FunctionName: aws.String(functionName),
	})
	require.NoError(t, err, "GetFunctionUrlConfig should succeed")
	assert.Equal(t, "NONE", string(urlOut.AuthType))
	assert.NotEmpty(t, aws.ToString(urlOut.FunctionUrl))

	rmcOut, err := lambdaClient.GetRuntimeManagementConfig(ctx, &lambdasvc.GetRuntimeManagementConfigInput{
		FunctionName: aws.String(functionName),
	})
	require.NoError(t, err, "GetRuntimeManagementConfig should succeed")
	assert.Equal(t, "Auto", string(rmcOut.UpdateRuntimeOn))

	layersOut, err := lambdaClient.ListLayerVersions(ctx, &lambdasvc.ListLayerVersionsInput{
		LayerName: aws.String("lagw-layer"),
	})
	require.NoError(t, err, "ListLayerVersions should succeed")
	require.Len(t, layersOut.LayerVersions, 1)
	versionNumber := layersOut.LayerVersions[0].Version

	policyOut, err := lambdaClient.GetLayerVersionPolicy(ctx, &lambdasvc.GetLayerVersionPolicyInput{
		LayerName:     aws.String("lagw-layer"),
		VersionNumber: aws.Int64(versionNumber),
	})
	require.NoError(t, err, "GetLayerVersionPolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "lagw-layer-perm")
}

func verifyLambdaAndApigatewayAPIGateway(ctx context.Context, t *testing.T, apiClient *apigwsvc.Client) {
	t.Helper()

	acctOut, err := apiClient.GetAccount(ctx, &apigwsvc.GetAccountInput{})
	require.NoError(t, err, "GetAccount should succeed")
	assert.Contains(t, aws.ToString(acctOut.CloudwatchRoleArn), "lagw-apigw-cw-role")

	apisOut, err := apiClient.GetRestApis(ctx, &apigwsvc.GetRestApisInput{})
	require.NoError(t, err, "GetRestApis should succeed")
	restAPIID := ""
	for _, api := range apisOut.Items {
		if aws.ToString(api.Name) == "lagw-api" {
			restAPIID = aws.ToString(api.Id)
		}
	}
	require.NotEmpty(t, restAPIID, "lagw-api should be listed")

	apiOut, err := apiClient.GetRestApi(ctx, &apigwsvc.GetRestApiInput{RestApiId: aws.String(restAPIID)})
	require.NoError(t, err, "GetRestApi should succeed")
	assert.Contains(t, aws.ToString(apiOut.Policy), "execute-api:Invoke")

	stageOut, err := apiClient.GetStage(ctx, &apigwsvc.GetStageInput{
		RestApiId: aws.String(restAPIID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err, "GetStage should succeed")
	require.Contains(t, stageOut.MethodSettings, "*/*")
	assert.True(t, stageOut.MethodSettings["*/*"].MetricsEnabled)

	gwrOut, err := apiClient.GetGatewayResponse(ctx, &apigwsvc.GetGatewayResponseInput{
		RestApiId:    aws.String(restAPIID),
		ResponseType: "DEFAULT_4XX",
	})
	require.NoError(t, err, "GetGatewayResponse should succeed")
	assert.Equal(t, "404", aws.ToString(gwrOut.StatusCode))

	docPartsOut, err := apiClient.GetDocumentationParts(ctx, &apigwsvc.GetDocumentationPartsInput{
		RestApiId: aws.String(restAPIID),
	})
	require.NoError(t, err, "GetDocumentationParts should succeed")
	assert.NotEmpty(t, docPartsOut.Items)

	_, err = apiClient.GetDocumentationVersion(ctx, &apigwsvc.GetDocumentationVersionInput{
		RestApiId:            aws.String(restAPIID),
		DocumentationVersion: aws.String("v1"),
	})
	require.NoError(t, err, "GetDocumentationVersion should succeed")

	modelOut, err := apiClient.GetModel(ctx, &apigwsvc.GetModelInput{
		RestApiId: aws.String(restAPIID),
		ModelName: aws.String("ItemModel"),
	})
	require.NoError(t, err, "GetModel should succeed")
	assert.Equal(t, "application/json", aws.ToString(modelOut.ContentType))

	keysOut, err := apiClient.GetApiKeys(ctx, &apigwsvc.GetApiKeysInput{})
	require.NoError(t, err, "GetApiKeys should succeed")
	apiKeyID := ""
	for _, k := range keysOut.Items {
		if aws.ToString(k.Name) == "lagw-key" {
			apiKeyID = aws.ToString(k.Id)
		}
	}
	require.NotEmpty(t, apiKeyID, "lagw-key should be listed")

	plansOut, err := apiClient.GetUsagePlans(ctx, &apigwsvc.GetUsagePlansInput{})
	require.NoError(t, err, "GetUsagePlans should succeed")
	usagePlanID := ""
	for _, p := range plansOut.Items {
		if aws.ToString(p.Name) == "lagw-usage-plan" {
			usagePlanID = aws.ToString(p.Id)
			assert.Len(t, p.ApiStages, 1)
		}
	}
	require.NotEmpty(t, usagePlanID, "lagw-usage-plan should be listed")

	_, err = apiClient.GetUsagePlanKey(ctx, &apigwsvc.GetUsagePlanKeyInput{
		UsagePlanId: aws.String(usagePlanID),
		KeyId:       aws.String(apiKeyID),
	})
	require.NoError(t, err, "GetUsagePlanKey should succeed")

	authsOut, err := apiClient.GetAuthorizers(ctx, &apigwsvc.GetAuthorizersInput{RestApiId: aws.String(restAPIID)})
	require.NoError(t, err, "GetAuthorizers should succeed")
	foundAuth := false
	for _, a := range authsOut.Items {
		if aws.ToString(a.Name) == "lagw-authorizer" {
			foundAuth = true
			assert.Equal(t, "TOKEN", string(a.Type))
		}
	}
	assert.True(t, foundAuth, "lagw-authorizer should be listed")

	certsOut, err := apiClient.GetClientCertificates(ctx, &apigwsvc.GetClientCertificatesInput{})
	require.NoError(t, err, "GetClientCertificates should succeed")
	foundCert := false
	for _, c := range certsOut.Items {
		if aws.ToString(c.Description) == "lagw client certificate" {
			foundCert = true
		}
	}
	assert.True(t, foundCert, "client certificate should be listed")

	domainOut, err := apiClient.GetDomainName(ctx, &apigwsvc.GetDomainNameInput{
		DomainName: aws.String("lagw.example.test"),
	})
	require.NoError(t, err, "GetDomainName should succeed")
	assert.NotEmpty(t, aws.ToString(domainOut.RegionalCertificateArn))

	bpmOut, err := apiClient.GetBasePathMapping(ctx, &apigwsvc.GetBasePathMappingInput{
		DomainName: aws.String("lagw.example.test"),
		BasePath:   aws.String("v1"),
	})
	require.NoError(t, err, "GetBasePathMapping should succeed")
	assert.Equal(t, restAPIID, aws.ToString(bpmOut.RestApiId))
	assert.Equal(t, "prod", aws.ToString(bpmOut.Stage))

	vpcLinksOut, err := apiClient.GetVpcLinks(ctx, &apigwsvc.GetVpcLinksInput{})
	require.NoError(t, err, "GetVpcLinks should succeed")
	foundVpcLink := false
	for _, l := range vpcLinksOut.Items {
		if aws.ToString(l.Name) == "lagw-vpc-link" {
			foundVpcLink = true
			assert.Len(t, l.TargetArns, 1)
		}
	}
	assert.True(t, foundVpcLink, "lagw-vpc-link should be listed")
}
