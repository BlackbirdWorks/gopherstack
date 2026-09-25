package terraform_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwv2svc "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	apprunnertypes "github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_Apigatewayv2ApprunnerAndMacie provisions API Gateway V2 (API mapping,
// authorizer, deployment, domain name, integration response, model, route
// response, VPC link), App Runner (connection, deployment, observability
// configuration, VPC connector, VPC ingress connection), and Macie2
// (classification export configuration, classification job, custom data
// identifier, findings filter, member, organization admin account,
// organization configuration) resources via Terraform and verifies each
// through its own SDK client's Get/List path.
func TestTerraform_Apigatewayv2ApprunnerAndMacie(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "apigatewayv2-apprunner-and-macie",
			providerFn: macie2ProviderBlock,
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()

				functionZip := filepath.Join(dir, "agam-fn.zip")
				writeZipFixture(t, functionZip, "index.py",
					"def handler(event, context):\n    return {}\n")

				return map[string]any{
					"FunctionZip": functionZip,
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyApigatewayv2ApprunnerAndMacieAPIGatewayV2(ctx, t)
				verifyApigatewayv2ApprunnerAndMacieAppRunner(ctx, t)
				verifyApigatewayv2ApprunnerAndMacieMacie2(ctx, t)
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

func verifyApigatewayv2ApprunnerAndMacieAPIGatewayV2(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createAPIGatewayV2Client(t)

	apisOut, err := client.GetApis(ctx, &apigwv2svc.GetApisInput{})
	require.NoError(t, err, "GetApis should succeed")

	var httpAPIID, wsAPIID string

	for _, api := range apisOut.Items {
		switch aws.ToString(api.Name) {
		case "agam-http-api":
			httpAPIID = aws.ToString(api.ApiId)
		case "agam-ws-api":
			wsAPIID = aws.ToString(api.ApiId)
		}
	}

	require.NotEmpty(t, httpAPIID, "agam HTTP API should be listed")
	require.NotEmpty(t, wsAPIID, "agam WebSocket API should be listed")

	authOut, err := client.GetAuthorizers(ctx, &apigwv2svc.GetAuthorizersInput{ApiId: aws.String(httpAPIID)})
	require.NoError(t, err, "GetAuthorizers should succeed")
	require.Len(t, authOut.Items, 1)
	assert.Equal(t, "agam-authorizer", aws.ToString(authOut.Items[0].Name))

	modelsOut, err := client.GetModels(ctx, &apigwv2svc.GetModelsInput{ApiId: aws.String(httpAPIID)})
	require.NoError(t, err, "GetModels should succeed")
	require.Len(t, modelsOut.Items, 1)
	assert.Equal(t, "AgamModel", aws.ToString(modelsOut.Items[0].Name))

	integsOut, err := client.GetIntegrations(ctx, &apigwv2svc.GetIntegrationsInput{ApiId: aws.String(httpAPIID)})
	require.NoError(t, err, "GetIntegrations should succeed")
	require.Len(t, integsOut.Items, 1)
	integrationID := aws.ToString(integsOut.Items[0].IntegrationId)

	intRespOut, err := client.GetIntegrationResponses(ctx, &apigwv2svc.GetIntegrationResponsesInput{
		ApiId:         aws.String(httpAPIID),
		IntegrationId: aws.String(integrationID),
	})
	require.NoError(t, err, "GetIntegrationResponses should succeed")
	require.Len(t, intRespOut.Items, 1)
	assert.Equal(t, "/200/", aws.ToString(intRespOut.Items[0].IntegrationResponseKey))

	deploysOut, err := client.GetDeployments(ctx, &apigwv2svc.GetDeploymentsInput{ApiId: aws.String(httpAPIID)})
	require.NoError(t, err, "GetDeployments should succeed")
	require.Len(t, deploysOut.Items, 1)

	domOut, err := client.GetDomainName(ctx, &apigwv2svc.GetDomainNameInput{
		DomainName: aws.String("agam.example.test"),
	})
	require.NoError(t, err, "GetDomainName should succeed")
	require.NotNil(t, domOut.DomainNameConfigurations)

	mappingsOut, err := client.GetApiMappings(ctx, &apigwv2svc.GetApiMappingsInput{
		DomainName: aws.String("agam.example.test"),
	})
	require.NoError(t, err, "GetApiMappings should succeed")
	require.Len(t, mappingsOut.Items, 1)
	assert.Equal(t, httpAPIID, aws.ToString(mappingsOut.Items[0].ApiId))

	routesOut, err := client.GetRoutes(ctx, &apigwv2svc.GetRoutesInput{ApiId: aws.String(wsAPIID)})
	require.NoError(t, err, "GetRoutes should succeed")
	require.Len(t, routesOut.Items, 1)
	routeID := aws.ToString(routesOut.Items[0].RouteId)

	routeRespOut, err := client.GetRouteResponses(ctx, &apigwv2svc.GetRouteResponsesInput{
		ApiId:   aws.String(wsAPIID),
		RouteId: aws.String(routeID),
	})
	require.NoError(t, err, "GetRouteResponses should succeed")
	require.Len(t, routeRespOut.Items, 1)
	assert.Equal(t, "$default", aws.ToString(routeRespOut.Items[0].RouteResponseKey))

	vpcLinksOut, err := client.GetVpcLinks(ctx, &apigwv2svc.GetVpcLinksInput{})
	require.NoError(t, err, "GetVpcLinks should succeed")

	var foundVPCLink bool

	for _, link := range vpcLinksOut.Items {
		if aws.ToString(link.Name) == "agam-vpc-link" {
			foundVPCLink = true
		}
	}

	assert.True(t, foundVPCLink, "agam VPC link should be listed")
}

func verifyApigatewayv2ApprunnerAndMacieAppRunner(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createAppRunnerClient(t)

	obsOut, err := client.ListObservabilityConfigurations(ctx, &apprunnersdk.ListObservabilityConfigurationsInput{
		ObservabilityConfigurationName: aws.String("agam-obs"),
	})
	require.NoError(t, err, "ListObservabilityConfigurations should succeed")
	require.Len(t, obsOut.ObservabilityConfigurationSummaryList, 1)

	connsOut, err := client.ListConnections(ctx, &apprunnersdk.ListConnectionsInput{
		ConnectionName: aws.String("agam-connection"),
	})
	require.NoError(t, err, "ListConnections should succeed")
	require.Len(t, connsOut.ConnectionSummaryList, 1)
	assert.Equal(t, apprunnertypes.ProviderTypeGithub, connsOut.ConnectionSummaryList[0].ProviderType)

	svcsOut, err := client.ListServices(ctx, &apprunnersdk.ListServicesInput{})
	require.NoError(t, err, "ListServices should succeed")

	var serviceArn string

	for _, svc := range svcsOut.ServiceSummaryList {
		if aws.ToString(svc.ServiceName) == "agam-service" {
			serviceArn = aws.ToString(svc.ServiceArn)
		}
	}

	require.NotEmpty(t, serviceArn, "agam service should be listed")

	opsOut, err := client.ListOperations(ctx, &apprunnersdk.ListOperationsInput{ServiceArn: aws.String(serviceArn)})
	require.NoError(t, err, "ListOperations should succeed")
	assert.NotEmpty(t, opsOut.OperationSummaryList, "starting a deployment should record an operation")

	vpcConnOut, err := client.ListVpcConnectors(ctx, &apprunnersdk.ListVpcConnectorsInput{})
	require.NoError(t, err, "ListVpcConnectors should succeed")

	var foundVPCConnector bool

	for _, c := range vpcConnOut.VpcConnectors {
		if aws.ToString(c.VpcConnectorName) == "agam-vpc-connector" {
			foundVPCConnector = true
		}
	}

	assert.True(t, foundVPCConnector, "agam VPC connector should be listed")

	ingressOut, err := client.ListVpcIngressConnections(ctx, &apprunnersdk.ListVpcIngressConnectionsInput{
		Filter: &apprunnertypes.ListVpcIngressConnectionsFilter{ServiceArn: aws.String(serviceArn)},
	})
	require.NoError(t, err, "ListVpcIngressConnections should succeed")
	require.Len(t, ingressOut.VpcIngressConnectionSummaryList, 1)
}

func verifyApigatewayv2ApprunnerAndMacieMacie2(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createMacie2Client(t)

	exportCfgOut, err := client.GetClassificationExportConfiguration(
		ctx, &macie2sdk.GetClassificationExportConfigurationInput{})
	require.NoError(t, err, "GetClassificationExportConfiguration should succeed")
	require.NotNil(t, exportCfgOut.Configuration)
	require.NotNil(t, exportCfgOut.Configuration.S3Destination)
	assert.Equal(t, "agam-macie-bucket", aws.ToString(exportCfgOut.Configuration.S3Destination.BucketName))

	jobsOut, err := client.ListClassificationJobs(ctx, &macie2sdk.ListClassificationJobsInput{})
	require.NoError(t, err, "ListClassificationJobs should succeed")

	var foundJob bool

	for _, j := range jobsOut.Items {
		if aws.ToString(j.Name) == "agam-classification-job" {
			foundJob = true
		}
	}

	assert.True(t, foundJob, "agam classification job should be listed")

	cdiOut, err := client.ListCustomDataIdentifiers(ctx, &macie2sdk.ListCustomDataIdentifiersInput{})
	require.NoError(t, err, "ListCustomDataIdentifiers should succeed")

	var foundCDI bool

	for _, c := range cdiOut.Items {
		if aws.ToString(c.Name) == "agam-custom-data-identifier" {
			foundCDI = true
		}
	}

	assert.True(t, foundCDI, "agam custom data identifier should be listed")

	filtersOut, err := client.ListFindingsFilters(ctx, &macie2sdk.ListFindingsFiltersInput{})
	require.NoError(t, err, "ListFindingsFilters should succeed")

	var foundFilter bool

	for _, f := range filtersOut.FindingsFilterListItems {
		if aws.ToString(f.Name) == "agam-findings-filter" {
			foundFilter = true
		}
	}

	assert.True(t, foundFilter, "agam findings filter should be listed")

	memberOut, err := client.GetMember(ctx, &macie2sdk.GetMemberInput{Id: aws.String("111111111111")})
	require.NoError(t, err, "GetMember should succeed")
	assert.Equal(t, "agam-member@example.test", aws.ToString(memberOut.Email))

	orgAdminsOut, err := client.ListOrganizationAdminAccounts(ctx, &macie2sdk.ListOrganizationAdminAccountsInput{})
	require.NoError(t, err, "ListOrganizationAdminAccounts should succeed")

	var foundOrgAdmin bool

	for _, a := range orgAdminsOut.AdminAccounts {
		if aws.ToString(a.AccountId) == "222222222222" {
			foundOrgAdmin = true
		}
	}

	assert.True(t, foundOrgAdmin, "agam organization admin account should be listed")

	orgCfgOut, err := client.DescribeOrganizationConfiguration(ctx, &macie2sdk.DescribeOrganizationConfigurationInput{})
	require.NoError(t, err, "DescribeOrganizationConfiguration should succeed")
	assert.True(t, aws.ToBool(orgCfgOut.AutoEnable))
}
