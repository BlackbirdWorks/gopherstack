package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_Integrations drives connector and code-security-integration
// ops through a real aws-sdk-go-v2 inspector2 client.
func TestRealClient_Integrations(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *inspector2sdk.Client)
		name string
	}{
		{
			name: "connector_lifecycle",
			run: func(t *testing.T, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				createOut, err := client.CreateConnector(ctx, &inspector2sdk.CreateConnectorInput{
					Name:     aws.String("azure-connector"),
					Provider: types.ConnectorCloudProviderAzure,
					ProviderDetail: &types.ProviderDetailCreateMemberAzure{
						Value: types.AzureProviderDetailCreate{
							AwsConfigConnectorArn: aws.String(
								"arn:aws:config:us-east-1:123456789012:config-connector/azure-1",
							),
							AzureRegions: []string{"eastus"},
							ScopeConfiguration: &types.AzureScopeConfigurationInput{
								VmScanning: &types.ScopeConfigurationInput{ScopeType: types.ScopeTypeTenant},
							},
						},
					},
				})
				require.NoError(t, err)
				require.NotNil(t, createOut.ConnectorArn)

				_, err = client.UpdateConnector(ctx, &inspector2sdk.UpdateConnectorInput{
					ConnectorArn: createOut.ConnectorArn,
					Description:  aws.String("updated description"),
					ProviderDetail: &types.ProviderDetailUpdateMemberAzure{
						Value: types.AzureProviderDetailUpdate{
							AzureRegions: []string{"eastus", "westus"},
						},
					},
				})
				require.NoError(t, err)

				listOut, err := client.ListConnectors(ctx, &inspector2sdk.ListConnectorsInput{})
				require.NoError(t, err)
				require.Len(t, listOut.Items, 1)
				assert.Equal(t, "updated description", aws.ToString(listOut.Items[0].Description))

				listScanCfgOut, err := client.ListConnectorScanConfigurations(
					ctx, &inspector2sdk.ListConnectorScanConfigurationsInput{},
				)
				require.NoError(t, err)
				assert.NotNil(t, listScanCfgOut.ScanConfigurations)

				_, err = client.UpdateConnectorScanConfiguration(
					ctx, &inspector2sdk.UpdateConnectorScanConfigurationInput{
						AwsConfigConnectorArn: aws.String(
							"arn:aws:config:us-east-1:123456789012:config-connector/azure-1",
						),
						ScanConfiguration: &types.ConnectorScanConfiguration{
							ContainerImageScanning: &types.ConnectorContainerImageScanConfiguration{
								PullDuration: types.ContainerImagePullDateRescanDurationDays7,
							},
						},
					},
				)
				require.NoError(t, err)

				_, err = client.DeleteConnector(ctx, &inspector2sdk.DeleteConnectorInput{
					ConnectorArn: createOut.ConnectorArn,
				})
				require.NoError(t, err)

				listOut2, err := client.ListConnectors(ctx, &inspector2sdk.ListConnectorsInput{})
				require.NoError(t, err)
				assert.Empty(t, listOut2.Items)
			},
		},
		{
			name: "code_security_scan_configuration",
			run: func(t *testing.T, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				createOut, err := client.CreateCodeSecurityScanConfiguration(
					ctx, &inspector2sdk.CreateCodeSecurityScanConfigurationInput{
						Name:  aws.String("code-scan-config"),
						Level: types.ConfigurationLevelAccount,
						Configuration: &types.CodeSecurityScanConfiguration{
							RuleSetCategories: []types.RuleSetCategory{types.RuleSetCategorySast},
						},
					},
				)
				require.NoError(t, err)
				require.NotNil(t, createOut.ScanConfigurationArn)

				getOut, err := client.GetCodeSecurityScanConfiguration(
					ctx, &inspector2sdk.GetCodeSecurityScanConfigurationInput{
						ScanConfigurationArn: createOut.ScanConfigurationArn,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "code-scan-config", aws.ToString(getOut.Name))
				require.NotNil(t, getOut.Configuration)
				assert.Equal(
					t, []types.RuleSetCategory{types.RuleSetCategorySast}, getOut.Configuration.RuleSetCategories,
				)

				_, err = client.UpdateCodeSecurityScanConfiguration(
					ctx, &inspector2sdk.UpdateCodeSecurityScanConfigurationInput{
						ScanConfigurationArn: createOut.ScanConfigurationArn,
						Configuration: &types.CodeSecurityScanConfiguration{
							RuleSetCategories: []types.RuleSetCategory{
								types.RuleSetCategorySast, types.RuleSetCategoryIac,
							},
						},
					},
				)
				require.NoError(t, err)

				listOut, err := client.ListCodeSecurityScanConfigurations(
					ctx, &inspector2sdk.ListCodeSecurityScanConfigurationsInput{},
				)
				require.NoError(t, err)
				require.Len(t, listOut.Configurations, 1)

				assocOut, err := client.ListCodeSecurityScanConfigurationAssociations(
					ctx, &inspector2sdk.ListCodeSecurityScanConfigurationAssociationsInput{
						ScanConfigurationArn: createOut.ScanConfigurationArn,
					},
				)
				require.NoError(t, err)
				assert.NotNil(t, assocOut.Associations)

				integOut, err := client.CreateCodeSecurityIntegration(
					ctx, &inspector2sdk.CreateCodeSecurityIntegrationInput{
						Name: aws.String("gh-integration"),
						Type: types.IntegrationTypeGithub,
					},
				)
				require.NoError(t, err)
				require.NotNil(t, integOut.IntegrationArn)

				updIntegOut, err := client.UpdateCodeSecurityIntegration(
					ctx, &inspector2sdk.UpdateCodeSecurityIntegrationInput{
						IntegrationArn: integOut.IntegrationArn,
						Details: &types.UpdateIntegrationDetailsMemberGithub{
							Value: types.UpdateGitHubIntegrationDetail{
								Code:           aws.String("oauth-code-slice12"),
								InstallationId: aws.String("installation-slice12"),
							},
						},
					},
				)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(integOut.IntegrationArn), aws.ToString(updIntegOut.IntegrationArn))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, client := newRealClient(t)
			tc.run(t, client)
		})
	}
}
