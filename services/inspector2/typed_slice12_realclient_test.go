package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// newSlice12InspectorClient returns a fresh backend/handler/typed-client
// trio for one subtest.
func newSlice12InspectorClient(t *testing.T) (*inspector2.InMemoryBackend, *inspector2sdk.Client) {
	t.Helper()

	backend := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := inspector2.NewHandler(backend)

	return backend, newRoundTripClient(t, h)
}

func TestTypedSlice12_Inspector2(t *testing.T) {
	t.Parallel()

	t.Run("enablement_and_delegated_admin", func(t *testing.T) {
		t.Parallel()
		_, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		enableOut, err := client.Enable(ctx, &inspector2sdk.EnableInput{
			ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
		})
		require.NoError(t, err)
		require.Len(t, enableOut.Accounts, 1)
		assert.Equal(t, types.StatusEnabled, enableOut.Accounts[0].ResourceStatus.Ec2)

		batchOut, err := client.BatchGetAccountStatus(ctx, &inspector2sdk.BatchGetAccountStatusInput{})
		require.NoError(t, err)
		require.Len(t, batchOut.Accounts, 1)
		assert.Equal(t, types.StatusEnabled, batchOut.Accounts[0].ResourceState.Ec2.Status)

		_, err = client.UpdateConfiguration(ctx, &inspector2sdk.UpdateConfigurationInput{
			Ec2Configuration: &types.Ec2Configuration{ScanMode: types.Ec2ScanModeEc2Hybrid},
		})
		require.NoError(t, err)

		_, err = client.EnableDelegatedAdminAccount(ctx, &inspector2sdk.EnableDelegatedAdminAccountInput{
			DelegatedAdminAccountId: aws.String("222222222222"),
		})
		require.NoError(t, err)

		getAdmin, err := client.GetDelegatedAdminAccount(ctx, &inspector2sdk.GetDelegatedAdminAccountInput{})
		require.NoError(t, err)
		require.NotNil(t, getAdmin.DelegatedAdmin)
		assert.Equal(t, "222222222222", aws.ToString(getAdmin.DelegatedAdmin.AccountId))
		assert.Equal(t, types.RelationshipStatusEnabled, getAdmin.DelegatedAdmin.RelationshipStatus,
			"real GetDelegatedAdminAccountOutput.DelegatedAdmin.RelationshipStatus must decode -- "+
				"previously wire-keyed \"status\" (the sibling ListDelegatedAdminAccounts shape), always nil")

		_, err = client.DisableDelegatedAdminAccount(ctx, &inspector2sdk.DisableDelegatedAdminAccountInput{
			DelegatedAdminAccountId: aws.String("222222222222"),
		})
		require.NoError(t, err)

		_, err = client.GetDelegatedAdminAccount(ctx, &inspector2sdk.GetDelegatedAdminAccountInput{})
		require.Error(t, err)

		_, err = client.Disable(ctx, &inspector2sdk.DisableInput{
			ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
		})
		require.NoError(t, err)
	})

	t.Run("member_management", func(t *testing.T) {
		t.Parallel()
		_, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		_, err := client.AssociateMember(ctx, &inspector2sdk.AssociateMemberInput{
			AccountId: aws.String("333333333333"),
		})
		require.NoError(t, err)

		getMember, err := client.GetMember(ctx, &inspector2sdk.GetMemberInput{
			AccountId: aws.String("333333333333"),
		})
		require.NoError(t, err)
		require.NotNil(t, getMember.Member)
		assert.Equal(t, "333333333333", aws.ToString(getMember.Member.AccountId))
		assert.Equal(t, types.RelationshipStatusEnabled, getMember.Member.RelationshipStatus)

		listMembers, err := client.ListMembers(ctx, &inspector2sdk.ListMembersInput{})
		require.NoError(t, err)
		require.Len(t, listMembers.Members, 1)

		_, err = client.DisassociateMember(ctx, &inspector2sdk.DisassociateMemberInput{
			AccountId: aws.String("333333333333"),
		})
		require.NoError(t, err)

		_, err = client.GetMember(ctx, &inspector2sdk.GetMemberInput{
			AccountId: aws.String("333333333333"),
		})
		require.Error(t, err)
	})

	t.Run("cis_scan_lifecycle", func(t *testing.T) {
		t.Parallel()
		_, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		createOut, err := client.CreateCisScanConfiguration(ctx, &inspector2sdk.CreateCisScanConfigurationInput{
			ScanName:      aws.String("nightly-cis-scan"),
			SecurityLevel: types.CisSecurityLevelLevel1,
			Schedule: &types.ScheduleMemberDaily{
				Value: types.DailySchedule{
					StartTime: &types.Time{TimeOfDay: aws.String("02:00"), Timezone: aws.String("UTC")},
				},
			},
			Targets: &types.CreateCisTargets{
				AccountIds:         []string{rtTestAccountID},
				TargetResourceTags: map[string][]string{},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, createOut.ScanConfigurationArn)

		_, err = client.UpdateCisScanConfiguration(ctx, &inspector2sdk.UpdateCisScanConfigurationInput{
			ScanConfigurationArn: createOut.ScanConfigurationArn,
			ScanName:             aws.String("nightly-cis-scan-renamed"),
		})
		require.NoError(t, err)

		listCfgs, err := client.ListCisScanConfigurations(ctx, &inspector2sdk.ListCisScanConfigurationsInput{})
		require.NoError(t, err)
		require.Len(t, listCfgs.ScanConfigurations, 1)

		listScans, err := client.ListCisScans(ctx, &inspector2sdk.ListCisScansInput{})
		require.NoError(t, err)
		require.Len(t, listScans.Scans, 1)
		scanArn := listScans.Scans[0].ScanArn
		require.NotNil(t, scanArn)

		reportOut, err := client.GetCisScanReport(ctx, &inspector2sdk.GetCisScanReportInput{
			ScanArn: scanArn,
		})
		require.NoError(t, err)
		assert.Equal(t, types.CisReportStatusSucceeded, reportOut.Status)

		resourcesOut, err := client.ListCisScanResultsAggregatedByTargetResource(
			ctx, &inspector2sdk.ListCisScanResultsAggregatedByTargetResourceInput{ScanArn: scanArn},
		)
		require.NoError(t, err)
		require.Len(t, resourcesOut.TargetResourceAggregations, 1)
		target := resourcesOut.TargetResourceAggregations[0]

		// AccountId/TargetResourceId are both real, required
		// GetCisScanResultDetailsInput members (gopherstack-n3zi typed
		// slice 12): previously silently dropped, so every real client's
		// scoped request returned every check result for the whole scan.
		detailsOut, err := client.GetCisScanResultDetails(ctx, &inspector2sdk.GetCisScanResultDetailsInput{
			ScanArn:          scanArn,
			AccountId:        target.AccountId,
			TargetResourceId: target.TargetResourceId,
		})
		require.NoError(t, err)
		require.NotEmpty(t, detailsOut.ScanResultDetails)
		for _, d := range detailsOut.ScanResultDetails {
			assert.Equal(t, aws.ToString(target.TargetResourceId), aws.ToString(d.TargetResourceId))
		}

		checksOut, err := client.ListCisScanResultsAggregatedByChecks(
			ctx, &inspector2sdk.ListCisScanResultsAggregatedByChecksInput{ScanArn: scanArn},
		)
		require.NoError(t, err)
		assert.NotNil(t, checksOut.CheckAggregations)

		startOut, err := client.StartCisSession(ctx, &inspector2sdk.StartCisSessionInput{
			ScanJobId: aws.String("scan-job-slice12"),
			Message:   &types.StartCisSessionMessage{SessionToken: aws.String("session-token-1")},
		})
		require.NoError(t, err)
		_ = startOut

		// SessionToken is required client-side on all three ops below, but
		// this package's CIS session health/telemetry/stop lifecycle is a
		// disclosed no-op beyond routing/basic state (PARITY.md deferred
		// list) -- not re-litigated here, just satisfied so the real client
		// can send the request at all.
		_, err = client.SendCisSessionHealth(ctx, &inspector2sdk.SendCisSessionHealthInput{
			ScanJobId:    aws.String("scan-job-slice12"),
			SessionToken: aws.String("session-token-1"),
		})
		require.NoError(t, err)

		_, err = client.SendCisSessionTelemetry(ctx, &inspector2sdk.SendCisSessionTelemetryInput{
			ScanJobId:    aws.String("scan-job-slice12"),
			SessionToken: aws.String("session-token-1"),
			Messages:     []types.CisSessionMessage{},
		})
		require.NoError(t, err)

		_, err = client.StopCisSession(ctx, &inspector2sdk.StopCisSessionInput{
			ScanJobId:    aws.String("scan-job-slice12"),
			SessionToken: aws.String("session-token-1"),
			Message: &types.StopCisSessionMessage{
				Status:   types.StopCisSessionStatusSuccess,
				Progress: &types.StopCisMessageProgress{},
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteCisScanConfiguration(ctx, &inspector2sdk.DeleteCisScanConfigurationInput{
			ScanConfigurationArn: createOut.ScanConfigurationArn,
		})
		require.NoError(t, err)
	})

	t.Run("connector_lifecycle", func(t *testing.T) {
		t.Parallel()
		_, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		createOut, err := client.CreateConnector(ctx, &inspector2sdk.CreateConnectorInput{
			Name:     aws.String("azure-connector"),
			Provider: types.ConnectorCloudProviderAzure,
			ProviderDetail: &types.ProviderDetailCreateMemberAzure{
				Value: types.AzureProviderDetailCreate{
					AwsConfigConnectorArn: aws.String("arn:aws:config:us-east-1:123456789012:config-connector/azure-1"),
					AzureRegions:          []string{"eastus"},
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

		_, err = client.UpdateConnectorScanConfiguration(ctx, &inspector2sdk.UpdateConnectorScanConfigurationInput{
			AwsConfigConnectorArn: aws.String("arn:aws:config:us-east-1:123456789012:config-connector/azure-1"),
			ScanConfiguration: &types.ConnectorScanConfiguration{
				ContainerImageScanning: &types.ConnectorContainerImageScanConfiguration{
					PullDuration: types.ContainerImagePullDateRescanDurationDays7,
				},
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteConnector(ctx, &inspector2sdk.DeleteConnectorInput{
			ConnectorArn: createOut.ConnectorArn,
		})
		require.NoError(t, err)

		listOut2, err := client.ListConnectors(ctx, &inspector2sdk.ListConnectorsInput{})
		require.NoError(t, err)
		assert.Empty(t, listOut2.Items)
	})

	t.Run("code_security_scan_configuration", func(t *testing.T) {
		t.Parallel()
		_, client := newSlice12InspectorClient(t)
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
		assert.Equal(t, []types.RuleSetCategory{types.RuleSetCategorySast}, getOut.Configuration.RuleSetCategories)

		_, err = client.UpdateCodeSecurityScanConfiguration(
			ctx, &inspector2sdk.UpdateCodeSecurityScanConfigurationInput{
				ScanConfigurationArn: createOut.ScanConfigurationArn,
				Configuration: &types.CodeSecurityScanConfiguration{
					RuleSetCategories: []types.RuleSetCategory{types.RuleSetCategorySast, types.RuleSetCategoryIac},
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
	})

	t.Run("ec2_deep_inspection_configuration", func(t *testing.T) {
		t.Parallel()
		_, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		getOut, err := client.GetEc2DeepInspectionConfiguration(
			ctx, &inspector2sdk.GetEc2DeepInspectionConfigurationInput{},
		)
		require.NoError(t, err)
		assert.Equal(t, types.Ec2DeepInspectionStatusDeactivated, getOut.Status)

		updOut, err := client.UpdateEc2DeepInspectionConfiguration(
			ctx, &inspector2sdk.UpdateEc2DeepInspectionConfigurationInput{
				PackagePaths: []string{"/opt/custom/paths"},
			},
		)
		require.NoError(t, err)
		assert.Contains(t, updOut.PackagePaths, "/opt/custom/paths")

		_, err = client.UpdateOrgEc2DeepInspectionConfiguration(
			ctx, &inspector2sdk.UpdateOrgEc2DeepInspectionConfigurationInput{
				OrgPackagePaths: []string{"/opt/org/paths"},
			},
		)
		require.NoError(t, err)
	})

	t.Run("encryption_key_lifecycle", func(t *testing.T) {
		t.Parallel()
		_, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		getOut, err := client.GetEncryptionKey(ctx, &inspector2sdk.GetEncryptionKeyInput{
			ResourceType: types.ResourceTypeAwsEcrContainerImage,
			ScanType:     types.ScanTypeNetwork,
		})
		require.NoError(t, err)
		assert.Equal(t, "AWS_OWNED_KEY", aws.ToString(getOut.KmsKeyId))

		_, err = client.UpdateEncryptionKey(ctx, &inspector2sdk.UpdateEncryptionKeyInput{
			KmsKeyId:     aws.String("arn:aws:kms:us-east-1:123456789012:key/my-key"),
			ResourceType: types.ResourceTypeAwsEcrContainerImage,
			ScanType:     types.ScanTypeNetwork,
		})
		require.NoError(t, err)

		getOut2, err := client.GetEncryptionKey(ctx, &inspector2sdk.GetEncryptionKeyInput{
			ResourceType: types.ResourceTypeAwsEcrContainerImage,
			ScanType:     types.ScanTypeNetwork,
		})
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/my-key", aws.ToString(getOut2.KmsKeyId))

		_, err = client.ResetEncryptionKey(ctx, &inspector2sdk.ResetEncryptionKeyInput{
			ResourceType: types.ResourceTypeAwsEcrContainerImage,
			ScanType:     types.ScanTypeNetwork,
		})
		require.NoError(t, err)

		getOut3, err := client.GetEncryptionKey(ctx, &inspector2sdk.GetEncryptionKeyInput{
			ResourceType: types.ResourceTypeAwsEcrContainerImage,
			ScanType:     types.ScanTypeNetwork,
		})
		require.NoError(t, err)
		assert.Equal(t, "AWS_OWNED_KEY", aws.ToString(getOut3.KmsKeyId))
	})

	t.Run("coverage_and_clusters", func(t *testing.T) {
		t.Parallel()
		backend, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		_, err := backend.SeedCoverage(inspector2.CoverageEntry{
			ResourceID:   "i-slice12coverage",
			ResourceType: "AWS_EC2_INSTANCE",
			ScanType:     "NETWORK",
		})
		require.NoError(t, err)

		listOut, err := client.ListCoverage(ctx, &inspector2sdk.ListCoverageInput{})
		require.NoError(t, err)
		require.Len(t, listOut.CoveredResources, 1)
		assert.Equal(t, "i-slice12coverage", aws.ToString(listOut.CoveredResources[0].ResourceId))

		statsOut, err := client.ListCoverageStatistics(ctx, &inspector2sdk.ListCoverageStatisticsInput{})
		require.NoError(t, err)
		assert.NotZero(t, aws.ToInt64(statsOut.TotalCounts))

		clustersOut, err := client.GetClustersForImage(ctx, &inspector2sdk.GetClustersForImageInput{
			Filter: &types.ClusterForImageFilterCriteria{
				ResourceId: aws.String("i-slice12coverage"),
			},
		})
		require.NoError(t, err)
		assert.Empty(t, clustersOut.Cluster)
	})

	t.Run("findings_reports_and_sbom", func(t *testing.T) {
		t.Parallel()
		backend, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		reportOut, err := client.CreateFindingsReport(ctx, &inspector2sdk.CreateFindingsReportInput{
			ReportFormat: types.ReportFormatCsv,
			S3Destination: &types.Destination{
				BucketName: aws.String("my-findings-bucket"),
				KeyPrefix:  aws.String("reports/"),
				KmsKeyArn:  aws.String("arn:aws:kms:us-east-1:123456789012:key/report-key"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, reportOut.ReportId)

		statusOut, err := client.GetFindingsReportStatus(ctx, &inspector2sdk.GetFindingsReportStatusInput{
			ReportId: reportOut.ReportId,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(reportOut.ReportId), aws.ToString(statusOut.ReportId))

		_, err = client.CancelFindingsReport(ctx, &inspector2sdk.CancelFindingsReportInput{
			ReportId: reportOut.ReportId,
		})
		require.NoError(t, err)

		sbomOut, err := client.CreateSbomExport(ctx, &inspector2sdk.CreateSbomExportInput{
			ReportFormat: types.SbomReportFormatCyclonedx14,
			S3Destination: &types.Destination{
				BucketName: aws.String("my-sbom-bucket"),
				KeyPrefix:  aws.String("sboms/"),
				KmsKeyArn:  aws.String("arn:aws:kms:us-east-1:123456789012:key/sbom-key"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, sbomOut.ReportId)

		getSbomOut, err := client.GetSbomExport(ctx, &inspector2sdk.GetSbomExportInput{
			ReportId: sbomOut.ReportId,
		})
		require.NoError(t, err)
		assert.Equal(t, types.SbomReportFormatCyclonedx14, getSbomOut.Format)

		_, err = client.CancelSbomExport(ctx, &inspector2sdk.CancelSbomExportInput{
			ReportId: sbomOut.ReportId,
		})
		require.NoError(t, err)

		findingArn := inspector2.SeedFinding(
			backend, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE",
			"slice12 finding", "seeded for BatchGetFindingDetails", nil,
		)

		detailsOut, err := client.BatchGetFindingDetails(ctx, &inspector2sdk.BatchGetFindingDetailsInput{
			FindingArns: []string{findingArn},
		})
		require.NoError(t, err)
		require.Len(t, detailsOut.FindingDetails, 1)
		assert.Equal(t, findingArn, aws.ToString(detailsOut.FindingDetails[0].FindingArn))
		assert.Empty(t, detailsOut.Errors)

		snippetOut, err := client.BatchGetCodeSnippet(ctx, &inspector2sdk.BatchGetCodeSnippetInput{
			FindingArns: []string{findingArn},
		})
		require.NoError(t, err)
		require.Len(t, snippetOut.Errors, 1, "no code snippet was ever seeded for this finding")
		assert.Equal(t, findingArn, aws.ToString(snippetOut.Errors[0].FindingArn))
		assert.Equal(t, types.CodeSnippetErrorCode("CODE_SNIPPET_NOT_FOUND"), snippetOut.Errors[0].ErrorCode)

		trialOut, err := client.BatchGetFreeTrialInfo(ctx, &inspector2sdk.BatchGetFreeTrialInfoInput{
			AccountIds: []string{rtTestAccountID},
		})
		require.NoError(t, err)
		require.Len(t, trialOut.Accounts, 1)
		assert.Equal(t, rtTestAccountID, aws.ToString(trialOut.Accounts[0].AccountId))

		searchOut, err := client.SearchVulnerabilities(ctx, &inspector2sdk.SearchVulnerabilitiesInput{
			FilterCriteria: &types.SearchVulnerabilitiesFilterCriteria{
				VulnerabilityIds: []string{"CVE-2024-0001"},
			},
		})
		require.NoError(t, err)
		assert.NotNil(t, searchOut.Vulnerabilities)

		aggOut, err := client.ListFindingAggregations(ctx, &inspector2sdk.ListFindingAggregationsInput{
			AggregationType: types.AggregationTypeAccount,
		})
		require.NoError(t, err)
		assert.NotNil(t, aggOut.Responses)
	})

	t.Run("usage_and_permissions", func(t *testing.T) {
		t.Parallel()
		backend, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		_, err := client.Enable(ctx, &inspector2sdk.EnableInput{
			ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
		})
		require.NoError(t, err)

		_, err = backend.SeedCoverage(inspector2.CoverageEntry{
			ResourceID:   "i-usage-slice12",
			ResourceType: "AWS_EC2_INSTANCE",
			ScanType:     "EC2",
		})
		require.NoError(t, err)

		usageOut, err := client.ListUsageTotals(ctx, &inspector2sdk.ListUsageTotalsInput{})
		require.NoError(t, err)
		require.Len(t, usageOut.Totals, 1)
		assert.Equal(t, rtTestAccountID, aws.ToString(usageOut.Totals[0].AccountId))
		require.NotEmpty(t, usageOut.Totals[0].Usage)

		permsOut, err := client.ListAccountPermissions(ctx, &inspector2sdk.ListAccountPermissionsInput{})
		require.NoError(t, err)
		assert.NotNil(t, permsOut.Permissions)

		delegatedOut, err := client.ListDelegatedAdminAccounts(ctx, &inspector2sdk.ListDelegatedAdminAccountsInput{})
		require.NoError(t, err)
		assert.NotNil(t, delegatedOut.DelegatedAdminAccounts)
	})

	t.Run("filter_update_and_tags", func(t *testing.T) {
		t.Parallel()
		_, client := newSlice12InspectorClient(t)
		ctx := t.Context()

		createOut, err := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
			Name:           aws.String("slice12-filter"),
			Action:         types.FilterActionNone,
			FilterCriteria: &types.FilterCriteria{},
		})
		require.NoError(t, err)
		require.NotNil(t, createOut.Arn)

		updOut, err := client.UpdateFilter(ctx, &inspector2sdk.UpdateFilterInput{
			FilterArn:   createOut.Arn,
			Action:      types.FilterActionSuppress,
			Description: aws.String("suppress noisy findings"),
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.Arn), aws.ToString(updOut.Arn))

		_, err = client.TagResource(ctx, &inspector2sdk.TagResourceInput{
			ResourceArn: createOut.Arn,
			Tags:        map[string]string{"team": "security"},
		})
		require.NoError(t, err)

		listTagsOut, err := client.ListTagsForResource(ctx, &inspector2sdk.ListTagsForResourceInput{
			ResourceArn: createOut.Arn,
		})
		require.NoError(t, err)
		assert.Equal(t, "security", listTagsOut.Tags["team"])

		_, err = client.UntagResource(ctx, &inspector2sdk.UntagResourceInput{
			ResourceArn: createOut.Arn,
			TagKeys:     []string{"team"},
		})
		require.NoError(t, err)

		listTagsOut2, err := client.ListTagsForResource(ctx, &inspector2sdk.ListTagsForResourceInput{
			ResourceArn: createOut.Arn,
		})
		require.NoError(t, err)
		assert.NotContains(t, listTagsOut2.Tags, "team")
	})
}
