package securityhub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestRealClient_HubStandardsAndAutomation covers securityhub's highest-priority
// typed-client-uncovered op families (gopherstack-n3zi): hub v1/v2
// lifecycle, standards/controls, security control definitions, organization
// admin, invitations/members, automation rules (v1+v2), configuration
// policies, finding aggregators, connectors (v1+v2), aggregators v2,
// products, and misc findings v2 ops. Each subtest creates real state
// through the typed aws-sdk-go-v2 securityhub client and asserts decoded
// response values.
func TestRealClient_HubStandardsAndAutomation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testHubV2RealClient, "hub_v2"},
		{testStandardsControlsRealClient, "standards_controls"},
		{testSecurityControlsRealClient, "security_controls"},
		{testOrganizationRealClient, "organization"},
		{testMembersInvitationsRealClient, "members_invitations"},
		{testAutomationRulesRealClient, "automation_rules"},
		{testConfigurationPoliciesRealClient, "configuration_policies"},
		{testFindingAggregatorsRealClient, "finding_aggregators"},
		{testConnectorsRealClient, "connectors"},
		{testAggregatorsV2RealClient, "aggregators_v2"},
		{testProductsRealClient, "products"},
		{testFindingsMiscRealClient, "findings_misc"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newRealClientBackendAndClient(
	t *testing.T,
) (*securityhub.InMemoryBackend, *securityhubsdk.Client) {
	t.Helper()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	return backend, client
}

func testHubV2RealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	require.NoError(t, backend.EnableHub(true, "", nil))

	_, err := client.UpdateSecurityHubConfiguration(
		ctx,
		&securityhubsdk.UpdateSecurityHubConfigurationInput{
			AutoEnableControls: aws.Bool(false),
		},
	)
	require.NoError(t, err)

	_, err = client.DisableSecurityHub(ctx, &securityhubsdk.DisableSecurityHubInput{})
	require.NoError(t, err)

	_, err = client.EnableSecurityHubV2(ctx, &securityhubsdk.EnableSecurityHubV2Input{})
	require.NoError(t, err)

	descOut, err := client.DescribeSecurityHubV2(ctx, &securityhubsdk.DescribeSecurityHubV2Input{})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(descOut.HubV2Arn))

	_, err = client.EnableSecurityHubFeatureV2(ctx, &securityhubsdk.EnableSecurityHubFeatureV2Input{
		FeatureName: types.FeatureNameNetworkScanning,
	})
	require.NoError(t, err)

	_, err = client.DisableSecurityHubFeatureV2(
		ctx,
		&securityhubsdk.DisableSecurityHubFeatureV2Input{
			FeatureName: types.FeatureNameNetworkScanning,
		},
	)
	require.NoError(t, err)

	_, err = client.DisableSecurityHubV2(ctx, &securityhubsdk.DisableSecurityHubV2Input{})
	require.NoError(t, err)
}

func testStandardsControlsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	require.NoError(t, backend.EnableHub(false, "", nil))

	subs, _ := backend.BatchEnableStandards([]map[string]any{
		{
			"StandardsArn": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
		},
	})
	require.Len(t, subs, 1)
	subscriptionArn := subs[0].StandardsSubscriptionArn

	descOut, err := client.DescribeStandards(ctx, &securityhubsdk.DescribeStandardsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, descOut.Standards)

	ctlsOut, err := client.DescribeStandardsControls(
		ctx,
		&securityhubsdk.DescribeStandardsControlsInput{
			StandardsSubscriptionArn: aws.String(subscriptionArn),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, ctlsOut.Controls)
	controlArn := aws.ToString(ctlsOut.Controls[0].StandardsControlArn)
	secCtlID := aws.ToString(ctlsOut.Controls[0].ControlId)

	_, err = client.UpdateStandardsControl(ctx, &securityhubsdk.UpdateStandardsControlInput{
		StandardsControlArn: aws.String(controlArn),
		ControlStatus:       types.ControlStatusDisabled,
		DisabledReason:      aws.String("slice8 test"),
	})
	require.NoError(t, err)

	assocsOut, err := client.ListStandardsControlAssociations(
		ctx,
		&securityhubsdk.ListStandardsControlAssociationsInput{
			SecurityControlId: aws.String(secCtlID),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, assocsOut)

	batchGetOut, err := client.BatchGetStandardsControlAssociations(
		ctx, &securityhubsdk.BatchGetStandardsControlAssociationsInput{
			StandardsControlAssociationIds: []types.StandardsControlAssociationId{},
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, batchGetOut)

	batchUpdateOut, err := client.BatchUpdateStandardsControlAssociations(
		ctx, &securityhubsdk.BatchUpdateStandardsControlAssociationsInput{
			StandardsControlAssociationUpdates: []types.StandardsControlAssociationUpdate{},
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, batchUpdateOut)

	_, err = client.BatchDisableStandards(ctx, &securityhubsdk.BatchDisableStandardsInput{
		StandardsSubscriptionArns: []string{subscriptionArn},
	})
	require.NoError(t, err)
}

func testSecurityControlsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	listOut, err := client.ListSecurityControlDefinitions(
		ctx, &securityhubsdk.ListSecurityControlDefinitionsInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, listOut.SecurityControlDefinitions)
	secCtlID := aws.ToString(listOut.SecurityControlDefinitions[0].SecurityControlId)

	_, err = client.UpdateSecurityControl(ctx, &securityhubsdk.UpdateSecurityControlInput{
		SecurityControlId: aws.String(secCtlID),
		LastUpdateReason:  aws.String("slice8 test"),
		Parameters:        map[string]types.ParameterConfiguration{},
	})
	require.NoError(t, err)
}

func testOrganizationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	descOut, err := client.DescribeOrganizationConfiguration(
		ctx, &securityhubsdk.DescribeOrganizationConfigurationInput{},
	)
	require.NoError(t, err)
	assert.NotNil(t, descOut)

	_, err = client.UpdateOrganizationConfiguration(
		ctx,
		&securityhubsdk.UpdateOrganizationConfigurationInput{
			AutoEnable: aws.Bool(true),
		},
	)
	require.NoError(t, err)

	_, err = client.EnableOrganizationAdminAccount(
		ctx,
		&securityhubsdk.EnableOrganizationAdminAccountInput{
			AdminAccountId: aws.String("111111111111"),
		},
	)
	require.NoError(t, err)

	_, err = client.DisableOrganizationAdminAccount(
		ctx,
		&securityhubsdk.DisableOrganizationAdminAccountInput{
			AdminAccountId: aws.String("111111111111"),
		},
	)
	require.NoError(t, err)
}

func testMembersInvitationsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	created, unprocessed := backend.CreateMembers([]map[string]any{
		{"AccountId": "222222222222", "Email": "member@example.com"},
	})
	require.Empty(t, unprocessed)
	require.Len(t, created, 1)

	inviteOut, err := client.InviteMembers(ctx, &securityhubsdk.InviteMembersInput{
		AccountIds: []string{"222222222222"},
	})
	require.NoError(t, err)
	assert.Empty(t, inviteOut.UnprocessedAccounts)

	countOut, err := client.GetInvitationsCount(ctx, &securityhubsdk.GetInvitationsCountInput{})
	require.NoError(t, err)
	assert.NotNil(t, countOut)

	require.NoError(t, backend.AcceptInvitation("333333333333", "inv-1"))

	//nolint:staticcheck // SA1019: deliberately exercising a deprecated-but-real op
	_, err = client.DisassociateFromMasterAccount(
		ctx,
		&securityhubsdk.DisassociateFromMasterAccountInput{},
	)
	require.NoError(t, err)

	require.NoError(t, backend.AcceptAdministratorInvitation("444444444444", "inv-2"))

	_, err = client.DisassociateFromAdministratorAccount(
		ctx, &securityhubsdk.DisassociateFromAdministratorAccountInput{},
	)
	require.NoError(t, err)

	listOut, err := client.ListInvitations(ctx, &securityhubsdk.ListInvitationsInput{})
	require.NoError(t, err)
	assert.NotNil(t, listOut)

	declineOut, err := client.DeclineInvitations(ctx, &securityhubsdk.DeclineInvitationsInput{
		AccountIds: []string{"999999999999"},
	})
	require.NoError(t, err)
	assert.NotNil(t, declineOut)

	deleteInvOut, err := client.DeleteInvitations(ctx, &securityhubsdk.DeleteInvitationsInput{
		AccountIds: []string{"999999999999"},
	})
	require.NoError(t, err)
	assert.NotNil(t, deleteInvOut)

	_, err = client.DisassociateMembers(ctx, &securityhubsdk.DisassociateMembersInput{
		AccountIds: []string{"222222222222"},
	})
	require.NoError(t, err)
}

func testAutomationRulesRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateAutomationRule(ctx, &securityhubsdk.CreateAutomationRuleInput{
		RuleName:    aws.String("slice8-rule"),
		Description: aws.String("slice8 automation rule"),
		RuleOrder:   aws.Int32(1),
		Criteria:    &types.AutomationRulesFindingFilters{},
		Actions: []types.AutomationRulesAction{
			{
				Type: types.AutomationRulesActionTypeFindingFieldsUpdate,
				FindingFieldsUpdate: &types.AutomationRulesFindingFieldsUpdate{
					Note: &types.NoteUpdate{
						Text:      aws.String("slice8 note"),
						UpdatedBy: aws.String("slice8"),
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.RuleArn))

	listOut, err := client.ListAutomationRules(ctx, &securityhubsdk.ListAutomationRulesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.AutomationRulesMetadata)

	createV2Out, err := client.CreateAutomationRuleV2(
		ctx,
		&securityhubsdk.CreateAutomationRuleV2Input{
			RuleName:    aws.String("slice8-rule-v2"),
			Description: aws.String("slice8 automation rule v2"),
			RuleOrder:   aws.Float32(1),
			Criteria: &types.CriteriaMemberOcsfFindingCriteria{
				Value: types.OcsfFindingFilters{},
			},
			Actions: []types.AutomationRulesActionV2{
				{
					Type: types.AutomationRulesActionTypeV2FindingFieldsUpdate,
					FindingFieldsUpdate: &types.AutomationRulesFindingFieldsUpdateV2{
						Comment: aws.String("slice8 v2 comment"),
					},
				},
			},
		},
	)
	require.NoError(t, err)
	ruleID := aws.ToString(createV2Out.RuleId)
	require.NotEmpty(t, ruleID)

	getV2Out, err := client.GetAutomationRuleV2(ctx, &securityhubsdk.GetAutomationRuleV2Input{
		Identifier: aws.String(ruleID),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-rule-v2", aws.ToString(getV2Out.RuleName))

	listV2Out, err := client.ListAutomationRulesV2(
		ctx,
		&securityhubsdk.ListAutomationRulesV2Input{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listV2Out.Rules)

	_, err = client.UpdateAutomationRuleV2(ctx, &securityhubsdk.UpdateAutomationRuleV2Input{
		Identifier:  aws.String(ruleID),
		Description: aws.String("slice8 automation rule v2 updated"),
	})
	require.NoError(t, err)

	getUpdatedOut, err := client.GetAutomationRuleV2(ctx, &securityhubsdk.GetAutomationRuleV2Input{
		Identifier: aws.String(ruleID),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8 automation rule v2 updated", aws.ToString(getUpdatedOut.Description))

	_, err = client.DeleteAutomationRuleV2(ctx, &securityhubsdk.DeleteAutomationRuleV2Input{
		Identifier: aws.String(ruleID),
	})
	require.NoError(t, err)
}

func testConfigurationPoliciesRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateConfigurationPolicy(
		ctx,
		&securityhubsdk.CreateConfigurationPolicyInput{
			Name: aws.String("slice8-policy"),
			ConfigurationPolicy: &types.PolicyMemberSecurityHub{
				Value: types.SecurityHubPolicy{
					ServiceEnabled:             aws.Bool(true),
					EnabledStandardIdentifiers: []string{},
				},
			},
		},
	)
	require.NoError(t, err)
	policyID := aws.ToString(createOut.Id)
	require.NotEmpty(t, policyID)

	getOut, err := client.GetConfigurationPolicy(ctx, &securityhubsdk.GetConfigurationPolicyInput{
		Identifier: aws.String(policyID),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-policy", aws.ToString(getOut.Name))

	listOut, err := client.ListConfigurationPolicies(
		ctx,
		&securityhubsdk.ListConfigurationPoliciesInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.ConfigurationPolicySummaries)

	updateOut, err := client.UpdateConfigurationPolicy(
		ctx,
		&securityhubsdk.UpdateConfigurationPolicyInput{
			Identifier:  aws.String(policyID),
			Description: aws.String("slice8 updated"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8 updated", aws.ToString(updateOut.Description))

	assocOut, err := client.StartConfigurationPolicyAssociation(
		ctx, &securityhubsdk.StartConfigurationPolicyAssociationInput{
			ConfigurationPolicyIdentifier: aws.String(policyID),
			Target:                        &types.TargetMemberAccountId{Value: "555555555555"},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "555555555555", aws.ToString(assocOut.TargetId))

	getAssocOut, err := client.GetConfigurationPolicyAssociation(
		ctx, &securityhubsdk.GetConfigurationPolicyAssociationInput{
			Target: &types.TargetMemberAccountId{Value: "555555555555"},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, policyID, aws.ToString(getAssocOut.ConfigurationPolicyId))

	listAssocOut, err := client.ListConfigurationPolicyAssociations(
		ctx, &securityhubsdk.ListConfigurationPolicyAssociationsInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listAssocOut.ConfigurationPolicyAssociationSummaries)

	batchGetOut, err := client.BatchGetConfigurationPolicyAssociations(
		ctx, &securityhubsdk.BatchGetConfigurationPolicyAssociationsInput{
			ConfigurationPolicyAssociationIdentifiers: []types.ConfigurationPolicyAssociation{
				{Target: &types.TargetMemberAccountId{Value: "555555555555"}},
			},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, batchGetOut.ConfigurationPolicyAssociations)

	_, err = client.StartConfigurationPolicyDisassociation(
		ctx, &securityhubsdk.StartConfigurationPolicyDisassociationInput{
			ConfigurationPolicyIdentifier: aws.String(policyID),
			Target:                        &types.TargetMemberAccountId{Value: "555555555555"},
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteConfigurationPolicy(ctx, &securityhubsdk.DeleteConfigurationPolicyInput{
		Identifier: aws.String(policyID),
	})
	require.NoError(t, err)
}

func testFindingAggregatorsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateFindingAggregator(
		ctx,
		&securityhubsdk.CreateFindingAggregatorInput{
			RegionLinkingMode: aws.String("ALL_REGIONS"),
		},
	)
	require.NoError(t, err)
	arn := aws.ToString(createOut.FindingAggregatorArn)
	require.NotEmpty(t, arn)

	getOut, err := client.GetFindingAggregator(ctx, &securityhubsdk.GetFindingAggregatorInput{
		FindingAggregatorArn: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Equal(t, "ALL_REGIONS", aws.ToString(getOut.RegionLinkingMode))

	listOut, err := client.ListFindingAggregators(
		ctx,
		&securityhubsdk.ListFindingAggregatorsInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.FindingAggregators)

	updateOut, err := client.UpdateFindingAggregator(
		ctx,
		&securityhubsdk.UpdateFindingAggregatorInput{
			FindingAggregatorArn: aws.String(arn),
			RegionLinkingMode:    aws.String("NO_REGIONS"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "NO_REGIONS", aws.ToString(updateOut.RegionLinkingMode))

	_, err = client.DeleteFindingAggregator(ctx, &securityhubsdk.DeleteFindingAggregatorInput{
		FindingAggregatorArn: aws.String(arn),
	})
	require.NoError(t, err)
}

func testConnectorsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateConnector(ctx, &securityhubsdk.CreateConnectorInput{
		Name: aws.String("slice8-connector"),
		Provider: &types.CspmProviderConfigurationMemberAzure{
			Value: types.AzureProviderConfiguration{
				AWSConfigConnectorArn: aws.String("arn:aws:config::000000000000:connector/slice8"),
				AzureRegions:          []string{"eastus"},
				ScopeConfiguration: &types.AzureScopeConfiguration{
					ScopeType:   types.ScopeTypeTenant,
					ScopeValues: []string{"tenant-1"},
				},
			},
		},
	})
	require.NoError(t, err)
	connectorID := aws.ToString(createOut.ConnectorId)
	require.NotEmpty(t, connectorID)

	getOut, err := client.GetConnector(
		ctx,
		&securityhubsdk.GetConnectorInput{ConnectorId: aws.String(connectorID)},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-connector", aws.ToString(getOut.Name))

	listOut, err := client.ListConnectors(ctx, &securityhubsdk.ListConnectorsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.Connectors)

	_, err = client.UpdateConnector(ctx, &securityhubsdk.UpdateConnectorInput{
		ConnectorId: aws.String(connectorID),
		Description: aws.String("slice8 updated"),
	})
	require.NoError(t, err)

	_, err = client.DeleteConnector(
		ctx,
		&securityhubsdk.DeleteConnectorInput{ConnectorId: aws.String(connectorID)},
	)
	require.NoError(t, err)

	createV2Out, err := client.CreateConnectorV2(ctx, &securityhubsdk.CreateConnectorV2Input{
		Name: aws.String("slice8-connector-v2"),
		Provider: &types.ProviderConfigurationMemberAzure{
			Value: types.AzureProviderConfiguration{
				AWSConfigConnectorArn: aws.String(
					"arn:aws:config::000000000000:connector/slice8v2",
				),
				AzureRegions: []string{"eastus"},
				ScopeConfiguration: &types.AzureScopeConfiguration{
					ScopeType:   types.ScopeTypeTenant,
					ScopeValues: []string{"tenant-1"},
				},
			},
		},
	})
	require.NoError(t, err)
	connectorV2ID := aws.ToString(createV2Out.ConnectorId)
	require.NotEmpty(t, connectorV2ID)

	_, err = client.DeleteConnectorV2(
		ctx, &securityhubsdk.DeleteConnectorV2Input{ConnectorId: aws.String(connectorV2ID)},
	)
	require.NoError(t, err)
}

func testAggregatorsV2RealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateAggregatorV2(ctx, &securityhubsdk.CreateAggregatorV2Input{
		RegionLinkingMode: aws.String("ALL_REGIONS"),
	})
	require.NoError(t, err)
	arn := aws.ToString(createOut.AggregatorV2Arn)
	require.NotEmpty(t, arn)

	getOut, err := client.GetAggregatorV2(
		ctx,
		&securityhubsdk.GetAggregatorV2Input{AggregatorV2Arn: aws.String(arn)},
	)
	require.NoError(t, err)
	assert.Equal(t, "ALL_REGIONS", aws.ToString(getOut.RegionLinkingMode))

	listOut, err := client.ListAggregatorsV2(ctx, &securityhubsdk.ListAggregatorsV2Input{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.AggregatorsV2)

	updateOut, err := client.UpdateAggregatorV2(ctx, &securityhubsdk.UpdateAggregatorV2Input{
		AggregatorV2Arn:   aws.String(arn),
		RegionLinkingMode: aws.String("NO_REGIONS"),
	})
	require.NoError(t, err)
	assert.Equal(t, "NO_REGIONS", aws.ToString(updateOut.RegionLinkingMode))

	_, err = client.DeleteAggregatorV2(
		ctx,
		&securityhubsdk.DeleteAggregatorV2Input{AggregatorV2Arn: aws.String(arn)},
	)
	require.NoError(t, err)
}

func testProductsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	require.NoError(t, backend.EnableHub(false, "", nil))

	descOut, err := client.DescribeProducts(ctx, &securityhubsdk.DescribeProductsInput{})
	require.NoError(t, err)
	assert.NotNil(t, descOut)

	require.NotEmpty(t, descOut.Products)
	productArn := aws.ToString(descOut.Products[0].ProductArn)

	_, err = client.EnableImportFindingsForProduct(
		ctx,
		&securityhubsdk.EnableImportFindingsForProductInput{
			ProductArn: aws.String(productArn),
		},
	)
	require.NoError(t, err)

	listOut, err := client.ListEnabledProductsForImport(
		ctx,
		&securityhubsdk.ListEnabledProductsForImportInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.ProductSubscriptions)
}

func testFindingsMiscRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	require.NoError(t, backend.EnableHub(false, "", nil))

	_, err := client.UpdateFindings(ctx, &securityhubsdk.UpdateFindingsInput{
		Filters: &types.AwsSecurityFindingFilters{},
		Note: &types.NoteUpdate{
			Text:      aws.String("slice8 note"),
			UpdatedBy: aws.String("slice8"),
		},
	})
	require.NoError(t, err)

	insightArn, err := backend.CreateInsight("slice8-insight", "ResourceId", map[string]any{})
	require.NoError(t, err)

	_, err = client.UpdateInsight(ctx, &securityhubsdk.UpdateInsightInput{
		InsightArn: aws.String(insightArn),
		Name:       aws.String("slice8-insight-renamed"),
	})
	require.NoError(t, err)

	findingsV2Out, err := client.GetFindingsV2(ctx, &securityhubsdk.GetFindingsV2Input{})
	require.NoError(t, err)
	assert.NotNil(t, findingsV2Out)

	batchUpdateV2Out, err := client.BatchUpdateFindingsV2(
		ctx,
		&securityhubsdk.BatchUpdateFindingsV2Input{
			MetadataUids: []string{},
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, batchUpdateV2Out)

	resourcesV2Out, err := client.GetResourcesV2(ctx, &securityhubsdk.GetResourcesV2Input{})
	require.NoError(t, err)
	assert.NotNil(t, resourcesV2Out)
}
